/**
 * JeanClaudeSessionManager - Manages jean-claude stdio JSON-RPC sessions.
 *
 * Spawns `jean --server` as a long-lived child process per session, sends
 * JSON-RPC 2.0 requests over stdin, and reads newline-delimited JSON
 * notifications from stdout. Translates jean-claude events into
 * `ProviderEvent` objects and emits them via EventEmitter.
 *
 * Protocol (jean serve):
 *   stdin  → newline-delimited JSON-RPC requests
 *   stdout ← newline-delimited JSON-RPC notifications (events) and responses
 *   stderr ← diagnostic output (logged, not parsed)
 *
 * @module JeanClaudeSessionManager
 */
import { type ChildProcessWithoutNullStreams, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import readline from "node:readline";

import {
  ApprovalRequestId,
  EventId,
  ProviderItemId,
  type ProviderApprovalDecision,
  type ProviderEvent,
  type ProviderSession,
  type ProviderTurnStartResult,
  type ProviderUserInputAnswers,
  RuntimeMode,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";

// ── Shared utilities ──────────────────────────────────────────────────

function asObject(value: unknown): Record<string, unknown> | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  return value as Record<string, unknown>;
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

// ── Types ─────────────────────────────────────────────────────────────

interface PendingRequest {
  method: string;
  timeout: ReturnType<typeof setTimeout>;
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
}

interface JeanClaudeSessionContext {
  session: ProviderSession;
  child: ChildProcessWithoutNullStreams;
  output: readline.Interface;
  pending: Map<string | number, PendingRequest>;
  nextRequestId: number;
  stopping: boolean;
  /** Turn ID currently in-flight (set at turn_start, cleared at turn_end/error). */
  activeTurnId: TurnId | undefined;
  /** Captured non-JSON stdout lines (startup errors, diagnostics). */
  capturedStdoutLines: string[];
}

interface JsonRpcResponse {
  id: string | number;
  result?: unknown;
  error?: { code?: number; message?: string };
}

interface JsonRpcNotification {
  method: string;
  params?: unknown;
}

const REQUEST_TIMEOUT_MS = 30_000;

// ── Public input/output types ─────────────────────────────────────────

export interface JeanClaudeStartSessionInput {
  readonly threadId: ThreadId;
  readonly cwd?: string;
  readonly model?: string;
  readonly binaryPath: string;
  readonly runtimeMode: RuntimeMode;
}

export interface JeanClaudeSendTurnInput {
  readonly threadId: ThreadId;
  readonly input?: string;
}

// ── Manager ───────────────────────────────────────────────────────────

export class JeanClaudeSessionManager extends EventEmitter<{ event: [ProviderEvent] }> {
  private readonly sessions = new Map<ThreadId, JeanClaudeSessionContext>();

  // ── Session lifecycle ───────────────────────────────────────────────

  startSession(input: JeanClaudeStartSessionInput): Promise<ProviderSession> {
    return new Promise((resolve, reject) => {
      const { threadId, cwd, model, binaryPath, runtimeMode } = input;

      if (this.sessions.has(threadId)) {
        resolve(this.sessions.get(threadId)!.session);
        return;
      }

      const args = ["--server"];
      if (model) args.push("--model", model);

      const child = spawn(binaryPath, args, {
        cwd: cwd ?? process.cwd(),
        stdio: ["pipe", "pipe", "pipe"],
        env: {
          ...process.env,
          // Disable interactive mode signals inside the child process
          TERM: "dumb",
        },
      }) as ChildProcessWithoutNullStreams;

      const context: JeanClaudeSessionContext = {
        session: {
          provider: "jeanClaude",
          status: "connecting",
          runtimeMode,
          threadId,
          ...(cwd ? { cwd } : {}),
          ...(model ? { model } : {}),
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        },
        child,
        output: readline.createInterface({ input: child.stdout, crlfDelay: Infinity }),
        pending: new Map(),
        nextRequestId: 1,
        stopping: false,
        activeTurnId: undefined,
        capturedStdoutLines: [],
      };

      this.sessions.set(threadId, context);

      // Emit session/connecting right away
      this.emitProviderEvent({
        threadId,
        kind: "session",
        method: "session/connecting",
        message: "Starting jean-claude session",
      });

      // Wire stdout line handler
      context.output.on("line", (line: string) => {
        this.handleLine(threadId, line.trim());
      });

      // Wire stderr for logging only
      child.stderr.on("data", (chunk: Buffer) => {
        const text = chunk.toString("utf8").trim();
        if (text) {
          // Forward notable stderr as runtime.warning events
          this.emitProviderEvent({
            threadId,
            kind: "notification",
            method: "stderr",
            message: text,
          });
        }
      });

      // Wire process exit
      child.on("exit", (code, signal) => {
        const ctx = this.sessions.get(threadId);
        if (!ctx) return;

        // Build an informative error including any captured stdout (e.g., config errors)
        const capturedOutput = ctx.capturedStdoutLines.join(" ").substring(0, 500);
        const exitDetail = capturedOutput
          ? `jean-claude exited (code=${code}): ${capturedOutput}`
          : `jean-claude process exited (code=${code}, signal=${signal})`;

        // Reject any pending requests
        for (const pending of ctx.pending.values()) {
          clearTimeout(pending.timeout);
          pending.reject(new Error(exitDetail));
        }
        ctx.pending.clear();

        const wasGraceful = ctx.stopping;
        this.sessions.delete(threadId);

        this.emitProviderEvent({
          threadId,
          kind: "session",
          method: wasGraceful ? "session/closed" : "session/exited",
          message: wasGraceful
            ? "jean-claude session closed gracefully."
            : `jean-claude process exited unexpectedly (code=${code}).`,
        });
      });

      child.on("error", (err) => {
        const ctx = this.sessions.get(threadId);
        if (ctx) {
          for (const pending of ctx.pending.values()) {
            clearTimeout(pending.timeout);
            pending.reject(err);
          }
          ctx.pending.clear();
          this.sessions.delete(threadId);
        }
        this.emitProviderEvent({
          threadId,
          kind: "error",
          method: "session/error",
          message: err.message,
        });
        reject(err);
      });

      // Wait for the child process to actually spawn before writing to stdin.
      // In some runtimes (Bun), writing before the spawn event can race with
      // pipe setup and cause the data to be lost or the process to exit.
      child.on("spawn", () => {
        this.sendRequest(context, "session/start", {
          sessionFile: undefined,
          cwd: cwd ?? process.cwd(),
        })
          .then(() => {
            context.session = {
              ...context.session,
              status: "ready",
              updatedAt: new Date().toISOString(),
            };
            this.emitProviderEvent({
              threadId,
              kind: "session",
              method: "session/ready",
              message: "jean-claude session ready.",
            });
            resolve(context.session);
          })
          .catch((err: unknown) => {
            this.stopSession(threadId);
            reject(err instanceof Error ? err : new Error(String(err)));
          });
      });
    });
  }

  sendTurn(input: JeanClaudeSendTurnInput): Promise<ProviderTurnStartResult> {
    return new Promise((resolve, reject) => {
      const { threadId } = input;
      const context = this.sessions.get(threadId);
      if (!context) {
        reject(new Error(`Unknown session: ${threadId}`));
        return;
      }

      const turnId = TurnId.makeUnsafe(randomUUID());
      context.activeTurnId = turnId;
      context.session = {
        ...context.session,
        status: "running",
        activeTurnId: turnId,
        updatedAt: new Date().toISOString(),
      };

      this.emitProviderEvent({
        threadId,
        kind: "session",
        method: "turn/start",
        turnId,
        message: "Turn started.",
      });

      // Send the turn request — jean --server reads it and begins processing.
      // Resolve immediately with the turn ID; the actual result arrives via
      // turn_end / turn_error / turn_interrupted notifications.
      this.sendRequest(context, "turn/send", {
        input: input.input ?? "",
      })
        .then(() => {
          resolve({ threadId, turnId });
        })
        .catch((err: unknown) => {
          context.activeTurnId = undefined;
          context.session = {
            ...context.session,
            status: "ready",
            activeTurnId: undefined,
            updatedAt: new Date().toISOString(),
          };
          reject(err instanceof Error ? err : new Error(String(err)));
        });
    });
  }

  interruptTurn(threadId: ThreadId, _turnId?: TurnId): Promise<void> {
    return new Promise((resolve, reject) => {
      const context = this.sessions.get(threadId);
      if (!context) {
        reject(new Error(`Unknown session: ${threadId}`));
        return;
      }

      this.sendRequest(context, "turn/interrupt", {})
        .then(() => resolve())
        .catch((err: unknown) => reject(err instanceof Error ? err : new Error(String(err))));
    });
  }

  // Approval relay is not yet supported — resolve void immediately.
  respondToRequest(
    _threadId: ThreadId,
    _requestId: ApprovalRequestId,
    _decision: ProviderApprovalDecision,
  ): Promise<void> {
    return Promise.resolve();
  }

  respondToUserInput(
    _threadId: ThreadId,
    _requestId: ApprovalRequestId,
    _answers: ProviderUserInputAnswers,
  ): Promise<void> {
    return Promise.resolve();
  }

  stopSession(threadId: ThreadId): void {
    const context = this.sessions.get(threadId);
    if (!context) return;
    context.stopping = true;
    try {
      context.child.stdin.end();
      context.child.kill("SIGTERM");
    } catch {
      // Ignore errors during teardown.
    }
    this.sessions.delete(threadId);
  }

  listSessions(): ProviderSession[] {
    return Array.from(this.sessions.values()).map((ctx) => ctx.session);
  }

  hasSession(threadId: ThreadId): boolean {
    return this.sessions.has(threadId);
  }

  stopAll(): void {
    for (const threadId of Array.from(this.sessions.keys())) {
      this.stopSession(threadId);
    }
  }

  // ── Internal helpers ────────────────────────────────────────────────

  private sendRequest(
    context: JeanClaudeSessionContext,
    method: string,
    params: Record<string, unknown>,
  ): Promise<unknown> {
    return new Promise((resolve, reject) => {
      const id = context.nextRequestId++;
      const request = JSON.stringify({ jsonrpc: "2.0", id, method, params });

      const timeout = setTimeout(() => {
        context.pending.delete(id);
        reject(
          new Error(`jean-claude request '${method}' timed out after ${REQUEST_TIMEOUT_MS}ms`),
        );
      }, REQUEST_TIMEOUT_MS);

      context.pending.set(id, { method, timeout, resolve, reject });

      try {
        context.child.stdin.write(request + "\n");
      } catch (err) {
        clearTimeout(timeout);
        context.pending.delete(id);
        reject(err instanceof Error ? err : new Error(String(err)));
      }
    });
  }

  private handleLine(threadId: ThreadId, line: string): void {
    if (!line) return;

    let msg: unknown;
    try {
      msg = JSON.parse(line);
    } catch {
      // Capture non-JSON lines — these are typically startup errors or diagnostics.
      const context = this.sessions.get(threadId);
      if (context) {
        context.capturedStdoutLines.push(line);
      }
      return;
    }

    const obj = asObject(msg);
    if (!obj) return;

    // JSON-RPC response (has `id` and `result` or `error`)
    if (obj.id !== undefined && (obj.result !== undefined || obj.error !== undefined)) {
      this.handleResponse(threadId, obj as unknown as JsonRpcResponse);
      return;
    }

    // JSON-RPC notification (has `method`, no `id`)
    if (typeof obj.method === "string") {
      this.handleNotification(threadId, obj as unknown as JsonRpcNotification);
    }
  }

  private handleResponse(threadId: ThreadId, response: JsonRpcResponse): void {
    const context = this.sessions.get(threadId);
    if (!context) return;

    const pending = context.pending.get(response.id);
    if (!pending) return;

    clearTimeout(pending.timeout);
    context.pending.delete(response.id);

    if (response.error) {
      pending.reject(
        new Error(response.error.message ?? `jean-claude RPC error (code=${response.error.code})`),
      );
    } else {
      pending.resolve(response.result);
    }
  }

  private handleNotification(threadId: ThreadId, notification: JsonRpcNotification): void {
    const context = this.sessions.get(threadId);
    if (!context) return;

    const { method, params } = notification;
    const p = asObject(params) ?? {};

    switch (method) {
      case "startup/error": {
        // Configuration or startup failure from jean-claude.
        // Surface it as an error event and capture for exit message.
        const errorMsg = asString(p.error) ?? "jean-claude startup error";
        context.capturedStdoutLines.push(errorMsg);
        this.emitProviderEvent({
          threadId,
          kind: "error",
          method: "session/error",
          turnId: undefined,
          message: errorMsg,
        });
        break;
      }

      case "turn_start": {
        const rawTurnId = asString(p.turn_id);
        const turnId = rawTurnId ? TurnId.makeUnsafe(rawTurnId) : context.activeTurnId;
        if (turnId) {
          context.activeTurnId = turnId;
          this.emitProviderEvent({
            threadId,
            kind: "notification",
            method: "turn/started",
            turnId,
          });
        }
        break;
      }

      case "assistant_iteration_start":
        // Emitted at each tool-call iteration — surface as a lightweight tool progress event.
        this.emitProviderEvent({
          threadId,
          kind: "notification",
          method: "turn/iteration",
          turnId: context.activeTurnId,
          payload: p,
        });
        break;

      case "assistant_thinking": {
        const text = asString(p.text) ?? asString(p.content);
        if (text) {
          this.emitProviderEvent({
            threadId,
            kind: "notification",
            method: "content/reasoning_text",
            turnId: context.activeTurnId,
            payload: { text },
          });
        }
        break;
      }

      case "assistant_text": {
        const text = asString(p.text) ?? asString(p.content);
        if (text) {
          this.emitProviderEvent({
            threadId,
            kind: "notification",
            method: "content/assistant_text",
            turnId: context.activeTurnId,
            payload: { text },
          });
        }
        break;
      }

      case "tool_start":
        this.emitProviderEvent({
          threadId,
          kind: "notification",
          method: "tool/start",
          turnId: context.activeTurnId,
          itemId: asString(p.tool_use_id)
            ? ProviderItemId.makeUnsafe(asString(p.tool_use_id)!)
            : undefined,
          payload: p,
        });
        break;

      case "tool_end":
        this.emitProviderEvent({
          threadId,
          kind: "notification",
          method: "tool/end",
          turnId: context.activeTurnId,
          itemId: asString(p.tool_use_id)
            ? ProviderItemId.makeUnsafe(asString(p.tool_use_id)!)
            : undefined,
          payload: p,
        });
        break;

      case "turn_end": {
        const turnId = context.activeTurnId;
        context.activeTurnId = undefined;
        context.session = {
          ...context.session,
          status: "ready",
          activeTurnId: undefined,
          updatedAt: new Date().toISOString(),
        };
        this.emitProviderEvent({
          threadId,
          kind: "notification",
          method: "turn/end",
          turnId,
          payload: p,
        });
        break;
      }

      case "turn_error": {
        const turnId = context.activeTurnId;
        context.activeTurnId = undefined;
        context.session = {
          ...context.session,
          status: "ready",
          activeTurnId: undefined,
          updatedAt: new Date().toISOString(),
        };
        this.emitProviderEvent({
          threadId,
          kind: "error",
          method: "turn/error",
          turnId,
          message: asString(p.error) ?? "Turn failed.",
          payload: p,
        });
        break;
      }

      case "turn_interrupted": {
        const turnId = context.activeTurnId;
        context.activeTurnId = undefined;
        context.session = {
          ...context.session,
          status: "ready",
          activeTurnId: undefined,
          updatedAt: new Date().toISOString(),
        };
        this.emitProviderEvent({
          threadId,
          kind: "notification",
          method: "turn/interrupted",
          turnId,
          payload: p,
        });
        break;
      }

      case "auto_compact_performed":
        this.emitProviderEvent({
          threadId,
          kind: "notification",
          method: "context/compacted",
          turnId: context.activeTurnId,
          payload: p,
        });
        break;

      case "context_usage":
      case "context_update":
        // Token usage snapshot — emit as-is for downstream translation.
        // jean-claude emits context_update with: used_tokens, output_tokens, max_tokens, percentage
        this.emitProviderEvent({
          threadId,
          kind: "notification",
          method: "thread/token-usage",
          turnId: context.activeTurnId,
          payload: p,
        });
        break;

      default:
        // Pass all unrecognized notifications through — the adapter layer may handle them.
        this.emitProviderEvent({
          threadId,
          kind: "notification",
          method,
          turnId: context.activeTurnId,
          payload: p,
        });
    }
  }

  private emitProviderEvent(parts: {
    threadId: ThreadId;
    kind: ProviderEvent["kind"];
    method: string;
    turnId?: TurnId | undefined;
    itemId?: ProviderItemId | undefined;
    message?: string | undefined;
    payload?: unknown;
  }): void {
    const event: ProviderEvent = {
      id: EventId.makeUnsafe(randomUUID()),
      kind: parts.kind,
      provider: "jeanClaude",
      threadId: parts.threadId,
      createdAt: new Date().toISOString(),
      method: parts.method,
      ...(parts.turnId !== undefined ? { turnId: parts.turnId } : {}),
      ...(parts.itemId !== undefined ? { itemId: parts.itemId } : {}),
      ...(parts.message !== undefined ? { message: parts.message } : {}),
      ...(parts.payload !== undefined ? { payload: parts.payload } : {}),
    };
    this.emit("event", event);
  }
}
