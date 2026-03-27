/**
 * JeanClaudeAdapterLive - Scoped live implementation for the Jean-Claude provider adapter.
 *
 * Wraps `JeanClaudeSessionManager` (which spawns `jean --server` as a stdio
 * JSON-RPC child process) behind the generic provider adapter contract and
 * emits canonical runtime events.
 *
 * @module JeanClaudeAdapterLive
 */
import {
  EventId,
  type CanonicalItemType,
  type ProviderEvent,
  type ProviderRuntimeEvent,
  type ProviderRuntimeTurnStatus,
  type ThreadTokenUsageSnapshot,
  RuntimeItemId,
  ThreadId,
} from "@t3tools/contracts";
import { randomUUID } from "node:crypto";
import { Effect, Layer, Queue, Stream } from "effect";

import {
  JeanClaudeSessionManager,
  type JeanClaudeStartSessionInput,
  type JeanClaudeSendTurnInput,
} from "../../jeanClaudeSessionManager.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import { JeanClaudeAdapter, type JeanClaudeAdapterShape } from "../Services/JeanClaudeAdapter.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";

const PROVIDER = "jeanClaude" as const;

// ── Utility helpers ───────────────────────────────────────────────────

function asObject(value: unknown): Record<string, unknown> | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  return value as Record<string, unknown>;
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function asNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function toMessage(cause: unknown, fallback: string): string {
  if (cause instanceof Error && cause.message.length > 0) return cause.message;
  return fallback;
}

function toRequestError(threadId: ThreadId, method: string, cause: unknown): ProviderAdapterError {
  const msg = toMessage(cause, "").toLowerCase();
  if (msg.includes("unknown session") || msg.includes("unknown provider session")) {
    return new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId, cause });
  }
  return new ProviderAdapterRequestError({
    provider: PROVIDER,
    method,
    detail: toMessage(cause, `${method} failed`),
    cause,
  });
}

function toRuntimeItemId(id: string | undefined): RuntimeItemId | undefined {
  return id?.trim() ? RuntimeItemId.makeUnsafe(id) : undefined;
}

// ── Token usage translation ───────────────────────────────────────────

function normalizeTokenUsage(
  payload: Record<string, unknown>,
): ThreadTokenUsageSnapshot | undefined {
  const inputTokens = asNumber(payload.input_tokens);
  const outputTokens = asNumber(payload.output_tokens);
  const _cacheCreationTokens = asNumber(payload.cache_creation_input_tokens); // reserved for future use
  const cacheReadTokens = asNumber(payload.cache_read_input_tokens);
  // jean-claude context_update uses used_tokens / max_tokens directly
  const usedTokensDirect = asNumber(payload.used_tokens);
  const maxTokensDirect = asNumber(payload.max_tokens);
  const usedTokens = usedTokensDirect ?? (inputTokens ?? 0) + (outputTokens ?? 0);
  if (usedTokens <= 0) return undefined;
  return {
    usedTokens,
    ...(inputTokens !== undefined ? { inputTokens, lastInputTokens: inputTokens } : {}),
    ...(cacheReadTokens !== undefined
      ? { cachedInputTokens: cacheReadTokens, lastCachedInputTokens: cacheReadTokens }
      : {}),
    ...(maxTokensDirect !== undefined ? { maxTokens: maxTokensDirect } : {}),
    ...(outputTokens !== undefined ? { outputTokens, lastOutputTokens: outputTokens } : {}),
    compactsAutomatically: true,
  };
}

// ── Event base builder ────────────────────────────────────────────────

function runtimeEventBase(
  event: ProviderEvent,
  canonicalThreadId: ThreadId,
): Omit<ProviderRuntimeEvent, "type" | "payload"> {
  return {
    eventId: EventId.makeUnsafe(randomUUID()),
    provider: PROVIDER,
    threadId: canonicalThreadId,
    createdAt: event.createdAt,
    ...(event.turnId !== undefined ? { turnId: event.turnId } : {}),
    ...(event.itemId !== undefined ? { itemId: RuntimeItemId.makeUnsafe(event.itemId) } : {}),
  };
}

// ── Tool name → canonical item type ──────────────────────────────────

function toolNameToItemType(toolName: string | undefined): CanonicalItemType {
  if (!toolName) return "unknown";
  if (toolName === "bash") return "command_execution";
  if (
    toolName === "edit_file" ||
    toolName === "apply_unified_diff" ||
    toolName === "replace_text" ||
    toolName === "batch_edit_files"
  )
    return "file_change";
  if (toolName === "read_file" || toolName === "list_tree" || toolName === "grep_text")
    return "file_change";
  return "dynamic_tool_call";
}

// ── Main event translator ─────────────────────────────────────────────

function mapToRuntimeEvents(
  event: ProviderEvent,
  canonicalThreadId: ThreadId,
): ReadonlyArray<ProviderRuntimeEvent> {
  const payload = asObject(event.payload);
  const base = runtimeEventBase(event, canonicalThreadId);

  // ── Error events ────────────────────────────────────────────────────
  if (event.kind === "error") {
    if (event.method === "turn/error") {
      return [
        {
          ...base,
          type: "turn.completed",
          payload: {
            state: "failed" satisfies ProviderRuntimeTurnStatus,
            ...(event.message ? { errorMessage: event.message } : {}),
          },
        },
      ];
    }
    return [
      {
        ...base,
        type: "runtime.error",
        payload: {
          message: event.message ?? "Unknown jean-claude error.",
          class: "provider_error",
          ...(event.payload !== undefined ? { detail: event.payload } : {}),
        },
      },
    ];
  }

  // ── Session lifecycle ───────────────────────────────────────────────
  if (event.method === "session/connecting") {
    return [{ ...base, type: "session.state.changed", payload: { state: "starting" } }];
  }

  if (event.method === "session/ready") {
    return [{ ...base, type: "session.state.changed", payload: { state: "ready" } }];
  }

  if (event.method === "session/started") {
    return [
      {
        ...base,
        type: "session.started",
        payload: event.message ? { message: event.message } : {},
      },
    ];
  }

  if (event.method === "session/exited" || event.method === "session/closed") {
    return [
      {
        ...base,
        type: "session.exited",
        payload: {
          ...(event.message ? { reason: event.message } : {}),
          ...(event.method === "session/closed" ? { exitKind: "graceful" as const } : {}),
        },
      },
    ];
  }

  // ── Turn lifecycle ──────────────────────────────────────────────────
  if (event.method === "turn/start") {
    return [
      {
        ...base,
        type: "turn.started",
        payload: {},
      },
    ];
  }

  if (event.method === "turn/started") {
    return [{ ...base, type: "turn.started", payload: {} }];
  }

  if (event.method === "turn/end") {
    const answer = asString(payload?.answer);
    const results: ProviderRuntimeEvent[] = [];

    // Jean-claude delivers the full response in turn_end.answer (no streaming).
    // Emit it as an assistant_text delta so the UI has content to display.
    if (answer) {
      results.push({
        ...base,
        type: "content.delta",
        payload: { streamKind: "assistant_text", delta: answer },
      });
    }

    results.push({
      ...base,
      type: "turn.completed",
      payload: {
        state: "completed" satisfies ProviderRuntimeTurnStatus,
        stopReason: "end_turn",
      },
    });

    return results;
  }

  if (event.method === "turn/interrupted") {
    return [
      {
        ...base,
        type: "turn.aborted",
        payload: { reason: "interrupted" },
      },
    ];
  }

  // ── Content streaming ───────────────────────────────────────────────
  if (event.method === "content/assistant_text") {
    const text = asString(payload?.text);
    if (!text) return [];
    return [
      {
        ...base,
        type: "content.delta",
        payload: { streamKind: "assistant_text", delta: text },
      },
    ];
  }

  if (event.method === "content/reasoning_text") {
    const text = asString(payload?.text);
    if (!text) return [];
    return [
      {
        ...base,
        type: "content.delta",
        payload: { streamKind: "reasoning_text", delta: text },
      },
    ];
  }

  // ── Tool lifecycle ──────────────────────────────────────────────────
  if (event.method === "tool/start") {
    const toolName = asString(payload?.tool_name) ?? asString(payload?.name);
    const itemType = toolNameToItemType(toolName);
    const itemId = toRuntimeItemId(asString(payload?.tool_use_id)) ?? toRuntimeItemId(randomUUID());
    return [
      {
        ...base,
        ...(itemId ? { itemId } : {}),
        type: "item.started",
        payload: {
          itemType,
          title: toolName ?? "tool",
          ...(payload?.input ? { detail: JSON.stringify(payload.input) } : {}),
        },
      },
    ];
  }

  if (event.method === "tool/end") {
    const toolName = asString(payload?.tool_name) ?? asString(payload?.name);
    const itemType = toolNameToItemType(toolName);
    const outputText = asString(payload?.output);
    const results: ProviderRuntimeEvent[] = [
      {
        ...base,
        type: "item.completed",
        payload: {
          itemType,
          status: "completed",
          title: toolName ?? "tool",
        },
      },
    ];
    // Surface command/file output as a content delta
    if (outputText) {
      const streamKind =
        itemType === "command_execution"
          ? ("command_output" as const)
          : ("file_change_output" as const);
      results.push({
        ...base,
        type: "content.delta",
        payload: { streamKind, delta: outputText },
      });
    }
    return results;
  }

  // ── Turn iteration progress ─────────────────────────────────────────
  if (event.method === "turn/iteration") {
    return [
      {
        ...base,
        type: "tool.progress",
        payload: payload?.iteration !== undefined ? { elapsedSeconds: 0 } : {},
      },
    ];
  }

  // ── Token usage ─────────────────────────────────────────────────────
  if (event.method === "thread/token-usage" && payload) {
    const usage = normalizeTokenUsage(payload);
    if (usage) {
      return [{ ...base, type: "thread.token-usage.updated", payload: { usage } }];
    }
    return [];
  }

  // ── Auto-compact ────────────────────────────────────────────────────
  if (event.method === "context/compacted") {
    return [
      {
        ...base,
        type: "thread.state.changed",
        payload: { state: "compacted" },
      },
    ];
  }

  // ── Stderr forwarded as warning ─────────────────────────────────────
  if (event.method === "stderr" && event.message) {
    return [
      {
        ...base,
        type: "runtime.warning",
        payload: { message: event.message },
      },
    ];
  }

  return [];
}

// ── Adapter live options ──────────────────────────────────────────────

export interface JeanClaudeAdapterLiveOptions {
  readonly nativeEventLogger?: EventNdjsonLogger;
  readonly nativeEventLogPath?: string;
  /** Inject a pre-built manager (for tests). */
  readonly manager?: JeanClaudeSessionManager;
}

// ── makeJeanClaudeAdapter ─────────────────────────────────────────────

function makeJeanClaudeAdapter(options?: JeanClaudeAdapterLiveOptions) {
  return Effect.gen(function* () {
    const nativeEventLogger =
      options?.nativeEventLogger ??
      (options?.nativeEventLogPath !== undefined
        ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, { stream: "native" })
        : undefined);

    const manager = yield* Effect.acquireRelease(
      Effect.sync(() => options?.manager ?? new JeanClaudeSessionManager()),
      (mgr) =>
        Effect.sync(() => {
          try {
            mgr.stopAll();
          } catch {
            // Finalizers must not throw.
          }
        }),
    );

    const serverSettingsService = yield* ServerSettingsService;

    // ── startSession ──────────────────────────────────────────────────

    const startSession: JeanClaudeAdapterShape["startSession"] = Effect.fn(function* (input) {
      if (input.provider !== undefined && input.provider !== PROVIDER) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
        });
      }

      const jeanSettings = yield* serverSettingsService.getSettings.pipe(
        Effect.map((s) => s.providers.jeanClaude),
        Effect.mapError(
          (error) =>
            new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId: input.threadId,
              detail: error.message,
              cause: error,
            }),
        ),
      );

      const managerInput: JeanClaudeStartSessionInput = {
        threadId: input.threadId,
        binaryPath: jeanSettings.binaryPath,
        runtimeMode: input.runtimeMode,
        ...(input.cwd !== undefined ? { cwd: input.cwd } : {}),
        ...(input.modelSelection?.provider === "jeanClaude"
          ? { model: input.modelSelection.model }
          : {}),
      };

      return yield* Effect.tryPromise({
        try: () => manager.startSession(managerInput),
        catch: (cause) =>
          new ProviderAdapterProcessError({
            provider: PROVIDER,
            threadId: input.threadId,
            detail: toMessage(cause, "Failed to start jean-claude session."),
            cause,
          }),
      });
    });

    // ── sendTurn ──────────────────────────────────────────────────────

    const sendTurn: JeanClaudeAdapterShape["sendTurn"] = (input) =>
      Effect.tryPromise({
        try: () => {
          const managerInput: JeanClaudeSendTurnInput = {
            threadId: input.threadId,
            ...(input.input !== undefined ? { input: input.input } : {}),
          };
          return manager.sendTurn(managerInput);
        },
        catch: (cause) => toRequestError(input.threadId, "turn/send", cause),
      }).pipe(Effect.map((result) => ({ ...result, threadId: input.threadId })));

    // ── interruptTurn ─────────────────────────────────────────────────

    const interruptTurn: JeanClaudeAdapterShape["interruptTurn"] = (threadId, turnId) =>
      Effect.tryPromise({
        try: () => manager.interruptTurn(threadId, turnId),
        catch: (cause) => toRequestError(threadId, "turn/interrupt", cause),
      });

    // ── respondToRequest / respondToUserInput (v1: no-op) ─────────────

    const respondToRequest: JeanClaudeAdapterShape["respondToRequest"] = (
      _threadId,
      _requestId,
      _decision,
    ) => Effect.void;

    const respondToUserInput: JeanClaudeAdapterShape["respondToUserInput"] = (
      _threadId,
      _requestId,
      _answers,
    ) => Effect.void;

    // ── readThread / rollbackThread (v1: empty snapshots) ─────────────

    const readThread: JeanClaudeAdapterShape["readThread"] = (threadId) =>
      Effect.succeed({ threadId, turns: [] });

    const rollbackThread: JeanClaudeAdapterShape["rollbackThread"] = (threadId, _numTurns) =>
      Effect.succeed({ threadId, turns: [] });

    // ── stopSession ───────────────────────────────────────────────────

    const stopSession: JeanClaudeAdapterShape["stopSession"] = (threadId) =>
      Effect.sync(() => {
        manager.stopSession(threadId);
      });

    const listSessions: JeanClaudeAdapterShape["listSessions"] = () =>
      Effect.sync(() => manager.listSessions());

    const hasSession: JeanClaudeAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => manager.hasSession(threadId));

    const stopAll: JeanClaudeAdapterShape["stopAll"] = () =>
      Effect.sync(() => {
        manager.stopAll();
      });

    // ── Runtime event queue ───────────────────────────────────────────

    const runtimeEventQueue = yield* Queue.unbounded<ProviderRuntimeEvent>();

    yield* Effect.acquireRelease(
      Effect.gen(function* () {
        const writeNativeEvent = (event: ProviderEvent) =>
          Effect.gen(function* () {
            if (!nativeEventLogger) return;
            yield* nativeEventLogger.write(event, event.threadId);
          });

        const services = yield* Effect.services<never>();
        const listener = (event: ProviderEvent) =>
          Effect.gen(function* () {
            yield* writeNativeEvent(event);
            const runtimeEvents = mapToRuntimeEvents(event, event.threadId);
            if (runtimeEvents.length === 0) {
              yield* Effect.logDebug("ignoring unhandled jean-claude provider event", {
                method: event.method,
                threadId: event.threadId,
              });
              return;
            }
            yield* Queue.offerAll(runtimeEventQueue, runtimeEvents);
          }).pipe(Effect.runPromiseWith(services));

        manager.on("event", listener);
        return listener;
      }),
      (listener) =>
        Effect.gen(function* () {
          yield* Effect.sync(() => {
            manager.off("event", listener);
          });
          yield* Queue.shutdown(runtimeEventQueue);
        }),
    );

    return {
      provider: PROVIDER,
      capabilities: {
        sessionModelSwitch: "restart-session",
      },
      startSession,
      sendTurn,
      interruptTurn,
      readThread,
      rollbackThread,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      stopAll,
      streamEvents: Stream.fromQueue(runtimeEventQueue),
    } satisfies JeanClaudeAdapterShape;
  });
}

export const JeanClaudeAdapterLive = Layer.effect(JeanClaudeAdapter, makeJeanClaudeAdapter());

export function makeJeanClaudeAdapterLive(options?: JeanClaudeAdapterLiveOptions) {
  return Layer.effect(JeanClaudeAdapter, makeJeanClaudeAdapter(options));
}
