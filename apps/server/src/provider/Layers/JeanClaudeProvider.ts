/**
 * JeanClaudeProviderLive - Provider health/status layer for Jean-Claude.
 *
 * Runs `jean --version` to check installation, and reports authentication
 * as always "authenticated" (jean-claude uses AWS Bedrock credentials from
 * the environment — no separate auth command is needed).
 *
 * @module JeanClaudeProviderLive
 */
import type {
  JeanClaudeSettings,
  ModelCapabilities,
  ServerProvider,
  ServerProviderModel,
} from "@t3tools/contracts";
import { Effect, Equal, Layer, Option, Result, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  buildServerProvider,
  collectStreamAsString,
  DEFAULT_TIMEOUT_MS,
  detailFromResult,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
} from "../providerSnapshot";
import { makeManagedServerProvider } from "../makeManagedServerProvider";
import { JeanClaudeProvider } from "../Services/JeanClaudeProvider";
import { ServerSettingsError, ServerSettingsService } from "../../serverSettings";

const PROVIDER = "jeanClaude" as const;

const BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: "claude-sonnet-4.6",
    name: "Claude Sonnet 4.6 (via Jean-Claude)",
    isCustom: false,
    capabilities: {
      reasoningEffortLevels: [],
      supportsFastMode: false,
      supportsThinkingToggle: false,
      contextWindowOptions: [],
      promptInjectedEffortLevels: [],
    } satisfies ModelCapabilities,
  },
  {
    slug: "claude-haiku-4.5",
    name: "Claude Haiku 4.5 (via Jean-Claude)",
    isCustom: false,
    capabilities: {
      reasoningEffortLevels: [],
      supportsFastMode: false,
      supportsThinkingToggle: false,
      contextWindowOptions: [],
      promptInjectedEffortLevels: [],
    } satisfies ModelCapabilities,
  },
  {
    slug: "claude-opus-4.6",
    name: "Claude Opus 4.6 (via Jean-Claude)",
    isCustom: false,
    capabilities: {
      reasoningEffortLevels: [],
      supportsFastMode: false,
      supportsThinkingToggle: false,
      contextWindowOptions: [],
      promptInjectedEffortLevels: [],
    } satisfies ModelCapabilities,
  },
];

const runJeanCommand = (binaryPath: string, args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const command = ChildProcess.make(binaryPath, [...args], {
      shell: process.platform === "win32",
    });
    const child = yield* spawner.spawn(command);
    const [stdout, stderr, exitCode] = yield* Effect.all(
      [
        collectStreamAsString(child.stdout),
        collectStreamAsString(child.stderr),
        child.exitCode.pipe(Effect.map(Number)),
      ],
      { concurrency: "unbounded" },
    );
    return { stdout, stderr, code: exitCode };
  }).pipe(Effect.scoped);

export const checkJeanClaudeProviderStatus = Effect.fn("checkJeanClaudeProviderStatus")(
  function* (): Effect.fn.Return<
    ServerProvider,
    ServerSettingsError,
    ChildProcessSpawner.ChildProcessSpawner | ServerSettingsService
  > {
    const jeanSettings = yield* Effect.service(ServerSettingsService).pipe(
      Effect.flatMap((service) => service.getSettings),
      Effect.map((settings) => settings.providers.jeanClaude),
    );

    const checkedAt = new Date().toISOString();
    const models = providerModelsFromSettings(BUILT_IN_MODELS, PROVIDER, jeanSettings.customModels);

    if (!jeanSettings.enabled) {
      return buildServerProvider({
        provider: PROVIDER,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          authStatus: "unknown",
          message: "Jean-Claude is disabled in T3 Code settings.",
        },
      });
    }

    const versionProbe = yield* runJeanCommand(jeanSettings.binaryPath, ["--version"]).pipe(
      Effect.timeoutOption(DEFAULT_TIMEOUT_MS),
      Effect.result,
    );

    if (Result.isFailure(versionProbe)) {
      const error = versionProbe.failure;
      return buildServerProvider({
        provider: PROVIDER,
        enabled: true,
        checkedAt,
        models,
        probe: {
          installed: !isCommandMissingCause(error),
          version: null,
          status: "error",
          authStatus: "unknown",
          message: isCommandMissingCause(error)
            ? "Jean-Claude CLI (`jean`) is not installed or not on PATH."
            : `Failed to execute Jean-Claude CLI health check: ${error instanceof Error ? error.message : String(error)}.`,
        },
      });
    }

    if (Option.isNone(versionProbe.success)) {
      return buildServerProvider({
        provider: PROVIDER,
        enabled: true,
        checkedAt,
        models,
        probe: {
          installed: true,
          version: null,
          status: "error",
          authStatus: "unknown",
          message: "Jean-Claude CLI is installed but timed out during version check.",
        },
      });
    }

    const versionResult = versionProbe.success.value;
    const parsedVersion = parseGenericCliVersion(
      `${versionResult.stdout}\n${versionResult.stderr}`,
    );

    if (versionResult.code !== 0) {
      const detail = detailFromResult(versionResult);
      return buildServerProvider({
        provider: PROVIDER,
        enabled: true,
        checkedAt,
        models,
        probe: {
          installed: true,
          version: parsedVersion,
          status: "error",
          authStatus: "unknown",
          message: detail
            ? `Jean-Claude CLI is installed but failed to run. ${detail}`
            : "Jean-Claude CLI is installed but failed to run.",
        },
      });
    }

    // Jean-Claude uses AWS Bedrock credentials from environment — no separate auth step.
    // If `jean --version` succeeded, we consider it ready and authenticated.
    return buildServerProvider({
      provider: PROVIDER,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: parsedVersion,
        status: "ready",
        authStatus: "authenticated",
      },
    });
  },
);

export const JeanClaudeProviderLive = Layer.effect(
  JeanClaudeProvider,
  Effect.gen(function* () {
    const serverSettings = yield* ServerSettingsService;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

    const checkProvider = checkJeanClaudeProviderStatus().pipe(
      Effect.provideService(ServerSettingsService, serverSettings),
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    );

    return yield* makeManagedServerProvider<JeanClaudeSettings>({
      getSettings: serverSettings.getSettings.pipe(
        Effect.map((settings) => settings.providers.jeanClaude),
        Effect.orDie,
      ),
      streamSettings: serverSettings.streamChanges.pipe(
        Stream.map((settings) => settings.providers.jeanClaude),
      ),
      haveSettingsChanged: (previous, next) => !Equal.equals(previous, next),
      checkProvider,
    });
  }),
);
