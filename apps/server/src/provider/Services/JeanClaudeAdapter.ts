/**
 * JeanClaudeAdapter - Jean-Claude implementation of the generic provider adapter contract.
 *
 * This service owns jean-claude runtime/session semantics and emits canonical
 * provider runtime events. It does not perform cross-provider routing, shared
 * event fan-out, or checkpoint orchestration.
 *
 * @module JeanClaudeAdapter
 */
import { ServiceMap } from "effect";

import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

/**
 * JeanClaudeAdapterShape - Service API for the Jean-Claude provider adapter.
 */
export interface JeanClaudeAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {
  readonly provider: "jeanClaude";
}

/**
 * JeanClaudeAdapter - Service tag for Jean-Claude provider adapter operations.
 */
export class JeanClaudeAdapter extends ServiceMap.Service<
  JeanClaudeAdapter,
  JeanClaudeAdapterShape
>()("t3/provider/Services/JeanClaudeAdapter") {}
