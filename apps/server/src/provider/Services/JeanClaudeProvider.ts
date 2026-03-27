import { ServiceMap } from "effect";

import type { ServerProviderShape } from "./ServerProvider";

export interface JeanClaudeProviderShape extends ServerProviderShape {}

export class JeanClaudeProvider extends ServiceMap.Service<
  JeanClaudeProvider,
  JeanClaudeProviderShape
>()("t3/provider/Services/JeanClaudeProvider") {}
