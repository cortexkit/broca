# Vendored model catalog

`models.dev.json` is a snapshot of the models.dev catalog (https://models.dev).
It is distributed under the MIT license, Copyright (c) 2025 models.dev.

The complete copyright and permission notice is in
[THIRD-PARTY-NOTICES.md](../../../THIRD-PARTY-NOTICES.md#modelsdev--mit).

The vendored snapshot supports offline tooling and historical fixture checks. Live
admission reads Fusiform's served row facts, including `wire.npm`, `wire.api`,
`wire.shape`, reasoning tier routes and retirement successors. Do not generate
endpoint or provider-family copies into `overlay.json`; its patch/add/remove maps
are empty. Native provider defaults and mixed-protocol Go routes come from the
published package, shape and API facts.

The `full_url_parity` example compares the 66-model baseline and every Go row using
the unchanged family renderers, and separately audits all Google-format rows.
Its historical pins fixture is an audit oracle, not serving data. Unrouted Go models,
including Kimi K3.1 Pro, have no published `wire.api` and refuse through the general
missing-fact rule. There is no local model-name hold or guessed endpoint fallback.

Fusiform's Anthropic roots follow models.dev's version-included convention.
The catalog strips a final `/v1` before freezing an Anthropic-family served root,
because Broca's renderer appends `/v1/messages`. Roots without `/v1`, explicit
operator overrides and already recorded configs are not rewritten. OpenAI and
Google roots retain their published version paths.
