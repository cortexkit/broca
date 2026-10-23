# Vendored model catalog

`models.dev.json` is a snapshot of the models.dev catalog (https://models.dev).
It is distributed under the MIT license, Copyright (c) 2025 models.dev.

The complete copyright and permission notice is in
[THIRD-PARTY-NOTICES.md](../../../THIRD-PARTY-NOTICES.md#modelsdev--mit).

The vendored snapshot supports offline tooling and historical fixture checks. Live
admission reads Fusiform's served row facts, including `wire.npm`, `wire.api`,
`wire.shape`, reasoning tier routes and retirement successors. Do not generate
endpoint or provider-family copies into `overlay.json`. Its remaining native
entries preserve the existing OpenAI and xAI resolver paths until Fusiform
publishes their native default endpoints as curated facts. Anthropic and Gemini
retain their existing native defaults through the same closed hold list in
`family.rs`, without adding endpoint copies here.
