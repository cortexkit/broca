# Third-party notices

This file records notices for code and data adapted or vendored in broca. broca itself
is licensed under the MIT License in [LICENSE](LICENSE). Dependencies retain their own
licenses.

## OpenAI Codex — Apache-2.0

Source: https://github.com/openai/codex at revision
`a6baf8867cb4c9726213c0884a5c8b11f0cfd8bf`.

The ChatGPT Responses WebSocket transport in
`crates/broca-provider/src/transport_websocket.rs`, the 300-second idle limit in
`crates/broca-provider/src/transport.rs`, and the refusal classifier in the published
`cortexkit-chatgpt-policy` crate are adapted from Codex. broca
connects these to its run-local transport, shared Responses normalizer, HTTPS
fallback and account-limit error handling.

The full upstream license is in [LICENSES/Apache-2.0.txt](LICENSES/Apache-2.0.txt).
The applicable upstream NOTICE is:

```text
OpenAI Codex
Copyright 2025 OpenAI
```

The Ratatui portion of Codex's NOTICE does not apply: broca does not use Ratatui.

The upstream files were fetched from
`https://raw.githubusercontent.com/openai/codex/a6baf8867cb4c9726213c0884a5c8b11f0cfd8bf/`:

- `LICENSE` SHA-256: `d17f227e4df5da1600391338865ce0f3055211760a36688f816941d58232d8dc`
- `NOTICE` SHA-256: `9d71575ecfd9a843fc1677b0efb08053c6ba9fd686a0de1a6f5382fd3c220915`

## OpenCode — MIT

Source: https://github.com/anomalyco/opencode.

`crates/broca-provider/src/variant.rs` follows `reasoningVariants`,
`settingsForEffort`, `budgetVariants`, `toggleVariants` and `settingsForBudget`
from OpenCode's `packages/core/src/models-dev.ts` at revision
`5716f8ba60e79ec60ec485b6e5291c0b0bc1f252`. broca maps settings by wire family
instead of npm package and freezes them at admission.

The ChatGPT header construction in `crates/broca-catalog/src/live.rs` and the
account-claim extraction in the published `cortexkit-chatgpt-policy` crate follow
OpenCode's `codex.ts` and `extractAccountIdFromClaims`. broca adds vault credential
resolution, frozen session scope and non-empty claim checks.

```text
MIT License

Copyright (c) 2025 opencode

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
```

## models.dev — MIT

The vendored model catalog at `crates/broca-catalog/data/models.dev.json` comes
from https://models.dev (source: https://github.com/anomalyco/models.dev).
Its data is distributed under the following notice:

```text
MIT License

Copyright (c) 2025 models.dev

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
```

## Howard Hinnant's date algorithms — public domain

The civil-date conversion in `crates/ck-session-import/src/opencode.rs` is adapted
from Howard Hinnant's public-domain `civil_from_days` algorithm:
https://howardhinnant.github.io/date_algorithms.html#civil_from_days.
