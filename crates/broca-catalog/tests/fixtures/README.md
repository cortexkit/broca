# Fusiform wire-family fixtures

These are offline catalog test inputs, not credentials or captured user traffic.
`served-payloads.json` is Fusiform's synthetic served-response golden, copied
byte-for-byte from `cortexkit/fusiform` at commit
`119258da721c5fac0e197d38f318e62100f8598e` (protocol 0.49.0), path
`crates/fusiform-module/fixtures/served-payloads.json`.

Its SHA-256 is
`ff2adc9493b0a390875b002197bec14218aa7c00789a0505e866940d61562ac2`.
The fixture was checked for credentials, private keys, bearer tokens and private
filesystem paths before vendoring; it contains only synthetic catalog examples
and public provider/source metadata.

To refresh just the exact served golden from a local Fusiform checkout:

```sh
git -C "$FUSIFORM_CHECKOUT" show 119258da721c5fac0e197d38f318e62100f8598e:crates/fusiform-module/fixtures/served-payloads.json > crates/broca-catalog/tests/fixtures/served-payloads.json
```

## Bulk rule coverage

The served golden has only 70 model-row occurrences, of which 27 publish a
family. It cannot provide broad package-table coverage. `wire-family-rows.jsonl`
is a compact projection of the **public models.dev seed**, not a live store or
private traffic. It contains 8,936 seed rows plus 64 distinct auth-plane/alias
routes from the same pinned Fusiform commit. The script preserves model ids,
provider ids, package and shape, and joins them against Fusiform's own curated
`data/wire-families.json`; it never consults Broca's table to produce expectations.
Model-level npm overrides provider npm; shape has no provider fallback.

This bulk projection tests the producer's exact rule lookup **before** current-read
endpoint corrections/withholding, historical filtering or fact-prefix selection.
It is not a full `catalog.get` export. The exact served golden independently tests
the served shape and those omissions. Auth-plane declarations come from
`data/billing-planes.json`; Code Assist aliases carry their own family from
`data/model-aliases.json`, not their target model's protocol.

Producer implementation references, all at the pinned commit:

- `crates/fusiform-core/src/normalize/mod.rs:518-527,646-654`: npm/shape projection.
- `crates/fusiform-module/src/wire_families.rs:58-86,96-100`: exact selector lookup.
- `crates/fusiform-module/src/route.rs:1791-1793,2392-2401,5737-5739`: auth-plane,
  plain-row and alias family facts respectively.

`wire-family-rows.meta.json` records every source path and SHA-256, the pinned
commit, row/token counts and both output hashes. The bulk extract SHA-256 is
`7350e9ab518d73dd94d2942679258cbdbd83026bb3deb7ddebd91a2f9f0e6506`.
To regenerate **both** fixtures and their provenance, from the Broca root:

```sh
python3 crates/broca-catalog/tests/fixtures/refresh-wire-families.py "$FUSIFORM_CHECKOUT"
```

Add `--check` to compare the committed bytes against a fresh projection of the
pinned Git objects without writing anything. The script reads only the named
commit, regardless of the sibling checkout's current branch or working tree.

## Comparison and interpretation

Run the adjacent unit tests with output to see counts and every one-sided row:

```sh
cargo test --locked -p broca-catalog -F broca-catalog/live --lib family::agreement_tests:: -- --nocapture
```

The `live` feature makes the production serving layer's auth-method mapping
available; the tests themselves are offline. Only rows with a family on both
sides must agree. An absent Fusiform fact is **not verified**, not unsupported.
One-sided rows are listed, not failed. A token outside the closed vocabulary is
an error even when Broca cannot map that row. The bulk test requires at least
1,000 comparisons and a comparison for every token below.

| Fusiform token | Broca `WireFamilyId` | Production authority |
| --- | --- | --- |
| `anthropic.messages` | `AnthropicMessages` | `crates/broca-catalog/src/family.rs:31` |
| `openai.responses` | `OpenAiResponses` | `crates/broca-catalog/src/family.rs:25-30`; Meta OAuth: `:11-16` |
| `openai.chat_completions` | `OpenAiChat` | `crates/broca-catalog/src/family.rs:27,34-37` |
| `openai.chatgpt_responses` | `OpenAiResponsesChatgpt` | `crates/broca-module-serve/src/auth_routing.rs:42-49` |
| `google.generate_content` | `Gemini` | `crates/broca-catalog/src/family.rs:32` |
| `google.code_assist` | `GoogleCodeAssist` | `crates/broca-module-serve/src/auth_routing.rs:42-49` |
| `aws.bedrock_converse` | `BedrockConverse` | `crates/broca-catalog/src/family.rs:33` |

The bulk fixture compares 7,647 rows: messages 240, responses 391, chat
completions 6,717, ChatGPT responses 52, generate content 56, Code Assist 5,
Bedrock Converse 186. Its remaining 1,353 rows have no family on either side;
it has no one-sided rows.

The served fixture compares 24 rows: messages 6, responses 4, chat completions
10 and ChatGPT responses 4. Its three family-bearing alias occurrences have no
selected auth plane and no npm, so they are reported as Fusiform-only:
`google/antigravity-claude-sonnet-5-5` once and
`google/antigravity-gemini-3.8-flash` twice. Bulk coverage selects the Antigravity
plane explicitly and compares all five published aliases. The served fixture
also reports 11 Broca-only occurrences (unverified on Fusiform's side):

- `anthropic/claude-sonnet-4-5`: the historical corrected-interval example.
- `openai/gpt-5.6-luna`: six prefix-filtered ChatGPT examples (bulk and named,
  each with cache-write, capabilities or rates only).
- `openai/gpt-image-1`: three prefix-filtered ChatGPT bulk examples (cache-write,
  capabilities and rates).
- `opencode-go/deepseek-v4-flash`: the withheld-endpoint bulk quota example.

The fixtures contain no credentials, private keys, bearer tokens or private
filesystem paths. The extract intentionally drops upstream environment-variable
names, endpoints, rate cards and prose; it retains only public routing data.
The models.dev data is covered by the MIT notice in the repository's
`THIRD-PARTY-NOTICES.md` ("models.dev — MIT").
