# Late-answer notice, v1

Target bytes for the `consent_answered` purpose of core's `session.notice`,
specified in [consent-requester-v2.md](../../docs/contracts/consent-requester-v2.md#late-answers-on-generic-consent-cards).
They were written to pin the wire before either side was built. Cingulate's
implementation must serialize exactly these bytes for the same inputs, and core
must decode them. Each file is compact JSON with no trailing newline. Its keys
appear in the order the files themselves use, the same as the example in the
contract, and the implementation must emit that order.

- `notice-answered.json`: a detached card answered by the operator.
- `notice-expired.json`: a detached card that expired. No `choice_id` or `choice_label`.
- `notice-withdrawn.json`: a detached card withdrawn by its requester. No `choice_id` or `choice_label`.

Check the files with `shasum -a 256 -c SHA256SUMS` from this directory.
