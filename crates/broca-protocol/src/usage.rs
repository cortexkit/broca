//! Token usage, returned by the provider each step. Authoritative and free
//! (provider-reported). Context fill counts fresh input, cache reads and cache
//! writes: all three are prompt tokens occupying the context window. Output and
//! reasoning tokens are not part of this request-side occupancy metric.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    /// Fresh (non-cached) request input tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    /// Cache-READ input tokens (cache hit). Counts toward fill.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_input_tokens: Option<u64>,
    /// Cache-CREATION (write) input tokens. Counts toward fill.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_tokens: Option<u64>,
    /// Output tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    /// Reasoning tokens. ALWAYS A SUBSET of `output_tokens`, never additional to it, on every
    /// family that reports the split -- so a consumer summing the two double-counts.
    ///
    /// The subset relation is normalized here rather than inherited from providers, which do
    /// not agree: OpenAI reports it as a subset already, while Gemini's `thoughtsTokenCount` and
    /// xAI's reasoning count are SIBLINGS of their visible-output counts. Both paths sum the two
    /// into `output_tokens` to restore the invariant, so this field means the same thing whatever
    /// produced it.
    ///
    /// SO THE INVARIANT DESCRIBES BROCA'S CANONICAL OUTPUT, NOT ANY VENDOR'S WIRE. The convention
    /// belongs to whoever parsed the response: the same vendor recorded through a different
    /// adapter can land either way, and an external store measured the opposite convention for
    /// providers this normalizer treats as subset. Do not carry this invariant to facts broca
    /// did not produce.
    ///
    /// IT ALSO HAS A START DATE. `reasoning_tokens > output_tokens` is arithmetically impossible
    /// under subset semantics, and broca's own export lane contains rows that violate it: xAI was
    /// read as a subset before its sibling shape was found by live probe, so 324 of 487 pre-fix
    /// xAI facts are lopsided enough to expose it. Facts written after the fix carry
    /// `usage_policy_version`.
    ///
    /// THAT FIELD IS A PROVIDER-SCOPED BOUNDARY, NOT A GLOBAL ONE, and reading it globally is the
    /// mistake it invites. It is stamped only where the separate-reasoning policy applies, so an
    /// absent version means EITHER the row predates enforcement OR its provider never needed the
    /// fix -- and a global `IS NULL` partition mixes pre-fix rows with every ordinary row ever
    /// written. Measured when this was documented: absent-and-after-the-boundary covered 3,901
    /// openai rows and 567 anthropic rows, none of which predate anything.
    ///
    /// Scoped to the affected provider it is exact: zero xAI rows carry an absent version after
    /// the first stamped one, so `provider = 'xai' AND version IS NULL` selects precisely the
    /// pre-fix population and nothing else.
    ///
    /// That inequality CONVICTS AND NEVER ACQUITS. Zero violations across a provider proves only
    /// that no row was lopsided enough to show a difference; a short reasoning trace under a long
    /// answer is consistent with either convention. Only a live probe settles a provider, which
    /// is how xAI was settled and is why no other provider here is claimed as confirmed.
    ///
    /// `None` means not reported; `Some(0)` means a reported zero. Whether a family
    /// reports the split is a property of the provider's current response shape rather than a
    /// fixed fact about the family: Anthropic surfaces it today via
    /// `output_tokens_details.thinking_tokens`, which older responses omit entirely, and Bedrock
    /// Converse surfaces nothing. So this is a FLOOR on reasoning spend across a mixed fleet,
    /// never a measurement of it, and a per-family list of who reports it goes stale as
    /// providers add the field -- read the normalizers rather than trusting a list here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
}

pub use cortexkit_role_llm_runner::usage::{ModelUsage, RunUsage};

impl Usage {
    /// Context-fill metric: fresh request input + cache reads + cache writes.
    /// All three occupy the model's context window for the measured request.
    ///
    /// This is prompt occupancy, not a measure of what the call cost. Output and
    /// reasoning are output-side, so a consumer wanting spend needs the whole struct.
    ///
    /// Meaningful only on a SINGLE step's usage. Accumulated usage sums input across steps, and
    /// since every step re-sends the growing prompt, the sum counts the same prompt repeatedly
    /// and exceeds any window the model ever held.
    /// Missing input makes occupancy unknown, even if a cache count is known. When
    /// input is reported without a cache split, use that reported input alone; an
    /// unreported cache count is not itself evidence of additional occupancy.
    pub fn fill_tokens(&self) -> Option<u64> {
        self.input_tokens.map(|input| {
            input
                .saturating_add(self.cached_input_tokens.unwrap_or(0))
                .saturating_add(self.cache_write_tokens.unwrap_or(0))
        })
    }

    /// Accumulate another step's usage into this one. Each step is a SEPARATE
    /// billed provider call (it re-sends the growing prompt), so a run's total
    /// spend is the SUM across steps — never the last step's usage alone.
    /// Sum the reported contributions only: a total stays absent until at least
    /// one step reports it. With partial reporting this is a measured lower bound,
    /// not a claim that unreported steps spent nothing.
    pub fn accumulate(&mut self, other: &Usage) {
        self.input_tokens = sum_reported(self.input_tokens, other.input_tokens);
        self.cached_input_tokens =
            sum_reported(self.cached_input_tokens, other.cached_input_tokens);
        self.cache_write_tokens = sum_reported(self.cache_write_tokens, other.cache_write_tokens);
        self.output_tokens = sum_reported(self.output_tokens, other.output_tokens);
        self.reasoning_tokens = sum_reported(self.reasoning_tokens, other.reasoning_tokens);
    }
}

impl std::ops::Add for Usage {
    type Output = Usage;
    fn add(mut self, rhs: Usage) -> Usage {
        self.accumulate(&rhs);
        self
    }
}

impl std::ops::AddAssign for Usage {
    fn add_assign(&mut self, rhs: Usage) {
        self.accumulate(&rhs);
    }
}

/// Sum available measurements without turning two absences into a measured zero.
/// A single reported sibling is a lower bound when the other is unreported.
pub fn sum_reported(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.saturating_add(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

/// One entry of a provider's per-model usage split, as Anthropic reports it for a response it
/// handed from one model to another (`usage.iterations`). Kept verbatim: the named members are
/// the ones broca reads, and every other member stays in `other` so nothing the provider sent
/// is lost.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UsageIteration {
    /// The entry's kind (`message`, `fallback_message`, ...); kept open.
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// The model id the provider reported for this entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_input_tokens: Option<u64>,
    /// Every other member of the entry, verbatim.
    #[serde(flatten)]
    pub other: serde_json::Map<String, serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_usage_treats_each_single_reported_zero_as_reported() {
        let mut total = RunUsage::new();
        for field in [
            "input_tokens",
            "cached_input_tokens",
            "cache_write_tokens",
            "output_tokens",
            "reasoning_tokens",
        ] {
            let usage: Usage = serde_json::from_value(serde_json::json!({field:0})).unwrap();
            let step = match field {
                "input_tokens" => cortexkit_role_llm_runner::usage::StepUsage::default()
                    .with_input_tokens(usage.input_tokens.unwrap()),
                "cached_input_tokens" => cortexkit_role_llm_runner::usage::StepUsage::default()
                    .with_cached_input_tokens(usage.cached_input_tokens.unwrap()),
                "cache_write_tokens" => cortexkit_role_llm_runner::usage::StepUsage::default()
                    .with_cache_write_tokens(usage.cache_write_tokens.unwrap()),
                "output_tokens" => cortexkit_role_llm_runner::usage::StepUsage::default()
                    .with_output_tokens(usage.output_tokens.unwrap()),
                "reasoning_tokens" => cortexkit_role_llm_runner::usage::StepUsage::default()
                    .with_reasoning_tokens(usage.reasoning_tokens.unwrap()),
                _ => unreachable!(),
            };
            total.record_step("provider", "model", None, step);
        }
        assert_eq!(total.steps, 5);
        assert_eq!(total.unreported_steps, 0);
        assert_eq!(total.by_model[0].unreported_steps, 0);
        assert_eq!(total.input_tokens, Some(0));
        assert_eq!(total.cached_input_tokens, Some(0));
        assert_eq!(total.cache_write_tokens, Some(0));
        assert_eq!(total.output_tokens, Some(0));
        assert_eq!(total.reasoning_tokens, Some(0));
        total.record_step(
            "provider",
            "model",
            None,
            cortexkit_role_llm_runner::usage::StepUsage::unreported(),
        );
        assert_eq!(total.steps, 6);
        assert_eq!(total.unreported_steps, 1);
        assert_eq!(total.by_model.len(), 1);
        assert!(total.consistent());
    }

    use serde_json::json;

    #[test]
    fn fill_counts_cache_writes_and_saturates() {
        let usage = Usage {
            input_tokens: Some(3),
            cached_input_tokens: Some(7),
            cache_write_tokens: Some(3170),
            ..Usage::default()
        };
        assert_eq!(usage.fill_tokens(), Some(3180));
        assert_eq!(
            Usage {
                input_tokens: None,
                ..usage
            }
            .fill_tokens(),
            None
        );
        assert_eq!(
            Usage {
                input_tokens: Some(u64::MAX),
                ..usage
            }
            .fill_tokens(),
            Some(u64::MAX)
        );
    }

    #[test]
    fn absent_measurements_omit_keys_but_reported_zero_roundtrips() {
        assert_eq!(serde_json::to_value(Usage::default()).unwrap(), json!({}));
        let zero = Usage {
            input_tokens: Some(0),
            cached_input_tokens: Some(0),
            cache_write_tokens: Some(0),
            output_tokens: Some(0),
            reasoning_tokens: Some(0),
        };
        let wire = json!({"input_tokens":0,"cached_input_tokens":0,"cache_write_tokens":0,"output_tokens":0,"reasoning_tokens":0});
        assert_eq!(serde_json::to_value(zero).unwrap(), wire);
        assert_eq!(serde_json::from_value::<Usage>(wire).unwrap(), zero);
        assert_eq!(
            serde_json::from_value::<Usage>(json!({})).unwrap(),
            Usage::default()
        );
    }

    #[test]
    fn fill_requires_input_and_accumulation_sums_only_reported_contributions() {
        let mut total = Usage::default();
        assert_eq!(total.fill_tokens(), None);
        total.cached_input_tokens = Some(5);
        assert_eq!(total.fill_tokens(), None);
        total.input_tokens = Some(0);
        assert_eq!(total.fill_tokens(), Some(5));
        total += Usage {
            input_tokens: Some(7),
            output_tokens: Some(0),
            ..Usage::default()
        };
        assert_eq!(total.input_tokens, Some(7));
        assert_eq!(total.output_tokens, Some(0));
        assert_eq!(total.cache_write_tokens, None);
        total += Usage::default();
        assert_eq!(total.output_tokens, Some(0));
        assert_eq!(
            Usage {
                input_tokens: Some(7),
                ..Usage::default()
            }
            .fill_tokens(),
            Some(7)
        );
    }
}
