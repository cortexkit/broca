use broca_protocol::{usage::GenerationUsage, StepId, Usage};
use broca_records::record::WalRecord;

#[derive(Debug, Clone)]
pub(super) struct AttemptTiming {
    pub step_id: StepId,
    pub sent: Option<u64>,
    pub first: Option<u64>,
    pub last: Option<u64>,
}

/// Derive content intervals only from recorded stamps. Missing stamps hide only the interval
/// that needs them; contradictory recorded ordering invalidates the timing sample.
pub fn content_timing_intervals(
    sent: Option<u64>,
    first: Option<u64>,
    last: Option<u64>,
) -> (Option<u64>, Option<u64>) {
    if first.zip(sent).is_some_and(|(first, sent)| first < sent)
        || last.zip(first).is_some_and(|(last, first)| last < first)
    {
        return (None, None);
    }
    (
        last.zip(sent)
            .and_then(|(last, sent)| last.checked_sub(sent)),
        last.zip(first)
            .and_then(|(last, first)| last.checked_sub(first)),
    )
}

/// What a finished step reports about how it was served: its id, and the service tier and speed
/// the provider said it used. A refused step reports none and takes no generation sample.
#[derive(Debug, Clone, Copy)]
pub struct StepServed<'a> {
    pub step_id: StepId,
    pub service_tier_used: Option<&'a str>,
    pub speed_used: Option<&'a str>,
}

impl<'a> StepServed<'a> {
    pub(super) fn of(record: &'a WalRecord) -> Option<Self> {
        let WalRecord::ModelStepFinished {
            step_id,
            service_tier_used,
            speed_used,
            ..
        } = record
        else {
            return None;
        };
        Some(Self {
            step_id: *step_id,
            service_tier_used: service_tier_used.as_deref(),
            speed_used: speed_used.as_deref(),
        })
    }
}

pub(super) fn record(
    entries: &mut Vec<GenerationUsage>,
    attempt: Option<AttemptTiming>,
    served: Option<StepServed<'_>>,
    by_model: &[(&str, &str, Option<&str>, Usage)],
) {
    let Some(StepServed {
        step_id,
        service_tier_used,
        speed_used,
    }) = served
    else {
        return;
    };
    let Some(attempt) = attempt.filter(|attempt| attempt.step_id == step_id) else {
        return;
    };
    let (Some(sent), Some(first), Some(last)) = (attempt.sent, attempt.first, attempt.last) else {
        return;
    };
    let (Some(sent_to_last), Some(first_to_last)) =
        content_timing_intervals(Some(sent), Some(first), Some(last))
    else {
        return;
    };
    let Some((_, model, _, _)) = by_model.first() else {
        return;
    };
    // A server handoff may bill several models, but the attempt has only one pair
    // of content timestamps. Assigning that whole interval to each model would
    // invent per-model throughput, so mixed-model calls have no sample.
    if by_model.iter().any(|(_, other, _, _)| other != model) {
        return;
    }
    let Some(output_tokens) = by_model.iter().try_fold(0u64, |total, (_, _, _, usage)| {
        Some(total + usage.output_tokens?)
    }) else {
        return;
    };
    let reasoning_tokens = by_model.iter().try_fold(0u64, |total, (_, _, _, usage)| {
        Some(total + usage.reasoning_tokens?)
    });
    if let Some(entry) = entries.iter_mut().find(|entry| {
        entry.model == *model
            && entry.service_tier.as_deref() == service_tier_used
            && entry.speed.as_deref() == speed_used
    }) {
        entry.model_calls += 1;
        entry.output_tokens += output_tokens;
        entry.reasoning_tokens = entry
            .reasoning_tokens
            .zip(reasoning_tokens)
            .map(|(a, b)| a + b);
        entry.sent_to_last_content_ms += sent_to_last;
        entry.first_to_last_content_ms += first_to_last;
    } else {
        entries.push(GenerationUsage {
            model: (*model).to_owned(),
            service_tier: service_tier_used.map(str::to_owned),
            speed: speed_used.map(str::to_owned),
            model_calls: 1,
            output_tokens,
            reasoning_tokens,
            sent_to_last_content_ms: sent_to_last,
            first_to_last_content_ms: first_to_last,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_event_content_intervals_preserve_unknowns_and_reject_reversed_stamps() {
        for (sent, first, last, expected) in [
            (Some(100), Some(120), Some(180), (Some(80), Some(60))),
            (Some(100), None, Some(180), (Some(80), None)),
            (None, Some(120), Some(180), (None, Some(60))),
            (Some(100), Some(120), None, (None, None)),
            (None, None, None, (None, None)),
            (Some(100), Some(90), Some(180), (None, None)),
            (Some(100), Some(120), Some(110), (None, None)),
            (Some(100), None, Some(90), (None, None)),
            (Some(100), Some(100), Some(100), (Some(0), Some(0))),
        ] {
            assert_eq!(content_timing_intervals(sent, first, last), expected);
        }
    }
}
