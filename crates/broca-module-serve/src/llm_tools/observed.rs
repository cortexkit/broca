//! What the private-content check concluded for each model-tool call, and what became of the
//! calls it found marked, counted for `health.check`.
//!
//! A call can reach its provider without a consent card because its provider needs no question
//! (a local model, one the operator allows, or one a flow's approved version names), because
//! cerebellum said the run read nothing private, or because a standing grant released a call
//! that did. These look the same in the reply, so these counters keep them apart.
//!
//! They count calls only. No scope, agent, prompt, answer, item or card text enters them. Like
//! the other `health.check` counters they live for this process and start at zero after a
//! restart; every name is reported at zero so a quiet process reads differently from an unwired
//! one.

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{Map, Value};

/// What the private-content check concluded about a call's scope. A flow that Basal says has no
/// approved version is refused before the question, so that call has no verdict.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Verdict {
    /// The provider is a local model or one the operator allows for private content (for a
    /// flow, also one its approved version names), so nothing was asked.
    NotAsked,
    /// Cerebellum answered that the scope read nothing private.
    Unmarked,
    /// Cerebellum answered, or had earlier answered, that the scope read private content.
    Marked,
    /// Nothing answered, so the scope counts as marked with no source a card could name.
    Unreachable,
}

impl Verdict {
    const ALL: [Self; 4] = [
        Self::NotAsked,
        Self::Unmarked,
        Self::Marked,
        Self::Unreachable,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::NotAsked => "not_asked",
            Self::Unmarked => "unmarked",
            Self::Marked => "marked",
            Self::Unreachable => "unreachable",
        }
    }
}

/// How a call the check found marked (or could not check) ended at the consent step. Each such
/// call lands in exactly one of these, so their sum equals the `marked` and `unreachable`
/// verdicts together.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Outcome {
    /// A live grant let the call reach its provider without a card.
    ReleasedByGrant,
    /// The call was held behind a consent card (filed now, or joined to one already filed).
    CardFiled,
    /// A standing deny for this agent, run and provider refused the call.
    DeniedByStandingDeny,
    /// The call needed approval but this route cannot hold it: the caller sent no `call_key`,
    /// does not read held replies, or the route has no consent module wired.
    RefusedConsentRequired,
    /// Cerebellum marked the scope with no source a card could name.
    RefusedPrivateContentUnknown,
    /// Nothing answered the private-content question and no grant applied.
    RefusedPrivateContentUnreachable,
    /// A marked flow asked for a provider outside its approved set. Flows never raise cards.
    RefusedByFlowPolicy,
    /// The consent step itself failed: grants storage, card text or card filing.
    ConsentUnavailable,
}

impl Outcome {
    const ALL: [Self; 8] = [
        Self::ReleasedByGrant,
        Self::CardFiled,
        Self::DeniedByStandingDeny,
        Self::RefusedConsentRequired,
        Self::RefusedPrivateContentUnknown,
        Self::RefusedPrivateContentUnreachable,
        Self::RefusedByFlowPolicy,
        Self::ConsentUnavailable,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::ReleasedByGrant => "released_by_grant",
            Self::CardFiled => "card_filed",
            Self::DeniedByStandingDeny => "denied_by_standing_deny",
            Self::RefusedConsentRequired => "refused_consent_required",
            Self::RefusedPrivateContentUnknown => "refused_private_content_unknown",
            Self::RefusedPrivateContentUnreachable => "refused_private_content_unreachable",
            Self::RefusedByFlowPolicy => "refused_by_flow_policy",
            Self::ConsentUnavailable => "consent_unavailable",
        }
    }

    /// The refusal outcome for a consent code the private-content check returned.
    pub(super) fn refused(consent: &str) -> Self {
        match consent {
            super::private_content::PRIVATE_CONTENT_UNKNOWN => Self::RefusedPrivateContentUnknown,
            super::private_content::PRIVATE_CONTENT_UNREACHABLE => {
                Self::RefusedPrivateContentUnreachable
            }
            _ => Self::RefusedConsentRequired,
        }
    }
}

#[derive(Default)]
pub(super) struct Counters {
    verdicts: [AtomicU64; Verdict::ALL.len()],
    outcomes: [AtomicU64; Outcome::ALL.len()],
}

impl Counters {
    pub(super) fn verdict(&self, verdict: Verdict) {
        self.verdicts[verdict as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn outcome(&self, outcome: Outcome) {
        self.outcomes[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// `{"verdicts": {...}, "marked_outcomes": {...}}`, every name present.
    pub(super) fn health(&self) -> Value {
        let verdicts: Map<String, Value> = Verdict::ALL
            .iter()
            .map(|verdict| {
                let count = self.verdicts[*verdict as usize].load(Ordering::Relaxed);
                (verdict.name().to_owned(), count.into())
            })
            .collect();
        let outcomes: Map<String, Value> = Outcome::ALL
            .iter()
            .map(|outcome| {
                let count = self.outcomes[*outcome as usize].load(Ordering::Relaxed);
                (outcome.name().to_owned(), count.into())
            })
            .collect();
        serde_json::json!({"verdicts": verdicts, "marked_outcomes": outcomes})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_name_is_reported_at_zero_and_counts_land_under_their_own_name() {
        let counters = Counters::default();
        let health = counters.health();
        assert_eq!(health["verdicts"].as_object().unwrap().len(), 4);
        assert_eq!(health["marked_outcomes"].as_object().unwrap().len(), 8);
        for verdict in Verdict::ALL {
            assert_eq!(health["verdicts"][verdict.name()], 0);
        }
        for outcome in Outcome::ALL {
            assert_eq!(health["marked_outcomes"][outcome.name()], 0);
        }
        counters.verdict(Verdict::Unreachable);
        counters.outcome(Outcome::ConsentUnavailable);
        counters.outcome(Outcome::ConsentUnavailable);
        let health = counters.health();
        assert_eq!(health["verdicts"]["unreachable"], 1);
        assert_eq!(health["verdicts"]["marked"], 0);
        assert_eq!(health["marked_outcomes"]["consent_unavailable"], 2);
        assert_eq!(health["marked_outcomes"]["card_filed"], 0);
    }
}
