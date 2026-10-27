//! The refusal-spend rule of `cortexkit-llm-tools`: every refusal a model tool answers states
//! what the caller charges for it, so a caller never keeps its own list of free codes. A
//! refusal carries exactly one of `detail.cost_usd` (measured), `detail.cost_unknown: true`
//! (a provider may have billed it, nothing measured it) or `detail.no_spend: true`; the
//! `cost_unmeasured` code is its own mark and carries none of them.
//!
//! Only these tools mark refusals. The `classifier/v1` route shares the System One code and
//! must keep answering refusals without any spend member, so System One marks a refusal only
//! when it serves a tool call.

use cortexkit_llm_tools::errors::{COST_UNKNOWN_KEY, COST_UNMEASURED, COST_USD_KEY, NO_SPEND_KEY};
use cortexkit_role_classifier::errors::{ItemError, Refusal};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What one refusal tells the caller to charge.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Spend {
    /// Nothing was spent: the invocation ended before anything reached a provider, or the
    /// provider answered with an error it does not bill.
    Nothing,
    /// A request may have reached the provider and been billed, but nothing measured it. Used
    /// whenever a failure cannot be proven to have happened before the request was sent.
    Unknown,
    /// The provider call's billed cost in USD, measured the same way a success measures it.
    Measured(f64),
}

impl Spend {
    /// `refusal` carrying exactly this mark, replacing any mark it had. A `cost_unmeasured`
    /// refusal is returned unchanged: its code already states its spend, and a second mark
    /// would make it invalid.
    pub(crate) fn mark(self, mut refusal: Refusal) -> Refusal {
        if refusal.code == COST_UNMEASURED {
            return refusal;
        }
        let detail = &mut refusal.detail;
        detail.cost_usd = None;
        detail.cost_unknown = None;
        detail.no_spend = None;
        match self.checked() {
            Self::Nothing => detail.no_spend = Some(true),
            Self::Unknown => detail.cost_unknown = Some(true),
            Self::Measured(cost) => detail.cost_usd = Some(cost),
        }
        refusal
    }

    /// `refusal` with this mark only when it states no spend yet. A refusal some earlier step
    /// already marked keeps that mark.
    pub(crate) fn mark_undeclared(self, refusal: Refusal) -> Refusal {
        if declares(&refusal) {
            refusal
        } else {
            self.mark(refusal)
        }
    }

    /// An encoded refusal body carrying exactly this mark, for the bodies that are built as
    /// JSON rather than as a [`Refusal`]. A body that is not a JSON object, or that is a
    /// `cost_unmeasured` refusal, is returned unchanged.
    pub(crate) fn mark_body(self, body: Vec<u8>) -> Vec<u8> {
        let Ok(Value::Object(mut object)) = serde_json::from_slice::<Value>(&body) else {
            return body;
        };
        if object.get("code").and_then(Value::as_str) == Some(COST_UNMEASURED) {
            return body;
        }
        let detail = object
            .entry("detail")
            .or_insert_with(|| Value::Object(Default::default()));
        if !detail.is_object() {
            *detail = Value::Object(Default::default());
        }
        let detail = detail.as_object_mut().expect("detail was made an object");
        detail.remove(COST_USD_KEY);
        detail.remove(COST_UNKNOWN_KEY);
        detail.remove(NO_SPEND_KEY);
        match self.checked() {
            Self::Nothing => detail.insert(NO_SPEND_KEY.into(), Value::Bool(true)),
            Self::Unknown => detail.insert(COST_UNKNOWN_KEY.into(), Value::Bool(true)),
            Self::Measured(cost) => detail.insert(COST_USD_KEY.into(), cost.into()),
        };
        serde_json::to_vec(&Value::Object(object)).unwrap_or(body)
    }

    /// A measured cost the contract would refuse (negative or not finite) is no measurement:
    /// it falls back to unknown spend, never to free.
    fn checked(self) -> Self {
        match self {
            Self::Measured(cost) if !cost.is_finite() || cost < 0.0 => Self::Unknown,
            spend => spend,
        }
    }
}

/// Whether `refusal` already states its spend: a spend member, or the `cost_unmeasured` code.
pub(crate) fn declares(refusal: &Refusal) -> bool {
    refusal.code == COST_UNMEASURED
        || refusal.detail.cost_usd.is_some()
        || refusal.detail.cost_unknown.is_some()
        || refusal.detail.no_spend.is_some()
}

/// How an item error in a model-tool batch was spent, kept with the stored error so a replay
/// of the call answers the same mark as its first answer did. Item errors are never measured:
/// a measured item is an answered one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ItemSpend {
    Nothing,
    Unknown,
}

impl From<ItemSpend> for Spend {
    fn from(spend: ItemSpend) -> Self {
        match spend {
            ItemSpend::Nothing => Spend::Nothing,
            ItemSpend::Unknown => Spend::Unknown,
        }
    }
}

/// An item error as a model-tool batch stores and replies it: the role's `ItemError` with the
/// spend members of the refusal it becomes. A `classifier/v1` batch never carries them, and
/// an item error stored before tools marked refusals has none: it replays unmarked, because a
/// repeat returns the same spend members as the first answer did.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub(crate) struct MarkedItemError {
    #[serde(flatten)]
    pub error: ItemError,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub no_spend: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_unknown: Option<bool>,
}

impl MarkedItemError {
    /// `error` with `spend`'s mark, or with none when `spend` is `None` (a `classifier/v1`
    /// batch). A `cost_unmeasured` error is never marked: its code is its mark.
    pub(crate) fn new(error: ItemError, spend: Option<ItemSpend>) -> Self {
        let spend = spend.filter(|_| error.code != COST_UNMEASURED);
        Self {
            error,
            no_spend: (spend == Some(ItemSpend::Nothing)).then_some(true),
            cost_unknown: (spend == Some(ItemSpend::Unknown)).then_some(true),
        }
    }

    /// The same error without any spend mark, as a `classifier/v1` reply carries it.
    pub(crate) fn unmarked(self) -> Self {
        Self::new(self.error, None)
    }

    /// The model-tool refusal this item error becomes, with the spend members it carried.
    pub(crate) fn into_refusal(self) -> Refusal {
        let mut refusal = Refusal::new(self.error.code, self.error.message);
        refusal.detail.class = self.error.class;
        refusal.detail.retry_after_ms = self.error.retry_after_ms;
        refusal.detail.no_spend = self.no_spend;
        refusal.detail.cost_unknown = self.cost_unknown;
        refusal
    }
}

#[cfg(test)]
#[path = "spend_tests.rs"]
mod tests;
