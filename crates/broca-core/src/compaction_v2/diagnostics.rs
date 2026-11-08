use broca_protocol::BindIdentity;
use cortexkit_role_compaction::view::{View, ViewRefusal};
use serde_json::value::RawValue;

/// Only publication identity and policy presence are logged, never edits or content.
pub struct Publication {
    version: Option<u64>,
    built_through: bool,
    new_signed: bool,
}

impl Publication {
    pub fn from_view(view: &View) -> Self {
        Self {
            version: Some(view.version),
            built_through: view.built_through.is_some(),
            new_signed: view.new_signed.is_some(),
        }
    }

    pub fn from_raw(view: &RawValue) -> Self {
        let value: serde_json::Value = serde_json::from_str(view.get()).expect("RawValue is JSON");
        Self {
            version: value.get("version").and_then(serde_json::Value::as_u64),
            built_through: value.get("built_through").is_some(),
            new_signed: value.get("new_signed").is_some(),
        }
    }

    pub fn warn(&self, bind: &BindIdentity, code: &str) {
        tracing::warn!(
            target: "compaction",
            session_ref = %crate::agent_session_ref(bind),
            view_version = %self.version.map_or_else(|| "unknown".into(), |version| version.to_string()),
            refusal_code = code,
            built_through = self.built_through,
            new_signed = self.new_signed,
            "compaction published view refused"
        );
    }
}

/// The refusal's code as logged. A `rule` refusal names the rule it broke too
/// (`rule:signed_part_invalid`, `rule:final_turn_removed`): the bare code would log a
/// signed part gone stale exactly like a view that empties the request's last turn, and
/// those two call for different follow-up.
pub fn refusal_code(refusal: &ViewRefusal) -> String {
    let value = serde_json::to_value(refusal).expect("view refusal serializes");
    let code = value["code"].as_str().expect("view refusal has a code");
    match value["rule"].as_str() {
        Some(rule) if code == "rule" => format!("{code}:{rule}"),
        _ => code.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::refusal_code;
    use cortexkit_role_compaction::rules::RuleBreak;
    use cortexkit_role_compaction::view::ViewRefusal;

    /// Each rule a view breaks is logged under its own name, and a refusal that isn't a
    /// rule keeps its bare code.
    #[test]
    fn a_rule_refusal_names_the_rule_it_broke() {
        let stale = ViewRefusal::Rule(RuleBreak::SignedPartInvalid {
            mid: "m1".into(),
            part: "p0".into(),
        });
        assert_eq!(refusal_code(&stale), "rule:signed_part_invalid");
        let emptied = ViewRefusal::Rule(RuleBreak::FinalTurnRemoved { index: 3 });
        assert_eq!(refusal_code(&emptied), "rule:final_turn_removed");
        assert_eq!(refusal_code(&ViewRefusal::DuplicateCut), "duplicate_cut");
    }
}
