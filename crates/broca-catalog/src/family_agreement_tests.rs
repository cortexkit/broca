//! Compare the independent, pinned producer facts with the renderer selection Broca runs.

use super::{credential_persona_route, wire_family};
use broca_provider::spec::WireFamilyId;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

const TOKENS: [(&str, WireFamilyId); 7] = [
    ("anthropic.messages", WireFamilyId::AnthropicMessages),
    ("openai.responses", WireFamilyId::OpenAiResponses),
    ("openai.chat_completions", WireFamilyId::OpenAiChat),
    (
        "openai.chatgpt_responses",
        WireFamilyId::OpenAiResponsesChatgpt,
    ),
    ("google.generate_content", WireFamilyId::Gemini),
    ("google.code_assist", WireFamilyId::GoogleCodeAssist),
    ("aws.bedrock_converse", WireFamilyId::BedrockConverse),
];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    model_id: String,
    provider: String,
    auth_method: Option<String>,
    #[serde(rename = "wire.npm")]
    npm: Option<String>,
    #[serde(rename = "wire.shape")]
    shape: Option<String>,
    #[serde(rename = "wire.family")]
    family: Option<String>,
}

fn broca_family(row: &Row) -> (Option<WireFamilyId>, String) {
    if let Some(method) = row.auth_method.as_deref() {
        if let Some((family, _)) = credential_persona_route(&row.provider, method) {
            return (
                Some(family),
                format!("credential_persona_route({:?}, {method:?})", row.provider),
            );
        }
        // Use the serving layer's authority for wire-changing credentials, not a second
        // ChatGPT/Code-Assist table that could agree with the fixture while production drifts.
        if let Some(method) = broca_catalog::live::AuthMethod::from_token(method) {
            if let Some(family) = broca_module_serve::auth_routing::family_for_method(method) {
                return (Some(family), format!("family_for_method({method:?})"));
            }
        }
    }
    (
        row.npm
            .as_deref()
            .and_then(|npm| wire_family(npm, row.shape.as_deref())),
        format!("wire_family(npm={:?}, shape={:?})", row.npm, row.shape),
    )
}

#[derive(Default)]
struct Agreement {
    compared: BTreeMap<String, usize>,
    broca_only: Vec<String>,
    fusiform_only: Vec<String>,
    mismatches: Vec<String>,
}

impl Agreement {
    fn check(&mut self, context: &str, row: &Row) {
        let fusiform = row.family.as_deref().map(|token| {
            TOKENS
                .iter()
                .find(|(known, _)| *known == token)
                .unwrap_or_else(|| {
                    panic!(
                        "{context}: {}: unknown wire.family token {token:?}",
                        row.model_id
                    )
                })
                .1
        });
        let (broca, rule) = broca_family(row);
        let detail = format!(
            "{context}: {}: Fusiform={:?}, Broca={broca:?}; rule={rule}",
            row.model_id, row.family
        );
        match (fusiform, broca) {
            (Some(expected), Some(actual)) => {
                *self
                    .compared
                    .entry(row.family.clone().unwrap())
                    .or_default() += 1;
                if expected != actual {
                    self.mismatches.push(detail);
                }
            }
            (None, Some(_)) => self.broca_only.push(detail),
            (Some(_), None) => self.fusiform_only.push(detail),
            (None, None) => {}
        }
    }

    fn finish(&self, name: &str, minimum: usize, required_tokens: &[&str]) {
        let total: usize = self.compared.values().sum();
        eprintln!(
            "{name}: compared {total} rows; per token: {:?}",
            self.compared
        );
        eprintln!(
            "{name}: Broca-only (Fusiform not verified): {}\n{}",
            self.broca_only.len(),
            self.broca_only.join("\n")
        );
        eprintln!(
            "{name}: Fusiform-only (Broca has no family): {}\n{}",
            self.fusiform_only.len(),
            self.fusiform_only.join("\n")
        );
        assert!(self.mismatches.is_empty(), "{}", self.mismatches.join("\n"));
        assert!(
            total >= minimum,
            "{name}: compared {total}, need >= {minimum}"
        );
        for token in required_tokens {
            assert!(
                self.compared.get(*token).copied().unwrap_or_default() > 0,
                "{name}: never compared {token}"
            );
        }
    }
}

#[test]
fn pinned_fusiform_bulk_wire_families_agree() {
    let mut agreement = Agreement::default();
    for line in include_str!("../tests/fixtures/wire-family-rows.jsonl").lines() {
        let row: Row = serde_json::from_str(line).expect("pinned extract row parses");
        agreement.check("bulk", &row);
    }
    agreement.finish("bulk", 1_000, &TOKENS.map(|(token, _)| token));
}

#[test]
fn pinned_fusiform_served_wire_families_agree() {
    let fixture: BTreeMap<String, Value> =
        serde_json::from_str(include_str!("../tests/fixtures/served-payloads.json")).unwrap();
    let mut agreement = Agreement::default();
    for (name, case) in fixture {
        let Some(models) = case.pointer("/response/models").and_then(Value::as_object) else {
            continue;
        };
        for (model_id, facts) in models {
            let (provider, _) = model_id.split_once('/').expect("qualified model id");
            let fact = |key: &str| {
                facts.get(key).map(|value| {
                    value["value"]
                        .as_str()
                        .expect("wire fact value is a string")
                        .to_owned()
                })
            };
            let row = Row {
                model_id: model_id.clone(),
                provider: provider.to_owned(),
                auth_method: case
                    .pointer("/response/plane/auth_method")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                npm: fact("wire.npm"),
                shape: fact("wire.shape"),
                family: facts
                    .get("wire.family")
                    .map(|value| value.as_str().expect("wire.family is a string").to_owned()),
            };
            agreement.check(&name, &row);
        }
    }
    // The served golden is deliberately small; bulk coverage lives in the seed extract.
    // Alias rows without a selected auth plane stay one-sided, rather than guessing a family.
    agreement.finish(
        "served",
        24,
        &[
            "anthropic.messages",
            "openai.responses",
            "openai.chat_completions",
            "openai.chatgpt_responses",
        ],
    );
}
