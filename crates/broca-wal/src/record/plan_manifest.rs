//! The frozen plan manifest a `RunStarted` may carry: the fetch plan a session's first send
//! named, what each of its tool providers answered when fetched, and what was built from those
//! answers (the tools offered to the model and the joined system text), frozen so that every
//! later episode and every resume renders from exactly those bytes and never fetches again.
//!
//! The plan itself is stored as the opaque JSON it arrived as, never as fields of this record:
//! the plan's layout belongs to the caller that writes it, and may grow without a WAL change.
//!
//! Nothing here decides how a manifest is fetched, joined or checked; `broca-fetch-plan` builds
//! it at admission and the run loop records it. A reader must carry every value through
//! untouched, because the digests recorded beside them were computed over the values' bytes as
//! the providers answered them.

use std::{collections::BTreeMap, sync::Arc};

use broca_protocol::request::Tool;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// JSON that broca stores and passes on but never interprets, such as the session's fetch
/// plan. Held as its JSON text.
///
/// On disk it is a JSON STRING whose content is that JSON text, not an embedded JSON value.
/// `WalRecord` is an internally tagged enum, and serde decodes every field of such an enum
/// through an intermediate buffer that keeps numbers only as `u64`, `i64` or `f64` and strings
/// only by value. An embedded value would therefore come back with its keys re-sorted, `2.50`
/// re-spelled `2.5`, `1e3` re-spelled `1000.0` and escapes normalized, and a reader could no
/// longer reproduce the bytes a digest was computed over. A string survives that buffer exactly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OpaqueJson(String);

/// The reply declaration stored in a frozen plan. The WAL owns this wire shape so a
/// provider role's serde changes cannot silently change durable record bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenReply {
    pub max_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_argument: Option<FrozenHoldArgument>,
}

/// The provider's own result-format declaration for one tool, frozen with the plan. The WAL
/// treats it as an open string so a contract's later format is stored verbatim rather than
/// refused; only the surface that reads it decides which names it understands.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FrozenResultFormat(pub String);

/// Session-level declarations of one tool provider, frozen with the plan.
///
/// A run routes and decodes a tool reply by what its provider declared when the session was
/// admitted, never by what a later catalog says: a provider that starts (or stops) supporting a
/// result format or late results between admission and a resumed step must not change how
/// broca reads a reply. Everything here is optional and absent when the provider did not
/// declare it, so a manifest from a provider with no such declarations keeps its exact bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenProviderDeclarations {
    /// Per-tool result formats, by tool name, frozen only for tools that opt in. Absent means no
    /// tool of this provider declared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_formats: Option<BTreeMap<String, FrozenResultFormat>>,
    /// Whether the provider declared the `late_results` session capability, frozen only when it
    /// answered yes. Absent means the provider cannot settle a call after its reply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub late_results: Option<bool>,
    /// Actual tool.catalog identities frozen at admission. Old manifests and
    /// non-plan tools carry none; a later catalog must not fill the missing facts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_pins: Option<BTreeMap<String, super::DispatchSchemaPin>>,
}

impl FrozenProviderDeclarations {
    /// Whether nothing was declared, so the declaration writes no bytes at all.
    pub fn is_empty(&self) -> bool {
        self.result_formats.is_none() && self.late_results.is_none() && self.schema_pins.is_none()
    }
}

/// The numeric hold argument saved with a tool's reply declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenHoldArgument {
    pub name: String,
    pub default_ms: u64,
    pub grace_ms: u64,
}

impl OpaqueJson {
    /// Wrap `text`, refusing it unless it is one well-formed JSON value. Only a writer checks
    /// this; decoding a stored record never parses the text.
    pub fn from_json_text(text: impl Into<String>) -> Result<Self, serde_json::Error> {
        let text = text.into();
        serde_json::from_str::<serde::de::IgnoredAny>(&text)?;
        Ok(Self(text))
    }

    /// The JSON text exactly as it was stored.
    pub fn as_json_text(&self) -> &str {
        &self.0
    }
}

/// One reference slot a catalog tool declared, as frozen: the argument's top-level name and its
/// kind (`token`, or `source:<dialect>`). Kinds stay open strings, as the catalog sends them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenReferenceSlot {
    pub name: String,
    pub kind: String,
}

/// What one provider's fetch at admission came back with.
///
/// An answered fetch with no `text` (no system-text item was asked of the provider), one whose
/// `text` is present but empty (asked, and the provider answered with no text), and an absent
/// fetch (an optional item the provider did not answer, so nothing of it was joined) are three
/// different facts and stay different through a decode.
#[derive(Debug, Clone, PartialEq)]
pub enum FetchOutcome {
    /// The provider answered. Written as `catalog_digest`, `tools` and, when a system-text item
    /// was asked of it, `text`.
    Answered {
        /// The provider's digest of its answer.
        catalog_digest: String,
        /// The tools the provider answered, exactly as answered and in its order; empty for a
        /// provider that was asked only for system text.
        tools: Arc<Vec<Tool>>,
        /// The system text the provider answered, byte for byte: `Some` when a system-text item
        /// was asked of it (possibly `""`), `None` when none was asked.
        text: Option<String>,
        /// Per-tool operation permissions frozen from the catalog, including its default.
        /// Absent means permission was not frozen; it never authorizes result replacement.
        result_ops: Option<BTreeMap<String, Vec<String>>>,
        /// Reply deadlines by tool name, frozen only when the provider declares them.
        /// Absent keeps manifests from providers without deadlines byte-compatible.
        reply: Option<BTreeMap<String, FrozenReply>>,
        /// Result formats and session capabilities the provider declared, frozen only when it
        /// declared some. Absent keeps manifests from providers without declarations
        /// byte-compatible.
        declarations_frozen: Option<FrozenProviderDeclarations>,
        /// The catalog's reference slots by tool name: the top-level string arguments in which
        /// output references are read (llm-runner §9.13). Frozen only when the provider's plan
        /// item opts into output references and one of its tools declares slots; absent
        /// otherwise, which keeps every other manifest's bytes. Must-understand (feature
        /// `plan-reference-slots/v1`): a reader that dropped it would re-freeze later episodes
        /// without the slots.
        reference_slots: Option<BTreeMap<String, Vec<FrozenReferenceSlot>>>,
    },
    /// An optional tool or system-text provider that gave no usable answer, so the session was
    /// admitted without it. Written as `"absent": "<reason>"`. The reason is an open string
    /// (for example `timeout`, `unknown_module`, `plan_stale` or `refused:<code>`) that a reader keeps and
    /// never interprets.
    Absent {
        /// Why the provider did not answer.
        reason: String,
    },
    /// A fetch carrying neither `catalog_digest` nor `absent`: an outcome a newer broca release
    /// writes that this release does not know. Decoding it keeps the record readable; it is never
    /// written, because nothing of the original outcome survives to write back.
    Unknown,
}

/// One provider's final fetch outcome at admission, combining its tool and system-text items.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderFetch {
    /// The provider module that was fetched.
    pub provider: String,
    /// What the fetch came back with.
    pub outcome: FetchOutcome,
}

/// The on-disk shape of a [`ProviderFetch`], flat: an answered fetch writes `catalog_digest`,
/// `tools` and optionally `text`; an absent one writes `absent`. Field order is part of the
/// byte layout.
#[derive(Serialize)]
struct ProviderFetchOut<'a> {
    provider: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog_digest: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<&'a [Tool]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    absent: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result_ops: Option<&'a BTreeMap<String, Vec<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply: Option<&'a BTreeMap<String, FrozenReply>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    declarations_frozen: Option<&'a FrozenProviderDeclarations>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reference_slots: Option<&'a BTreeMap<String, Vec<FrozenReferenceSlot>>>,
}

#[derive(Deserialize)]
struct ProviderFetchIn {
    provider: String,
    #[serde(default)]
    catalog_digest: Option<String>,
    #[serde(default)]
    tools: Option<Arc<Vec<Tool>>>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    absent: Option<String>,
    #[serde(default)]
    result_ops: Option<BTreeMap<String, Vec<String>>>,
    #[serde(default)]
    reply: Option<BTreeMap<String, FrozenReply>>,
    #[serde(default)]
    declarations_frozen: Option<FrozenProviderDeclarations>,
    #[serde(default)]
    reference_slots: Option<BTreeMap<String, Vec<FrozenReferenceSlot>>>,
}

impl Serialize for ProviderFetch {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut out = ProviderFetchOut {
            provider: &self.provider,
            catalog_digest: None,
            tools: None,
            text: None,
            absent: None,
            result_ops: None,
            reply: None,
            declarations_frozen: None,
            reference_slots: None,
        };
        match &self.outcome {
            FetchOutcome::Answered {
                catalog_digest,
                tools,
                text,
                result_ops,
                reply,
                declarations_frozen,
                reference_slots,
            } => {
                out.catalog_digest = Some(catalog_digest);
                out.tools = Some(tools.as_slice());
                out.text = text.as_deref();
                out.result_ops = result_ops.as_ref();
                out.reply = reply.as_ref();
                out.declarations_frozen = declarations_frozen.as_ref();
                out.reference_slots = reference_slots.as_ref();
            }
            FetchOutcome::Absent { reason } => out.absent = Some(reason),
            FetchOutcome::Unknown => {
                return Err(serde::ser::Error::custom(
                    "a provider fetch whose outcome this build does not know cannot be written",
                ))
            }
        }
        out.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ProviderFetch {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let fetch = ProviderFetchIn::deserialize(deserializer)?;
        // A declaration is frozen only beside an answered fetch: an absent provider declared
        // nothing, so a record carrying one next to `absent` is malformed.
        if fetch.absent.is_some() && fetch.declarations_frozen.is_some() {
            return Err(serde::de::Error::custom(
                "an absent provider fetch cannot carry frozen declarations",
            ));
        }
        let outcome = match (fetch.catalog_digest, fetch.absent) {
            (Some(catalog_digest), None) => FetchOutcome::Answered {
                catalog_digest,
                tools: fetch.tools.ok_or_else(|| {
                    serde::de::Error::custom("an answered provider fetch must carry `tools`")
                })?,
                text: fetch.text,
                result_ops: fetch.result_ops,
                reply: fetch.reply,
                declarations_frozen: fetch.declarations_frozen,
                reference_slots: fetch.reference_slots,
            },
            (None, Some(reason)) => {
                if fetch.tools.is_some() || fetch.text.is_some() {
                    return Err(serde::de::Error::custom(
                        "an absent provider fetch cannot carry `tools` or `text`",
                    ));
                }
                FetchOutcome::Absent { reason }
            }
            (None, None) => FetchOutcome::Unknown,
            (Some(_), Some(_)) => {
                return Err(serde::de::Error::custom(
                    "a provider fetch cannot carry both `catalog_digest` and `absent`",
                ))
            }
        };
        Ok(ProviderFetch {
            provider: fetch.provider,
            outcome,
        })
    }
}

/// Everything a session's first send fetched and froze: the plan as received, each provider's
/// answer, the tools offered to the model, the joined system text, and the versions that decided
/// how they were combined. Every `RunStarted` of a session admitted with a plan carries the same
/// manifest, because a WAL fold can move the session's first `RunStarted` out of the live WAL
/// into the archive, and the episodes left in the live WAL must still render from it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanManifest {
    /// The `plan` member of the session's first `session.send`, exactly as received.
    pub plan: OpaqueJson,
    /// The plan's identity: SHA-256, as 64 lowercase hex characters, of the RFC 8785 (JCS)
    /// canonical bytes of `plan`. A later send's plan is the same plan exactly when its
    /// identity is equal.
    pub plan_identity: String,
    /// The composition digest the plan stated and the writer verified against the plan's
    /// composition, as 64 lowercase hex characters.
    pub composition_digest: String,
    /// One entry per provider fetched, sorted by provider id bytewise.
    pub fetches: Vec<ProviderFetch>,
    /// The tools offered to the model, in the order they were offered, in the same form as a
    /// run's frozen tools.
    pub tools: Arc<Vec<Tool>>,
    /// The session's system text: the answered texts joined.
    pub system_text: String,
    /// The digest of `system_text`.
    pub system_digest: String,
    /// Which joining rule produced `system_text` from the answered texts.
    pub join_version: u32,
    /// The version of each role's definition the session was admitted with, by role name.
    pub role_versions: BTreeMap<String, String>,
    /// One declaration per step-transform item, in plan order. Empty on sessions admitted
    /// without transforms, including every manifest written before declarations existed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub declarations: Vec<FrozenDeclaration>,
}

/// A provider's `transform.declare` answer frozen at admission, never fetched again on resume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenDeclaration {
    pub provider: String,
    /// Stored as opaque JSON text for the same byte-preservation reason as `PlanManifest.plan`.
    pub answer: OpaqueJson,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::WalRecord;
    use sha2::{Digest, Sha256};

    const FIXTURE: &str = include_str!("../../tests/fixtures/plan_manifest_run_started.json");

    const TOOL: &str = r#"{"name":"read","description":"Read a file","input_schema":{"properties":{},"type":"object"},"execution_mode":"parallel"}"#;

    #[test]
    fn absent_reply_keeps_old_manifest_bytes() {
        let bytes = FIXTURE.trim_end();
        let record: WalRecord = serde_json::from_str(bytes).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), bytes);
    }

    /// Frozen reference slots round-trip through a whole `RunStarted`, written after `reply`,
    /// and make the record require `plan-reference-slots/v1`; the fixture without them requires
    /// no such feature.
    #[test]
    fn frozen_reference_slots_round_trip_and_are_must_understand() {
        let mut record: WalRecord = serde_json::from_str(FIXTURE.trim_end()).unwrap();
        assert!(!record
            .required_features()
            .contains(&crate::framing::PLAN_REFERENCE_SLOTS_FEATURE));
        let WalRecord::RunStarted {
            manifest: Some(manifest),
            ..
        } = &mut record
        else {
            panic!("fixture must carry a manifest")
        };
        let FetchOutcome::Answered {
            reference_slots, ..
        } = &mut manifest.fetches[0].outcome
        else {
            panic!("fixture must carry an answered fetch")
        };
        *reference_slots = Some(
            [(
                "read".into(),
                vec![FrozenReferenceSlot {
                    name: "data".into(),
                    kind: "token".into(),
                }],
            )]
            .into(),
        );
        let bytes = serde_json::to_string(&record).unwrap();
        assert!(bytes.contains(r#""reference_slots":{"read":[{"name":"data","kind":"token"}]}"#));
        let decoded: WalRecord = serde_json::from_str(&bytes).unwrap();
        assert_eq!(decoded, record);
        assert_eq!(serde_json::to_string(&decoded).unwrap(), bytes);
        assert!(record
            .required_features()
            .contains(&crate::framing::PLAN_REFERENCE_SLOTS_FEATURE));
    }

    /// A manifest that froze both reference slots and provider declarations writes both members,
    /// declarations first, round-trips byte for byte, and requires both features.
    #[test]
    fn reference_slots_and_provider_declarations_together_require_both_features() {
        let mut record: WalRecord = serde_json::from_str(FIXTURE.trim_end()).unwrap();
        let WalRecord::RunStarted {
            manifest: Some(manifest),
            ..
        } = &mut record
        else {
            panic!("fixture must carry a manifest")
        };
        let FetchOutcome::Answered {
            reference_slots,
            declarations_frozen,
            ..
        } = &mut manifest.fetches[0].outcome
        else {
            panic!("fixture must carry an answered fetch")
        };
        *reference_slots = Some(BTreeMap::from([(
            "read".to_owned(),
            vec![FrozenReferenceSlot {
                name: "data".into(),
                kind: "token".into(),
            }],
        )]));
        *declarations_frozen = Some(FrozenProviderDeclarations {
            result_formats: None,
            late_results: Some(true),
            schema_pins: None,
        });
        let bytes = serde_json::to_string(&record).unwrap();
        assert!(bytes.contains(
            r#""declarations_frozen":{"late_results":true},"reference_slots":{"read":[{"name":"data","kind":"token"}]}"#
        ));
        let decoded: WalRecord = serde_json::from_str(&bytes).unwrap();
        assert_eq!(serde_json::to_string(&decoded).unwrap(), bytes);
        let features = record.required_features();
        assert!(features.contains(&crate::framing::PLAN_REFERENCE_SLOTS_FEATURE));
        assert!(features.contains(&crate::framing::PROVIDER_DECLARATIONS_FEATURE));
    }

    #[test]
    fn frozen_reply_round_trips_through_wal_record() {
        let mut record: WalRecord = serde_json::from_str(FIXTURE.trim_end()).unwrap();
        let WalRecord::RunStarted {
            manifest: Some(manifest),
            ..
        } = &mut record
        else {
            panic!("fixture must carry a manifest")
        };
        let FetchOutcome::Answered { reply, .. } = &mut manifest.fetches[0].outcome else {
            panic!("fixture must carry an answered fetch")
        };
        *reply = Some(
            [(
                "read".into(),
                FrozenReply {
                    max_ms: 180_000,
                    hold_argument: None,
                },
            )]
            .into(),
        );
        let bytes = serde_json::to_string(&record).unwrap();
        assert!(bytes.contains(r#""reply":{"read":{"max_ms":180000}}"#));
        let decoded: WalRecord = serde_json::from_str(&bytes).unwrap();
        assert_eq!(decoded, record);
        assert_eq!(serde_json::to_string(&decoded).unwrap(), bytes);
    }

    #[test]
    fn frozen_reply_storage_mirror_decodes_published_reply_vectors() {
        let vectors: serde_json::Value = serde_json::from_str(include_str!(
            "../../../broca-fetch-plan/tests/fixtures/tool-provider-v1/reply-deadline.json"
        ))
        .unwrap();
        for group in ["deadlines", "valid", "invalid"] {
            let cases = vectors[group].as_array().unwrap();
            assert!(!cases.is_empty(), "missing published {group} cases");
            for case in cases {
                let name = case["name"].as_str().unwrap();
                // Semantic range/schema refusals belong to admission; storage mirrors only fields.
                let saved: Option<FrozenReply> = serde_json::from_value(case["reply"].clone())
                    .unwrap_or_else(|error| panic!("{name}: frozen reply decode: {error}"));
                assert_eq!(
                    serde_json::to_value(saved).unwrap(),
                    case["reply"],
                    "{name}"
                );
            }
        }
        let malformed = vectors["malformed"].as_array().unwrap();
        assert!(!malformed.is_empty(), "missing published malformed cases");
        for case in malformed {
            assert!(
                serde_json::from_value::<FrozenReply>(case["reply"].clone()).is_err(),
                "{}",
                case["name"]
            );
        }
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// The fixture's `RunStarted` with its manifest replaced by `manifest`, as record bytes.
    fn run_started_with_manifest(manifest: &str) -> String {
        let fixture = FIXTURE.trim_end().replace("{RUN_ID}", "run-1");
        let at = fixture.find(r#","manifest":"#).unwrap();
        format!(r#"{},"manifest":{manifest}}}"#, &fixture[..at])
    }

    fn answered(tools: usize, text: Option<&str>) -> FetchOutcome {
        let tool: Tool = serde_json::from_str(TOOL).unwrap();
        FetchOutcome::Answered {
            catalog_digest: String::new(),
            tools: Arc::new(vec![tool; tools]),
            text: text.map(str::to_owned),
            result_ops: None,
            reply: None,
            declarations_frozen: None,
            reference_slots: None,
        }
    }

    /// Strips `catalog_digest` so outcomes compare on their shape and contents alone.
    fn shape(outcome: &FetchOutcome) -> FetchOutcome {
        match outcome {
            FetchOutcome::Answered {
                tools,
                text,
                result_ops,
                reply,
                declarations_frozen,
                reference_slots,
                ..
            } => FetchOutcome::Answered {
                catalog_digest: String::new(),
                tools: tools.clone(),
                text: text.clone(),
                result_ops: result_ops.clone(),
                reply: reply.clone(),
                declarations_frozen: declarations_frozen.clone(),
                reference_slots: reference_slots.clone(),
            },
            other => other.clone(),
        }
    }

    /// A manifest holding every fetch shape (answered with text, answered with present-empty
    /// text, answered with no text requested, and absent) decodes through a whole `RunStarted`
    /// into those four distinct outcomes and re-encodes to exactly the bytes it was read from.
    /// The opaque plan keeps its key order, number spellings and escapes through the decode, and
    /// an absence reason, an open string, is kept verbatim.
    #[test]
    fn every_fetch_shape_round_trips_byte_for_byte_and_stays_distinct() {
        let plan_text = r#"{"z":1,"a":2.50,"big":100000000000000000000,"e":1e3,"s":"\u00e9"}"#;
        let digest = "0".repeat(64);
        let fetches = [
            format!(
                r#"{{"provider":"aft","catalog_digest":"{digest}","tools":[{TOOL}],"text":"Read first."}}"#
            ),
            format!(
                r#"{{"provider":"magic-context","catalog_digest":"{digest}","tools":[{TOOL},{TOOL}],"text":""}}"#
            ),
            format!(r#"{{"provider":"plexus","catalog_digest":"{digest}","tools":[{TOOL}]}}"#),
            r#"{"provider":"x-notes","absent":"unknown_module"}"#.to_owned(),
        ]
        .join(",");
        let manifest = format!(
            r#"{{"plan":{},"plan_identity":"{digest}","composition_digest":"{digest}","fetches":[{fetches}],"tools":[{TOOL}],"system_text":"Read first.","system_digest":"{digest}","join_version":1,"role_versions":{{}}}}"#,
            serde_json::to_string(plan_text).unwrap()
        );
        let json = run_started_with_manifest(&manifest);

        let record: WalRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), json);
        let WalRecord::RunStarted {
            manifest: Some(manifest),
            ..
        } = &record
        else {
            panic!("the manifest must come through the decode");
        };
        assert_eq!(manifest.plan.as_json_text(), plan_text);
        let outcomes: Vec<(&str, FetchOutcome)> = manifest
            .fetches
            .iter()
            .map(|f| (f.provider.as_str(), shape(&f.outcome)))
            .collect();
        assert_eq!(
            outcomes,
            [
                ("aft", answered(1, Some("Read first."))),
                ("magic-context", answered(2, Some(""))),
                ("plexus", answered(1, None)),
                (
                    "x-notes",
                    FetchOutcome::Absent {
                        reason: "unknown_module".into()
                    }
                ),
            ]
        );

        // An absence reason is an open string: one this build has never seen is kept and
        // written back exactly.
        for reason in ["timeout", "refused:quota_exceeded", "later-reason"] {
            let bytes = format!(r#"{{"provider":"x","absent":"{reason}"}}"#);
            let decoded: ProviderFetch = serde_json::from_str(&bytes).unwrap();
            assert_eq!(
                decoded.outcome,
                FetchOutcome::Absent {
                    reason: reason.into()
                }
            );
            assert_eq!(serde_json::to_string(&decoded).unwrap(), bytes);
        }
    }

    /// A fetch with neither `catalog_digest` nor `absent` comes from a later build: it decodes,
    /// so the record stays readable, but it is never written, because its real outcome is not
    /// known. A fetch that contradicts itself does not decode: both `catalog_digest` and
    /// `absent`, an answer without `tools`, or an absence carrying `tools` or `text`.
    #[test]
    fn a_fetch_outcome_this_build_does_not_know_decodes_and_is_never_written() {
        for unknown in [
            r#"{"provider":"x"}"#.to_owned(),
            format!(r#"{{"provider":"x","tools":[{TOOL}],"text":"t"}}"#),
        ] {
            let decoded: ProviderFetch = serde_json::from_str(&unknown).unwrap();
            assert_eq!(decoded.outcome, FetchOutcome::Unknown);
            assert!(serde_json::to_string(&decoded).is_err());
        }
        for contradiction in [
            r#"{"provider":"x","catalog_digest":"d","tools":[],"absent":"timeout"}"#,
            r#"{"provider":"x","catalog_digest":"d","absent":"timeout"}"#,
            r#"{"provider":"x","catalog_digest":"d","text":"t"}"#,
            r#"{"provider":"x","absent":"timeout","tools":[]}"#,
            r#"{"provider":"x","absent":"timeout","text":""}"#,
        ] {
            assert!(
                serde_json::from_str::<ProviderFetch>(contradiction).is_err(),
                "{contradiction}"
            );
        }
    }

    /// Opaque JSON is kept as its exact text; a writer may only wrap well-formed JSON.
    #[test]
    fn opaque_json_keeps_its_text_and_a_writer_must_give_it_json() {
        let text = r#"{"z":1,"a":1e3}"#;
        let opaque = OpaqueJson::from_json_text(text).unwrap();
        assert_eq!(opaque.as_json_text(), text);
        assert_eq!(
            serde_json::to_string(&opaque).unwrap(),
            r#""{\"z\":1,\"a\":1e3}""#
        );
        assert!(OpaqueJson::from_json_text("{not json").is_err());
        assert!(OpaqueJson::from_json_text("{} {}").is_err());
    }

    /// The fixture's stored digests are what their definitions say, recomputed here with an
    /// independent RFC 8785 implementation: `plan_identity` is the SHA-256 of the canonical bytes
    /// of the stored plan text, and `composition_digest` is the SHA-256 of the canonical bytes of
    /// the plan's `composition` and equals the digest the plan states. The plan identity is also
    /// pinned as a literal, copied from the plan's published test vector, so a fixture and a
    /// canonicalizer that drifted together would still fail.
    #[test]
    fn the_fixture_digests_are_the_jcs_sha256_of_its_plan() {
        let record: WalRecord = serde_json::from_str(FIXTURE.trim_end()).unwrap();
        let WalRecord::RunStarted {
            manifest: Some(manifest),
            ..
        } = record
        else {
            panic!("the fixture carries a manifest");
        };
        let plan: serde_json::Value = serde_json::from_str(manifest.plan.as_json_text()).unwrap();
        let identity = hex(&Sha256::digest(serde_jcs::to_vec(&plan).unwrap()));
        assert_eq!(manifest.plan_identity, identity);
        assert_eq!(
            identity,
            "b66c352ab9cbe2c12d1a1c2bceb553c6e7223a846692f945de6bd293be4e20dc"
        );
        let composition = hex(&Sha256::digest(
            serde_jcs::to_vec(&plan["composition"]).unwrap(),
        ));
        assert_eq!(manifest.composition_digest, composition);
        assert_eq!(plan["composition_digest"], composition.as_str());
    }

    #[test]
    fn frozen_catalog_permissions_round_trip_and_absence_keeps_old_bytes() {
        let bytes = include_str!("../../tests/golden/catalog_result_ops.json").trim_end();
        let fetch: ProviderFetch = serde_json::from_str(bytes).unwrap();
        let FetchOutcome::Answered {
            result_ops: Some(permissions),
            ..
        } = &fetch.outcome
        else {
            panic!("missing frozen permissions")
        };
        assert_eq!(permissions["read"], ["prepend", "append", "replace"]);
        assert_eq!(serde_json::to_string(&fetch).unwrap(), bytes);
        let old = r#"{"provider":"aft","catalog_digest":"digest","tools":[]}"#;
        let fetch: ProviderFetch = serde_json::from_str(old).unwrap();
        assert!(matches!(
            fetch.outcome,
            FetchOutcome::Answered {
                result_ops: None,
                ..
            }
        ));
        assert_eq!(serde_json::to_string(&fetch).unwrap(), old);
        // The full old manifest frame remains pinned independently of the new member.
        let fixture: WalRecord = serde_json::from_str(FIXTURE.trim_end()).unwrap();
        assert_eq!(serde_json::to_string(&fixture).unwrap(), FIXTURE.trim_end());
    }
    /// A frozen declaration writes its fields only when the provider declared them, and a
    /// provider that declared nothing keeps the exact bytes it wrote before this field existed.
    #[test]
    fn frozen_declarations_are_optional_and_absent_keeps_old_bytes() {
        let declared = ProviderFetch {
            provider: "aft".into(),
            outcome: FetchOutcome::Answered {
                catalog_digest: "digest".into(),
                tools: Arc::new(Vec::new()),
                text: None,
                result_ops: None,
                reply: None,
                declarations_frozen: Some(FrozenProviderDeclarations {
                    result_formats: Some(BTreeMap::from([(
                        "ask".to_owned(),
                        FrozenResultFormat("content/v1".into()),
                    )])),
                    late_results: Some(true),
                    schema_pins: None,
                }),
                reference_slots: None,
            },
        };
        let bytes = serde_json::to_string(&declared).unwrap();
        assert_eq!(
            bytes,
            r#"{"provider":"aft","catalog_digest":"digest","tools":[],"declarations_frozen":{"result_formats":{"ask":"content/v1"},"late_results":true}}"#
        );
        let read: ProviderFetch = serde_json::from_str(&bytes).unwrap();
        assert_eq!(read, declared);

        // A provider that declared nothing writes none of it.
        let old = r#"{"provider":"aft","catalog_digest":"digest","tools":[]}"#;
        let fetch: ProviderFetch = serde_json::from_str(old).unwrap();
        assert_eq!(serde_json::to_string(&fetch).unwrap(), old);

        // An absent fetch cannot carry declarations at all.
        assert!(serde_json::from_str::<ProviderFetch>(
            r#"{"provider":"aft","absent":"timeout","declarations_frozen":{"late_results":true}}"#
        )
        .is_err());
    }

    /// A `RunStarted` whose manifest froze a provider declaration is must-understand: a reader
    /// that skipped the fields would read replies by a catalog that may have changed. A
    /// manifest with no declarations keeps the frame it wrote before the feature existed.
    #[test]
    fn a_frozen_declaration_gates_the_frame_that_carries_it() {
        use crate::framing::{decode_next, encode, DecodeStep, PROVIDER_DECLARATIONS_FEATURE};
        let fixture: WalRecord = serde_json::from_str(FIXTURE.trim_end()).unwrap();
        let mut declared = fixture.clone();
        let WalRecord::RunStarted {
            manifest: Some(manifest),
            ..
        } = &mut declared
        else {
            panic!("the fixture carries a manifest")
        };
        for fetch in &mut manifest.fetches {
            if let FetchOutcome::Answered {
                declarations_frozen,
                ..
            } = &mut fetch.outcome
            {
                *declarations_frozen = Some(FrozenProviderDeclarations {
                    result_formats: None,
                    late_results: Some(true),
                    schema_pins: None,
                });
            }
        }
        assert!(declared
            .required_features()
            .contains(&PROVIDER_DECLARATIONS_FEATURE));
        let bytes = encode(1, 1, &declared).unwrap();
        match decode_next(&bytes) {
            DecodeStep::Record { framed, .. } => assert_eq!(framed.record, declared),
            _ => panic!("this build writes and reads its own declaration"),
        }
        // The same manifest without a declaration requires nothing new.
        assert!(!fixture
            .required_features()
            .contains(&PROVIDER_DECLARATIONS_FEATURE));
    }

    #[test]
    fn frozen_tool_schema_pins_require_a_reader_feature_and_round_trip() {
        use crate::framing::{decode_next, encode, DecodeStep, TOOL_SCHEMA_PINS_FEATURE};
        let fixture: WalRecord = serde_json::from_str(FIXTURE.trim_end()).unwrap();
        assert!(!fixture
            .required_features()
            .contains(&TOOL_SCHEMA_PINS_FEATURE));
        let mut pinned = fixture;
        let WalRecord::RunStarted {
            manifest: Some(manifest),
            ..
        } = &mut pinned
        else {
            panic!("fixture manifest")
        };
        for fetch in &mut manifest.fetches {
            if let FetchOutcome::Answered {
                declarations_frozen,
                ..
            } = &mut fetch.outcome
            {
                *declarations_frozen = Some(FrozenProviderDeclarations {
                    schema_pins: Some(
                        [(
                            "work".into(),
                            super::super::DispatchSchemaPin {
                                schema_digest: "0".repeat(64),
                                semantics: 7,
                            },
                        )]
                        .into(),
                    ),
                    ..Default::default()
                });
            }
        }
        assert!(pinned
            .required_features()
            .contains(&TOOL_SCHEMA_PINS_FEATURE));
        match decode_next(&encode(1, 1, &pinned).unwrap()) {
            DecodeStep::Record { framed, .. } => assert_eq!(framed.record, pinned),
            _ => panic!("the pin-writing build must read its own feature"),
        }
    }
}
