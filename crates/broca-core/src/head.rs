//! Small, replayable head facts, committed with the projection's exact WAL watermark.

use broca_protocol::RunId;
use broca_wal::WalRecord;
use cortexkit_role_llm_runner::{read::HeadModel, usage::SessionUsage};
use serde::{Deserialize, Serialize};

/// A log uses mode-3 semantics exactly when its first RunStarted has a plan version.
/// Later runs cannot change that choice. HeadProjection caches this predicate, with None
/// meaning unknown rather than legacy.
pub fn is_mode3_log<'a>(records: impl IntoIterator<Item = &'a WalRecord>) -> bool {
    records
        .into_iter()
        .find_map(|record| match record {
            WalRecord::RunStarted {
                compaction_plan_version,
                ..
            } => Some(compaction_plan_version.is_some()),
            _ => None,
        })
        .unwrap_or(false)
}

/// Learn an old cache's missing mode from bounded first-run records, without replaying its
/// transcript or recomputing any materialized legacy message times.
pub fn mode3_log_from_wal(
    wal: &dyn crate::WalStore,
    session: &broca_protocol::BindIdentity,
) -> Result<bool, broca_wal::WalError> {
    let mut after = 0_u64;
    let mut anchor = None;
    loop {
        let through = after.saturating_add(64);
        let range = wal.read_records_range(session, after, Some(through), anchor.as_ref())?;
        let records = match range.records() {
            broca_wal::WalRangeRecords::Complete(records)
            | broca_wal::WalRangeRecords::Unknown(records) => records,
            broca_wal::WalRangeRecords::Absent => return Ok(false),
            broca_wal::WalRangeRecords::Partial(_) => {
                return Err(broca_wal::WalError::Io(std::io::Error::other(
                    "cannot establish the log's first run from incomplete history",
                )))
            }
        };
        if records
            .iter()
            .any(|record| matches!(record.record, WalRecord::RunStarted { .. }))
        {
            return Ok(is_mode3_log(records.iter().map(|record| &record.record)));
        }
        // No record past `after` means the log ends here without a run (a writer that opened
        // and wrote only its lineage frame); keep reading and the loop would never end.
        if range.complete_byte_end.is_some() || records.is_empty() || through == u64::MAX {
            return Ok(false);
        }
        if records.last().is_some_and(|record| record.seq <= after) {
            return Err(broca_wal::WalError::Io(std::io::Error::other(
                "history reader did not advance while locating its first run",
            )));
        }
        after = through;
        anchor = range.next_anchor;
    }
}

/// A content-free retained summary with a watermark separate from transcript projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetainedHeadCheckpoint {
    pub lineage_id: String,
    pub at_seq: u64,
    pub facts: HeadProjection,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HeadProjection {
    /// Set once from the first RunStarted. None is unknown, never evidence of a legacy log;
    /// callers must obtain the shared is_mode3_log predicate's answer before choosing semantics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode3: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode3_times: Option<crate::message_times::MessageTimeSeed>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creation_times: Option<crate::creation_times::CreationTimeSeed>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_rich_ui: Option<broca_protocol::RichUiStamp>,
    pub model: Option<HeadModel>,
    pub usage: Option<SessionUsage>,
    run_id: Option<RunId>,
    ownership: crate::SessionOwnershipV1,
    senders: Vec<cortexkit_role_llm_runner::delivery_order::PlanSender>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    queued_times: std::collections::BTreeMap<String, u64>,
    /// Mode-3 placement and provenance continuation; native message bytes stay in rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_metadata: Option<crate::compaction_v2::metadata::Fold>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_newest: Option<crate::compaction_v2::protocol::read::Cursor>,
}

impl Default for HeadProjection {
    fn default() -> Self {
        Self {
            mode3: None,
            mode3_times: None,
            creation_times: None,
            last_run_rich_ui: None,
            model: None,
            usage: None,
            run_id: None,
            ownership: crate::SessionOwnershipV1::default().for_owner_access(),
            senders: vec![],
            queued_times: Default::default(),
            compaction_metadata: None,
            compaction_newest: None,
        }
    }
}

impl HeadProjection {
    /// Cached heads written before sender-policy folding ignored accepted refreshes.
    pub fn sender_admission_current(&self, session_change_recorded: bool) -> bool {
        !session_change_recorded
            || self.ownership.sender_admission_complete() && self.ownership.session_change_recorded
    }

    fn set_senders(&mut self, manifest: &broca_wal::PlanManifest) {
        self.senders = crate::admission::manifest_senders(manifest);
        for sender in &mut self.senders {
            sender.templates.clear();
        }
    }

    pub(crate) fn restore_sender_admission(&mut self, ownership: &crate::SessionOwnershipV1) {
        self.senders = ownership.effective_senders();
        for sender in &mut self.senders {
            sender.templates.clear();
        }
        self.restore_sender_marker(ownership);
    }

    pub(crate) fn restore_sender_marker(&mut self, ownership: &crate::SessionOwnershipV1) {
        self.ownership.session_change_recorded = ownership.session_change_recorded;
        self.ownership.sender_admission =
            ownership
                .sender_admission
                .as_ref()
                .map(|fold| crate::admission::SenderAdmissionV1 {
                    version: fold.version,
                    accepted_senders: fold.accepted_senders.as_ref().map(|_| Vec::new()),
                });
    }

    /// Restore display metadata from its bounded WAL sources, not from stored text.
    pub fn from_read_fold(
        fold: &HeadReadFold,
        first: Option<&WalRecord>,
        current: Option<&WalRecord>,
        ownership: crate::SessionOwnershipV1,
    ) -> Option<Self> {
        if fold.version != 1 {
            return None;
        }
        let mut head = Self::default();
        if let Some(first) = first {
            head.observe(first);
        }
        if let Some(current) = current {
            head.observe(current);
        }
        if head.run_id != fold.run_id || head.model != fold.model {
            return None;
        }
        head.ownership = ownership.for_owner_access();
        head.model = fold.model.clone();
        head.run_id = fold.run_id.clone();
        head.usage = fold.usage.clone();
        head.mode3 = head.mode3.or(fold.mode3);
        Some(head)
    }

    pub fn queued_times(&self) -> &std::collections::BTreeMap<String, u64> {
        &self.queued_times
    }

    pub fn observe(&mut self, record: &WalRecord) {
        match record {
            WalRecord::RunStarted {
                run_id,
                config,
                principal,
                scope,
                manifest,
                submission_id,
                ..
            } => {
                if self.run_id.is_none() {
                    self.mode3 = Some(crate::is_mode3_log(std::iter::once(record)));
                }
                if let Some(id) = submission_id {
                    self.queued_times.remove(&id.0);
                }
                if !self
                    .ownership
                    .sender_admission
                    .as_ref()
                    .is_some_and(|fold| fold.accepted_senders.is_some())
                {
                    if let Some(manifest) = manifest {
                        self.set_senders(manifest);
                    }
                }
                self.ownership
                    .fold_run_started(principal.as_ref(), scope.as_ref());
                self.run_id = Some(run_id.clone());
                self.last_run_rich_ui = manifest
                    .as_ref()
                    .and_then(|manifest| manifest.rich_ui.clone());
                let mut model =
                    HeadModel::new(&config.model.provider_module_id, &config.model.model_id);
                model.variant = config.variant.clone().filter(|value| !value.is_empty());
                self.model = Some(model);
            }
            // A refused call was billed, so it counts as a call of the model that refused; a
            // step another model answered counts under that model.
            WalRecord::ModelStepFinished { .. } | WalRecord::ModelStepRefused { .. } => {
                if let (Some(run_id), Some(model)) = (&self.run_id, &self.model) {
                    let episode = broca_protocol::request::ModelHandle::new(
                        model.provider.as_str(),
                        model.model.as_str(),
                    );
                    for (provider, model, variant, usage) in crate::refusal_fallback::usage_by_model(
                        &episode,
                        model.variant.as_deref(),
                        record,
                    ) {
                        self.usage
                            .get_or_insert_with(|| SessionUsage::new(&run_id.0))
                            .record_step(
                                provider,
                                model,
                                variant,
                                crate::replay::reported_step_usage(&usage),
                            );
                    }
                }
            }
            WalRecord::Queued {
                submission_id,
                written_at_ms,
                ts_ms,
                ..
            } => {
                if let Some(at) = written_at_ms.or(*ts_ms).filter(|at| *at != 0) {
                    self.queued_times.insert(submission_id.0.clone(), at);
                }
            }
            WalRecord::Retracted { submission_id, .. }
            | WalRecord::SendEnded { submission_id, .. }
            | WalRecord::SendDelivered { submission_id, .. } => {
                self.queued_times.remove(&submission_id.0);
            }
            WalRecord::SessionRefreshAccepted { manifest, .. } => {
                self.set_senders(manifest);
                self.ownership.session_change_recorded = true;
                // The head keeps template-free grants above; only presence is needed here.
                self.ownership.sender_admission = Some(crate::admission::SenderAdmissionV1 {
                    version: 1,
                    accepted_senders: Some(Vec::new()),
                });
            }
            WalRecord::SessionRefreshPolicyChanged { .. }
            | WalRecord::SessionPrefixFlushed { .. } => {
                self.ownership.session_change_recorded = true;
                self.ownership.sender_admission.get_or_insert(
                    crate::admission::SenderAdmissionV1 {
                        version: 1,
                        accepted_senders: None,
                    },
                );
            }
            _ => {}
        }
        if self.mode3 == Some(true) {
            let seed = self.mode3_times.get_or_insert_with(Default::default);
            if matches!(record, WalRecord::RunStarted { .. }) {
                seed.last_record_ms = None;
            }
            seed.last_record_ms =
                crate::message_times::record_written_ms(record).or(seed.last_record_ms);
        }
    }

    pub fn sender(
        &self,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
        modes: &[cortexkit_role_llm_runner::delivery_order::Mode],
    ) -> Option<cortexkit_role_llm_runner::read::HeadSender> {
        use cortexkit_role_llm_runner::{delivery_order, read::HeadSender};
        if self.ownership.episodes == 0 {
            return None;
        }
        if crate::scope_admission::decide_owner_access(&self.ownership, principal, scope).is_ok() {
            return Some(HeadSender::for_owner(
                modes.iter().map(|mode| mode.as_str().into()).collect(),
            ));
        }
        let broca_protocol::BindPrincipal::Reserved { module_id } = principal? else {
            return None;
        };
        let owner = self
            .ownership
            .scope
            .as_ref()
            .map(|scope| &scope.owner)
            .or(self.ownership.first_principal.as_ref())?
            .as_owner_string();
        let granted = modes
            .iter()
            .filter(|mode| {
                delivery_order::admit(module_id, Some(&owner), &self.senders, &[**mode]).is_ok()
            })
            .map(|mode| mode.as_str().to_owned())
            .collect::<Vec<_>>();
        self.senders
            .iter()
            .find(|sender| &sender.module == module_id)?;
        let interrupt = delivery_order::admit_delivery(
            module_id,
            Some(&owner),
            &self.senders,
            cortexkit_role_llm_runner::send::Delivery::Interrupt,
        )
        .is_ok();
        Some(HeadSender::new(granted, interrupt))
    }
}

/// A display-only continuation: identifiers and numeric counters, never a billing source.
/// Display metadata and sender authorization are restored from bounded WAL manifest sources.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HeadReadFold {
    pub version: u32,
    model: Option<HeadModel>,
    run_id: Option<RunId>,
    usage: Option<SessionUsage>,
    /// Whether the session's first run started under mode 3. Fixed for the session's life, so
    /// a reader restored from the fold never has to read the log's first run to learn it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mode3: Option<bool>,
}

impl<'de> Deserialize<'de> for HeadReadFold {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            version: u32,
            model: Option<serde_json::Value>,
            run_id: Option<RunId>,
            usage: Option<serde_json::Value>,
            #[serde(default)]
            mode3: Option<bool>,
        }
        fn counters(value: &serde_json::Value, model: bool) -> bool {
            let Some(object) = value.as_object() else {
                return false;
            };
            object.iter().all(|(key, value)| match key.as_str() {
                "provider" | "model" | "variant" if model => value.is_string(),
                "since" if !model => value.is_string(),
                "by_model" if !model => value
                    .as_array()
                    .is_some_and(|rows| rows.iter().all(|row| counters(row, true))),
                "steps" | "unreported_steps" => value.is_u64(),
                "input_tokens"
                | "output_tokens"
                | "cached_input_tokens"
                | "cache_write_tokens"
                | "reasoning_tokens" => value.is_u64(),
                _ => false,
            })
        }
        let fields = Fields::deserialize(deserializer)?;
        if fields.model.as_ref().is_some_and(|value| {
            value.as_object().is_none_or(|object| {
                object.iter().any(|(key, value)| {
                    !matches!(key.as_str(), "provider" | "model" | "variant") || !value.is_string()
                })
            })
        }) || fields
            .usage
            .as_ref()
            .is_some_and(|value| !counters(value, false))
        {
            return Err(serde::de::Error::custom(
                "head read fold contains a non-identifier/non-counter field",
            ));
        }
        Ok(Self {
            version: fields.version,
            model: fields
                .model
                .map(serde_json::from_value)
                .transpose()
                .map_err(serde::de::Error::custom)?,
            run_id: fields.run_id,
            usage: fields
                .usage
                .map(serde_json::from_value)
                .transpose()
                .map_err(serde::de::Error::custom)?,
            mode3: fields.mode3,
        })
    }
}

impl Default for HeadReadFold {
    fn default() -> Self {
        Self {
            version: 1,
            model: None,
            run_id: None,
            usage: None,
            mode3: None,
        }
    }
}

impl HeadReadFold {
    pub fn observe(&mut self, record: &WalRecord) {
        let mut head = HeadProjection {
            model: self.model.clone(),
            run_id: self.run_id.clone(),
            usage: self.usage.take(),
            mode3: self.mode3,
            ..HeadProjection::default()
        };
        head.observe(record);
        self.model = head.model;
        self.run_id = head.run_id;
        self.usage = head.usage;
        self.mode3 = head.mode3;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn start(run: &str, variant: Option<&str>) -> WalRecord {
        let mut value: serde_json::Value = serde_json::from_str(
            include_str!("../../broca-wal/tests/fixtures/before_message_times.jsonl")
                .lines()
                .next()
                .unwrap(),
        )
        .unwrap();
        value["run_id"] = json!(run);
        if let Some(variant) = variant {
            value["config"]["variant"] = json!(variant);
        }
        serde_json::from_value(value).unwrap()
    }

    fn step(usage: serde_json::Value) -> WalRecord {
        serde_json::from_value(json!({"type":"model_step_finished", "step_id":1,
            "assistant_message":{"message_id":"m","content":[]},"usage":usage,"retries_used":0}))
        .unwrap()
    }

    #[test]
    fn history_mode_is_fixed_by_the_first_run_not_a_later_writer() {
        let legacy = start("legacy", None);
        let mut mode3 = start("mode3", None);
        if let WalRecord::RunStarted {
            compaction_plan_version,
            ..
        } = &mut mode3
        {
            *compaction_plan_version = Some(2);
        }
        assert!(!is_mode3_log([&legacy, &mode3]));
        assert!(is_mode3_log([&mode3, &legacy]));
        let mut head = HeadProjection::default();
        assert_eq!(head.mode3, None);
        head.observe(&legacy);
        head.observe(&mode3);
        assert_eq!(head.mode3, Some(false));
        let mut head = HeadProjection::default();
        head.observe(&mode3);
        head.observe(&legacy);
        assert_eq!(head.mode3, Some(true));
    }

    #[test]
    fn head_read_fold_matches_replay_across_model_sender_and_rich_ui_changes() {
        let mut first = start("first", Some("deep"));
        if let WalRecord::RunStarted { principal, .. } = &mut first {
            *principal = Some(broca_protocol::BindPrincipal::Reserved {
                module_id: "sender-one".into(),
            });
        }
        let mut second = serde_json::to_value(start("second", None)).unwrap();
        second["config"]["model"]["model_id"] = json!("other-model");
        second["principal"] = serde_json::to_value(broca_protocol::BindPrincipal::Reserved {
            module_id: "sender-two".into(),
        })
        .unwrap();
        second["manifest"] = json!({
            "rich_ui":{"dialect":"2.0","catalog_digest":"d".repeat(64)},
            "plan":"{}", "plan_identity":"id", "composition_digest":"digest",
            "fetches":[], "tools":[], "system_text":"must-not-be-stored", "system_digest":"system",
            "join_version":1, "role_versions":{}
        });
        let second: WalRecord = serde_json::from_value(second).unwrap();
        let records = [
            first.clone(),
            step(json!({"input_tokens":7,"output_tokens":3})),
            second.clone(),
            step(json!({"input_tokens":11,"output_tokens":5,"reasoning_tokens":2})),
        ];
        let mut fold = HeadReadFold::default();
        let mut expected = HeadProjection::default();
        let mut current = &first;
        for record in &records {
            if matches!(record, WalRecord::RunStarted { .. }) {
                current = record;
            }
            fold.observe(record);
            expected.observe(record);
            let encoded = serde_json::to_string(&fold).unwrap();
            assert!(!encoded.contains("must-not-be-stored"));
            assert!(!encoded.contains("rich_ui"));
            assert!(!encoded.contains("sender-one"));
            let restored: HeadReadFold = serde_json::from_str(&encoded).unwrap();
            let actual = HeadProjection::from_read_fold(
                &restored,
                Some(&first),
                Some(current),
                expected.ownership.clone(),
            )
            .unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn head_read_fold_rejects_payload_fields_in_every_nested_object() {
        let mut fold = HeadReadFold::default();
        fold.observe(&start("run", None));
        fold.observe(&step(json!({"input_tokens":1,"output_tokens":2})));
        let clean = serde_json::to_value(&fold).unwrap();
        for parent in ["", "/model", "/usage", "/usage/by_model/0"] {
            for field in [
                "title",
                "prompt",
                "payload",
                "content",
                "price",
                "credentials",
            ] {
                let mut value = clean.clone();
                value
                    .pointer_mut(parent)
                    .unwrap()
                    .as_object_mut()
                    .unwrap()
                    .insert(field.into(), json!("sentinel"));
                assert!(
                    serde_json::from_value::<HeadReadFold>(value).is_err(),
                    "{parent}/{field}"
                );
            }
        }
        let mut old = fold;
        old.version = 0;
        assert!(HeadProjection::from_read_fold(&old, None, None, Default::default()).is_none());
    }

    /// A refused call counts under the model that refused it, and a step routed to the
    /// refusal-fallback model, or served by a server-side fallback, under the model that
    /// answered it, without the caller's variant.
    #[test]
    fn session_spend_counts_refused_and_fallback_calls_under_the_model_that_answered() {
        let mut head = HeadProjection::default();
        head.observe(&start("run", Some("deep")));
        head.observe(&step(json!({"input_tokens":2,"output_tokens":1})));
        head.observe(
            &serde_json::from_value(json!({"type":"model_step_refused","step_id":2,
                "usage":{"input_tokens":4,"output_tokens":1},"discarded_blocks":0,
                "schedule":{"source":{"provider_module_id":"p","model_id":"m"},
                "fallback":{"provider_module_id":"p","model_id":"m-fallback"},
                "phase":{"kind":"fallback","remaining":10},"re_refusals":0}}))
            .unwrap(),
        );
        let mut routed =
            serde_json::to_value(step(json!({"input_tokens":8,"output_tokens":2}))).unwrap();
        routed["route"] =
            json!({"model":{"provider_module_id":"p","model_id":"m-fallback"},"reason":"refusal"});
        head.observe(&serde_json::from_value(routed).unwrap());
        let mut served =
            serde_json::to_value(step(json!({"input_tokens":16,"output_tokens":4}))).unwrap();
        served["served_model"] = json!("m-served");
        head.observe(&serde_json::from_value(served).unwrap());
        let usage = serde_json::to_value(head.usage.unwrap()).unwrap();
        assert_eq!(usage["steps"], 4);
        assert_eq!(usage["input_tokens"], 30);
        assert_eq!(
            usage["by_model"],
            json!([
                {"provider":"p","model":"m","variant":"deep","steps":2,"unreported_steps":0,"input_tokens":6,"output_tokens":2},
                {"provider":"p","model":"m-fallback","steps":1,"unreported_steps":0,"input_tokens":8,"output_tokens":2},
                {"provider":"p","model":"m-served","steps":1,"unreported_steps":0,"input_tokens":16,"output_tokens":4}
            ])
        );
    }

    #[test]
    fn session_spend_keeps_model_keys_partial_counts_and_first_counted_run() {
        let mut head = HeadProjection::default();
        head.observe(&start("empty-run", None));
        head.observe(&start("first-counted", Some("deep")));
        head.observe(&step(json!({"input_tokens":7,"output_tokens":3})));
        let persisted = serde_json::to_string(&head).unwrap();
        let mut head: HeadProjection = serde_json::from_str(&persisted).unwrap();
        head.observe(&start("second-counted", None));
        head.observe(&step(json!({})));
        head.observe(&step(
            json!({"input_tokens":0,"output_tokens":5,"reasoning_tokens":2}),
        ));
        assert_eq!(
            serde_json::to_value(&head.model).unwrap(),
            json!({"provider":"p","model":"m"})
        );
        let usage = head.usage.unwrap();
        assert!(usage.consistent());
        assert_eq!(
            serde_json::to_value(usage).unwrap(),
            json!({
                "since":"first-counted", "steps":3,"unreported_steps":1,"input_tokens":7,"output_tokens":8,"reasoning_tokens":2,
                "by_model":[
                    {"provider":"p","model":"m","variant":"deep","steps":1,"unreported_steps":0,"input_tokens":7,"output_tokens":3},
                    {"provider":"p","model":"m","steps":2,"unreported_steps":1,"input_tokens":0,"output_tokens":5,"reasoning_tokens":2}
                ]
            })
        );
    }
    #[test]
    fn rich_ui_stamp_tracks_the_last_start_and_survives_projection_serialization() {
        let stamp = json!({"dialect":"1.0", "catalog_digest":"a".repeat(64)});
        let mut value = serde_json::to_value(start("stamped", None)).unwrap();
        value["manifest"] = json!({
            "rich_ui":stamp, "plan":"{}", "plan_identity":"plan", "composition_digest":"composition",
            "fetches":[], "tools":[], "system_text":"teaching", "system_digest":"system",
            "join_version":1, "role_versions":{}
        });
        let mut head = HeadProjection::default();
        head.observe(&serde_json::from_value(value).unwrap());
        let stored = serde_json::to_vec(&head).unwrap();
        let mut restored: HeadProjection = serde_json::from_slice(&stored).unwrap();
        assert_eq!(
            serde_json::to_value(&restored.last_run_rich_ui).unwrap(),
            stamp
        );
        restored.observe(&start("unstamped", None));
        assert!(restored.last_run_rich_ui.is_none());
        assert!(serde_json::to_value(restored)
            .unwrap()
            .get("last_run_rich_ui")
            .is_none());
    }
}
