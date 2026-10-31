use super::*;
use broca_protocol::{request::SubmissionRequest, RunId};
use cortexkit_role_llm_runner::read::{HeadInput, HeadModel};
use std::collections::VecDeque;

#[derive(Clone)]
pub(super) struct Frozen {
    lineage: Option<broca_wal::WalLineageId>,
    at_seq: u64,
    run_id: RunId,
    media: Option<serde_json::Value>,
    interrupt_reason: Option<String>,
    interrupt_known: bool,
}

#[derive(Default)]
pub(super) struct Cache {
    entries: HashMap<(LineageKey, broca_wal::WalLineageId, u64), Frozen>,
    order: VecDeque<(LineageKey, broca_wal::WalLineageId, u64)>,
}

impl Cache {
    fn get(&self, key: &LineageKey, request: &Request<'_>) -> Option<&Frozen> {
        self.entries
            .get(&(key.clone(), request.lineage, request.at_seq))
            .filter(|entry| Some(&entry.run_id) == request.run_id)
    }
    fn insert(&mut self, key: LineageKey, value: Frozen) {
        let Some(lineage) = value.lineage else {
            return;
        };
        let key = (key, lineage, value.at_seq);
        if self.entries.insert(key.clone(), value).is_some() {
            return;
        }
        self.order.push_back(key);
        while self.order.len() > 4096 {
            if let Some(old) = self.order.pop_front() {
                self.entries.remove(&old);
            }
        }
    }
}

/// Resident frozen media metadata: two snapshots cover head readers racing an admission
/// without retaining configurations for the whole session history.
#[derive(Default)]
pub(crate) struct Live {
    entries: Mutex<VecDeque<Frozen>>,
}

impl Live {
    fn push(entries: &mut VecDeque<Frozen>, value: Frozen) {
        if entries
            .back()
            .is_some_and(|last| last.at_seq >= value.at_seq)
        {
            return;
        }
        entries.push_back(value);
        while entries.len() > 2 {
            entries.pop_front();
        }
    }
    pub(crate) fn seed(
        &self,
        lineage: Option<broca_wal::WalLineageId>,
        seq: u64,
        episode: &broca_core::LastEpisodeV1,
    ) {
        let finished = episode.disposition == broca_core::EpisodeDisposition::Finished;
        Self::push(
            &mut self.entries.lock().unwrap(),
            Frozen {
                lineage,
                at_seq: seq,
                run_id: episode.run_id.clone(),
                media: episode.config.frozen_render_config.get("media").cloned(),
                interrupt_reason: None,
                interrupt_known: !finished,
            },
        );
    }
    pub(crate) fn observe(
        &self,
        seq: u64,
        lineage: Option<broca_wal::WalLineageId>,
        record: &broca_wal::WalRecord,
    ) {
        use broca_wal::WalRecord;
        match record {
            WalRecord::RunStarted { run_id, config, .. } => Self::push(
                &mut self.entries.lock().unwrap(),
                Frozen {
                    lineage,
                    at_seq: seq,
                    run_id: run_id.clone(),
                    media: config.frozen_render_config.get("media").cloned(),
                    interrupt_reason: None,
                    interrupt_known: true,
                },
            ),
            WalRecord::RunConfigRefrozen {
                frozen_render_config,
                ..
            } => {
                let mut entries = self.entries.lock().unwrap();
                if let Some(mut current) = entries
                    .back()
                    .filter(|last| last.lineage == lineage)
                    .cloned()
                {
                    current.at_seq = seq;
                    current.media = frozen_render_config.get("media").cloned();
                    Self::push(&mut entries, current);
                }
            }
            WalRecord::RunFinished {
                reason,
                interrupt_reason,
                ..
            } => {
                let mut entries = self.entries.lock().unwrap();
                if let Some(mut current) = entries
                    .back()
                    .filter(|last| last.lineage == lineage)
                    .cloned()
                {
                    current.at_seq = seq;
                    current.interrupt_known = true;
                    current.interrupt_reason = (*reason
                        == broca_protocol::RunFinishReason::Interrupted)
                        .then(|| interrupt_reason.clone())
                        .flatten();
                    Self::push(&mut entries, current);
                }
            }
            _ => {}
        }
    }
    fn get(&self, request: &Request<'_>) -> Option<Frozen> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|entry| {
                entry.lineage == Some(request.lineage)
                    && entry.at_seq <= request.at_seq
                    && Some(&entry.run_id) == request.run_id
            })
            .cloned()
    }
}

pub(crate) struct Request<'a> {
    pub lineage: broca_wal::WalLineageId,
    pub at_seq: u64,
    pub run_id: Option<&'a RunId>,
    pub state: &'a str,
    pub model: Option<&'a HeadModel>,
}

pub(crate) struct Facts {
    pub input: HeadInput,
    pub interrupt_reason: Option<String>,
}

fn input(profile: &broca_provider::media_profile::MediaProfile) -> HeadInput {
    use broca_protocol::MediaKind;
    let modalities: Vec<String> = [
        MediaKind::Image,
        MediaKind::Document,
        MediaKind::Audio,
        MediaKind::Video,
    ]
    .into_iter()
    .filter(|kind| {
        profile.accepts_modality(*kind)
            && profile.kind(*kind).user_message
                != broca_provider::media_profile::MediaCarrier::Marker
    })
    .map(|kind| broca_provider::person_media::modality(kind).into())
    .collect();
    if modalities.is_empty() {
        return broca_wire::head_input();
    }
    HeadInput::new("yes")
        .with_modalities(modalities)
        .with_mime_types(broca_provider::person_media::input_mime_types(profile))
}

fn quote_request(model: &HeadModel) -> SubmissionRequest {
    SubmissionRequest {
        staged_uploads: None,
        content: Vec::new(),
        directives: None,
        on_restart: None,
        keep_warm: None,
        scope: Default::default(),
        project_id: None,
        principal: None,
        model: broca_protocol::ModelHandle::new(&model.provider, &model.model),
        input: Vec::new(),
        tools: None,
        policy_allowed: Default::default(),
        tool_choice: Default::default(),
        generation: Default::default(),
        cache: None,
        budget: None,
        auth: None,
        context_limit: None,
        work_class: None,
        variant: model.variant.clone(),
        service_tier: None,
        stop_when: Vec::new(),
        context_management: None,
        inherit_frozen: Default::default(),
        structured_output: None,
        client_message_id: None,
    }
}

impl LineageRegistry {
    async fn quoted_media_head(&self, model: Option<&HeadModel>) -> HeadInput {
        let Some(model) = model else {
            return HeadInput::new("unknown");
        };
        // The executor quotes its in-memory catalog and adapter tables, never a provider call.
        match self.media_profile(&quote_request(model)).await {
            Ok(Some(profile)) => input(&profile),
            _ => HeadInput::new("unknown"),
        }
    }

    pub(crate) async fn media_facts_for_head(
        &self,
        key: &LineageKey,
        request: Request<'_>,
    ) -> Result<Facts, OpenError> {
        if request.run_id.is_none() {
            return Ok(Facts {
                input: HeadInput::new("unknown"),
                interrupt_reason: None,
            });
        }
        if matches!(
            request.state,
            "completed" | "cancelled" | "error" | "exhausted"
        ) {
            return Ok(Facts {
                input: self.quoted_media_head(request.model).await,
                interrupt_reason: None,
            });
        }
        let resident = {
            let slots = self.inner.slots.lock().unwrap();
            match slots.get(key) {
                Some(Entry::Open { handle, .. }) => {
                    Some(if handle.approvals.healthy.load(Ordering::Acquire) {
                        handle
                            .media_head
                            .as_ref()
                            .and_then(|head| head.get(&request))
                    } else {
                        None
                    })
                }
                _ => None,
            }
        };
        let frozen = match resident {
            Some(Some(frozen)) if request.state != "interrupted" || frozen.interrupt_known => {
                Some(frozen)
            }
            Some(None) => None,
            _ => Some(self.cached_media_head(key, &request).await?),
        };
        let Some(frozen) = frozen else {
            return Ok(Facts {
                input: HeadInput::new("unknown"),
                interrupt_reason: None,
            });
        };
        let input = match frozen.media {
            Some(value) => serde_json::from_value(value)
                .ok()
                .as_ref()
                .map(input)
                .unwrap_or_else(|| HeadInput::new("unknown")),
            None => broca_wire::head_input(),
        };
        Ok(Facts {
            input,
            interrupt_reason: frozen.interrupt_reason,
        })
    }

    async fn cached_media_head(
        &self,
        key: &LineageKey,
        request: &Request<'_>,
    ) -> Result<Frozen, OpenError> {
        if let Some(frozen) = self
            .inner
            .media_heads
            .lock()
            .unwrap()
            .get(key, request)
            .cloned()
        {
            return Ok(frozen);
        }
        let reader = self
            .inner
            .reader
            .as_ref()
            .ok_or_else(|| OpenError::Open("session.head media reader unavailable".into()))?;
        let range = self
            .read_durable_range_for(
                reader,
                key,
                ReadRangeRequest {
                    after_seq: 0,
                    anchor: None,
                    live_head: None,
                    through_seq: Some(request.at_seq),
                    purpose: ReadPurpose::Session,
                },
            )
            .await?;
        let records = crate::durable_read::DurableRead::classify(&range, 0, Some(request.at_seq))
            .records()
            .map_err(|reason| OpenError::Classified {
                reason,
                class: broca_protocol::ErrorClass::Transient,
            })?;
        if range.lineage_id != Some(request.lineage) {
            return Err(OpenError::Classified {
                reason: "session.head media lineage changed during read".into(),
                class: broca_protocol::ErrorClass::Transient,
            });
        }
        let checkpoint = broca_core::AdmissionCheckpointV1::from_frames(records)
            .map_err(|error| OpenError::Open(error.to_string()))?;
        let episode = checkpoint
            .last_episode
            .filter(|episode| Some(&episode.run_id) == request.run_id)
            .ok_or_else(|| OpenError::Classified {
                reason: "session.head media episode differs from the page".into(),
                class: broca_protocol::ErrorClass::Transient,
            })?;
        let frozen = Frozen {
            lineage: Some(request.lineage),
            at_seq: request.at_seq,
            interrupt_reason: crate::serve::recorded_interrupt_reason(records, &episode.run_id),
            run_id: episode.run_id,
            media: episode.config.frozen_render_config.get("media").cloned(),
            interrupt_known: true,
        };
        self.inner
            .media_heads
            .lock()
            .unwrap()
            .insert(key.clone(), frozen.clone());
        Ok(frozen)
    }
}
