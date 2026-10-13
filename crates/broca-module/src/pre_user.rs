//! Input transforms run in the actor, before a steer can take the inbox lock or interrupt a run.

use super::*;
use broca_core::hooks::{HookPlan, HookRuntime, Subject, Trace};
use broca_protocol::content::{ContentBlock, Role};

pub(super) struct PreparedInput {
    pub input: Vec<broca_protocol::Message>,
    pub hooks: Option<Box<broca_wal::Hooks>>,
    pub refused: Option<String>,
}

impl PreparedInput {
    /// Queued.request remains the caller's payload; only the final input belongs in hooks.
    pub fn queued_hooks(&self) -> Option<Box<broca_wal::Hooks>> {
        self.hooks.clone().map(|mut hooks| {
            if hooks.original.take().is_some() {
                hooks.final_value = Some(serde_json::to_value(&self.input).expect("message JSON"));
            }
            hooks
        })
    }
}

impl<W: WalStore + 'static> LineageActor<W> {
    pub(super) async fn prepare_input(
        &mut self,
        input: Vec<broca_protocol::Message>,
        delivery: broca_protocol::Delivery,
        manifest: Option<Arc<broca_wal::PlanManifest>>,
    ) -> Result<PreparedInput, SendOutcome> {
        let mut prepared = PreparedInput {
            input,
            hooks: None,
            refused: None,
        };
        let Some(manifest) =
            manifest.filter(|manifest| broca_core::hooks::has_transforms(manifest))
        else {
            return Ok(prepared);
        };
        let plan = HookPlan::from_manifest(&manifest).map_err(SendOutcome::Error)?;
        let plane = self.executor.hook_plane().ok_or_else(|| {
            SendOutcome::Error("plan.step_transform_items requires an installed hook plane".into())
        })?;
        let writer = Arc::clone(
            self.writer
                .as_ref()
                .expect("writer live while preparing input"),
        );
        let bind = self.key.clone();
        let runtime = HookRuntime {
            plan: &plan,
            plane: &plane,
            bind: &bind,
            writer: &writer,
            // A stop or interrupt aimed at the active run cannot cancel a different send.
            cancel: None,
            stop: None,
            seal: None,
        };
        let original = serde_json::to_value(&prepared.input).expect("message JSON");
        let mut trace = Trace::default();
        let seal = self.registry.seal_signal();
        for message in prepared
            .input
            .iter_mut()
            .filter(|message| message.role == Role::User)
        {
            let blocks = message
                .content
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            // Commons decodes delivery open; the module supplies its selected wire spelling.
            // The sender's mark is intentionally absent.
            let subject = Subject::PreUser {
                blocks,
                mark: None,
                delivery: Some(delivery.as_str().to_owned()),
            };
            let output = tokio::select! {
                biased;
                _ = seal.cancelled() => return Err(SendOutcome::Transient("module is stopping".into())),
                _ = async {
                    match self.send_reply.as_mut() {
                        Some(reply) => reply.closed().await,
                        None => std::future::pending().await,
                    }
                } => return Err(SendOutcome::Transient("send cancelled".into())),
                output = runtime.chain(subject, trace.clone(), |_| true) => output,
            };
            trace = output.trace;
            broca_core::hooks::apply_message_ops(&mut message.content, &output.operations)
                .map_err(|error| {
                    SendOutcome::Error(format!("invalid input hook operation: {error:?}"))
                })?;
            if let Some((provider, _, _)) = output.refusal {
                prepared.refused = Some(provider);
                break;
            }
        }
        prepared.hooks = trace.metadata(
            original,
            serde_json::to_value(&prepared.input).expect("message JSON"),
            false,
        );
        Ok(prepared)
    }

    pub(super) async fn frozen_input_manifest(
        &mut self,
    ) -> Result<Option<Arc<broca_wal::PlanManifest>>, SendOutcome> {
        if let Some(first) = self.first_episode.get() {
            return Ok(first.manifest.clone());
        }
        let ownership = self.load_ownership().await.map_err(SendOutcome::Error)?;
        Ok(ownership.first_episode.and_then(|first| first.manifest))
    }
}
