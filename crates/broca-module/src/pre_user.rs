//! Input transforms run in the actor, before a steer can take the inbox lock or interrupt a run.
//!
//! The transforms themselves are free functions, so the actor for a session kept in the new
//! storage ([`crate::store_actor`]) runs a send's input through the same rules; the
//! `LineageActor` methods below feed them from today's actor's own state.

use super::*;
use broca_core::hooks::{HookPlan, HookRuntime, Subject, Trace};
use broca_protocol::content::{ContentBlock, Role};

pub(crate) struct PreparedInput {
    pub at_ms: Option<u64>,
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

/// A plan's `pre_user` transforms and the hook plane that calls them.
pub(crate) type InputTransforms = (HookPlan, Arc<dyn broca_core::HookPlane>);

/// The `pre_user` transforms a session's plan declares, with the hook plane that calls them.
/// `None` when the plan declares no step transforms, so the input goes on as sent and nothing
/// is recorded about it. A plan whose transforms can't be read, or that has no plane to call
/// them, is refused.
pub(crate) fn input_transforms(
    manifest: Option<&broca_wal::PlanManifest>,
    plane: Option<Arc<dyn broca_core::HookPlane>>,
) -> Result<Option<InputTransforms>, SendOutcome> {
    let Some(manifest) = manifest.filter(|manifest| broca_core::hooks::has_transforms(manifest))
    else {
        return Ok(None);
    };
    let plan = HookPlan::from_manifest(manifest).map_err(SendOutcome::Error)?;
    let plane = plane.ok_or_else(|| {
        SendOutcome::Error("plan.step_transform_items requires an installed hook plane".into())
    })?;
    Ok(Some((plan, plane)))
}

/// Asks each provider `plan` subscribes to `pre_user` to transform `prepared`'s user messages,
/// one message at a time, for a send delivered as `delivery` under `scope`. Each answer's edits
/// are applied to the message in place. The first provider that refuses stops the chain and is
/// named in `refused`. `hooks` records the input before and after when any provider acted.
/// `abandon` stops the work early with its answer, for a module that is stopping or a sender
/// that stopped waiting; nothing a provider answered after that is kept.
pub(crate) async fn transform_input(
    plan: &HookPlan,
    plane: &Arc<dyn broca_core::HookPlane>,
    bind: &broca_protocol::BindIdentity,
    scope: Option<&broca_protocol::SessionScope>,
    delivery: broca_protocol::Delivery,
    mut prepared: PreparedInput,
    abandon: impl std::future::Future<Output = SendOutcome>,
) -> Result<PreparedInput, SendOutcome> {
    let runtime = HookRuntime {
        scope,
        plan,
        plane,
        bind,
        // A stop or interrupt aimed at the active run cannot cancel a different send.
        cancel: None,
        stop: None,
        seal: None,
    };
    let at_ms = prepared.at_ms;
    let original = serde_json::to_value(&prepared.input).expect("message JSON");
    let mut trace = Trace::default();
    tokio::pin!(abandon);
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
            at_ms,
        };
        let output = tokio::select! {
            biased;
            outcome = &mut abandon => return Err(outcome),
            output = runtime.chain(subject, trace.clone(), |_| true) => output,
        };
        trace = output.trace;
        broca_core::hooks::apply_message_ops(&mut message.content, &output.operations).map_err(
            |error| SendOutcome::Error(format!("invalid input hook operation: {error:?}")),
        )?;
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

/// The error a fresh run ends with when a `pre_user` provider refused its input: the run is
/// admitted, then ends with this error before any model call.
pub(crate) fn refusal_error(provider: &str) -> broca_protocol::ProviderError {
    serde_json::from_value(serde_json::json!({
        "class": "permanent", "provider_code": cortexkit_role_llm_runner::errors::PRE_USER_UNAVAILABLE,
        "message": format!("pre_user unavailable: {provider}"),
    }))
    .expect("input refusal error JSON")
}

/// The hooks a run started from a queued send records. The send's transforms ran when it was
/// queued, and their final value is the input the run sends, so they never run again. `input`
/// is the send's input as its caller sent it, which the queue kept, and becomes `original`.
pub(crate) fn drained_hooks(
    queued: Option<Box<broca_wal::Hooks>>,
    input: &[broca_protocol::Message],
) -> Option<Box<broca_wal::Hooks>> {
    queued.map(|mut hooks| {
        hooks.original = Some(serde_json::to_value(input).expect("message JSON"));
        hooks
    })
}

impl<W: WalStore + 'static> LineageActor<W> {
    pub(super) async fn prepare_input(
        &mut self,
        input: Vec<broca_protocol::Message>,
        delivery: broca_protocol::Delivery,
        manifest: Option<Arc<broca_wal::PlanManifest>>,
        scope: Option<&broca_protocol::SessionScope>,
        at_ms: Option<u64>,
    ) -> Result<PreparedInput, SendOutcome> {
        let prepared = PreparedInput {
            at_ms,
            input,
            hooks: None,
            refused: None,
        };
        let Some((plan, plane)) =
            input_transforms(manifest.as_deref(), self.executor.hook_plane())?
        else {
            return Ok(prepared);
        };
        // A first send has not recorded its scope yet; its admission supplies that scope.
        // Steering messages and queued inputs use the scope the session already froze.
        let frozen_scope = match scope {
            Some(scope) => Some(scope.clone()),
            None => {
                self.load_ownership()
                    .await
                    .map_err(SendOutcome::Transient)?
                    .scope
            }
        };
        let seal = self.registry.seal_signal();
        let send_reply = &mut self.send_reply;
        let abandon = async move {
            tokio::select! {
                biased;
                _ = seal.cancelled() => SendOutcome::Transient("module is stopping".into()),
                _ = async {
                    match send_reply.as_mut() {
                        Some(reply) => reply.closed().await,
                        None => std::future::pending().await,
                    }
                } => SendOutcome::Transient("send cancelled".into()),
            }
        };
        transform_input(
            &plan,
            &plane,
            &self.key,
            frozen_scope.as_ref(),
            delivery,
            prepared,
            abandon,
        )
        .await
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
