//! Input transforms fed from the lineage actor's own state.

use super::*;
use pre_user::{input_transforms, transform_input, PreparedInput};

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
