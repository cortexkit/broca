//! Run-owned provider transport; a dropped or interrupted future never returns a lease.

use broca_core::provider::Provider;
use broca_protocol::{BindIdentity, PauseReason, RunFinishReason};

pub(crate) async fn seed<W: crate::writer::Writer, P: Provider>(
    writer: &W,
    provider: &P,
    account: Option<&str>,
    frozen: &serde_json::Value,
    now_ms: u64,
) -> Result<(), broca_session_store::session::SessionError> {
    if let Some(account) = account.filter(|account| {
        provider.accepted_send_evidence(account)
            == broca_core::provider::AcceptedSendEvidence::NotLoaded
    }) {
        let lifetime = broca_core::session_account_cache_expiry_ms(Some(0), frozen).unwrap_or(0);
        let evidence = writer
            .accepted_send(account, now_ms.saturating_sub(lifetime))
            .await?;
        provider.seed_accepted_send(account, evidence);
    }
    Ok(())
}

pub(crate) struct Guard<'a, P: Provider> {
    provider: &'a P,
    retain: bool,
}

impl<'a, P: Provider> Guard<'a, P> {
    pub(crate) fn bind(provider: &'a P, bind: &BindIdentity) -> Self {
        provider.bind_run_transport(bind);
        Self {
            provider,
            retain: false,
        }
    }

    pub(crate) fn finished(&mut self, finished: &crate::run::Finished) {
        self.retain = finished.paused == Some(PauseReason::ToolHeld)
            || (finished.reason == RunFinishReason::Completed && finished.paused.is_none());
    }
}

impl<P: Provider> Drop for Guard<'_, P> {
    fn drop(&mut self) {
        if self.retain {
            self.provider.complete_run_transport();
        } else {
            self.provider.close_run_transport();
        }
    }
}
