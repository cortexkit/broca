//! Consumers share the deadline announced on the module connection.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::LazyLock;
use std::time::Duration;

use subc_protocol::error_codes::MODULE_RELOADING;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::connection::{ConnError, RejectedMessage, SubcConnection};

#[derive(Clone, Copy)]
struct State {
    deadline: Instant,
    active: bool,
}

static UPGRADE: LazyLock<watch::Sender<Option<State>>> = LazyLock::new(|| watch::Sender::new(None));
static NOTICE_POSSIBLE: AtomicBool = AtomicBool::new(false);

/// A module connection holding a resume token can announce an upgrade on another socket.
pub fn expect_upgrade_notices(possible: bool) {
    NOTICE_POSSIBLE.store(possible, Ordering::Release);
}

/// Record the deadline from the module stream, without changing route admission.
pub fn announce(deadline: Instant) {
    UPGRADE.send_replace(Some(State {
        deadline,
        active: true,
    }));
}

/// The replacement module connection has been installed.
pub fn resumed(deadline: Instant) {
    UPGRADE.send_modify(|state| {
        if let Some(state) = state.as_mut().filter(|state| state.deadline == deadline) {
            state.active = false;
        }
    });
}

/// The upgrade could not be completed in this process.
pub fn abandon() {
    expect_upgrade_notices(false);
    UPGRADE.send_replace(None);
}

pub(crate) fn active_deadline() -> Option<Instant> {
    (*UPGRADE.borrow())
        .filter(|state| state.active && Instant::now() < state.deadline)
        .map(|state| state.deadline)
}

/// Keep the upgrade deadline visible to tasks that observe a lost route just after resume.
pub(crate) fn recent_deadline() -> Option<Instant> {
    (*UPGRADE.borrow())
        .filter(|state| Instant::now() < state.deadline)
        .map(|state| state.deadline)
}

/// When a scope read or capability discovery loses its socket, the module may still be reading
/// the announcement on another socket. Wait for that evidence only on a process that can resume.
pub(crate) async fn possible_deadline(until: Option<Instant>) -> Option<Instant> {
    let mut updates = UPGRADE.subscribe();
    if let Some(state) =
        (*updates.borrow_and_update()).filter(|state| Instant::now() < state.deadline)
    {
        return Some(state.deadline);
    }
    if !NOTICE_POSSIBLE.load(Ordering::Acquire) {
        return None;
    }
    let ceiling = Instant::now() + crate::connection::RESPONSE_TIMEOUT;
    let ceiling = until.map_or(ceiling, |end| end.min(ceiling));
    tokio::time::timeout_at(ceiling, updates.changed())
        .await
        .ok()?
        .ok()?;
    let state = *updates.borrow();
    state.map(|state| state.deadline)
}

pub(crate) fn is_switch_refusal(error: &RejectedMessage) -> bool {
    is_refusal(error.code(), error.provider_detail())
}

/// This structured refusal proves the daemon did not deliver the request.
pub fn is_refusal(code: &str, detail: Option<&serde_json::Value>) -> bool {
    code == MODULE_RELOADING
        && detail
            .and_then(|detail| detail.get("reason"))
            .and_then(serde_json::Value::as_str)
            == Some("daemon_upgrade")
}

// A consumer and the module read different sockets. Let the reader of the module's notice
// publish its ceiling if the refusal arrives first. Never invent a longer upgrade window.
async fn refusal_deadline() -> Option<Instant> {
    let mut updates = UPGRADE.subscribe();
    let seen = *updates.borrow_and_update();
    if let Some(state) = seen.filter(|state| Instant::now() < state.deadline) {
        return Some(state.deadline);
    }
    tokio::time::timeout(crate::connection::RESPONSE_TIMEOUT, updates.changed())
        .await
        .ok()?
        .ok()?;
    let seen = *updates.borrow();
    seen.map(|state| state.deadline)
}

pub(crate) async fn wait_for_module(deadline: Instant) -> bool {
    let mut updates = UPGRADE.subscribe();
    loop {
        let state = *updates.borrow_and_update();
        if Instant::now() >= deadline {
            return false;
        }
        match state {
            Some(state) if !state.active && state.deadline == deadline => return true,
            Some(state) if state.active && state.deadline == deadline => {}
            _ => return false,
        }
        if tokio::time::timeout_at(deadline, updates.changed())
            .await
            .is_err()
        {
            return false;
        }
    }
}

/// Bound retries for one caller-owned operation. Only a typed pre-delivery refusal
/// can start another attempt; transport errors can never authorize a resend.
#[derive(Default)]
pub struct Retry {
    deadline: Option<Instant>,
}

impl Retry {
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub async fn after_frame(&mut self, body: &[u8], conn: &SubcConnection) -> bool {
        let error = crate::connection::decode_error_body(body);
        self.after_refusal(error.code(), error.provider_detail(), conn)
            .await
    }

    pub async fn after_error(&mut self, error: &ConnError, conn: &SubcConnection) -> bool {
        self.after_connection_error(error, Some(conn)).await
    }

    pub(crate) async fn after_connection_error(
        &mut self,
        error: &ConnError,
        conn: Option<&SubcConnection>,
    ) -> bool {
        let ConnError::Rejected(refusal) = error else {
            return false;
        };
        self.wait(is_switch_refusal(refusal), conn).await
    }

    pub(crate) async fn after_fetch(
        &mut self,
        error: &broca_fetch_plan::FetchError,
        conn: &SubcConnection,
    ) -> bool {
        use broca_fetch_plan::FetchError;
        match error {
            FetchError::Refused { code, detail, .. }
            | FetchError::ProviderRefused { code, detail, .. } => {
                self.after_refusal(code, detail.as_ref(), conn).await
            }
            _ => false,
        }
    }

    pub async fn after_refusal(
        &mut self,
        code: &str,
        detail: Option<&serde_json::Value>,
        conn: &SubcConnection,
    ) -> bool {
        self.wait(is_refusal(code, detail), Some(conn)).await
    }

    pub(crate) async fn wait(
        &mut self,
        proved_unsent: bool,
        conn: Option<&SubcConnection>,
    ) -> bool {
        if !proved_unsent {
            return false;
        }
        let deadline = match self.deadline {
            Some(deadline) => {
                // A refusal can race the module registration on another socket.
                tokio::time::sleep_until(
                    (Instant::now() + Duration::from_millis(100)).min(deadline),
                )
                .await;
                deadline
            }
            None => {
                let Some(deadline) = refusal_deadline().await else {
                    return false;
                };
                self.deadline = Some(deadline);
                deadline
            }
        };
        wait_for_module(deadline).await
            && match conn {
                Some(conn) => conn.wait_for_active_generation(deadline).await,
                None => true,
            }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_structured_daemon_upgrade_refusal_proves_non_delivery() {
        let detail = serde_json::json!({"reason": "daemon_upgrade"});
        assert!(is_refusal(MODULE_RELOADING, Some(&detail)));
        assert!(!is_refusal("module_timeout", Some(&detail)));
        assert!(!is_refusal(MODULE_RELOADING, None));
        assert!(!is_refusal(
            MODULE_RELOADING,
            Some(&serde_json::json!("daemon_upgrade"))
        ));
    }
}
