//! The run ids `session.send` and `session.import` assign: minted for a send with no
//! `send_id`, derived from the session and the caller's id otherwise.

use std::sync::atomic::Ordering;

use broca_protocol::BindIdentity as BrocaBindIdentity;

use super::launch_nonce;

/// Mint a globally-unique run_id for a fresh send. `run-<session>-<launch-nonce>-<seq>`:
/// unique across connections (process-global seq) and restarts (per-launch nonce), so a
/// new run NEVER collides with a finished run's id — which would otherwise let the
/// durable run index clobber a terminal row, or `run.cancel` target the wrong run. A
/// RESUME reuses its OWN original run_id (the coordinator reads it from the session's log);
/// only a FRESH send mints here, so re-indexing the same id on resume stays correct.
pub(super) fn mint_run_id(session: &str) -> broca_protocol::RunId {
    use std::sync::atomic::AtomicU64;
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::SeqCst);
    broca_protocol::RunId(format!("run-{session}-{}-{seq}", launch_nonce()))
}

/// The stable session-scope bytes used when deriving a deterministic run id from a client
/// `send_id`. The trailing unit separator keeps `(session, send_id)` pairs unambiguous when
/// the caller bytes are appended.
fn send_id_scope(session: &BrocaBindIdentity) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}",
        session.project_root.display(),
        session.harness,
        session.session
    )
}

/// Resolve the run id for one `session.send`. A fresh send with no client id gets a unique
/// server-minted id; a send with `send_id` gets a deterministic session-scoped id so a retry
/// reuses the same durable episode identity.
pub(crate) fn run_id_for_send(
    session: &BrocaBindIdentity,
    send_id: Option<&str>,
) -> broca_protocol::RunId {
    match send_id {
        None => mint_run_id(&session.session),
        Some(send_id) => {
            let mut seed = send_id_scope(session);
            seed.push_str(send_id);
            let digest = broca_provider::hash::fnv1a128(&seed);
            broca_protocol::RunId(format!("run-sid-{}", broca_provider::hash::base62(digest)))
        }
    }
}

/// Derive an import's deterministic run id under the already resolved storage identity.
pub(super) fn run_id_for_import(
    session: &BrocaBindIdentity,
    import_id: &str,
) -> broca_protocol::RunId {
    let mut seed = send_id_scope(session);
    seed.push_str("import:");
    seed.push_str(import_id);
    let digest = broca_provider::hash::fnv1a128(&seed);
    broca_protocol::RunId(format!("run-iid-{}", broca_provider::hash::base62(digest)))
}
