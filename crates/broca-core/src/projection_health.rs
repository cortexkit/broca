//! Process-local state for sessions whose latest store projection failed.

use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

use broca_protocol::BindIdentity;

// Keep closed or evicted sessions behind until a successful projection catches them up. Closing
// a route cannot prove the store watermark advanced, and a background sweep would only obscure
// that recovery debt; process exit naturally clears this in-memory diagnostic.
static PROJECTION_BEHIND: OnceLock<Mutex<HashSet<BindIdentity>>> = OnceLock::new();

fn behind() -> &'static Mutex<HashSet<BindIdentity>> {
    PROJECTION_BEHIND.get_or_init(|| Mutex::new(HashSet::new()))
}

pub(crate) fn record_failure(session: &BindIdentity) {
    behind().lock().unwrap().insert(session.clone());
}

pub(crate) fn record_success(session: &BindIdentity) {
    behind().lock().unwrap().remove(session);
}

pub(crate) fn behind_sessions() -> u64 {
    behind().lock().unwrap().len() as u64
}
