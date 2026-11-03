//! Project attribution for spend facts, separate from the identity stored in the WAL.

use broca_protocol::{session::UNSCOPED_SESSION_ROOT, BindIdentity};
use serde::{ser::SerializeStruct, Deserialize, Deserializer, Serializer};
use std::{ffi::OsStr, path::PathBuf};

pub(super) fn serialize<S: Serializer>(
    identity: &BindIdentity,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let unscoped = identity.project_root.as_os_str() == OsStr::new(UNSCOPED_SESSION_ROOT);
    let mut session = serializer.serialize_struct("BindIdentity", if unscoped { 2 } else { 3 })?;
    if !unscoped {
        session.serialize_field("project_root", &identity.project_root)?;
    }
    session.serialize_field("harness", &identity.harness)?;
    session.serialize_field("session", &identity.session)?;
    session.end()
}

pub(super) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<BindIdentity, D::Error> {
    #[derive(Deserialize)]
    struct Session {
        #[serde(default, deserialize_with = "present_root")]
        project_root: Option<PathBuf>,
        harness: String,
        session: String,
    }
    let session = Session::deserialize(deserializer)?;
    Ok(BindIdentity::new(
        session
            .project_root
            .unwrap_or_else(|| UNSCOPED_SESSION_ROOT.into()),
        session.harness,
        session.session,
    ))
}

fn present_root<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<PathBuf>, D::Error> {
    PathBuf::deserialize(deserializer).map(Some)
}
