//! The runner's session identity — its OWN type, so the loop stays provider- and
//! transport-agnostic. It mirrors subc's `BindIdentity` field-for-field
//! ((project_root, harness, session)), but the loop crate does NOT depend on
//! subc's wire types; the subc tool-plane adapter maps this onto
//! `subc_protocol::BindIdentity` at its boundary.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// A durable, resumable conversation identity — the unit the WAL/store key on,
/// and the per-session single-writer lease scope.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BindIdentity {
    pub project_root: PathBuf,
    pub harness: String,
    pub session: String,
}

impl BindIdentity {
    pub fn new(
        project_root: impl Into<PathBuf>,
        harness: impl Into<String>,
        session: impl Into<String>,
    ) -> Self {
        Self {
            project_root: project_root.into(),
            harness: harness.into(),
            session: session.into(),
        }
    }

    /// Reject identities that cannot be mapped to one unambiguous durable partition.
    ///
    /// Existing WAL, lease, store, and idempotency keys join these three fields with U+001F.
    /// Allowing that separator inside a field makes the tuple encoding non-injective, so one
    /// identity can silently open another identity's state. Rejecting the separator preserves
    /// every existing valid session's stable keys instead of changing the encoding and requiring
    /// a migration of all durable sessions.
    pub fn validate_keyable(&self) -> Result<(), String> {
        const KEY_SEPARATOR: char = '\u{1f}';

        let invalid_field = if self
            .project_root
            .display()
            .to_string()
            .contains(KEY_SEPARATOR)
        {
            Some("project_root")
        } else if self.harness.contains(KEY_SEPARATOR) {
            Some("harness")
        } else if self.session.contains(KEY_SEPARATOR) {
            Some("session")
        } else {
            None
        };

        match invalid_field {
            Some(field) => Err(format!(
                "bind identity {field} contains reserved U+001F separator and would alias another durable session"
            )),
            None => Ok(()),
        }
    }
}

/// Who opened the route a send arrived on, as the subc daemon attested it on `route.bind`.
///
/// Mirrors `subc_protocol::Principal` and serializes to the same JSON, so a WAL record keeps
/// the daemon's value exactly as it was stamped: `{"kind":"reserved","module_id":"…"}`,
/// `{"kind":"direct"}` or `{"kind":"unverified"}`. Like [`BindIdentity`] it is broca's own
/// type so that the loop and WAL crates do not depend on subc's wire crate.
///
/// A bind that carried no principal is recorded as no principal at all (`Option::None`, no
/// bytes on the record), never as one of these values: absence is what an older daemon sends,
/// and it must stay distinguishable from any attested caller.
///
/// THIS SITS ON WAL RECORDS (`RunStarted`, `Queued`), so it must decode whatever a later build
/// writes. A rolled-back build that refused a principal kind it did not know would fail to
/// decode `RunStarted` and lose the whole session. Any JSON value therefore decodes, and one
/// that is not exactly a shape this build knows is kept verbatim as [`BindPrincipal::Unknown`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindPrincipal {
    /// A daemon-spawned module that proved possession of its launch nonce.
    Reserved { module_id: String },
    /// A caller that presented no consumer identity: a direct key-holder.
    Direct,
    /// The daemon's reserved kind for a caller it could not verify.
    Unverified,
    /// A value written by a later build that this build does not know, kept exactly as read.
    /// This build never constructs one of its own.
    Unknown(serde_json::Value),
}

impl BindPrincipal {
    /// The principal as one string, for comparing against a scope owner and for naming it in
    /// a refusal: `reserved:<module_id>`, `direct` or `unverified`. This is how a scope owner is
    /// spelled, so a recorded principal matches an owner exactly when the strings are equal.
    ///
    /// An unknown kind renders as its compact JSON, which starts with `{` and so can never
    /// equal a scope owner or any of the three known spellings.
    pub fn as_owner_string(&self) -> String {
        match self {
            BindPrincipal::Reserved { module_id } => format!("reserved:{module_id}"),
            BindPrincipal::Direct => "direct".to_owned(),
            BindPrincipal::Unverified => "unverified".to_owned(),
            BindPrincipal::Unknown(value) => value.to_string(),
        }
    }

    fn from_value(value: serde_json::Value) -> Self {
        let known = value.as_object().and_then(|object| {
            match (object.get("kind")?.as_str()?, object.len()) {
                ("reserved", 2) => Some(BindPrincipal::Reserved {
                    module_id: object.get("module_id")?.as_str()?.to_owned(),
                }),
                ("direct", 1) => Some(BindPrincipal::Direct),
                ("unverified", 1) => Some(BindPrincipal::Unverified),
                _ => None,
            }
        });
        known.unwrap_or(BindPrincipal::Unknown(value))
    }
}

impl Serialize for BindPrincipal {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            // Written as an explicit map so the known kinds keep a fixed key order (`kind`
            // first) whatever map type serde_json is built with.
            BindPrincipal::Reserved { module_id } => {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("kind", "reserved")?;
                map.serialize_entry("module_id", module_id)?;
                map.end()
            }
            BindPrincipal::Direct | BindPrincipal::Unverified => {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("kind", &self.as_owner_string())?;
                map.end()
            }
            BindPrincipal::Unknown(value) => value.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for BindPrincipal {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        serde_json::Value::deserialize(deserializer).map(BindPrincipal::from_value)
    }
}

/// The scope a session was admitted under, frozen on its `RunStarted` records: the scope's
/// owner, its `ref` and its `scope_epoch`, exactly as the subc daemon stamped them on the bind.
///
/// `(owner, ref, scope_epoch)` names one session of one owner: the owner reuses a `ref` for a
/// new session only with a higher epoch, so anything bound to the triple (an approval, a late
/// result, the session itself) can never carry over into the next session under that ref.
///
/// For automation scopes, the flow or agent-run identifier and sending principal are frozen:
/// an automation may not switch identity or borrow its owner agent's session. `agent_id` is
/// identity only for agent-run scopes; ordinary agent delegation attributes are not frozen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionScope {
    pub owner: BindPrincipal,
    #[serde(rename = "ref")]
    pub scope_ref: String,
    pub scope_epoch: u64,
    /// Automation identity, absent for ordinary agent scopes. Frozen with the first episode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow_id: Option<String>,
    /// The daemon-attested principal driving this automation, not its owner agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<BindPrincipal>,
    /// Agent identity on an agent-run scope, absent on ordinary and flow scopes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    /// The daemon-stamped agent run, not broca's episode `RunId`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
}

/// The daemon-attested kind of a scope, fixed for its epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeKind {
    Head,
    Worker,
    Ephemeral,
}

/// A parent reference is owner-local and names one epoch, not just a reusable scope ref.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeParent {
    pub owner: BindPrincipal,
    pub scope_ref: String,
    pub scope_epoch: u64,
}

/// Whether the daemon has validated the parent link against its live scope registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParentState {
    Linked,
    Pending,
    Ended,
}

/// The scope stamp the subc daemon put on the bind a send arrived over, as broca keeps it.
///
/// The identity (`scope`) takes part in broca's decisions. On a flow scope its `sender` comes
/// from the bind's attested principal: the daemon admits a scoped opener only as owner or
/// listed carrier. `owner_authorized` alone grants no permission. The stamp's `agent_id` is
/// retained for agent-run scopes, but `delegates` is not carried: broca never acts as the agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindScope {
    pub scope: SessionScope,
    pub kind: ScopeKind,
    pub parent: Option<Box<ScopeParent>>,
    pub parent_state: Option<ParentState>,
    /// Whether the daemon lists the scope's owner in its `scope_authority_owners`.
    pub owner_authorized: bool,
}

/// What the route a send arrived over says about scopes, which decides the owner check.
///
/// Set only by the serve layer, from the bind and from the daemon's capabilities. A send made
/// inside broca (a re-drive, a queue drain, a test harness driving the actor directly) is
/// `Internal`: it is not a caller's send, so no owner check applies, and its episode simply
/// carries whatever scope the session already froze.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum SendScope {
    #[default]
    Internal,
    /// The daemon does not advertise `scopes/v1`, so no bind can carry a stamp.
    DaemonWithoutScopes,
    /// The daemon advertises `scopes/v1` and this bind carried no stamp.
    Unstamped,
    /// The bind carried a scope stamp.
    Stamped(BindScope),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn principal_bytes_match_the_daemon_stamp_and_round_trip() {
        for (principal, bytes, owner) in [
            (
                BindPrincipal::Reserved {
                    module_id: "prefrontal-core".into(),
                },
                r#"{"kind":"reserved","module_id":"prefrontal-core"}"#,
                "reserved:prefrontal-core",
            ),
            (BindPrincipal::Direct, r#"{"kind":"direct"}"#, "direct"),
            (
                BindPrincipal::Unverified,
                r#"{"kind":"unverified"}"#,
                "unverified",
            ),
        ] {
            assert_eq!(serde_json::to_string(&principal).unwrap(), bytes);
            assert_eq!(
                serde_json::from_str::<BindPrincipal>(bytes).unwrap(),
                principal
            );
            assert_eq!(principal.as_owner_string(), owner);
        }
    }

    /// A principal kind from a later daemon must decode rather than fail the record it sits
    /// on, keep its value, and never read as one of the kinds this build knows.
    #[test]
    fn an_unknown_principal_decodes_and_is_kept_verbatim() {
        for bytes in [
            r#"{"kind":"operator","id":"ufuk"}"#,
            r#"{"kind":"direct","since":3}"#,
            r#"{"kind":"reserved"}"#,
            r#""reserved:broca""#,
        ] {
            let decoded: BindPrincipal = serde_json::from_str(bytes).expect("decodes");
            let BindPrincipal::Unknown(value) = &decoded else {
                panic!("{bytes} must decode as Unknown, got {decoded:?}");
            };
            let stored: serde_json::Value = serde_json::from_str(bytes).unwrap();
            assert_eq!(value, &stored);
            let reencoded: serde_json::Value =
                serde_json::from_str(&serde_json::to_string(&decoded).unwrap()).unwrap();
            assert_eq!(reencoded, stored);
            assert!(!matches!(
                decoded.as_owner_string().as_str(),
                "direct" | "unverified"
            ));
            assert!(!decoded.as_owner_string().starts_with("reserved:"));
        }
    }

    #[test]
    fn rejects_the_reserved_separator_in_every_identity_field() {
        for (field, identity) in [
            (
                "project_root",
                BindIdentity::new("/tmp/project\u{1f}other", "runner", "session"),
            ),
            (
                "harness",
                BindIdentity::new("/tmp/project", "runner\u{1f}other", "session"),
            ),
            (
                "session",
                BindIdentity::new("/tmp/project", "runner", "session\u{1f}other"),
            ),
        ] {
            let error = identity
                .validate_keyable()
                .expect_err("identity is ambiguous");
            assert!(
                error.contains(field),
                "unexpected validation error: {error}"
            );
            assert!(error.contains("U+001F"));
        }
    }

    #[test]
    fn accepts_realistic_identity_text_without_changing_it() {
        let identities = [
            BindIdentity::new("/tmp/project with spaces", "runner", "alfonso:597cb79e"),
            BindIdentity::new("/tmp/プロジェクト", "mcp:éditeur", "session-α"),
        ];

        for identity in identities {
            let before = identity.clone();
            identity
                .validate_keyable()
                .expect("identity is unambiguous");
            assert_eq!(identity, before);
        }
    }
}
