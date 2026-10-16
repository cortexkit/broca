//! Identity-only withdrawal work that survives deletion of a retained session's WAL.
use super::{SqliteStore, StoreError};
use broca_protocol::{BindIdentity, BindPrincipal, SessionScope};
use rusqlite::params;
use serde::{Deserialize, Serialize};

// CROSS JOIN fixes the outer loop at the small live-retention inventory. An
// ordinary JOIN can scan every run (and its large session row) even when there
// are no retained lineages, on every sweep and clock-change notification.
const DISCOVERY_SQL: &str = "SELECT DISTINCT r.session FROM retention_lineage l CROSS JOIN run_index r ON r.lineage_id=l.lineage_id WHERE l.expired_at_ms IS NULL AND r.withdrawals_owed=1";

/// BindPrincipal::Unknown preserves arbitrary JSON from later WAL writers. Retention
/// accepts only recognized principal identity fields to avoid keeping transcript content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum KnownBindPrincipal {
    Reserved { module_id: String },
    Direct,
    Unverified,
}

impl KnownBindPrincipal {
    fn from_principal(principal: &BindPrincipal) -> Option<Self> {
        match principal {
            BindPrincipal::Reserved { module_id } => Some(Self::Reserved {
                module_id: module_id.clone(),
            }),
            BindPrincipal::Direct => Some(Self::Direct),
            BindPrincipal::Unverified => Some(Self::Unverified),
            BindPrincipal::Unknown(_) => None,
        }
    }

    fn to_principal(&self) -> BindPrincipal {
        match self {
            Self::Reserved { module_id } => BindPrincipal::Reserved {
                module_id: module_id.clone(),
            },
            Self::Direct => BindPrincipal::Direct,
            Self::Unverified => BindPrincipal::Unverified,
        }
    }
}

/// Frozen owner, scope reference/version, flow-carrier and agent-run identities for
/// withdrawal routing, with no arbitrary JSON payloads. The agent-run fields are daemon
/// tokens validated as opaque identifiers, not conversation content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionScopeIdentity {
    pub owner: KnownBindPrincipal,
    #[serde(rename = "ref")]
    pub scope_ref: String,
    pub scope_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<KnownBindPrincipal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
}

impl RetentionScopeIdentity {
    /// Return no identity for an Unknown owner or sender: reconstructing that scope
    /// would require retaining its arbitrary JSON, so the worker must not route it.
    pub fn from_scope(scope: &SessionScope) -> Option<Self> {
        let owner = KnownBindPrincipal::from_principal(&scope.owner)?;
        let sender = match &scope.sender {
            Some(sender) => Some(KnownBindPrincipal::from_principal(sender)?),
            None => None,
        };
        Some(Self {
            owner,
            scope_ref: scope.scope_ref.clone(),
            scope_epoch: scope.scope_epoch,
            flow_id: scope.flow_id.clone(),
            sender,
            agent_id: scope.agent_id.clone(),
            run_id: scope.run_id.clone(),
        })
    }

    pub fn to_scope(&self) -> SessionScope {
        SessionScope {
            owner: self.owner.to_principal(),
            scope_ref: self.scope_ref.clone(),
            scope_epoch: self.scope_epoch,
            flow_id: self.flow_id.clone(),
            sender: self.sender.as_ref().map(KnownBindPrincipal::to_principal),
            agent_id: self.agent_id.clone(),
            run_id: self.run_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionQuestionId {
    Known(String),
    Unknown,
}

#[derive(Debug, Clone)]
pub struct RetentionWithdrawal {
    pub lineage_id: String,
    pub session: BindIdentity,
    pub call_key: String,
    /// Absent when the frozen owner or sender was unknown. The worker must keep the
    /// obligation until ask expiry rather than withdraw using a substitute scope.
    pub scope: Option<RetentionScopeIdentity>,
    pub question: RetentionQuestionId,
    pub provider_id: String,
    pub provider_incarnation: Option<String>,
    pub expires_at_ms: u64,
}

impl SqliteStore {
    pub fn pending_retention_withdrawals(&self) -> Result<Vec<RetentionWithdrawal>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT lineage_id, session, call_key, scope_json, elicitation_id, provider_id, provider_incarnation, expires_at_ms, state FROM retention_withdrawal ORDER BY lineage_id, call_key")?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
            ))
        })?;
        rows.map(|row| {
            let (
                lineage_id,
                session,
                call_key,
                scope,
                id,
                provider_id,
                provider_incarnation,
                expires_at_ms,
                state,
            ) = row?;
            let question = match (id, state.as_str()) {
                (Some(id), "ready") => RetentionQuestionId::Known(id),
                (None, "id_unknown") => RetentionQuestionId::Unknown,
                (Some(id), "scope_unknown") => RetentionQuestionId::Known(id),
                (None, "scope_unknown") => RetentionQuestionId::Unknown,
                _ => {
                    return Err(StoreError::InconsistentSnapshot(
                        "invalid content-free withdrawal identity",
                    ))
                }
            };
            let scope: Option<RetentionScopeIdentity> = serde_json::from_str(&scope)?;
            if scope.is_none() != (state == "scope_unknown") {
                return Err(StoreError::InconsistentSnapshot(
                    "invalid content-free scope state",
                ));
            }
            Ok(RetentionWithdrawal {
                lineage_id,
                session: serde_json::from_str(&session)?,
                call_key,
                scope,
                question,
                provider_id,
                provider_incarnation,
                expires_at_ms: expires_at_ms.parse().map_err(|_| {
                    StoreError::InconsistentSnapshot("invalid content-free ask expiry")
                })?,
            })
        })
        .collect()
    }

    /// The state transition is durable settlement. A false result means another pass settled it.
    pub fn settle_retention_withdrawal(
        &self,
        lineage: &str,
        call_key: &str,
    ) -> Result<bool, StoreError> {
        let conn = self.conn.lock().unwrap();
        Ok(conn.execute(
            "DELETE FROM retention_withdrawal WHERE lineage_id=?1 AND call_key=?2",
            params![lineage, call_key],
        )? == 1)
    }

    pub fn retention_lineages_with_discovery(&self) -> Result<Vec<BindIdentity>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(DISCOVERY_SQL)?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }
}

pub(crate) fn write_questions(
    tx: &rusqlite::Transaction<'_>,
    questions: &[RetentionWithdrawal],
) -> Result<(), StoreError> {
    for question in questions {
        let (id, state) = match &question.question {
            RetentionQuestionId::Known(id) => (Some(id.as_str()), "ready"),
            RetentionQuestionId::Unknown => (None, "id_unknown"),
        };
        let state = if question.scope.is_none() {
            "scope_unknown"
        } else {
            state
        };
        // Never serialize the WAL's SessionScope, whose unknown principals can contain
        // content. Store null when no known identity can be reconstructed; the session,
        // call key and ask deadline still identify the obligation until it expires.
        tx.execute("INSERT INTO retention_withdrawal (lineage_id, session, call_key, scope_json, elicitation_id, provider_id, provider_incarnation, expires_at_ms, state) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT(lineage_id,call_key) DO NOTHING", params![question.lineage_id, serde_json::to_string(&question.session)?, question.call_key, serde_json::to_string(&question.scope)?, id, question.provider_id, question.provider_incarnation, question.expires_at_ms.to_string(), state])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert_run(
        conn: &rusqlite::Connection,
        run: &str,
        session: &str,
        lineage: Option<&str>,
        owed: bool,
    ) {
        conn.execute("INSERT INTO run_index (run_id, session, state, lineage_id, withdrawals_owed) VALUES (?1, ?2, 'finished', ?3, ?4)", params![run, session, lineage, owed]).unwrap();
    }

    #[test]
    fn discovery_without_live_retention_does_not_scan_runs() {
        let store = SqliteStore::open_in_memory().unwrap();
        let conn = store.conn.lock().unwrap();
        let session = serde_json::to_string(&BindIdentity::new("/p", "h", "s")).unwrap();
        conn.execute_batch("BEGIN").unwrap();
        for i in 0..4000 {
            insert_run(
                &conn,
                &format!("run-{i}"),
                &session,
                Some("ordinary-lineage"),
                true,
            );
        }
        conn.execute_batch("COMMIT").unwrap();
        for expired_only in [false, true] {
            if expired_only {
                conn.execute("INSERT INTO retention_lineage VALUES ('expired', 'expired-session', 1, 0, 1, 0, 2)", []).unwrap();
            }
            let mut stmt = conn.prepare(DISCOVERY_SQL).unwrap();
            assert!(stmt.query([]).unwrap().next().unwrap().is_none());
            assert_eq!(
                stmt.get_status(rusqlite::StatementStatus::FullscanStep),
                0,
                "no live retention must not walk run_index"
            );
            assert!(
                stmt.get_status(rusqlite::StatementStatus::VmStep) < 100,
                "empty live retention must use constant query work"
            );
        }
    }

    #[test]
    fn discovery_matches_owed_runs_of_live_lineages_only() {
        let store = SqliteStore::open_in_memory().unwrap();
        let a = BindIdentity::new("/p", "h", "a");
        let b = BindIdentity::new("/p", "h", "b");
        {
            let conn = store.conn.lock().unwrap();
            conn.execute("INSERT INTO retention_lineage VALUES ('live', 'not-run-json', 1, 0, 1, 0, NULL), ('expired', 'old-session', 1, 0, 1, 0, 2)", []).unwrap();
            let a = serde_json::to_string(&a).unwrap();
            let b = serde_json::to_string(&b).unwrap();
            let not_owed =
                serde_json::to_string(&BindIdentity::new("/p", "h", "not-owed")).unwrap();
            let expired = serde_json::to_string(&BindIdentity::new("/p", "h", "expired")).unwrap();
            let unknown = serde_json::to_string(&BindIdentity::new("/p", "h", "unknown")).unwrap();
            for (run, session, lineage, owed) in [
                ("a1", &a, Some("live"), true),
                ("a2", &a, Some("live"), true),
                ("b", &b, Some("live"), true),
                ("not-owed", &not_owed, Some("live"), false),
                ("expired", &expired, Some("expired"), true),
                ("unknown", &unknown, None, true),
            ] {
                insert_run(&conn, run, session, lineage, owed);
            }
        }
        let mut actual = store.retention_lineages_with_discovery().unwrap();
        actual.sort_by(|a, b| a.session.cmp(&b.session));
        assert_eq!(actual, vec![a, b]);
    }

    #[test]
    fn retention_scope_round_trips_only_known_principal_identities() {
        for owner in [
            BindPrincipal::Reserved {
                module_id: "owner".into(),
            },
            BindPrincipal::Direct,
            BindPrincipal::Unverified,
        ] {
            let scope = SessionScope {
                owner,
                scope_ref: "reference".into(),
                scope_epoch: 7,
                flow_id: Some("flow".into()),
                sender: Some(BindPrincipal::Reserved {
                    module_id: "carrier".into(),
                }),
                agent_id: Some("agent".into()),
                run_id: Some("run".into()),
            };
            let identity = RetentionScopeIdentity::from_scope(&scope).unwrap();
            assert_eq!(identity.to_scope(), scope);
            let stored = serde_json::to_string(&identity).unwrap();
            assert_eq!(
                serde_json::from_str::<RetentionScopeIdentity>(&stored).unwrap(),
                identity
            );
        }
        let scope = serde_json::json!({"owner":{"kind":"future", "payload":"not-an-identity"}, "ref":"reference", "scope_epoch":7});
        assert!(RetentionScopeIdentity::from_scope(
            &serde_json::from_value(scope.clone()).unwrap()
        )
        .is_none());
        assert!(serde_json::from_value::<RetentionScopeIdentity>(scope).is_err());
    }
}
