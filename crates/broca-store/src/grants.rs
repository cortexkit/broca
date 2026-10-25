//! Consent authority and held tool calls. This database is not rebuilt from the session WAL:
//! losing a grant asks again, while losing a held request would lose the caller's work.

use std::{path::Path, sync::Mutex};

use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};

pub const HOLD_MS: i64 = 10 * 60 * 1000;
pub const RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1000;

#[derive(Debug)]
pub struct GrantsError(pub String);

impl std::fmt::Display for GrantsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for GrantsError {}

impl From<rusqlite::Error> for GrantsError {
    fn from(error: rusqlite::Error) -> Self {
        Self(error.to_string())
    }
}

type Result<T> = std::result::Result<T, GrantsError>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeIdentity {
    pub owner: String,
    pub scope_ref: String,
    pub scope_epoch: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HeldState {
    Filing,
    Held,
    Authorized,
    Final,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalKind {
    Result,
    NotStarted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalResult {
    pub kind: FinalKind,
    pub is_error: bool,
    /// Exact tool reply bytes, retained independently of late delivery acknowledgements.
    pub body: String,
    pub settled_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeldCall {
    pub batch_id: String,
    pub scope: ScopeIdentity,
    pub batch_scope: crate::system_one::BatchScope,
    pub principal: String,
    pub call_key: String,
    pub agent_id: String,
    pub provider: String,
    pub model: String,
    pub request: Option<String>,
    pub args_digest: String,
    /// Catalog and pricing facts from admission, not a later catalog lookup.
    pub frozen_row: String,
    pub dedup_key: String,
    pub state: HeldState,
    pub admitted_at_ms: i64,
    pub deadline_at_ms: i64,
    pub card_id: Option<String>,
    pub final_result: Option<FinalResult>,
}

/// Only first-admission fields are accepted. The store sets the state and immutable deadline.
pub struct HoldRequest {
    pub batch_id: String,
    pub scope: ScopeIdentity,
    pub batch_scope: crate::system_one::BatchScope,
    pub principal: String,
    pub call_key: String,
    pub agent_id: String,
    pub provider: String,
    pub model: String,
    pub request: String,
    pub args_digest: String,
    pub frozen_row: String,
    pub dedup_key: String,
    pub admitted_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HoldAdmission {
    Inserted(HeldCall),
    Replay(HeldCall),
    ScopeMismatch,
    ArgsMismatch,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CardState {
    Filing,
    Pending,
    Allowed,
    Denied,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Card {
    pub dedup_key: String,
    pub scope: ScopeIdentity,
    pub agent_id: String,
    pub provider: String,
    pub content: String,
    pub card_id: Option<String>,
    pub state: CardState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrantKind {
    Run,
    Hours24,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pub id: String,
    pub holder: String,
    pub project: Option<String>,
    pub provider: String,
    pub kind: GrantKind,
    pub scope: Option<ScopeIdentity>,
    pub source_card_id: String,
    pub created_at_ms: i64,
    pub expires_at_ms: Option<i64>,
}

/// A caller processes a whole answers page before committing its cursor. An ended or unknown
/// run supplies `run_live: false`, so a late run-only answer cannot create a live grant.
pub struct CardAnswer {
    pub dedup_key: String,
    pub card_id: String,
    pub decision: Decision,
    pub run_live: bool,
}

pub enum Decision {
    AllowRun,
    Allow24h,
    Deny,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LateDelivery {
    pub seq: u64,
    pub call: HeldCall,
}

pub struct GrantsStore {
    conn: Mutex<Connection>,
}

fn encode(value: &impl Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(|e| GrantsError(e.to_string()))
}

fn decode<T: serde::de::DeserializeOwned>(body: &str) -> Result<T> {
    serde_json::from_str(body).map_err(|e| GrantsError(e.to_string()))
}

fn integer(value: u64) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| GrantsError("consent cursor exceeds SQLite integer range".into()))
}

impl GrantsStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        super::create_private_sqlite(path).map_err(|e| GrantsError(e.to_string()))?;
        super::sqlite_open_observed();
        Self::from_connection(Connection::open(path)?)
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(conn: Connection) -> Result<Self> {
        crate::busy::configure_writer_connection(&conn)?;
        conn.execute_batch(
            "PRAGMA journal_mode=DELETE;
             PRAGMA synchronous=FULL;
             CREATE TABLE IF NOT EXISTS grants (
                 id TEXT PRIMARY KEY, holder TEXT NOT NULL, project TEXT, provider TEXT NOT NULL,
                 kind TEXT NOT NULL, scope TEXT, source_card_id TEXT NOT NULL UNIQUE,
                 created_at_ms INTEGER NOT NULL, expires_at_ms INTEGER, body TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS denies (
                 agent_id TEXT NOT NULL, scope TEXT NOT NULL, provider TEXT NOT NULL,
                 source_card_id TEXT NOT NULL, PRIMARY KEY(agent_id, scope, provider)
             );
             CREATE TABLE IF NOT EXISTS generations (
                 agent_id TEXT NOT NULL, provider TEXT NOT NULL, generation INTEGER NOT NULL,
                 PRIMARY KEY(agent_id, provider)
             );
             CREATE TABLE IF NOT EXISTS cards (
                 dedup_key TEXT PRIMARY KEY, body TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS held_calls (
                 batch_id TEXT PRIMARY KEY, principal TEXT NOT NULL, dedup_key TEXT NOT NULL,
                 state TEXT NOT NULL, admitted_at_ms INTEGER NOT NULL,
                 deadline_at_ms INTEGER NOT NULL, body TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS cursors (
                 scope TEXT PRIMARY KEY, cursor TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS late_results (
                 seq INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT NOT NULL UNIQUE,
                 principal TEXT NOT NULL, expires_at_ms INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS late_results_principal ON late_results(principal, seq);
             CREATE TABLE IF NOT EXISTS late_cursors (
                 principal TEXT PRIMARY KEY, seq INTEGER NOT NULL
             );",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn write<T>(&self, action: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        let mut conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = action(&tx)?;
        tx.commit()?;
        Ok(result)
    }

    pub fn lookup(&self, batch_id: &str, now_ms: i64) -> Result<Option<HeldCall>> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        read_call(&conn, batch_id, now_ms)
    }

    pub fn hold(&self, request: HoldRequest) -> Result<HoldAdmission> {
        self.write(|tx| {
            if let Some(row) = read_call(tx, &request.batch_id, request.admitted_at_ms)? {
                return Ok(
                    if row.scope != request.scope
                        || row.batch_scope != request.batch_scope
                        || row.principal != request.principal
                        || row.call_key != request.call_key
                    {
                        HoldAdmission::ScopeMismatch
                    } else if row.args_digest != request.args_digest {
                        HoldAdmission::ArgsMismatch
                    } else {
                        HoldAdmission::Replay(row)
                    },
                );
            }
            tx.execute(
                "DELETE FROM late_results WHERE batch_id=?1",
                [&request.batch_id],
            )?;
            tx.execute(
                "DELETE FROM held_calls WHERE batch_id=?1",
                [&request.batch_id],
            )?;
            let row = HeldCall {
                batch_id: request.batch_id,
                scope: request.scope,
                batch_scope: request.batch_scope,
                principal: request.principal,
                call_key: request.call_key,
                agent_id: request.agent_id,
                provider: request.provider,
                model: request.model,
                request: Some(request.request),
                args_digest: request.args_digest,
                frozen_row: request.frozen_row,
                dedup_key: request.dedup_key,
                state: HeldState::Filing,
                admitted_at_ms: request.admitted_at_ms,
                deadline_at_ms: request.admitted_at_ms.saturating_add(HOLD_MS),
                card_id: None,
                final_result: None,
            };
            tx.execute(
                "INSERT INTO held_calls VALUES (?1,?2,?3,'filing',?4,?5,?6)",
                params![
                    row.batch_id,
                    row.principal,
                    row.dedup_key,
                    row.admitted_at_ms,
                    row.deadline_at_ms,
                    encode(&row)?
                ],
            )?;
            Ok(HoldAdmission::Inserted(row))
        })
    }

    /// Storing a consent card updates every waiting call with the same dedup key, without
    /// changing its admission time or deadline. The transport must serialize request/reply
    /// pairs by dedup key as well.
    pub fn put_card(&self, card: &Card) -> Result<bool> {
        if !matches!(card.state, CardState::Filing | CardState::Pending) {
            return Err(GrantsError(
                "only answer processing may settle a consent card".into(),
            ));
        }
        self.write(|tx| {
            if read_card(tx, &card.dedup_key)?.is_some_and(|old| matches!(old.state, CardState::Allowed | CardState::Denied)) {
                return Ok(false);
            }
            tx.execute("INSERT INTO cards VALUES (?1,?2) ON CONFLICT(dedup_key) DO UPDATE SET body=excluded.body", params![card.dedup_key, encode(card)?])?;
            let mut stmt = tx.prepare("SELECT body FROM held_calls WHERE dedup_key=?1 AND state IN ('filing','held')")?;
            let rows = stmt.query_map([&card.dedup_key], |r| r.get::<_, String>(0))?.collect::<std::result::Result<Vec<_>, _>>()?;
            for body in rows {
                let mut row: HeldCall = decode(&body)?;
                row.card_id = card.card_id.clone();
                if card.card_id.is_some() { row.state = HeldState::Held; }
                save_call(tx, &row)?;
            }
            Ok(true)
        })
    }

    pub fn card(&self, dedup_key: &str) -> Result<Option<Card>> {
        read_card(
            &self.conn.lock().unwrap_or_else(|p| p.into_inner()),
            dedup_key,
        )
    }

    pub fn generation(&self, agent_id: &str, provider: &str) -> Result<u64> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        Ok(conn
            .query_row(
                "SELECT generation FROM generations WHERE agent_id=?1 AND provider=?2",
                params![agent_id, provider],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0))
    }

    pub fn grants(&self) -> Result<Vec<Grant>> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        let mut stmt = conn.prepare("SELECT body FROM grants ORDER BY id")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.map(|r| decode(&r?)).collect()
    }

    pub fn denied(&self, agent_id: &str, scope: &ScopeIdentity, provider: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        Ok(conn
            .query_row(
                "SELECT 1 FROM denies WHERE agent_id=?1 AND scope=?2 AND provider=?3",
                params![agent_id, encode(scope)?, provider],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    /// Revocation and the next card generation commit together. Keeping the settled card row
    /// prevents a replayed answer page from recreating the deleted grant.
    pub fn revoke(&self, id: &str) -> Result<bool> {
        self.write(|tx| {
            let key: Option<(String, String)> = tx.query_row("SELECT holder,provider FROM grants WHERE id=?1", [id], |r| Ok((r.get(0)?, r.get(1)?))).optional()?;
            let Some((holder, provider)) = key else { return Ok(false) };
            tx.execute("DELETE FROM grants WHERE id=?1", [id])?;
            tx.execute("INSERT INTO generations VALUES (?1,?2,1) ON CONFLICT(agent_id,provider) DO UPDATE SET generation=generation+1", params![holder, provider])?;
            Ok(true)
        })
    }

    pub fn answer_cursor(&self, scope: &ScopeIdentity) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        Ok(conn
            .query_row(
                "SELECT cursor FROM cursors WHERE scope=?1",
                [encode(scope)?],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Commit all decisions from a recovery page before advancing that scope's cursor. A
    /// conflicting or duplicate card answer is ignored after the first settled decision.
    pub fn apply_answer_page(
        &self,
        scope: &ScopeIdentity,
        cursor: &str,
        answers: &[CardAnswer],
        now_ms: i64,
    ) -> Result<()> {
        self.write(|tx| {
            for answer in answers {
                let Some(mut card) = read_card(tx, &answer.dedup_key)? else {
                    return Err(GrantsError("consent answer names an unknown card".into()));
                };
                if &card.scope != scope {
                    return Err(GrantsError("consent answer page has another scope identity".into()));
                }
                if matches!(card.state, CardState::Allowed | CardState::Denied) { continue; }
                // Filing recovery may find a card the transport already settled. Keep that
                // card's ID with its decision, even if a replacement was filed while recovering.
                card.card_id = Some(answer.card_id.clone());
                match answer.decision {
                    Decision::Deny => {
                        card.state = CardState::Denied;
                        tx.execute("INSERT INTO denies VALUES (?1,?2,?3,?4) ON CONFLICT(agent_id,scope,provider) DO NOTHING", params![card.agent_id, encode(scope)?, card.provider, answer.card_id])?;
                    }
                    Decision::AllowRun | Decision::Allow24h => {
                        card.state = CardState::Allowed;
                        let run = matches!(answer.decision, Decision::AllowRun);
                        if !run || answer.run_live {
                            let grant = Grant {
                                id: answer.card_id.clone(), holder: card.agent_id.clone(), project: None,
                                provider: card.provider.clone(), kind: if run { GrantKind::Run } else { GrantKind::Hours24 },
                                scope: run.then(|| scope.clone()), source_card_id: answer.card_id.clone(),
                                created_at_ms: now_ms,
                                expires_at_ms: (!run).then(|| now_ms.saturating_add(24 * 60 * 60 * 1000)),
                            };
                            tx.execute("INSERT INTO grants VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(source_card_id) DO NOTHING", params![grant.id, grant.holder, grant.project, grant.provider, if run { "run" } else { "24_hours" }, grant.scope.as_ref().map(encode).transpose()?, grant.source_card_id, grant.created_at_ms, grant.expires_at_ms, encode(&grant)?])?;
                        }
                    }
                }
                tx.execute("UPDATE cards SET body=?2 WHERE dedup_key=?1", params![card.dedup_key, encode(&card)?])?;
            }
            tx.execute("INSERT INTO cursors VALUES (?1,?2) ON CONFLICT(scope) DO UPDATE SET cursor=excluded.cursor", params![encode(scope)?, cursor])?;
            Ok(())
        })
    }

    /// The authorization and deadline checks share the write transaction with the state move.
    pub fn authorize(&self, batch_id: &str, now_ms: i64) -> Result<bool> {
        self.write(|tx| {
            let Some(mut row) = read_call(tx, batch_id, now_ms)? else {
                return Ok(false);
            };
            if row.state != HeldState::Held || now_ms >= row.deadline_at_ms {
                return Ok(false);
            }
            row.state = HeldState::Authorized;
            save_call(tx, &row)?;
            Ok(true)
        })
    }

    /// A dispatched call has only an execution-result transition. In particular, recovery must
    /// never relabel an authorized attempt as not started when its old timer fires.
    pub fn complete_authorized(&self, batch_id: &str, body: &str, now_ms: i64) -> Result<bool> {
        self.complete_execution(batch_id, body, false, now_ms)
    }

    pub fn complete_authorized_error(
        &self,
        batch_id: &str,
        body: &str,
        now_ms: i64,
    ) -> Result<bool> {
        self.complete_execution(batch_id, body, true, now_ms)
    }

    fn complete_execution(
        &self,
        batch_id: &str,
        body: &str,
        is_error: bool,
        now_ms: i64,
    ) -> Result<bool> {
        self.write(|tx| {
            let Some(row) = read_call(tx, batch_id, now_ms)? else {
                return Ok(false);
            };
            if row.state != HeldState::Authorized {
                return Ok(false);
            }
            finalize(tx, row, FinalKind::Result, body, is_error, now_ms)
        })
    }

    pub fn finish_unstarted(&self, batch_id: &str, body: &str, now_ms: i64) -> Result<bool> {
        self.write(|tx| {
            let Some(row) = read_call(tx, batch_id, now_ms)? else {
                return Ok(false);
            };
            if !matches!(row.state, HeldState::Filing | HeldState::Held) {
                return Ok(false);
            }
            finalize(tx, row, FinalKind::NotStarted, body, true, now_ms)
        })
    }

    pub fn expire_held(&self, batch_id: &str, body: &str, now_ms: i64) -> Result<bool> {
        self.write(|tx| {
            let Some(row) = read_call(tx, batch_id, now_ms)? else {
                return Ok(false);
            };
            if row.state != HeldState::Held || now_ms < row.deadline_at_ms {
                return Ok(false);
            }
            finalize(tx, row, FinalKind::NotStarted, body, true, now_ms)
        })
    }

    pub fn recovering(&self, now_ms: i64) -> Result<Vec<HeldCall>> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        let mut stmt = conn.prepare("SELECT body FROM held_calls WHERE state != 'final' AND admitted_at_ms > ?1 ORDER BY batch_id")?;
        let rows = stmt.query_map([now_ms.saturating_sub(RETENTION_MS)], |r| {
            r.get::<_, String>(0)
        })?;
        rows.map(|r| decode(&r?)).collect()
    }

    pub fn late_head(&self, principal: &str) -> Result<u64> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        Ok(conn
            .query_row(
                "SELECT seq FROM late_cursors WHERE principal=?1",
                [principal],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0))
    }

    pub fn late_results(
        &self,
        principal: &str,
        since: u64,
        limit: u32,
        now_ms: i64,
    ) -> Result<Vec<LateDelivery>> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        let mut stmt = conn.prepare("SELECT l.seq,h.body FROM late_results l JOIN held_calls h ON h.batch_id=l.batch_id WHERE l.principal=?1 AND l.seq>?2 AND l.expires_at_ms>?3 ORDER BY l.seq LIMIT ?4")?;
        let rows = stmt.query_map(params![principal, integer(since)?, now_ms, limit], |r| {
            Ok((r.get::<_, u64>(0)?, r.get::<_, String>(1)?))
        })?;
        rows.map(|r| {
            let (seq, body) = r?;
            Ok(LateDelivery {
                seq,
                call: decode(&body)?,
            })
        })
        .collect()
    }

    pub fn acknowledge(&self, principal: &str, through: u64) -> Result<usize> {
        self.write(|tx| {
            Ok(tx.execute(
                "DELETE FROM late_results WHERE principal=?1 AND seq<=?2",
                params![principal, integer(through)?],
            )?)
        })
    }

    pub fn sweep(&self, now_ms: i64) -> Result<usize> {
        self.write(|tx| {
            tx.execute("DELETE FROM late_results WHERE expires_at_ms<=?1", [now_ms])?;
            Ok(tx.execute(
                "DELETE FROM held_calls WHERE admitted_at_ms<=?1",
                [now_ms.saturating_sub(RETENTION_MS)],
            )?)
        })
    }
}

fn read_call(conn: &Connection, batch_id: &str, now_ms: i64) -> Result<Option<HeldCall>> {
    let body: Option<String> = conn
        .query_row(
            "SELECT body FROM held_calls WHERE batch_id=?1 AND admitted_at_ms>?2",
            params![batch_id, now_ms.saturating_sub(RETENTION_MS)],
            |r| r.get(0),
        )
        .optional()?;
    body.map(|body| decode(&body)).transpose()
}

fn read_card(conn: &Connection, dedup_key: &str) -> Result<Option<Card>> {
    let body: Option<String> = conn
        .query_row(
            "SELECT body FROM cards WHERE dedup_key=?1",
            [dedup_key],
            |r| r.get(0),
        )
        .optional()?;
    body.map(|body| decode(&body)).transpose()
}

fn save_call(tx: &Transaction<'_>, row: &HeldCall) -> Result<()> {
    let state = match row.state {
        HeldState::Filing => "filing",
        HeldState::Held => "held",
        HeldState::Authorized => "authorized",
        HeldState::Final => "final",
    };
    tx.execute(
        "UPDATE held_calls SET state=?2,body=?3 WHERE batch_id=?1",
        params![row.batch_id, state, encode(row)?],
    )?;
    Ok(())
}

fn finalize(
    tx: &Transaction<'_>,
    mut row: HeldCall,
    kind: FinalKind,
    body: &str,
    is_error: bool,
    now_ms: i64,
) -> Result<bool> {
    row.state = HeldState::Final;
    row.request = None;
    row.final_result = Some(FinalResult {
        kind,
        is_error,
        body: body.into(),
        settled_at_ms: now_ms,
    });
    save_call(tx, &row)?;
    tx.execute(
        "INSERT INTO late_results (batch_id,principal,expires_at_ms) VALUES (?1,?2,?3)",
        params![
            row.batch_id,
            row.principal,
            row.admitted_at_ms.saturating_add(RETENTION_MS)
        ],
    )?;
    let seq = tx.last_insert_rowid();
    tx.execute("INSERT INTO late_cursors VALUES (?1,?2) ON CONFLICT(principal) DO UPDATE SET seq=excluded.seq", params![row.principal, seq])?;
    Ok(true)
}
