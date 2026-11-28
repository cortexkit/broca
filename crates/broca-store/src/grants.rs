//! Consent authority and held tool calls. This database is not rebuilt from the sessions' records:
//! losing a grant asks again, while losing a held request would lose the caller's work.

use std::{
    path::Path,
    sync::{Arc, Mutex, Weak},
};

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
    /// The admission scope's attested project, retained for grant matching after restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
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
    /// Supplied only from the admission scope's daemon stamp.
    pub project_id: Option<String>,
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
    /// Frozen from the daemon stamp when this card's content was filed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
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
    Always,
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
    AllowAlways,
    Deny,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LateDelivery {
    pub seq: u64,
    pub call: HeldCall,
}

pub struct GrantsStore {
    conn: Mutex<Connection>,
    terminal_listeners: Mutex<Vec<Weak<HeldCallTerminal>>>,
}

/// In-memory cleanup runs synchronously after a held call's terminal transaction commits.
pub type HeldCallTerminal = dyn Fn(&str, i64) + Send + Sync;

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

    fn from_connection(mut conn: Connection) -> Result<Self> {
        crate::busy::configure_writer_connection(&conn)?;
        Self::initialize_schema(&mut conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            terminal_listeners: Mutex::new(Vec::new()),
        })
    }

    fn initialize_schema(conn: &mut Connection) -> Result<()> {
        conn.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;")?;
        // The tables and their indexes need one durable commit, not a flush per statement.
        let schema = conn.transaction()?;
        schema.execute_batch(
            "CREATE TABLE IF NOT EXISTS grants (
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
             CREATE INDEX IF NOT EXISTS held_calls_due ON held_calls(state, deadline_at_ms);
             CREATE INDEX IF NOT EXISTS held_calls_card ON held_calls(dedup_key, admitted_at_ms);
             CREATE TABLE IF NOT EXISTS late_cursors (
                 principal TEXT PRIMARY KEY, seq INTEGER NOT NULL
             );",
        )?;
        schema.commit()?;
        Ok(())
    }

    fn write<T>(&self, action: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        let mut conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = action(&tx)?;
        tx.commit()?;
        Ok(result)
    }

    /// The caller owns the listener; dropping it unregisters cleanup without keeping its
    /// in-memory state alive across a new service instance on the same database.
    pub fn on_held_call_terminal(&self, listener: &Arc<HeldCallTerminal>) {
        self.terminal_listeners
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(Arc::downgrade(listener));
    }

    fn terminal_write<T>(
        &self,
        action: impl FnOnce(&Transaction<'_>, &mut Vec<(String, i64)>) -> Result<T>,
    ) -> Result<T> {
        let mut terminal = Vec::new();
        let result = self.write(|tx| action(tx, &mut terminal))?;
        if !terminal.is_empty() {
            let mut listeners = self
                .terminal_listeners
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let active = listeners
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>();
            listeners.retain(|listener| listener.strong_count() > 0);
            drop(listeners);
            for (key, admitted_at) in terminal {
                for listener in &active {
                    listener(&key, admitted_at);
                }
            }
        }
        Ok(result)
    }

    pub fn lookup(&self, batch_id: &str, now_ms: i64) -> Result<Option<HeldCall>> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        read_call(&conn, batch_id, now_ms)
    }

    pub fn hold(&self, request: HoldRequest) -> Result<HoldAdmission> {
        self.hold_with_card(request, None)
    }

    /// Persist the initial display body with the call so a crash before the network send
    /// never leaves filing recovery dependent on a fresh catalog or private-content query.
    pub fn hold_with_card(
        &self,
        request: HoldRequest,
        card: Option<&Card>,
    ) -> Result<HoldAdmission> {
        self.terminal_write(|tx, terminal| {
            if let Some(card) = card {
                if card.dedup_key != request.dedup_key
                    || card.scope != request.scope
                    || card.agent_id != request.agent_id
                    || card.provider != request.provider
                    || card.project_id != request.project_id
                    || card.state != CardState::Filing
                    || card.card_id.is_some()
                {
                    return Err(GrantsError(
                        "initial consent card does not match held call".into(),
                    ));
                }
            }
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
            if let Some(card) = card {
                tx.execute(
                    "INSERT INTO cards VALUES (?1,?2) ON CONFLICT(dedup_key) DO NOTHING",
                    params![card.dedup_key, encode(card)?],
                )?;
            }
            tx.execute(
                "DELETE FROM late_results WHERE batch_id=?1",
                [&request.batch_id],
            )?;
            let replaced: Option<i64> = tx
                .query_row(
                    "SELECT admitted_at_ms FROM held_calls WHERE batch_id=?1",
                    [&request.batch_id],
                    |row| row.get(0),
                )
                .optional()?;
            tx.execute(
                "DELETE FROM held_calls WHERE batch_id=?1",
                [&request.batch_id],
            )?;
            if let Some(admitted_at) = replaced {
                terminal.push((request.batch_id.clone(), admitted_at));
            }
            let row = HeldCall {
                batch_id: request.batch_id,
                project_id: request.project_id,
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
            let mut row = row;
            if card.is_some() {
                if let Some(existing) = read_card(tx, &row.dedup_key)? {
                    if existing.card_id.is_some() {
                        row.card_id = existing.card_id;
                        row.state = HeldState::Held;
                        save_call(tx, &row)?;
                    }
                }
            }
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

    /// Final calls remain discoverable while their cards may still receive a late answer.
    pub fn card_calls(&self, dedup_key: &str, now_ms: i64) -> Result<Vec<HeldCall>> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        let mut stmt = conn.prepare("SELECT body FROM held_calls WHERE dedup_key=?1 AND admitted_at_ms>?2 ORDER BY admitted_at_ms")?;
        let rows = stmt.query_map(
            params![dedup_key, now_ms.saturating_sub(RETENTION_MS)],
            |r| r.get::<_, String>(0),
        )?;
        rows.map(|r| decode(&r?)).collect()
    }

    pub fn pending_cards(&self) -> Result<Vec<Card>> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        let mut stmt = conn.prepare("SELECT body FROM cards ORDER BY dedup_key")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        let cards: Vec<Card> = rows.map(|r| decode(&r?)).collect::<Result<_>>()?;
        Ok(cards
            .into_iter()
            .filter(|card| matches!(card.state, CardState::Filing | CardState::Pending))
            .collect())
    }

    /// A requester refusal means there is no card to wait on and mints no run denial.
    pub fn refuse_card(&self, dedup_key: &str, body: &str, now_ms: i64) -> Result<()> {
        self.terminal_write(|tx, terminal| {
            let Some(card) = read_card(tx, dedup_key)? else {
                return Ok(());
            };
            if matches!(card.state, CardState::Allowed | CardState::Denied) {
                return Ok(());
            }
            let mut stmt = tx.prepare(
                "SELECT body FROM held_calls WHERE dedup_key=?1 AND state IN ('filing','held')",
            )?;
            let rows = stmt
                .query_map([dedup_key], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            for row in rows {
                finalize(
                    tx,
                    decode(&row)?,
                    FinalKind::NotStarted,
                    body,
                    true,
                    now_ms,
                    terminal,
                )?;
            }
            tx.execute("DELETE FROM cards WHERE dedup_key=?1", [dedup_key])?;
            Ok(())
        })
    }

    pub fn generation(&self, agent_id: &str, provider: &str) -> Result<u64> {
        read_generation(
            &self.conn.lock().unwrap_or_else(|p| p.into_inner()),
            agent_id,
            provider,
        )
    }

    pub fn grants(&self) -> Result<Vec<Grant>> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        let mut stmt = conn.prepare("SELECT body FROM grants ORDER BY id")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.map(|r| decode(&r?)).collect()
    }

    /// The grants `holder` has for `provider`, selected by their columns so a consent check
    /// reads only the grants that could release its call.
    pub fn grants_for(&self, holder: &str, provider: &str) -> Result<Vec<Grant>> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        let mut stmt =
            conn.prepare("SELECT body FROM grants WHERE holder=?1 AND provider=?2 ORDER BY id")?;
        let rows = stmt.query_map([holder, provider], |r| r.get::<_, String>(0))?;
        rows.map(|r| decode(&r?)).collect()
    }

    /// The grant an answer to the consent card `card_id` minted, if it still exists.
    pub fn grant_from_card(&self, card_id: &str) -> Result<Option<Grant>> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        let body: Option<String> = conn
            .query_row(
                "SELECT body FROM grants WHERE source_card_id=?1",
                [card_id],
                |r| r.get(0),
            )
            .optional()?;
        body.map(|body| decode(&body)).transpose()
    }

    pub fn denied(&self, agent_id: &str, scope: &ScopeIdentity, provider: &str) -> Result<bool> {
        read_denied(
            &self.conn.lock().unwrap_or_else(|p| p.into_inner()),
            agent_id,
            scope,
            provider,
        )
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

    /// Move the consent generation of `agent_id` and `provider` past `from`, unless another
    /// caller already moved it. Returns the generation now current, so concurrent callers that
    /// saw the same expired card agree on one new card key.
    pub fn advance_generation(&self, agent_id: &str, provider: &str, from: u64) -> Result<u64> {
        self.write(|tx| {
            let current = read_generation(tx, agent_id, provider)?;
            if current != from {
                return Ok(current);
            }
            let next = from.saturating_add(1);
            tx.execute(
                "INSERT INTO generations VALUES (?1,?2,?3) ON CONFLICT(agent_id,provider) DO UPDATE SET generation=excluded.generation",
                params![agent_id, provider, integer(next)?],
            )?;
            Ok(next)
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
        self.apply_answers(scope, Some(cursor), answers, now_ms)
    }

    /// An await result settles a card without advancing the recovery feed cursor.
    pub fn apply_answer(
        &self,
        scope: &ScopeIdentity,
        answer: CardAnswer,
        now_ms: i64,
    ) -> Result<()> {
        self.apply_answers(scope, None, &[answer], now_ms)
    }

    /// Commit a correlated group of decisions before acknowledging a feed page that also
    /// contains terminal card records. A failed terminal cleanup must leave that page readable.
    pub fn apply_answer_batch(
        &self,
        scope: &ScopeIdentity,
        answers: &[CardAnswer],
        now_ms: i64,
    ) -> Result<()> {
        self.apply_answers(scope, None, answers, now_ms)
    }

    fn apply_answers(
        &self,
        scope: &ScopeIdentity,
        cursor: Option<&str>,
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
                // A recovery answer carries the id of the card that was decided. Store that id
                // with the decision, even if recovery filed a replacement card meanwhile; cards
                // already allowed or denied returned above.
                card.card_id = Some(answer.card_id.clone());
                match answer.decision {
                    Decision::Deny => {
                        card.state = CardState::Denied;
                        tx.execute("INSERT INTO denies VALUES (?1,?2,?3,?4) ON CONFLICT(agent_id,scope,provider) DO NOTHING", params![card.agent_id, encode(scope)?, card.provider, answer.card_id])?;
                    }
                    Decision::AllowRun | Decision::Allow24h | Decision::AllowAlways => {
                        let always = matches!(answer.decision, Decision::AllowAlways);
                        if always && card.project_id.is_none() { continue; }
                        card.state = CardState::Allowed;
                        let run = matches!(answer.decision, Decision::AllowRun);
                        if !run || answer.run_live {
                            let grant = Grant {
                                id: answer.card_id.clone(), holder: card.agent_id.clone(), project: if always { card.project_id.clone() } else { None },
                                provider: card.provider.clone(), kind: if run { GrantKind::Run } else if always { GrantKind::Always } else { GrantKind::Hours24 },
                                scope: run.then(|| scope.clone()), source_card_id: answer.card_id.clone(),
                                created_at_ms: now_ms,
                                expires_at_ms: matches!(answer.decision, Decision::Allow24h).then(|| now_ms.saturating_add(24 * 60 * 60 * 1000)),
                            };
                            tx.execute("INSERT INTO grants VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(source_card_id) DO NOTHING", params![grant.id, grant.holder, grant.project, grant.provider, if run { "run" } else if always { "always" } else { "24_hours" }, grant.scope.as_ref().map(encode).transpose()?, grant.source_card_id, grant.created_at_ms, grant.expires_at_ms, encode(&grant)?])?;
                        }
                    }
                }
                tx.execute("UPDATE cards SET body=?2 WHERE dedup_key=?1", params![card.dedup_key, encode(&card)?])?;
                let mut stmt = tx.prepare("SELECT body FROM held_calls WHERE dedup_key=?1 AND state IN ('filing','held')")?;
                let rows = stmt.query_map([&card.dedup_key], |r| r.get::<_, String>(0))?.collect::<std::result::Result<Vec<_>, _>>()?;
                for body in rows {
                    let mut row: HeldCall = decode(&body)?;
                    row.card_id = card.card_id.clone();
                    row.state = HeldState::Held;
                    save_call(tx, &row)?;
                }
            }
            if let Some(cursor) = cursor {
                tx.execute("INSERT INTO cursors VALUES (?1,?2) ON CONFLICT(scope) DO UPDATE SET cursor=excluded.cursor", params![encode(scope)?, cursor])?;
            }
            Ok(())
        })
    }

    /// Move a held call to authorized under the grant `grant_id`. The deadline, the grant and
    /// the consent generation are checked in the write transaction that moves the call, so a
    /// revoke or deny that committed first leaves the call waiting, even when the caller read
    /// the grant before a slow liveness check. `generation` is the caller's reading of
    /// [`Self::generation`], taken before it read the grant: any revoke since changes it.
    pub fn authorize(
        &self,
        batch_id: &str,
        grant_id: &str,
        generation: u64,
        now_ms: i64,
    ) -> Result<bool> {
        self.write(|tx| {
            let Some(mut row) = read_call(tx, batch_id, now_ms)? else {
                return Ok(false);
            };
            if row.state != HeldState::Held || now_ms >= row.deadline_at_ms {
                return Ok(false);
            }
            let grant: Option<String> = tx
                .query_row("SELECT body FROM grants WHERE id=?1", [grant_id], |r| {
                    r.get(0)
                })
                .optional()?;
            let Some(grant) = grant.map(|body| decode::<Grant>(&body)).transpose()? else {
                return Ok(false);
            };
            let covers = grant.holder == row.agent_id
                && grant.provider == row.provider
                && grant.expires_at_ms.is_none_or(|expiry| now_ms < expiry)
                && match grant.kind {
                    GrantKind::Run => grant.scope.as_ref() == Some(&row.scope),
                    GrantKind::Hours24 => true,
                    GrantKind::Always => {
                        row.project_id.is_some() && grant.project == row.project_id
                    }
                };
            if !covers
                || read_generation(tx, &row.agent_id, &row.provider)? != generation
                || read_denied(tx, &row.agent_id, &row.scope, &row.provider)?
            {
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
        self.terminal_write(|tx, terminal| {
            let Some(row) = read_call(tx, batch_id, now_ms)? else {
                return Ok(false);
            };
            if row.state != HeldState::Authorized {
                return Ok(false);
            }
            finalize(tx, row, FinalKind::Result, body, is_error, now_ms, terminal)
        })
    }

    pub fn finish_unstarted(&self, batch_id: &str, body: &str, now_ms: i64) -> Result<bool> {
        self.terminal_write(|tx, terminal| {
            let Some(row) = read_call(tx, batch_id, now_ms)? else {
                return Ok(false);
            };
            if !matches!(row.state, HeldState::Filing | HeldState::Held) {
                return Ok(false);
            }
            finalize(tx, row, FinalKind::NotStarted, body, true, now_ms, terminal)
        })
    }

    /// End a call still waiting for consent at its deadline. A call whose card filing never
    /// resolved (still `Filing`, possibly without a card id) has the same deadline as one
    /// waiting on a filed card, and ends the same way.
    pub fn expire_held(&self, batch_id: &str, body: &str, now_ms: i64) -> Result<bool> {
        self.terminal_write(|tx, terminal| {
            let Some(row) = read_call(tx, batch_id, now_ms)? else {
                return Ok(false);
            };
            if !matches!(row.state, HeldState::Filing | HeldState::Held)
                || now_ms < row.deadline_at_ms
            {
                return Ok(false);
            }
            finalize(tx, row, FinalKind::NotStarted, body, true, now_ms, terminal)
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

    /// The retained calls in `state`, selected by the state column so rows in other states are
    /// never read or decoded.
    pub fn calls_in_state(&self, state: HeldState, now_ms: i64) -> Result<Vec<HeldCall>> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        let mut stmt = conn.prepare(
            "SELECT body FROM held_calls WHERE state=?1 AND admitted_at_ms>?2 ORDER BY batch_id",
        )?;
        let rows = stmt.query_map(
            params![state_name(state), now_ms.saturating_sub(RETENTION_MS)],
            |r| r.get::<_, String>(0),
        )?;
        rows.map(|r| decode(&r?)).collect()
    }

    /// The ids of calls still waiting for consent whose deadline has passed, read from the
    /// state and deadline columns alone: the deadline timer runs every second and must not
    /// decode the request of every call that is still within its ten minutes.
    pub fn due_waiting(&self, now_ms: i64) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        let mut stmt = conn.prepare(
            "SELECT batch_id FROM held_calls WHERE state IN ('filing','held') AND deadline_at_ms<=?1 AND admitted_at_ms>?2 ORDER BY batch_id",
        )?;
        let rows = stmt.query_map(params![now_ms, now_ms.saturating_sub(RETENTION_MS)], |r| {
            r.get::<_, String>(0)
        })?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    pub fn late_head(&self, principal: &str) -> Result<u64> {
        let conn = self.conn.lock().unwrap_or_else(|p| p.into_inner());
        Ok(conn
            .query_row(
                "SELECT seq FROM late_cursors WHERE principal=?1",
                [broca_protocol::canonical_owner(principal)],
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
        let principal = broca_protocol::canonical_owner(principal);
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
                params![
                    broca_protocol::canonical_owner(principal),
                    integer(through)?
                ],
            )?)
        })
    }

    pub fn sweep(&self, now_ms: i64) -> Result<usize> {
        self.terminal_write(|tx, terminal| {
            tx.execute("DELETE FROM late_results WHERE expires_at_ms<=?1", [now_ms])?;
            terminal.extend(
                tx.prepare(
                    "SELECT batch_id,admitted_at_ms FROM held_calls WHERE admitted_at_ms<=?1",
                )?
                .query_map([now_ms.saturating_sub(RETENTION_MS)], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?,
            );
            let swept = tx.execute(
                "DELETE FROM held_calls WHERE admitted_at_ms<=?1",
                [now_ms.saturating_sub(RETENTION_MS)],
            )?;
            // A 24-hour grant stays for the retention period after it expired, as the held calls
            // it may have released do, and then goes.
            tx.execute(
                "DELETE FROM grants WHERE expires_at_ms IS NOT NULL AND expires_at_ms<=?1",
                [now_ms.saturating_sub(RETENTION_MS)],
            )?;
            // An answered card is kept while a held call names it or the grant its answer
            // minted exists: it stops a replayed answer from minting that grant again and lets
            // a new call see the answer. Once neither is left, nothing reads it. Answer intake
            // ignores records for a card it does not have, so a late replay cannot recreate one.
            tx.execute(
                "DELETE FROM cards WHERE json_extract(body,'$.state') IN ('allowed','denied')
                 AND NOT EXISTS (SELECT 1 FROM held_calls h WHERE h.dedup_key=cards.dedup_key)
                 AND NOT EXISTS (SELECT 1 FROM grants g
                     WHERE g.source_card_id=json_extract(cards.body,'$.card_id'))",
                [],
            )?;
            Ok(swept)
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

fn read_generation(conn: &Connection, agent_id: &str, provider: &str) -> Result<u64> {
    Ok(conn
        .query_row(
            "SELECT generation FROM generations WHERE agent_id=?1 AND provider=?2",
            params![agent_id, provider],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(0))
}

fn read_denied(
    conn: &Connection,
    agent_id: &str,
    scope: &ScopeIdentity,
    provider: &str,
) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM denies WHERE agent_id=?1 AND scope=?2 AND provider=?3",
            params![agent_id, encode(scope)?, provider],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
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

fn state_name(state: HeldState) -> &'static str {
    match state {
        HeldState::Filing => "filing",
        HeldState::Held => "held",
        HeldState::Authorized => "authorized",
        HeldState::Final => "final",
    }
}

fn save_call(tx: &Transaction<'_>, row: &HeldCall) -> Result<()> {
    let state = state_name(row.state);
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
    terminal: &mut Vec<(String, i64)>,
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
    // Keyed by the canonical spelling, so a custodian reads its results whichever of its ids
    // (across a module rename) the call was admitted under or it reads with.
    let principal = broca_protocol::canonical_owner(&row.principal);
    tx.execute(
        "INSERT INTO late_results (batch_id,principal,expires_at_ms) VALUES (?1,?2,?3)",
        params![
            row.batch_id,
            principal,
            row.admitted_at_ms.saturating_add(RETENTION_MS)
        ],
    )?;
    let seq = tx.last_insert_rowid();
    tx.execute("INSERT INTO late_cursors VALUES (?1,?2) ON CONFLICT(principal) DO UPDATE SET seq=excluded.seq", params![principal, seq])?;
    terminal.push((row.batch_id, row.admitted_at_ms));
    Ok(true)
}

#[cfg(test)]
mod bootstrap_tests {
    use super::*;
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
    use std::path::PathBuf;

    fn path(name: &str) -> PathBuf {
        let root = broca_test_support::scratch_root().join(name);
        broca_owner_only::create_dir_all(&root).unwrap();
        root.join("grants.db")
    }

    #[test]
    fn grants_cold_bootstrap_matches_legacy_schema() {
        let path = path("grants-bootstrap-schema");
        drop(GrantsStore::open(&path).unwrap());
        let conn = Connection::open(path).unwrap();
        let actual: String = conn
            .query_row(
                "SELECT group_concat(type || '|' || name || '|' || tbl_name || '|' || coalesce(sql, ''), char(10))
                 FROM (SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name)",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let legacy = include_str!("../tests/fixtures/grants-bootstrap.sqlite-master");
        assert_eq!(
            actual.lines().collect::<Vec<_>>(),
            legacy.lines().collect::<Vec<_>>()
        );
    }

    #[test]
    fn interrupted_grants_cold_bootstrap_leaves_no_schema() {
        let path = path("grants-bootstrap-interrupted");
        crate::create_private_sqlite(&path).unwrap();
        let mut conn = Connection::open(&path).unwrap();
        conn.authorizer(Some(|context: AuthContext<'_>| match context.action {
            AuthAction::CreateTable {
                table_name: "late_cursors",
            } => Authorization::Deny,
            _ => Authorization::Allow,
        }));
        let error = GrantsStore::initialize_schema(&mut conn).unwrap_err();
        assert!(error.0.contains("not authorized"), "{error}");
        drop(conn);
        let conn = Connection::open(&path).unwrap();
        let objects: i64 = conn
            .query_row("SELECT count(*) FROM sqlite_master", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            objects, 0,
            "an interrupted final CREATE must roll back earlier objects"
        );
        drop(conn);
        GrantsStore::open(&path).unwrap();
    }
}

#[cfg(test)]
mod terminal_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn held_call_terminal_listeners_run_after_commit_without_holding_store_locks() {
        let store = Arc::new(GrantsStore::open_in_memory().unwrap());
        let weak = Arc::downgrade(&store);
        let observed = Arc::new(AtomicUsize::new(0));
        let count = observed.clone();
        let listener: Arc<HeldCallTerminal> = Arc::new(move |key, admitted_at| {
            assert_eq!((key, admitted_at), ("call", 1000));
            assert!(weak.upgrade().unwrap().lookup(key, 1000).unwrap().is_none());
            count.fetch_add(1, Ordering::SeqCst);
        });
        store.on_held_call_terminal(&listener);
        let failed: Result<()> = store.terminal_write(|_, terminal| {
            terminal.push(("call".into(), 1000));
            Err(GrantsError("rollback".into()))
        });
        assert!(failed.is_err());
        assert_eq!(observed.load(Ordering::SeqCst), 0);
        store
            .terminal_write(|_, terminal| {
                terminal.push(("call".into(), 1000));
                Ok(())
            })
            .unwrap();
        assert_eq!(observed.load(Ordering::SeqCst), 1);
        drop(listener);
        store
            .terminal_write(|_, terminal| {
                terminal.push(("call".into(), 1000));
                Ok(())
            })
            .unwrap();
        assert_eq!(observed.load(Ordering::SeqCst), 1);
    }
}
