//! Durable consent filing and scoped answer intake. The network request and its persisted
//! reply share a per-card lock: a delayed filing reply cannot overwrite a replacement.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use broca_protocol::{BindIdentity, SessionScope};
use broca_store::grants::{
    Card, CardAnswer, CardState, Decision, GrantKind, GrantsError, GrantsStore, HeldCall,
    HeldState, HoldAdmission, HoldRequest, ScopeIdentity,
};
use broca_subc::{
    AnswersPage, ConsentClient, ConsentError, ConsentRecord, ConsentState, RouteOpenRetry,
    ScopeDescriber, SubcConnection,
};
use serde_json::{json, Value};

use super::held::not_started;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunLiveness {
    Live,
    Ended,
    Unknown,
}

#[async_trait::async_trait]
pub trait ConsentIo: Send + Sync {
    async fn request(&self, scope: &SessionScope, body: &Value) -> Result<String, ConsentError>;
    async fn answers(&self, scope: &SessionScope, since: u64) -> Result<AnswersPage, ConsentError>;
    async fn wait(&self, scope: &SessionScope, id: &str) -> Result<ConsentRecord, ConsentError>;
    async fn withdraw(&self, scope: &SessionScope, id: &str);
    async fn run_live(&self, scope: &SessionScope) -> RunLiveness;
}

pub struct SubcConsentIo {
    conn: Arc<SubcConnection>,
    identity: BindIdentity,
    describer: Arc<dyn ScopeDescriber>,
}

impl SubcConsentIo {
    pub fn new(
        conn: Arc<SubcConnection>,
        identity: BindIdentity,
        describer: Arc<dyn ScopeDescriber>,
    ) -> Self {
        Self {
            conn,
            identity,
            describer,
        }
    }

    async fn open(&self, scope: &SessionScope) -> Result<ConsentClient, ConsentError> {
        ConsentClient::open(
            self.conn.clone(),
            &self.identity,
            scope,
            RouteOpenRetry::default_patience(),
        )
        .await
    }
}

#[async_trait::async_trait]
impl ConsentIo for SubcConsentIo {
    async fn request(&self, scope: &SessionScope, body: &Value) -> Result<String, ConsentError> {
        let no_expiry = self
            .conn
            .catalog_list()
            .await?
            .iter()
            .any(|entry| supports_no_expiry(&entry.module_id, entry.capabilities.as_ref()));
        if !no_expiry {
            return Err(ConsentError::Refused {
                code: "consent_unavailable".into(),
                message: "Cingulate must advertise consent-no-expiry/v1 for a private-content consent card".into(),
                detail: None,
            });
        }
        let client = self.open(scope).await?;
        client.request(body).await.map(|reply| reply.elicitation_id)
    }

    async fn answers(&self, scope: &SessionScope, since: u64) -> Result<AnswersPage, ConsentError> {
        self.open(scope).await?.answers(since).await
    }

    async fn wait(&self, scope: &SessionScope, id: &str) -> Result<ConsentRecord, ConsentError> {
        self.open(scope).await?.r#await(id, 120_000).await
    }

    async fn withdraw(&self, scope: &SessionScope, id: &str) {
        if let Ok(client) = self.open(scope).await {
            let _ = client.withdraw(id).await;
        }
    }

    async fn run_live(&self, scope: &SessionScope) -> RunLiveness {
        let answer = self
            .describer
            .describe(
                &super::private_content::owner_principal(&scope.owner.as_owner_string()),
                &scope.scope_ref,
            )
            .await;
        use subc_protocol::scope::ScopeStatus;
        match answer {
            Ok(answer) => match answer.status {
                ScopeStatus::Ended => RunLiveness::Ended,
                ScopeStatus::NotLive if answer.owner_synced => RunLiveness::Ended,
                ScopeStatus::Live => match answer.scope_epoch {
                    Some(epoch) if epoch == scope.scope_epoch => RunLiveness::Live,
                    Some(_) => RunLiveness::Ended,
                    None => RunLiveness::Unknown,
                },
                ScopeStatus::NotLive => RunLiveness::Unknown,
            },
            Err(_) => RunLiveness::Unknown,
        }
    }
}

pub struct Consent {
    pub(super) store: Arc<GrantsStore>,
    pub(super) io: Arc<dyn ConsentIo>,
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    feeds: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl Consent {
    pub(super) async fn image_run_live(&self, scope: &SessionScope) -> RunLiveness {
        self.io.run_live(scope).await
    }
    pub fn new(store: Arc<GrantsStore>, io: Arc<dyn ConsentIo>) -> Self {
        Self {
            store,
            io,
            locks: Mutex::new(HashMap::new()),
            feeds: Mutex::new(HashMap::new()),
            clock: Arc::new(super::held::epoch_ms),
        }
    }

    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.clock = clock;
        self
    }

    fn key_lock(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.locks
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(key.into())
            .or_default()
            .clone()
    }

    fn reading_open(&self, key: &str, now: i64) -> Result<bool, GrantsError> {
        Ok(self
            .store
            .card_calls(key, now)?
            .last()
            .is_some_and(|row| now < row.deadline_at_ms.saturating_add(86_400_000)))
    }

    pub fn dedup_key(
        &self,
        agent: &str,
        provider: &str,
        scope: &ScopeIdentity,
    ) -> Result<String, GrantsError> {
        Ok(super::admission::hash(
            &json!({"agent_id":agent,"provider":provider,"owner":scope.owner,
            "scope_ref":scope.scope_ref,"scope_epoch":scope.scope_epoch,"generation":self.store.generation(agent,provider)?}),
        ))
    }

    pub async fn granted(
        &self,
        scope: &SessionScope,
        provider: &str,
        now: i64,
    ) -> Result<bool, GrantsError> {
        let Some(agent) = scope.agent_id.as_deref() else {
            return Ok(false);
        };
        let identity = identity(scope);
        if self.store.denied(agent, &identity, provider)? {
            return Ok(false);
        }
        for grant in self.store.grants()? {
            if grant.holder != agent || grant.provider != provider {
                continue;
            }
            match grant.kind {
                GrantKind::Hours24 if grant.expires_at_ms.is_some_and(|expiry| now < expiry) => {
                    return Ok(true)
                }
                GrantKind::Run
                    if grant.scope.as_ref() == Some(&identity)
                        && self.io.run_live(scope).await == RunLiveness::Live =>
                {
                    return Ok(true)
                }
                _ => (),
            }
        }
        Ok(false)
    }

    /// A grant can settle while another card's waiter is asleep, or just before a process
    /// exits. Reconcile waiting rows from the durable authority before resuming their waits.
    pub async fn resume_granted(&self, now: i64) -> Result<usize, GrantsError> {
        let mut authorized = 0;
        for row in self.store.recovering(now)? {
            if row.state != HeldState::Held {
                continue;
            }
            let observed = (self.clock)();
            if observed >= row.deadline_at_ms {
                self.store
                    .expire_held(&row.batch_id, &not_started("pending"), observed)?;
            } else if self
                .store
                .denied(&row.agent_id, &row.scope, &row.provider)?
            {
                self.store
                    .finish_unstarted(&row.batch_id, &not_started("denied"), observed)?;
            } else if self
                .granted(&session(&row), &row.provider, observed)
                .await?
            {
                authorized += usize::from(self.store.authorize(&row.batch_id, (self.clock)())?);
            }
        }
        Ok(authorized)
    }

    /// Admission writes both the validated request and the initial card body before filing.
    pub async fn file(
        &self,
        request: HoldRequest,
        body: Value,
        now: i64,
    ) -> Result<HeldCall, GrantsError> {
        self.file_with_admission(request, body, now, |_| {}).await
    }

    pub(super) async fn file_with_admission(
        &self,
        request: HoldRequest,
        body: Value,
        now: i64,
        admitted: impl FnOnce(&HeldCall) + Send,
    ) -> Result<HeldCall, GrantsError> {
        let lock = self.key_lock(&request.dedup_key);
        let filing_guard = lock.lock().await;
        let recovering = self
            .store
            .card(&request.dedup_key)?
            .is_some_and(|card| card.state == CardState::Filing);
        let card = Card {
            dedup_key: request.dedup_key.clone(),
            scope: request.scope.clone(),
            agent_id: request.agent_id.clone(),
            provider: request.provider.clone(),
            content: body.to_string(),
            card_id: None,
            state: CardState::Filing,
        };
        let row = match self.store.hold_with_card(request, Some(&card))? {
            HoldAdmission::Inserted(row) => row,
            HoldAdmission::Replay(row) => return Ok(row),
            HoldAdmission::ScopeMismatch => {
                return Err(GrantsError("held call scope mismatch".into()))
            }
            HoldAdmission::ArgsMismatch => {
                return Err(GrantsError("held call argument reuse".into()))
            }
        };
        // Install process-local resources only for the first persisted admission, before
        // filing yields to the consent transport. Replays never replace the first snapshot.
        admitted(&row);
        let current = self
            .store
            .card(&card.dedup_key)?
            .expect("hold persisted card");
        if current.state == CardState::Pending && current.content == card.content {
            self.store.put_card(&current)?;
        } else if matches!(current.state, CardState::Allowed | CardState::Denied) {
            self.settle_calls(&current, now)?;
        } else {
            self.store.put_card(&card)?;
            if recovering {
                // Recovery takes the scope feed lock before any card lock. Save the desired
                // replacement body, then release this key before entering that lock order.
                drop(filing_guard);
                self.recover(&card.dedup_key, now).await?;
            } else {
                self.send(&card, &session(&row), now, false).await?;
            }
        }
        self.store
            .lookup(&row.batch_id, now)?
            .ok_or_else(|| GrantsError("new held call disappeared".into()))
    }

    async fn send(
        &self,
        card: &Card,
        scope: &SessionScope,
        _now: i64,
        recovering: bool,
    ) -> Result<(), GrantsError> {
        let body = serde_json::from_str(&card.content).map_err(|e| GrantsError(e.to_string()))?;
        match self.io.request(scope, &body).await {
            Ok(id) => {
                let mut filed = card.clone();
                filed.card_id = Some(id.clone());
                filed.state = if recovering {
                    CardState::Filing
                } else {
                    CardState::Pending
                };
                if !self.store.put_card(&filed)? {
                    self.io.withdraw(scope, &id).await;
                }
            }
            Err(ConsentError::Refused {
                code,
                message,
                detail,
            }) => {
                // The caller only sees `consent_unavailable`, so Cingulate's own reason is
                // logged here; without it an operator cannot tell a card shape the consent
                // module rejects from one it was never asked about.
                tracing::warn!(
                    target: "llm_tools.consent",
                    %code,
                    %message,
                    detail = %detail.map(|d| d.to_string()).unwrap_or_default(),
                    dedup_key = %card.dedup_key,
                    "consent module refused the approval card; the held call ends consent_unavailable"
                );
                // A refusal is proof that no card was filed. Transport and unreadable replies
                // are different: they retain filing authority until the recovery feed is read.
                self.store.refuse_card(
                    &card.dedup_key,
                    &not_started("consent_unavailable"),
                    (self.clock)(),
                )?;
            }
            Err(_) => (),
        }
        Ok(())
    }

    /// Read the recovery feed before refiling, then persist the reply before reading the feed
    /// again. The second read can find an older card answered while the refile was in flight.
    pub async fn recover(&self, key: &str, now: i64) -> Result<(), GrantsError> {
        let rows = self.store.card_calls(key, now)?;
        let Some(row) = rows.last() else {
            return Ok(());
        };
        let scope = session(row);
        let feed_key =
            serde_json::to_string(&identity(&scope)).map_err(|e| GrantsError(e.to_string()))?;
        let feed = self
            .feeds
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(feed_key)
            .or_default()
            .clone();
        // A feed page can settle several keys. Taking the scope lock first prevents two
        // recoveries from each owning one key while waiting for the other's key.
        let _feed_guard = feed.lock().await;
        let lock = self.key_lock(key);
        let _guard = lock.lock().await;
        if self.store.card(key)?.is_none() {
            return Ok(());
        }
        if !self.reading_open(key, (self.clock)())? {
            return Ok(());
        }
        if !self.scan(&scope, key, now).await? {
            return Ok(());
        }
        let Some(card) = self.store.card(key)? else {
            return Ok(());
        };
        if card.state == CardState::Filing {
            self.send(&card, &scope, now, true).await?;
            // The returned id is durable, but the second lookup is still owed across a crash
            // or transport failure. Only a complete lookup can promote this card to pending.
            let lookup_complete = self.scan(&scope, key, now).await?;
            if !lookup_complete {
                return Ok(());
            }
            if let Some(mut current) = self.store.card(key)? {
                if current.state == CardState::Filing && current.card_id.is_some() {
                    current.state = CardState::Pending;
                    self.store.put_card(&current)?;
                }
            }
        }
        if let Some(card) = self.store.card(key)? {
            self.settle_calls(&card, (self.clock)())?;
        }
        Ok(())
    }

    async fn scan(
        &self,
        scope: &SessionScope,
        active_key: &str,
        _now: i64,
    ) -> Result<bool, GrantsError> {
        let identity = identity(scope);
        let mut since = self
            .store
            .answer_cursor(&identity)?
            .and_then(|cursor| cursor.parse().ok())
            .unwrap_or(0);
        loop {
            let Ok(page) = self.io.answers(scope, since).await else {
                return Ok(false);
            };
            if page.cursor < since
                || page
                    .continuation
                    .is_some_and(|next| next <= since || next != page.cursor)
            {
                return Ok(false);
            }
            let mut records = Vec::new();
            let mut keys = std::collections::BTreeSet::new();
            for value in page.records {
                let Ok(record) = serde_json::from_value::<ConsentRecord>(value) else {
                    return Ok(false);
                };
                let Some(key) = record.fields.get("requester_ref").and_then(Value::as_str) else {
                    continue;
                };
                if self
                    .store
                    .card(key)?
                    .is_some_and(|card| card.scope == identity)
                {
                    keys.insert(key.to_owned());
                    records.push(record);
                }
            }
            let mut key_guards = Vec::new();
            for key in keys {
                if key != active_key {
                    key_guards.push(self.key_lock(&key).lock_owned().await);
                }
            }
            let mut answers = Vec::new();
            let mut duplicates = Vec::new();
            let mut terminal = Vec::new();
            for record in records {
                let key = record.fields["requester_ref"]
                    .as_str()
                    .expect("correlated record");
                let Some(card) = self.store.card(key)? else {
                    continue;
                };
                if card.scope != identity {
                    continue;
                }
                let answer_in_window = self.reading_open(key, (self.clock)())?;
                if !answer_in_window {
                    continue;
                }
                if matches!(
                    record.state,
                    ConsentState::Withdrawn | ConsentState::Expired
                ) {
                    terminal.push((key.to_owned(), record));
                    continue;
                }
                if let Some(answer) = self.answer(&card, scope, &record).await {
                    if let Some(id) = &card.card_id {
                        if id != &record.elicitation_id {
                            duplicates.push(id.clone());
                        }
                    }
                    answers.push(answer);
                }
            }
            let decided_at = (self.clock)();
            self.store
                .apply_answer_batch(&identity, &answers, decided_at)?;
            for answer in &answers {
                if let Some(card) = self.store.card(&answer.dedup_key)? {
                    if card.card_id.as_deref() != Some(&answer.card_id) {
                        duplicates.push(answer.card_id.clone());
                    }
                    self.settle_calls(&card, decided_at)?;
                }
            }
            // An explicit decision, including one for an older id adopted during recovery,
            // takes precedence over a replacement card's expiry on the same feed page.
            for (key, record) in terminal {
                if let Some(card) = self.store.card(&key)? {
                    self.finish_terminal_card(&card, &record, decided_at)?;
                }
            }
            self.store
                .apply_answer_page(&identity, &page.cursor.to_string(), &[], decided_at)?;
            for id in duplicates {
                self.io.withdraw(scope, &id).await;
            }
            match page.continuation {
                Some(next) => since = next,
                None => return Ok(true),
            }
        }
    }

    async fn answer(
        &self,
        card: &Card,
        scope: &SessionScope,
        record: &ConsentRecord,
    ) -> Option<CardAnswer> {
        if record.state != ConsentState::Answered
            || record.fields.get("requester_ref").and_then(Value::as_str)
                != Some(card.dedup_key.as_str())
        {
            return None;
        }
        let decision = match record.fields.get("answered_choice_id")?.as_str()? {
            "allow_run" => Decision::AllowRun,
            "allow_24h" => Decision::Allow24h,
            "deny" => Decision::Deny,
            _ => return None,
        };
        let run_live = !matches!(decision, Decision::AllowRun)
            || self.io.run_live(scope).await == RunLiveness::Live;
        Some(CardAnswer {
            dedup_key: card.dedup_key.clone(),
            card_id: record.elicitation_id.clone(),
            decision,
            run_live,
        })
    }

    fn finish_terminal_card(
        &self,
        card: &Card,
        record: &ConsentRecord,
        now: i64,
    ) -> Result<bool, GrantsError> {
        let reason = match record.state {
            ConsentState::Withdrawn => "withdrawn",
            ConsentState::Expired => "expired",
            _ => return Ok(false),
        };
        // A terminal record for a replaced card must not settle calls waiting on its
        // replacement. Answered cards keep their committed decision as well.
        if card.card_id.as_deref() != Some(&record.elicitation_id)
            || matches!(card.state, CardState::Allowed | CardState::Denied)
        {
            return Ok(false);
        }
        self.store
            .refuse_card(&card.dedup_key, &not_started(reason), now)?;
        Ok(true)
    }

    /// Ordinary answer intake uses await, never the recovery feed. A replaced id cannot
    /// authorize the surviving card even if an old waiter wakes after the replacement.
    pub async fn wait_once(&self, key: &str, now: i64) -> Result<bool, GrantsError> {
        let Some(card) = self.store.card(key)? else {
            return Ok(false);
        };
        let rows = self.store.card_calls(key, now)?;
        let Some(row) = rows.last() else {
            return Ok(false);
        };
        if now >= row.deadline_at_ms.saturating_add(86_400_000) {
            return Ok(false);
        }
        let Some(id) = card.card_id.as_deref() else {
            return Ok(true);
        };
        let scope = session(row);
        let record = match self.io.wait(&scope, id).await {
            Ok(record) => record,
            Err(ConsentError::Refused { code, .. })
                if matches!(
                    code.as_str(),
                    "scope_not_live" | "scope_epoch_mismatch" | "scope_unverifiable"
                ) =>
            {
                return Ok(false)
            }
            Err(_) => return Ok(true),
        };
        if record.elicitation_id != id {
            return Ok(true);
        }
        // Await may have consumed the full two-minute bound. Authorization must compare
        // the decision against the clock after the reply, not the time the wait began.
        let lock = self.key_lock(key);
        let _guard = lock.lock().await;
        let Some(current) = self.store.card(key)? else {
            return Ok(false);
        };
        // A wait can cross the late-answer cutoff. Re-read the last deadline because another
        // call may have joined this card while the waiter was asleep.
        let reading_closed = !self.reading_open(key, (self.clock)())?;
        if reading_closed {
            return Ok(false);
        }
        if current.card_id != card.card_id {
            self.io.withdraw(&scope, id).await;
            return Ok(true);
        }
        if self.finish_terminal_card(&current, &record, (self.clock)())? {
            return Ok(false);
        }
        if let Some(answer) = self.answer(&current, &scope, &record).await {
            self.store
                .apply_answer(&current.scope, answer, (self.clock)())?;
        }
        let current = self.store.card(key)?.unwrap_or(current);
        self.settle_calls(&current, (self.clock)())?;
        Ok(matches!(
            current.state,
            CardState::Pending | CardState::Filing
        ))
    }

    fn settle_calls(&self, card: &Card, now: i64) -> Result<(), GrantsError> {
        for row in self.store.card_calls(&card.dedup_key, now)? {
            match card.state {
                CardState::Denied => {
                    let outcome = if now >= row.deadline_at_ms {
                        "pending"
                    } else {
                        "denied"
                    };
                    self.store
                        .finish_unstarted(&row.batch_id, &not_started(outcome), now)?;
                }
                CardState::Allowed => {
                    let admitted = self.store.grants()?.iter().any(|grant| {
                        grant.source_card_id == card.card_id.as_deref().unwrap_or("")
                            && ((grant.kind == GrantKind::Hours24
                                && grant.expires_at_ms.is_some_and(|expiry| now < expiry))
                                || (grant.kind == GrantKind::Run
                                    && grant.scope.as_ref() == Some(&row.scope)))
                    });
                    if admitted {
                        self.store.authorize(&row.batch_id, now)?;
                    }
                }
                _ => (),
            }
        }
        Ok(())
    }
}

pub(super) fn identity(scope: &SessionScope) -> ScopeIdentity {
    ScopeIdentity {
        owner: scope.owner.as_owner_string(),
        scope_ref: scope.scope_ref.clone(),
        scope_epoch: scope.scope_epoch,
    }
}

fn session(row: &HeldCall) -> SessionScope {
    let run_id = match &row.batch_scope {
        broca_store::system_one::BatchScope::Run { run_id, .. } => Some(run_id.clone()),
        _ => None,
    };
    let owner = if row.scope.owner == "direct" {
        broca_protocol::BindPrincipal::Direct
    } else if let Some(module_id) = row.scope.owner.strip_prefix("reserved:") {
        broca_protocol::BindPrincipal::Reserved {
            module_id: module_id.into(),
        }
    } else {
        broca_protocol::BindPrincipal::Unverified
    };
    SessionScope {
        owner,
        scope_ref: row.scope.scope_ref.clone(),
        scope_epoch: row.scope.scope_epoch,
        agent_id: Some(row.agent_id.clone()),
        run_id,
        flow_id: None,
        sender: None,
    }
}

fn supports_no_expiry(
    module: &str,
    capabilities: Option<&subc_protocol::manifest::CapabilityDeclarations>,
) -> bool {
    module == broca_subc::consent::CONSENT_MODULE
        && capabilities.is_some_and(|caps| {
            caps.provides
                .iter()
                .any(|cap| cap == "consent-no-expiry/v1")
        })
}

#[cfg(test)]
mod tests {
    #[test]
    fn no_expiry_requires_target_capability() {
        let caps = serde_json::from_value(serde_json::json!({"provides":["consent-no-expiry/v1"]}))
            .unwrap();
        assert!(super::supports_no_expiry("cingulate", Some(&caps)));
        assert!(!super::supports_no_expiry("other", Some(&caps)));
        assert!(!super::supports_no_expiry("cingulate", None));
        let empty = serde_json::from_value(serde_json::json!({"provides":[]})).unwrap();
        assert!(!super::supports_no_expiry("cingulate", Some(&empty)));
    }

    #[test]
    fn published_model_labels_never_fall_back_to_ids() {
        use crate::system_one::consent_model_labels;
        let mut row = broca_catalog::ModelBatchRow {
            model_name: Some("Jev 1.13".into()),
            provider_name: Some("TypeSafe".into()),
            kind: Some("decision".into()),
            context_tokens: None,
            attachment: false,
            input_rate_nanos: None,
            output_rate_nanos: None,
        };
        assert_eq!(consent_model_labels(&row), Some(("TypeSafe", "Jev 1.13")));
        row.provider_name = None;
        assert_eq!(consent_model_labels(&row), None);
        row.provider_name = Some(" ".into());
        assert_eq!(consent_model_labels(&row), None);
        row.provider_name = Some("TypeSafe".into());
        row.model_name = None;
        assert_eq!(consent_model_labels(&row), None);
    }
}
