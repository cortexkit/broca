include!("abandoned_call_tests.rs");
use super::*;
use broca_protocol::{PauseReason, RunFinishReason};
use broca_session_store::record::{
    BatchEnd, BatchStart, CallOutcome, CancelPoint, CancelRequest, Pause, Record, RunEnd, RunStart,
    ToolHeld, ToolIntent, ToolOutcome,
};
use broca_session_store::session::Session;
use cortexkit_role_tool_provider::late_results::kinds;
use serde_json::{json, Value};

/// One scripted feed: each pull answers the next reply and records what was asked.
#[derive(Default)]
struct ScriptedFeed {
    replies: Mutex<Vec<FeedReply>>,
    pulls: Mutex<Vec<(Option<Cursor>, Option<u32>)>>,
    acks: Mutex<Vec<Cursor>>,
    pull_error: Mutex<Option<FeedError>>,
    /// The next ack is lost on its way, as a transport failure would lose it.
    fail_ack: Mutex<bool>,
}

impl ScriptedFeed {
    fn fail_ack_once(&self) {
        *self.fail_ack.lock().unwrap() = true;
    }
    fn replies(&self, replies: Vec<FeedReply>) {
        *self.replies.lock().unwrap() = replies;
    }
    fn pulls(&self) -> Vec<(Option<Cursor>, Option<u32>)> {
        self.pulls.lock().unwrap().clone()
    }
    fn acks(&self) -> Vec<Cursor> {
        self.acks.lock().unwrap().clone()
    }
    fn fail_pull(&self, error: FeedError) {
        *self.pull_error.lock().unwrap() = Some(error);
    }
}

#[async_trait]
impl LateResultsFeed for ScriptedFeed {
    async fn pull(
        &self,
        _module: &str,
        since: Option<&Cursor>,
        limit: Option<u32>,
    ) -> Result<FeedReply, FeedError> {
        self.pulls.lock().unwrap().push((since.cloned(), limit));
        if let Some(error) = self.pull_error.lock().unwrap().clone() {
            return Err(error);
        }
        let mut replies = self.replies.lock().unwrap();
        if replies.is_empty() {
            return Err(FeedError::Retryable("no scripted reply".into()));
        }
        Ok(replies.remove(0))
    }

    async fn ack(&self, _module: &str, through: &Cursor) -> Result<(), FeedError> {
        if std::mem::take(&mut *self.fail_ack.lock().unwrap()) {
            return Err(FeedError::Retryable("ack lost".into()));
        }
        self.acks.lock().unwrap().push(through.clone());
        Ok(())
    }
}

struct RecordedDelivery {
    delivered: Mutex<Vec<(String, String)>>,
    refuse: Mutex<Option<String>>,
    refuse_only: Mutex<Option<String>>,
    /// Each notice the stand-in delivered for a handed-back call: the call's key, the call,
    /// and the event id of the late-results entry that carried its terminal.
    noticed: Mutex<Vec<(String, YieldedCall, String)>>,
}

impl Default for RecordedDelivery {
    fn default() -> Self {
        Self {
            delivered: Mutex::new(Vec::new()),
            refuse: Mutex::new(None),
            refuse_only: Mutex::new(None),
            noticed: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl HeldDelivery for RecordedDelivery {
    async fn notice(
        &self,
        registration: &LateResultRegistration,
        call: &YieldedCall,
        entry: &LateEntry,
    ) -> Result<NoticeOutcome, String> {
        self.noticed.lock().unwrap().push((
            registration.call_key.clone(),
            call.clone(),
            entry.event_id.clone(),
        ));
        Ok(NoticeOutcome::Delivered)
    }

    async fn deliver(
        &self,
        registration: &LateResultRegistration,
        _tool_call_id: &ToolCallId,
        entry: &LateEntry,
    ) -> Result<(), String> {
        if let Some(reason) = self.refuse.lock().unwrap().clone() {
            if self
                .refuse_only
                .lock()
                .unwrap()
                .as_ref()
                .is_none_or(|key| key == &registration.call_key)
            {
                return Err(reason);
            }
        }
        self.delivered
            .lock()
            .unwrap()
            .push((registration.call_key.clone(), entry.event_id.clone()));
        Ok(())
    }
}

fn cursor(seq: u64) -> Cursor {
    Cursor {
        provider_incarnation: "inc-1".into(),
        seq,
    }
}

fn entry(kind: &str, call_key: &str, event: &str) -> LateEntry {
    LateEntry::new(
        kind,
        "reserved:broca",
        "ref",
        1,
        "reserved:broca",
        call_key,
        event,
        1,
    )
    .with_result(json!({"content": [{"type": "text", "text": "late"}]}))
}

fn reply(entries: Vec<LateEntry>, seq: u64, more: bool) -> FeedReply {
    FeedReply {
        entries,
        cursor: cursor(seq),
        more,
    }
}

fn session(name: &str) -> BindIdentity {
    BindIdentity::new("/project", "broca", name)
}

/// The records of one held call whose batch is still waiting on the provider: the run paused
/// on it after the held record synced. The call is `a` in batch `b` of run `run`.
fn held_records(call_key: &str, module: &str) -> Vec<Record> {
    vec![
        run_start("run"),
        batch_start("run", "b", &["a"]),
        intent("run", "b", "a", module, Some(call_key)),
        held("b", "a", call_key, module),
        Record::Pause(Pause {
            run_id: "run".into(),
            reason: PauseReason::ToolHeld,
            ts_ms: Some(43),
            error: None,
            account_limit: None,
        }),
    ]
}

fn run_start(run: &str) -> Record {
    Record::RunStart(Box::new(RunStart {
        imported_ids: None,
        render_identity: None,
        message_marks: None,
        named_speaker: None,
        kept_notice: None,
        run_id: run.into(),
        model: serde_json::from_value(json!({"provider_module_id": "p", "model_id": "m"})).unwrap(),
        setup: Vec::new(),
        renderer_version: 1,
        submission_id: None,
        send_id: None,
        request_identity_digest: None,
        ts_ms: Some(1),
        written_at_ms: Some(1),
        session_class: None,
        retention_secs: None,
        title: None,
        context_limit: None,
        service_tier: None,
        route_decision_id: None,
        compaction_plan_version: None,
        auth_selection: None,
        run_choices: None,
        origin: None,
        principal: None,
        scope: None,
        hooks: None,
        keep_warm: None,
        on_restart: None,
        seeded: None,
        refusal_schedule: None,
    }))
}

fn batch_start(run: &str, batch: &str, calls: &[&str]) -> Record {
    Record::BatchStart(BatchStart {
        run_id: run.into(),
        batch_id: batch.into(),
        step_id: 1,
        planned: calls.iter().map(|id| ToolCallId((*id).into())).collect(),
        mid: None,
        shown_tags: Vec::new(),
    })
}

/// The dispatch intent of call `id`; a plain call (no `call_key`) settles inline and is never
/// held.
fn intent(run: &str, batch: &str, id: &str, module: &str, call_key: Option<&str>) -> Record {
    Record::ToolIntent(ToolIntent {
        run_id: run.into(),
        batch_id: batch.into(),
        tool_call_id: ToolCallId(id.into()),
        tool_name: Some("ask".into()),
        module_id: Some(module.into()),
        args_digest: broca_session_store::codec::Digest([0; 32]),
        args: None,
        call_key: call_key.map(Into::into),
        approval_digest: None,
        dispatched_at_ms: Some(1),
        schema_pin: None,
        hooks: None,
        key_derived: None,
    })
}

fn held(batch: &str, id: &str, call_key: &str, module: &str) -> Record {
    Record::ToolHeld(ToolHeld {
        batch_id: batch.into(),
        tool_call_id: ToolCallId(id.into()),
        call_key: call_key.into(),
        module: module.into(),
        reason: "running".into(),
        detail_digest: None,
        held_at_ms: 42,
        sequential: false,
    })
}

fn outcome(run: &str, batch: &str, id: &str, outcome: CallOutcome) -> Record {
    Record::ToolOutcome(ToolOutcome {
        yielded_by: None,
        run_id: run.into(),
        batch_id: batch.into(),
        tool_call_id: ToolCallId(id.into()),
        outcome,
        result_digest: broca_session_store::codec::Digest([0; 32]),
        is_error: false,
        reason: None,
        finished_at_ms: Some(50),
        detail: None,
        hooks: None,
        call_key: None,
    })
}

fn batch_end(run: &str, batch: &str) -> Record {
    Record::BatchEnd(BatchEnd {
        run_id: run.into(),
        batch_id: batch.into(),
    })
}

fn run_end(run: &str, reason: RunFinishReason) -> Record {
    Record::RunEnd(RunEnd {
        run_id: run.into(),
        reason,
        usage: None,
        ts_ms: Some(60),
        retries_used: 0,
        interrupt_reason: None,
        error: None,
    })
}

/// A session that registered for a feed and then finished its run before any tool call was
/// held. Its records name no held call, so it keeps no puller running.
async fn quiet_bind(name: &str, rig: &Rig) -> BindIdentity {
    let bind = BindIdentity::new("/project", "broca", name);
    // Nothing is held: the run stopped before any tool call was dispatched.
    rig.seed(&bind, &[run_start("run")]).await;
    bind
}

/// A session whose open run has sent `call_key` to the provider and seen nothing back yet: the
/// call's intent is durable and its held record is not, so an entry for it waits.
async fn dispatching_bind(name: &str, call_key: &str, rig: &Rig) -> BindIdentity {
    let bind = BindIdentity::new("/project", "broca", name);
    rig.seed(
        &bind,
        &[
            run_start("run"),
            batch_start("run", "b", &["a"]),
            intent("run", "b", "a", "tools", Some(call_key)),
        ],
    )
    .await;
    bind
}

/// Register one more call with an intake a rig already holds.
async fn register_more(store: &Arc<LateResultsStore>, calls: &[LateResultRegistration]) {
    use broca_core::tool_plane::LateResultsIntake as _;
    store.register_group(calls).await.unwrap();
}

fn rig_root(name: &str) -> std::path::PathBuf {
    broca_test_support::scratch_root().join(format!("late-results-{name}-{}", std::process::id()))
}

/// An executor for sessions these tests only read: nothing here runs a model step.
struct NoRuns;

#[async_trait::async_trait]
impl crate::executor::RunExecutor for NoRuns {}

/// The session store over `root` the intake reads lineages from, as the module installs it.
async fn session_store(root: &std::path::Path) -> Arc<crate::store_open::StoreRuntime> {
    Arc::new(
        crate::store_open::StoreRuntime::open(
            root,
            broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
            Arc::new(NoRuns),
            std::time::Duration::from_secs(600),
        )
        .await
        .unwrap(),
    )
}

/// Write one session's records to the store under `root`, as its run loop would have.
async fn seed_root(root: &std::path::Path, bind: &BindIdentity, records: &[Record]) {
    use broca_session_store::backend::{LocalBackend, SessionAddr};
    let backend =
        LocalBackend::new(broca_records::session_storage::StorageClass::Ordinary.store_root(root));
    let address = SessionAddr(broca_core::session_addr(bind));
    let triple = crate::store_open::triple(bind);
    let deployment = broca_session_store::keys::DeploymentKey::new(1, &[7; 32]);
    let exists = backend.stored_sessions().unwrap().contains(&address);
    let mut session = if exists {
        Session::load(&backend, address, &triple, &deployment)
            .await
            .unwrap()
    } else {
        Session::create(&backend, address, triple, &deployment, false)
            .await
            .unwrap()
    };
    session.append(records, true).await.unwrap();
    session.close().await.unwrap();
}

/// Every record `bind`'s session holds, oldest first.
async fn rig_records(
    runtime: &crate::store_open::StoreRuntime,
    bind: &BindIdentity,
) -> Vec<Record> {
    let view = runtime.current_view(bind).await.unwrap().unwrap();
    let mut records = view.ledger_tail_until(|_| false).await.unwrap();
    records.reverse();
    records.into_iter().map(|(_, record)| record).collect()
}

struct Rig {
    root: std::path::PathBuf,
    runtime: Arc<crate::store_open::StoreRuntime>,
    store: Arc<LateResultsStore>,
    intake: Arc<LateResultsIntake>,
    feed: Arc<ScriptedFeed>,
    delivery: Arc<RecordedDelivery>,
}

impl Rig {
    /// A rig whose session store holds `held` (a call waiting on the provider) and whose
    /// late-results store holds `registrations`.
    async fn new(
        name: &str,
        held: Option<(&str, &str, BindIdentity)>,
        registrations: Vec<LateResultRegistration>,
    ) -> Self {
        let root = rig_root(name);
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let runtime = session_store(&root).await;
        if let Some((call_key, module, bind)) = held {
            seed_root(&root, &bind, &held_records(call_key, module)).await;
        }
        let store = Arc::new(LateResultsStore::open(&root.join("late-results.db")).unwrap());
        if !registrations.is_empty() {
            use broca_core::tool_plane::LateResultsIntake as _;
            store.register_group(&registrations).await.unwrap();
        }
        let feed = Arc::new(ScriptedFeed::default());
        let delivery = Arc::new(RecordedDelivery::default());
        let intake = LateResultsIntake::new(
            Arc::clone(&store),
            "broca",
            Arc::clone(&delivery) as Arc<dyn HeldDelivery>,
            Arc::clone(&feed) as Arc<dyn LateResultsFeed>,
        );
        intake.set_session_store(Arc::clone(&runtime));
        Self {
            root,
            runtime,
            store,
            intake,
            feed,
            delivery,
        }
    }

    /// Every record `bind`'s session holds, oldest first.
    async fn records(&self, bind: &BindIdentity) -> Vec<Record> {
        rig_records(&self.runtime, bind).await
    }

    /// Append `records` to `bind`'s session, creating it first when it doesn't exist.
    async fn seed(&self, bind: &BindIdentity, records: &[Record]) {
        seed_root(&self.root, bind, records).await;
    }

    fn puller(&self, module: &str) -> Arc<Puller> {
        self.intake
            .pullers
            .lock()
            .unwrap()
            .entry(module.to_owned())
            .or_insert_with(|| Arc::new(Puller::new()))
            .clone()
    }
}

fn registration(call_key: &str, module: &str, session: &BindIdentity) -> LateResultRegistration {
    LateResultRegistration {
        lineage: call_key.rsplit_once(':').unwrap().0.to_owned(),
        call_key: call_key.to_owned(),
        module: module.to_owned(),
        bind: session.clone(),
        scope: None,
    }
}

// Both calls share a batch and a lineage, as inline answers followed by a held answer do: `a`
// was answered inline, then the provider held `b`.
fn inline_then_held_records() -> Vec<Record> {
    vec![
        run_start("run"),
        batch_start("run", "b", &["a", "b"]),
        intent("run", "b", "a", "basal", Some("lineage:4")),
        outcome("run", "b", "a", CallOutcome::Completed),
        intent("run", "b", "b", "basal", Some("lineage:6")),
        held("b", "b", "lineage:6", "basal"),
    ]
}

async fn inline_duplicate_before_held(name: &str, restart: bool) {
    let bind = session(name);
    let rig = Rig::new(
        name,
        None,
        vec![
            registration("lineage:4", "basal", &bind),
            registration("lineage:6", "basal", &bind),
        ],
    )
    .await;
    rig.seed(&bind, &inline_then_held_records()).await;
    let rig = if restart {
        let root = rig.root.clone();
        drop(rig);
        let runtime = session_store(&root).await;
        let store = Arc::new(LateResultsStore::open(&root.join("late-results.db")).unwrap());
        let feed = Arc::new(ScriptedFeed::default());
        let delivery = Arc::new(RecordedDelivery::default());
        let intake = LateResultsIntake::new(store.clone(), "broca", delivery.clone(), feed.clone());
        intake.set_session_store(Arc::clone(&runtime));
        Rig {
            root,
            runtime,
            store,
            intake,
            feed,
            delivery,
        }
    } else {
        rig
    };
    let _puller = rig.puller("basal");
    let duplicate = entry(kinds::RESULT, "lineage:4", "inline-duplicate");
    rig.feed.fail_ack_once();
    rig.feed.replies(vec![reply(
        vec![
            duplicate.clone(),
            entry(kinds::RESULT, "lineage:6", "held-result"),
        ],
        322,
        false,
    )]);
    rig.intake.tick("basal").await.unwrap();
    assert_eq!(
        *rig.delivery.delivered.lock().unwrap(),
        vec![("lineage:6".to_owned(), "held-result".to_owned())]
    );
    assert_eq!(
        rig.store.receipt(&duplicate.intake_key("basal")).unwrap(),
        Some(Disposition::NoOp)
    );
    rig.feed.replies(vec![reply(
        vec![duplicate, entry(kinds::RESULT, "lineage:6", "held-result")],
        322,
        false,
    )]);
    rig.intake.tick("basal").await.unwrap();
    assert_eq!(rig.delivery.delivered.lock().unwrap().len(), 1);
    assert_eq!(rig.feed.acks(), vec![cursor(322)]);
}

#[tokio::test]
async fn inline_duplicate_does_not_pin_a_held_result() {
    inline_duplicate_before_held("inline-duplicate", false).await;
}

#[tokio::test]
async fn inline_duplicate_does_not_pin_a_held_result_after_restart() {
    inline_duplicate_before_held("inline-duplicate-restart", true).await;
}

#[tokio::test]
async fn pending_intent_delivers_only_after_its_held_record_syncs() {
    let bind = session("pending-intent");
    let rig = Rig::new(
        "pending-intent",
        None,
        vec![registration("lineage:4", "basal", &bind)],
    )
    .await;
    let records = held_records("lineage:4", "basal");
    // The intent is durable; the held record isn't yet.
    rig.seed(&bind, &records[..3]).await;
    let puller = rig.puller("basal");
    let late = entry(kinds::RESULT, "lineage:4", "pending-result");
    assert_eq!(
        rig.intake
            .classify(
                "basal",
                &late,
                &rig.store.registration_rows().unwrap(),
                &mut HashMap::new()
            )
            .await
            .unwrap(),
        Classified::Pending
    );
    rig.feed.replies(vec![
        reply(vec![late.clone()], 1, false),
        reply(vec![late.clone()], 1, false),
    ]);
    rig.intake.tick("basal").await.unwrap();
    assert!(rig.delivery.delivered.lock().unwrap().is_empty());
    assert!(rig.feed.acks().is_empty());
    assert_eq!(puller.pinned(), vec![late.intake_key("basal")]);
    rig.seed(&bind, &records[3..]).await;
    rig.intake.tick("basal").await.unwrap();
    assert_eq!(rig.delivery.delivered.lock().unwrap().len(), 1);
    assert_eq!(rig.feed.acks(), vec![cursor(1)]);
}

#[tokio::test]
async fn an_interrupted_indeterminate_call_does_not_pin_a_held_result() {
    let ended = session("indeterminate");
    let held = session("after-indeterminate");
    let rig = Rig::new(
        "indeterminate",
        Some(("held:4", "basal", held.clone())),
        vec![
            registration("ended:4", "basal", &ended),
            registration("held:4", "basal", &held),
        ],
    )
    .await;
    // The call was dispatched and the run ended before anything came back or was held.
    let mut records = held_records("ended:4", "basal");
    records.truncate(3);
    records.push(run_end("run", RunFinishReason::Interrupted));
    rig.seed(&ended, &records).await;
    let seeded = rig.records(&ended).await.len();
    let _puller = rig.puller("basal");
    let stale = entry(kinds::RESULT, "ended:4", "indeterminate-result");
    rig.feed.fail_ack_once();
    rig.feed.replies(vec![reply(
        vec![stale.clone(), entry(kinds::RESULT, "held:4", "held-result")],
        2,
        false,
    )]);
    rig.intake.tick("basal").await.unwrap();
    assert_eq!(
        *rig.delivery.delivered.lock().unwrap(),
        vec![("held:4".to_owned(), "held-result".to_owned())]
    );
    assert_eq!(
        rig.store.receipt(&stale.intake_key("basal")).unwrap(),
        Some(Disposition::NoOp)
    );
    rig.feed.replies(vec![reply(
        vec![stale, entry(kinds::RESULT, "held:4", "held-result")],
        2,
        false,
    )]);
    rig.intake.tick("basal").await.unwrap();
    assert_eq!(rig.delivery.delivered.lock().unwrap().len(), 1);
    assert_eq!(rig.feed.acks(), vec![cursor(2)]);
    assert_eq!(
        rig.records(&ended).await.len(),
        seeded,
        "the indeterminate call stays untouched"
    );
}

#[tokio::test]
async fn an_inline_registration_is_cleaned_even_while_its_sibling_is_held() {
    let bind = session("inline-cleanup");
    let rig = Rig::new(
        "inline-cleanup",
        None,
        vec![
            registration("lineage:4", "basal", &bind),
            registration("lineage:6", "basal", &bind),
        ],
    )
    .await;
    rig.seed(&bind, &inline_then_held_records()).await;
    assert!(rig.intake.module_holds_calls("basal").await.unwrap());
    assert!(rig
        .store
        .registration("basal", "lineage:4")
        .unwrap()
        .is_none());
    assert!(rig
        .store
        .registration("basal", "lineage:6")
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn stale_inline_rows_are_swept_in_bounded_pages_without_a_feed() {
    let bind = session("sweep-inline");
    let rig = Rig::new("sweep-inline", None, Vec::new()).await;
    let count = REGISTRATION_SWEEP_LIMIT + 1;
    let ids: Vec<String> = (0..count).map(|index| format!("call-{index}")).collect();
    let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
    let mut records = vec![run_start("run"), batch_start("run", "b", &id_refs)];
    let mut calls = Vec::new();
    // Every call was answered inline, so each registration is settled and only the sweep
    // removes it.
    for (index, id) in ids.iter().enumerate() {
        let key = format!("lineage:{}", index + 4);
        records.push(intent("run", "b", id, "aft", Some(&key)));
        records.push(outcome("run", "b", id, CallOutcome::Completed));
        calls.push(registration(&key, "aft", &bind));
    }
    rig.seed(&bind, &records).await;
    register_more(&rig.store, &calls).await;
    assert!(rig.intake.running().is_empty());
    assert_eq!(
        rig.intake.sweep_registrations().await.unwrap(),
        REGISTRATION_SWEEP_LIMIT
    );
    assert_eq!(rig.store.registrations().unwrap().len(), 1);
    assert_eq!(rig.intake.sweep_registrations().await.unwrap(), 1);
    assert_eq!(rig.intake.sweep_registrations().await.unwrap(), 0);
    assert!(rig.store.registrations().unwrap().is_empty());
    assert!(
        rig.store.withdrawal_rows().unwrap().is_empty(),
        "an inline call's withdrawal watch is retired with its registration"
    );
}

/// The sweep retires a held call left unfinished when its run ended, but keeps its withdrawal
/// watch: a cancelled hold is withdrawn, so the watch is for withdrawal to settle, not the sweep.
/// A call answered inline beside it gives up both.
#[tokio::test]
async fn a_held_call_its_run_left_unfinished_keeps_its_withdrawal_watch() {
    let bind = session("sweep-held-ended");
    let rig = Rig::new("sweep-held-ended", None, Vec::new()).await;
    let mut records = inline_then_held_records();
    records.push(run_end("run", RunFinishReason::Cancelled));
    rig.seed(&bind, &records).await;
    register_more(
        &rig.store,
        &[
            registration("lineage:4", "basal", &bind),
            registration("lineage:6", "basal", &bind),
        ],
    )
    .await;
    assert_eq!(rig.intake.sweep_registrations().await.unwrap(), 2);
    assert!(rig.store.registrations().unwrap().is_empty());
    let watched: Vec<String> = rig
        .store
        .withdrawal_rows()
        .unwrap()
        .into_iter()
        .map(|(call, _, _)| call.call_key)
        .collect();
    assert_eq!(watched, ["lineage:6"]);
}

/// A held call the run closed itself when it was cancelled (its terminal is the run's own
/// interrupted close, not the provider's answer) still owes its provider a withdrawal, so the
/// sweep keeps its watch even though the call has a terminal: the provider may still be working
/// on it.
#[tokio::test]
async fn a_held_call_the_run_closed_keeps_its_withdrawal_watch() {
    let bind = session("sweep-held-closed");
    let rig = Rig::new("sweep-held-closed", None, Vec::new()).await;
    let mut records = inline_then_held_records();
    records.push(outcome("run", "b", "b", CallOutcome::Interrupted));
    records.push(batch_end("run", "b"));
    records.push(run_end("run", RunFinishReason::Cancelled));
    rig.seed(&bind, &records).await;
    register_more(
        &rig.store,
        &[
            registration("lineage:4", "basal", &bind),
            registration("lineage:6", "basal", &bind),
        ],
    )
    .await;
    assert_eq!(rig.intake.sweep_registrations().await.unwrap(), 2);
    let watched: Vec<String> = rig
        .store
        .withdrawal_rows()
        .unwrap()
        .into_iter()
        .map(|(call, _, _)| call.call_key)
        .collect();
    assert_eq!(watched, ["lineage:6"]);
}

struct NoInlineWithdrawal;

#[async_trait]
impl crate::held_withdrawals::WithdrawTransport for NoInlineWithdrawal {
    async fn withdraw(
        &self,
        _: &LateResultRegistration,
        _: bool,
    ) -> (
        cortexkit_role_tool_provider::withdraw::WithdrawVerdict,
        Value,
    ) {
        panic!("an inline call must never be withdrawn")
    }
}

/// Withdrawal recovery over `sessions` sessions whose `calls_per_session` calls were all answered
/// inline: no call needs withdrawing, so recovery retires every watch in one pass.
async fn recover_inline_outbox(name: &str, sessions: usize, calls_per_session: usize) {
    let rig = Rig::new(name, None, Vec::new()).await;
    let mut calls = Vec::new();
    for session_index in 0..sessions {
        let bind = session(&format!("{name}-{session_index:03}"));
        let ids: Vec<String> = (0..calls_per_session)
            .map(|index| format!("call-{index}"))
            .collect();
        let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let mut records = vec![run_start("run"), batch_start("run", "b", &id_refs)];
        for (index, id) in ids.iter().enumerate() {
            let key = format!("lineage-{session_index}:{}", index + 4);
            records.push(intent("run", "b", id, "aft", Some(&key)));
            records.push(outcome("run", "b", id, CallOutcome::Completed));
            calls.push(registration(&key, "aft", &bind));
        }
        records.push(batch_end("run", "b"));
        rig.seed(&bind, &records).await;
    }
    register_more(&rig.store, &calls).await;
    let withdrawals = crate::held_withdrawals::HeldWithdrawals::new(
        rig.store.clone(),
        Arc::new(NoInlineWithdrawal),
    );
    withdrawals.with_store_runtime(Arc::clone(&rig.runtime));
    withdrawals.bind_stopping_signal(tokio_util::sync::CancellationToken::new());
    withdrawals.recover().await.unwrap();
    assert_eq!(
        rig.store.withdrawal_rows().unwrap().len(),
        0,
        "inline watches converge to zero in one sweep"
    );
    assert!(rig.store.registrations().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn withdrawal_recovery_retires_inline_watches() {
    recover_inline_outbox("outbox-inline", 2, 3).await;
}

#[tokio::test(start_paused = true)]
async fn a_pin_warns_once_after_five_minutes() {
    let bind = session("pin-warn");
    let rig = Rig::new(
        "pin-warn",
        None,
        vec![registration("lineage:4", "basal", &bind)],
    )
    .await;
    let records = held_records("lineage:4", "basal");
    // The intent is durable; the held record isn't yet.
    rig.seed(&bind, &records[..3]).await;
    let _puller = rig.puller("basal");
    let page = || reply(vec![entry(kinds::RESULT, "lineage:4", "waiting")], 1, false);
    rig.feed
        .replies(vec![page(), page(), page(), page(), page()]);
    let ((), early) = broca_test_support::capture_logs(async {
        rig.intake.tick("basal").await.unwrap();
        tokio::time::advance(Duration::from_secs(299)).await;
        rig.intake.tick("basal").await.unwrap();
        assert_eq!(rig.intake.overdue_pins(), 0);
    })
    .await;
    assert!(
        !early
            .iter()
            .any(|event| event.target == "late_results" && event.level == tracing::Level::WARN),
        "{early:?}"
    );
    let ((), logs) = broca_test_support::capture_logs(async {
        tokio::time::advance(Duration::from_secs(2)).await;
        rig.intake.tick("basal").await.unwrap();
        assert_eq!(rig.intake.overdue_pins(), 1);
        rig.intake.tick("basal").await.unwrap();
    })
    .await;
    let warnings: Vec<_> = logs
        .iter()
        .filter(|event| event.target == "late_results" && event.level == tracing::Level::WARN)
        .collect();
    assert_eq!(warnings.len(), 1, "{logs:?}");
    assert_eq!(warnings[0].fields["module"], "basal");
    assert_eq!(warnings[0].fields["call_key"], "lineage:4");
    assert_eq!(warnings[0].fields["event_id"], "waiting");
    rig.seed(&bind, &records[3..]).await;
    rig.intake.tick("basal").await.unwrap();
    assert_eq!(rig.intake.overdue_pins(), 0);
}

#[tokio::test]
async fn an_entry_for_a_held_call_is_delivered_then_acked_through_the_providers_cursor() {
    let call_key = "lineage:4";
    let bind = session("held");
    let rig = Rig::new(
        "deliver",
        Some((call_key, "tools", bind.clone())),
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    let _puller = rig.puller("tools");
    rig.feed.replies(vec![reply(
        vec![entry(kinds::RESULT, call_key, "ev-1")],
        1,
        false,
    )]);
    rig.intake.tick("tools").await.unwrap();
    assert_eq!(
        rig.delivery.delivered.lock().unwrap().clone(),
        vec![(call_key.to_owned(), "ev-1".to_owned())]
    );
    assert_eq!(rig.feed.acks(), vec![cursor(1)]);
    assert_eq!(
        rig.store
            .feed("tools")
            .unwrap()
            .unwrap()
            .position
            .unwrap()
            .seq,
        1
    );
}

/// prefrontal was renamed from `prefrontal-core`. A call dispatched or held under either name
/// takes an answer its feed delivers under the other: both are one module.
#[tokio::test]
async fn a_held_call_takes_its_answer_under_its_modules_other_name() {
    for (case, intent_module, held_module, feed_module) in [
        ("new", "prefrontal", "prefrontal", "prefrontal-core"),
        ("old", "prefrontal-core", "prefrontal-core", "prefrontal"),
        ("mixed", "prefrontal-core", "prefrontal", "prefrontal-core"),
    ] {
        let call_key = "lineage:5";
        let bind = session(&format!("renamed-{case}"));
        let rig = Rig::new(
            &format!("renamed-{case}"),
            None,
            vec![registration(call_key, held_module, &bind)],
        )
        .await;
        let mut records = held_records(call_key, held_module);
        records[2] = intent("run", "b", "a", intent_module, Some(call_key));
        rig.seed(&bind, &records).await;
        let _puller = rig.puller(feed_module);
        rig.feed.replies(vec![reply(
            vec![entry(kinds::RESULT, call_key, "ev-1")],
            1,
            false,
        )]);
        rig.intake.tick(feed_module).await.unwrap();
        assert_eq!(
            rig.delivery.delivered.lock().unwrap().clone(),
            vec![(call_key.to_owned(), "ev-1".to_owned())],
            "{case}"
        );
    }
}

/// A session's earlier run was cancelled while a provider held one of its calls. A later
/// run of the same session dispatches a new held call, and its answer must still be
/// delivered: a cancel only closes the episode it belongs to.
#[tokio::test]
async fn an_earlier_cancelled_episode_does_not_drop_a_later_runs_held_answer() {
    let bind = session("earlier-cancel");
    let call_key = "lineage:20";
    let records = vec![
        run_start("old"),
        batch_start("old", "b", &["a"]),
        intent("old", "b", "a", "tools", Some("lineage:4")),
        held("b", "a", "lineage:4", "tools"),
        Record::CancelRequest(CancelRequest {
            run_id: "old".into(),
            at: CancelPoint::StepBoundary,
        }),
        outcome("old", "b", "a", CallOutcome::Interrupted),
        batch_end("old", "b"),
        run_end("old", RunFinishReason::Cancelled),
        // The later run's call has its own tool call id, batch and key.
        run_start("run"),
        batch_start("run", "later-b", &["later"]),
        intent("run", "later-b", "later", "tools", Some(call_key)),
        held("later-b", "later", call_key, "tools"),
    ];
    let rig = Rig::new(
        "earlier-cancel",
        None,
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    rig.seed(&bind, &records).await;
    let _puller = rig.puller("tools");
    rig.feed.replies(vec![reply(
        vec![entry(kinds::RESULT, call_key, "ev-later")],
        1,
        false,
    )]);
    rig.intake.tick("tools").await.unwrap();
    assert_eq!(
        rig.delivery.delivered.lock().unwrap().clone(),
        vec![(call_key.to_owned(), "ev-later".to_owned())],
        "the later run's held answer was settled without delivery"
    );
}

/// The other side of the episode scope: a cancel requested in the dispatching run itself
/// still settles that run's held answer as a no-op, and nothing is delivered into a run
/// that is being cancelled.
#[tokio::test]
async fn a_cancel_in_the_dispatching_run_settles_its_held_answer_without_delivery() {
    let call_key = "lineage:4";
    let bind = session("same-cancel");
    let mut records = held_records(call_key, "tools");
    records.push(Record::CancelRequest(CancelRequest {
        run_id: "run".into(),
        at: CancelPoint::StepBoundary,
    }));
    let rig = Rig::new(
        "same-cancel",
        None,
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    rig.seed(&bind, &records).await;
    let _puller = rig.puller("tools");
    rig.feed.replies(vec![reply(
        vec![entry(kinds::RESULT, call_key, "ev-1")],
        1,
        false,
    )]);
    rig.intake.tick("tools").await.unwrap();
    assert!(rig.delivery.delivered.lock().unwrap().is_empty());
    assert_eq!(rig.feed.acks(), vec![cursor(1)]);
}

#[tokio::test]
async fn nothing_is_acked_before_its_delivery_is_durable() {
    let call_key = "lineage:4";
    let bind = session("held");
    let rig = Rig::new(
        "no-early-ack",
        Some((call_key, "tools", bind.clone())),
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    let _puller = rig.puller("tools");
    *rig.delivery.refuse.lock().unwrap() = Some("session cannot be written".into());
    rig.feed.replies(vec![reply(
        vec![entry(kinds::RESULT, call_key, "ev-1")],
        1,
        false,
    )]);
    let outcome = rig.intake.tick("tools").await;
    assert_eq!(
        rig.store.feed("tools").unwrap().unwrap().position,
        None,
        "an undelivered entry leaves the feed where it was; tick said {outcome:?}"
    );
    assert!(
        rig.feed.acks().is_empty(),
        "an undelivered entry is never acked; tick said {outcome:?}"
    );
}

#[tokio::test]
async fn a_pinned_entry_pins_the_ack_but_a_later_page_is_still_delivered() {
    let call_key = "lineage:4";
    let pending_key = "lineage:5";
    let bind = session("held");
    let rig = Rig::new(
        "later-page",
        Some((call_key, "tools", bind.clone())),
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    let pending = dispatching_bind("late-sync", pending_key, &rig).await;
    register_more(&rig.store, &[registration(pending_key, "tools", &pending)]).await;
    let _puller = rig.puller("tools");
    rig.feed.replies(vec![
        // Page one holds only the pending entry, so nothing on it can be acked.
        reply(vec![entry(kinds::RESULT, pending_key, "ev-1")], 1, true),
        // Page two is read past the pin and delivered.
        reply(vec![entry(kinds::RESULT, call_key, "ev-2")], 2, false),
    ]);
    rig.intake.tick("tools").await.unwrap();
    assert_eq!(
        rig.delivery.delivered.lock().unwrap().clone(),
        vec![(call_key.to_owned(), "ev-2".to_owned())],
        "a later page is delivered although the ack is pinned"
    );
    assert!(
        rig.feed.acks().is_empty(),
        "no ack passes the pending entry: {:?}",
        rig.feed.acks()
    );
    assert!(rig
        .intake
        .puller("tools")
        .unwrap()
        .pinned()
        .contains(&entry(kinds::RESULT, pending_key, "ev-1").intake_key("tools")));
}

#[tokio::test]
async fn a_mismatched_limit_repull_acks_nothing() {
    let call_key = "lineage:4";
    let bind = session("held");
    let rig = Rig::new(
        "ack-mismatch",
        Some((call_key, "tools", bind.clone())),
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    let quiet = dispatching_bind("unregistered", "lineage:9", &rig).await;
    register_more(&rig.store, &[registration("lineage:9", "tools", &quiet)]).await;
    let _puller = rig.puller("tools");
    rig.feed.replies(vec![
        reply(
            vec![
                entry(kinds::RESULT, call_key, "ev-1"),
                entry(kinds::RESULT, "lineage:9", "ev-2"),
            ],
            2,
            false,
        ),
        // The re-pull answers the wrong entries.
        reply(vec![entry(kinds::RESULT, "lineage:9", "ev-2")], 2, false),
    ]);
    rig.intake.tick("tools").await.unwrap();
    assert!(
        rig.feed.acks().is_empty(),
        "a limit re-pull that answers other entries acks nothing: {:?}",
        rig.feed.acks()
    );
}

#[tokio::test]
async fn an_unknown_kind_is_acked_and_leaves_the_call_open_for_a_later_entry() {
    let call_key = "lineage:4";
    let bind = session("held");
    let rig = Rig::new(
        "unknown-kind",
        Some((call_key, "tools", bind.clone())),
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    let _puller = rig.puller("tools");
    rig.feed.replies(vec![reply(
        vec![entry("telemetry_note", call_key, "ev-1")],
        1,
        false,
    )]);
    rig.intake.tick("tools").await.unwrap();
    assert_eq!(rig.feed.acks(), vec![cursor(1)]);
    assert!(rig.delivery.delivered.lock().unwrap().is_empty());
    assert!(
        rig.store.registration("tools", call_key).unwrap().is_some(),
        "an unknown kind does not settle the call"
    );
    rig.feed.replies(vec![reply(
        vec![entry(kinds::RESULT, call_key, "ev-2")],
        2,
        false,
    )]);
    rig.intake.tick("tools").await.unwrap();
    assert_eq!(
        rig.delivery.delivered.lock().unwrap().clone(),
        vec![(call_key.to_owned(), "ev-2".to_owned())]
    );
}

#[tokio::test]
async fn an_entry_with_no_registration_is_acked_as_a_no_op() {
    let call_key = "lineage:4";
    let bind = session("held");
    let rig = Rig::new(
        "unregistered",
        Some((call_key, "tools", bind.clone())),
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    let _puller = rig.puller("tools");
    rig.feed.replies(vec![reply(
        vec![entry(kinds::RESULT, "lineage:9", "ev-1")],
        1,
        false,
    )]);
    rig.intake.tick("tools").await.unwrap();
    assert_eq!(rig.feed.acks(), vec![cursor(1)]);
    assert!(rig.delivery.delivered.lock().unwrap().is_empty());
}

#[tokio::test]
async fn an_entry_whose_scope_differs_from_its_registration_is_a_no_op() {
    let call_key = "lineage:4";
    let bind = session("held");
    let mut registered = registration(call_key, "tools", &bind);
    registered.scope = Some(broca_protocol::SessionScope {
        owner: broca_protocol::BindPrincipal::Reserved {
            module_id: "core".into(),
        },
        agent_id_frozen: false,
        scope_ref: "ref".into(),
        scope_epoch: 1,
        flow_id: None,
        sender: None,
        agent_id: None,
        run_id: None,
    });
    let rig = Rig::new(
        "scope-mismatch",
        Some((call_key, "tools", bind)),
        vec![registered],
    )
    .await;
    let _puller = rig.puller("tools");
    rig.feed.replies(vec![reply(
        vec![entry(kinds::RESULT, call_key, "ev-1")],
        1,
        false,
    )]);
    rig.intake.tick("tools").await.unwrap();
    assert!(rig.delivery.delivered.lock().unwrap().is_empty());
    assert_eq!(rig.feed.acks(), vec![cursor(1)]);
}

#[tokio::test]
async fn an_entry_for_a_call_dispatched_to_another_module_is_a_no_op() {
    let call_key = "lineage:4";
    let bind = session("held");
    // The log holds the call as held by `other`, while the entry arrives on `tools`' feed.
    let rig = Rig::new(
        "other-module",
        Some((call_key, "other", bind.clone())),
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    let _puller = rig.puller("tools");
    rig.feed.replies(vec![reply(
        vec![entry(kinds::RESULT, call_key, "ev-1")],
        1,
        false,
    )]);
    rig.intake.tick("tools").await.unwrap();
    assert!(rig.delivery.delivered.lock().unwrap().is_empty());
    assert_eq!(rig.feed.acks(), vec![cursor(1)]);
}

#[tokio::test]
async fn an_incarnation_change_forgets_the_cursor_and_keeps_the_owed_ack() {
    let call_key = "lineage:4";
    let bind = session("held");
    let rig = Rig::new(
        "incarnation",
        Some((call_key, "tools", bind.clone())),
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    let _puller = rig.puller("tools");
    rig.store
        .record_ack(
            "tools",
            &FeedPosition {
                provider_incarnation: "inc-1".into(),
                seq: 9,
            },
        )
        .unwrap();
    rig.store.set_ack_owed("tools", true).unwrap();
    rig.feed.fail_pull(FeedError::CursorIncarnationChanged);
    rig.intake.tick("tools").await.unwrap();
    let row = rig.store.feed("tools").unwrap().unwrap();
    assert_eq!(row.position, None, "the refused cursor is forgotten");
    assert!(
        row.ack_owed,
        "a provider restart does not settle an owed ack"
    );
}

#[tokio::test]
async fn an_ack_only_puller_clears_its_owed_ack_when_the_feed_is_abandoned() {
    let rig = Rig::new("abandoned", None, Vec::new()).await;
    rig.store.set_ack_owed("tools", true).unwrap();
    let _puller = rig.puller("tools");
    rig.feed
        .fail_pull(FeedError::Abandoned("module left the catalog".into()));
    rig.intake.tick("tools").await.unwrap();
    assert!(!rig.store.feed("tools").unwrap().unwrap().ack_owed);
}

#[tokio::test]
async fn a_module_with_an_open_held_call_keeps_its_owed_ack_on_abandonment() {
    let call_key = "lineage:4";
    let bind = session("held");
    let rig = Rig::new(
        "abandoned-held",
        Some((call_key, "tools", bind.clone())),
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    rig.store.set_ack_owed("tools", true).unwrap();
    let _puller = rig.puller("tools");
    rig.feed
        .fail_pull(FeedError::Abandoned("module left the catalog".into()));
    rig.intake.tick("tools").await.unwrap();
    assert!(rig.store.feed("tools").unwrap().unwrap().ack_owed);
}

#[tokio::test]
async fn an_unheld_registration_alone_never_starts_a_puller() {
    let rig = Rig::new("unheld", None, Vec::new()).await;
    let bind = session("unheld");
    // The call was dispatched as an ordinary call, and no provider held it.
    let mut records = held_records("unheld:4", "tools");
    records.truncate(3);
    rig.seed(&bind, &records).await;
    register_more(&rig.store, &[registration("unheld:4", "tools", &bind)]).await;
    assert!(!rig.intake.module_needs_feed("tools").await.unwrap());
    assert_eq!(
        rig.intake.ensure_puller("tools").await.unwrap(),
        StartOutcome::Idle
    );
    assert!(rig.intake.running().is_empty());
    assert!(rig.feed.pulls().is_empty());
}

#[tokio::test]
async fn a_module_holds_one_puller_however_many_scopes_register() {
    let first = "lineage:4";
    let bind = session("held");
    let rig = Rig::new(
        "one-puller",
        Some((first, "tools", bind.clone())),
        vec![registration(first, "tools", &bind)],
    )
    .await;
    let other = quiet_bind("other-scope", &rig).await;
    register_more(&rig.store, &[registration("lineage:5", "tools", &other)]).await;
    assert_eq!(
        rig.intake.ensure_puller("tools").await.unwrap(),
        StartOutcome::Started
    );
    assert_eq!(
        rig.intake.ensure_puller("tools").await.unwrap(),
        StartOutcome::Held
    );
    assert_eq!(rig.intake.running(), vec!["tools".to_owned()]);
}

/// The held records of `held_records`, followed by the call's delivered terminal and the
/// batch's commit: the call was answered and its run went on.
fn delivered_records(call_key: &str, module: &str) -> Vec<Record> {
    let mut records = held_records(call_key, module);
    records.push(outcome("run", "b", "a", CallOutcome::Completed));
    records.push(batch_end("run", "b"));
    records
}

/// What a cancel writes for a call the provider held: its interrupted terminal, the batch's
/// commit and the run's cancelled end. This is the only evidence that authorizes a withdrawal.
fn cancelled_held(records: &mut Vec<Record>, ids: &[&str]) {
    for id in ids {
        let mut closed = outcome("run", "b", id, CallOutcome::Interrupted);
        if let Record::ToolOutcome(outcome) = &mut closed {
            outcome.reason = Some("provider_held_cancelled".into());
            outcome.is_error = true;
        }
        records.push(closed);
    }
    records.push(batch_end("run", "b"));
    records.push(run_end("run", RunFinishReason::Cancelled));
}

struct WithdrawalRoute {
    seen: Mutex<Vec<String>>,
    mode: &'static str,
}

#[async_trait]
impl crate::held_withdrawals::WithdrawTransport for WithdrawalRoute {
    async fn withdraw(
        &self,
        call: &LateResultRegistration,
        _: bool,
    ) -> (
        cortexkit_role_tool_provider::withdraw::WithdrawVerdict,
        Value,
    ) {
        use cortexkit_role_tool_provider::withdraw::caller_verdict;
        self.seen.lock().unwrap().push(call.call_key.clone());
        if matches!(
            self.mode,
            "offline" | "scope_not_live" | "scope_ended" | "scope_not_carrier"
        ) {
            let code = if self.mode == "offline" {
                "target_unavailable"
            } else {
                self.mode
            };
            let error =
                serde_json::from_value(json!({"code": code, "message": "withdraw route refused"}))
                    .unwrap();
            (
                caller_verdict(Err(&error)),
                serde_json::to_value(error).unwrap(),
            )
        } else {
            let raw = if self.mode == "refused" {
                json!({"answer": "refused", "reason": "denied"})
            } else {
                json!({"answer": "withdrawn"})
            };
            (caller_verdict(Ok(&raw)), raw)
        }
    }
}

/// A late-results store over `root`, opened as the module opens it once registrations exist.
fn review_store(root: &Path) -> LateResultsStore {
    std::fs::create_dir_all(root).unwrap();
    std::fs::write(
        root.join(crate::late_results_store::REGISTRATION_MARKER),
        b"",
    )
    .unwrap();
    LateResultsStore::open(&root.join("late-results.db")).unwrap()
}

/// A withdrawal service over `store` that reads cancellations from the session store over
/// `runtime`, as the module wires it.
fn withdrawals(
    store: Arc<LateResultsStore>,
    runtime: &Arc<crate::store_open::StoreRuntime>,
    route: Arc<dyn crate::held_withdrawals::WithdrawTransport>,
    stopping: tokio_util::sync::CancellationToken,
) -> Arc<crate::held_withdrawals::HeldWithdrawals> {
    let service = crate::held_withdrawals::HeldWithdrawals::new(store, route);
    service.with_store_runtime(Arc::clone(runtime));
    service.bind_stopping_signal(stopping);
    service
}

/// A fresh state root holding one session's `records`, with the session store over it.
async fn withdrawal_root(
    name: &str,
    bind: &BindIdentity,
    records: &[Record],
) -> (std::path::PathBuf, Arc<crate::store_open::StoreRuntime>) {
    let root = rig_root(name);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let runtime = session_store(&root).await;
    seed_root(&root, bind, records).await;
    (root, runtime)
}

fn held_reply(call_key: &str) -> broca_core::tool_plane::ProviderHeldReply {
    broca_core::tool_plane::ProviderHeldReply {
        tool_call_id: ToolCallId("a".into()),
        call_key: call_key.into(),
        module: "tools".into(),
        reason: "approval".into(),
        detail_digest: None,
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test(start_paused = true)]
async fn a_closed_sessions_withdrawal_retries_do_not_reload_its_view() {
    use crate::held_slots::HeldWithdrawal;
    use std::sync::atomic::Ordering;

    let bind = session("closed-withdrawal");
    let call_key = "lineage:4";
    let mut records = held_records(call_key, "tools");
    cancelled_held(&mut records, &["a"]);
    let (root, runtime) = withdrawal_root("bounded-withdrawal", &bind, &records).await;
    // No view would fit, so a view-cache hit cannot hide a miss in the withdrawal memo.
    let runtime = Arc::new(
        Arc::into_inner(runtime)
            .unwrap()
            .with_view_bytes_for_test(1),
    );
    let store = Arc::new(review_store(&root));
    store
        .register_calls(&[registration(call_key, "tools", &bind)])
        .unwrap();
    let route = Arc::new(PendingWithdrawalRoute {
        hint_ms: None,
        pending: 2,
        seen: Mutex::default(),
    });
    let service = withdrawals(store.clone(), &runtime, route.clone(), Default::default());
    let counts = || {
        let views = runtime.views().memory(|_| 0);
        let loads = views
            .loads_by_reason
            .unwrap_or_default()
            .get("held_withdrawals")
            .map_or(0, |load| load.loads);
        (loads, runtime.log_reads.load(Ordering::Relaxed))
    };

    assert_eq!(counts(), (0, 0));
    service.withdraw(&held_reply(call_key)).await.unwrap();
    assert_eq!(counts(), (0, 1));
    assert_eq!(runtime.views().usage().0, 0);
    withdrawals_settled(&service).await;

    assert_eq!(route.seen.lock().unwrap().len(), 3);
    assert_eq!(
        store.withdrawal_state("tools", call_key).unwrap(),
        Some(("done".into(), 3))
    );
    assert_eq!(counts(), (0, 1));
}

#[tokio::test]
async fn withdrawal_checks_read_a_sealed_run_start_and_do_not_memoize_plan_errors() {
    use crate::held_slots::HeldWithdrawal;
    use broca_records::session_storage::StorageClass;
    use broca_session_loop::parts::{MemoryParts, Parts};
    use broca_session_store::{backend::SessionAddr, record::PartKind};

    let bind = session("sealed-withdrawal");
    let call_key = "lineage:4";
    let mut records = held_records(call_key, "tools");
    cancelled_held(&mut records, &["a"]);
    let tools = json!([]);
    let part = MemoryParts::default().put(PartKind::Tools, &tools).unwrap();
    let Record::RunStart(start) = &mut records[0] else {
        panic!("expected a run start");
    };
    start.setup.push(part.clone());
    let (root, runtime) = withdrawal_root("sealed-withdrawal", &bind, &records[..1]).await;
    let class = StorageClass::Ordinary;
    let backend = runtime.class_backend(class);
    let mut session = Session::load(
        &backend,
        SessionAddr(broca_core::session_addr(&bind)),
        &crate::store_open::triple(&bind),
        runtime.deployment(),
    )
    .await
    .unwrap();
    session.seal(None, 0).await.unwrap();
    session.append(&records[1..], true).await.unwrap();
    assert!(session.head().id_index.is_some());
    assert!(session
        .open_log_records()
        .await
        .unwrap()
        .iter()
        .all(|(_, record)| !matches!(record, Record::RunStart(_))));
    let cipher = session.head().cipher();
    session.close().await.unwrap();

    let store = Arc::new(review_store(&root));
    store
        .register_calls(&[registration(call_key, "tools", &bind)])
        .unwrap();
    let route = Arc::new(WithdrawalRoute {
        seen: Mutex::default(),
        mode: "ok",
    });
    let service = withdrawals(store, &runtime, route.clone(), Default::default());
    let reply = held_reply(call_key);
    let error = service.withdraw(&reply).await.unwrap_err();
    assert!(
        error.starts_with("withdrawal plan is unreadable:"),
        "{error}"
    );
    assert!(route.seen.lock().unwrap().is_empty());

    // Make the part readable without changing the ledger position that produced the error.
    assert_eq!(
        runtime
            .parts_for(class, cipher)
            .clone()
            .put(PartKind::Tools, &tools)
            .unwrap(),
        part
    );
    service.withdraw(&reply).await.unwrap();
    assert_eq!(*route.seen.lock().unwrap(), [call_key]);
}

#[tokio::test]
async fn withdrawal_ledger_keeps_its_log_boundary_and_index_across_a_seal() {
    use broca_protocol::content::{ContentBlock, Message, Role};
    use broca_session_store::backend::{MemoryBackend, SessionAddr};
    use broca_session_store::keys::DeploymentKey;
    use broca_session_store::record::{Input, Placement};
    use broca_session_store::session::StoredLedger;

    let backend = MemoryBackend::default();
    let bind = session("withdrawal-ledger-seal-race");
    let addr = SessionAddr(broca_core::session_addr(&bind));
    let triple = crate::store_open::triple(&bind);
    let deployment = DeploymentKey::new(1, &[7; 32]);
    let mut writer = Session::create(&backend, addr.clone(), triple.clone(), &deployment, true)
        .await
        .unwrap();
    let mut records = held_records("lineage:4", "tools");
    cancelled_held(&mut records, &["a"]);
    writer.append(&records[..1], true).await.unwrap();
    writer.seal(None, 0).await.unwrap();
    let input = Record::Input(Input {
        media: Vec::new(),
        run_id: "run".into(),
        placement: Placement::RunInput,
        message: Arc::new(Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "x".repeat(64 * 1024).into(),
            }],
        )),
        written_at_ms: None,
        submission_id: None,
        notice: None,
        tags_v1: None,
        receipt_seq: None,
        pruned: Vec::new(),
    });
    writer.append(&[input], true).await.unwrap();
    writer.append(&records[1..], true).await.unwrap();

    let pinned = StoredLedger::open(&backend, addr.clone(), &triple, &deployment)
        .await
        .unwrap()
        .unwrap();
    let position = pinned.position();
    assert_eq!(position, writer.position());
    let cipher = writer.head().cipher();

    // The old log grows after the pin, then moves. The new head's sealed index contains a
    // run that was not present in either the old index or the pinned prefix.
    let future = run_start("after-pin");
    writer
        .append(std::slice::from_ref(&future), true)
        .await
        .unwrap();
    writer.seal(None, 0).await.unwrap();
    assert_ne!(writer.position().log, position.log);
    assert_ne!(writer.position().version, position.version);
    assert!(writer.find_run_start("after-pin").await.unwrap().is_some());

    let before = (
        backend.log_reads(),
        backend.log_bytes_read(),
        backend.object_reads(),
        backend.object_bytes_read(),
    );
    let mut tail = pinned
        .ledger_tail_until(|record| matches!(record, Record::ToolIntent(_)))
        .await
        .unwrap();
    assert_eq!(tail.position(), position);
    assert_eq!(tail.cipher(), cipher);
    assert_eq!(
        tail.records()
            .iter()
            .map(|(_, record)| record)
            .collect::<Vec<_>>(),
        records[2..].iter().rev().collect::<Vec<_>>()
    );
    assert_eq!(backend.log_reads() - before.0, 1);
    assert_eq!(backend.log_bytes_read() - before.1, position.len as usize);
    assert_eq!(
        backend.object_reads() - before.2,
        0,
        "the intent was in the open log"
    );

    let Record::RunStart(original) = &records[0] else {
        panic!("missing fixture run start")
    };
    assert_eq!(
        tail.find_run_start("run").await.unwrap().as_ref(),
        Some(original.as_ref())
    );
    let sealed_bytes = backend.object_bytes_read() - before.3;
    let sealed_reads = backend.object_reads() - before.2;
    assert!(sealed_reads > 0);
    assert!(tail.find_run_start("after-pin").await.unwrap().is_none());
    assert_eq!(
        backend.log_reads() - before.0,
        1,
        "the indexed starts must not reread the log"
    );
    eprintln!("pinned withdrawal read: {} open-log bytes in one scan; {sealed_bytes} object bytes in {sealed_reads} reads for the sealed start", position.len);
}

#[tokio::test]
async fn withdrawal_ledger_distinguishes_missing_sessions_and_damaged_logs() {
    use broca_session_store::backend::{BackendError, MemoryBackend, SessionAddr, SessionBackend};
    use broca_session_store::keys::DeploymentKey;
    use broca_session_store::session::{SessionError, StoredLedger};

    let backend = MemoryBackend::default();
    let bind = session("withdrawal-damaged-log");
    let addr = SessionAddr(broca_core::session_addr(&bind));
    let triple = crate::store_open::triple(&bind);
    let deployment = DeploymentKey::new(1, &[7; 32]);
    let open = || StoredLedger::open(&backend, addr.clone(), &triple, &deployment);
    assert!(open().await.unwrap().is_none());

    let writer = Session::create(&backend, addr.clone(), triple.clone(), &deployment, true)
        .await
        .unwrap();
    let position = writer.position();
    backend.set_log(&addr, &position.log, vec![0]);
    let ledger = open().await.unwrap().unwrap();
    assert!(matches!(
        ledger.ledger_tail_until(|_| false).await,
        Err(SessionError::Corrupt { .. })
    ));
    let pinned = open().await.unwrap().unwrap();
    backend
        .delete_log(&addr, &position.log, position.version)
        .await
        .unwrap();
    assert!(matches!(
        open().await,
        Err(SessionError::Backend(BackendError::NoSuchLog))
    ));
    assert!(matches!(
        pinned.ledger_tail_until(|_| false).await,
        Err(SessionError::Backend(BackendError::NoSuchLog))
    ));
}

#[tokio::test]
async fn withdrawal_checks_reconsider_a_hold_after_a_cancel_is_written() {
    use crate::held_slots::HeldWithdrawal;

    let bind = session("held-before-cancel");
    let call_key = "lineage:4";
    let (root, runtime) = withdrawal_root(
        "held-before-cancel",
        &bind,
        &held_records(call_key, "tools"),
    )
    .await;
    let store = Arc::new(review_store(&root));
    store
        .register_calls(&[
            registration(call_key, "tools", &bind),
            registration("lineage:8", "tools", &bind),
        ])
        .unwrap();
    let route = Arc::new(WithdrawalRoute {
        seen: Mutex::default(),
        mode: "ok",
    });
    let service = withdrawals(store, &runtime, route.clone(), Default::default());
    let error = service
        .withdraw(&held_reply("lineage:8"))
        .await
        .unwrap_err();
    assert_eq!(error, "withdrawal dispatch intent is missing");
    service.withdraw(&held_reply(call_key)).await.unwrap();
    assert!(route.seen.lock().unwrap().is_empty());

    let mut cancel = Vec::new();
    cancelled_held(&mut cancel, &["a"]);
    seed_root(&root, &bind, &cancel).await;
    service.withdraw(&held_reply(call_key)).await.unwrap();
    assert_eq!(*route.seen.lock().unwrap(), [call_key]);
}

#[tokio::test]
async fn review_regression_completed_sibling_is_not_withdrawn_on_recovery() {
    let bind = session("siblings");
    // Two calls of one batch were held; `a` was answered, then the run was cancelled with `b`
    // still held.
    let mut records = vec![
        run_start("run"),
        batch_start("run", "b", &["a", "b"]),
        intent("run", "b", "a", "tools", Some("lineage:4")),
        intent("run", "b", "b", "tools", Some("lineage:5")),
        held("b", "a", "lineage:4", "tools"),
        held("b", "b", "lineage:5", "tools"),
        outcome("run", "b", "a", CallOutcome::Completed),
    ];
    cancelled_held(&mut records, &["b"]);
    let (root, runtime) = withdrawal_root("review-completed-sibling", &bind, &records).await;
    let store = Arc::new(review_store(&root));
    store
        .register_calls(&[
            registration("lineage:4", "tools", &bind),
            registration("lineage:5", "tools", &bind),
        ])
        .unwrap();
    let route = Arc::new(WithdrawalRoute {
        seen: Mutex::default(),
        mode: "ok",
    });
    let service = withdrawals(store, &runtime, route.clone(), Default::default());
    service.recover().await.unwrap();
    assert_eq!(
        *route.seen.lock().unwrap(),
        ["lineage:5"],
        "A completed; only B's cancellation authorizes withdrawal"
    );
}

#[tokio::test]
async fn withdrawal_recovery_checks_watching_rows_without_loading_views() {
    let root = rig_root("withdrawal-recovery-without-views");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let runtime = session_store(&root).await;
    let store = Arc::new(review_store(&root));
    let mut expected = Vec::new();
    for n in 0..3 {
        let bind = session(&format!("held-{n}"));
        let key = format!("lineage:{}", n + 4);
        let mut records = held_records(&key, "tools");
        if n != 1 {
            cancelled_held(&mut records, &["a"]);
            expected.push(key.clone());
        }
        seed_root(&root, &bind, &records).await;
        store
            .register_calls(&[registration(&key, "tools", &bind)])
            .unwrap();
        assert_eq!(
            store.withdrawal_state("tools", &key).unwrap(),
            Some(("watching".into(), 0))
        );
    }

    let route = Arc::new(WithdrawalRoute {
        seen: Mutex::default(),
        mode: "ok",
    });
    let service = withdrawals(store.clone(), &runtime, route.clone(), Default::default());
    service.recover().await.unwrap();
    assert_eq!(*route.seen.lock().unwrap(), expected);
    assert_eq!(
        store.withdrawal_state("tools", "lineage:5").unwrap(),
        Some(("watching".into(), 0))
    );
    let loads = runtime
        .views()
        .memory(|_| 0)
        .loads_by_reason
        .unwrap_or_default();
    assert_eq!(
        loads.get("held_withdrawals").map_or(0, |load| load.loads),
        0
    );
}

/// A session retention deleted can't have its held calls withdrawn or answered, so recovery
/// settles their rows: a watch goes, a withdrawal already attempted fails with its scope ended,
/// and recovery reports no failure. Recovery used to keep such rows for a later sweep that could
/// never read them, and every start after the deletion reported broca degraded.
#[tokio::test]
async fn a_deleted_sessions_held_calls_are_settled_on_recovery() {
    let bind = session("deleted");
    let root = rig_root("deleted-session-withdrawals");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    // No root keeps the session: it was deleted after its calls were registered.
    let runtime = session_store(&root).await;
    let store = Arc::new(review_store(&root));
    let (watched, attempted) = (
        registration("lineage:4", "tools", &bind),
        registration("lineage:5", "tools", &bind),
    );
    store
        .register_calls(&[watched.clone(), attempted.clone()])
        .unwrap();
    store
        .record_withdrawal(&attempted, "pending", 1, &serde_json::json!({"attempt": 1}))
        .unwrap();
    let route = Arc::new(WithdrawalRoute {
        seen: Mutex::default(),
        mode: "ok",
    });
    let service = withdrawals(store.clone(), &runtime, route.clone(), Default::default());
    service.recover().await.unwrap();
    assert!(route.seen.lock().unwrap().is_empty(), "nothing is asked");
    let health = service.health();
    assert_eq!(
        (&health["failed"], &health["watching"], &health["pending"]),
        (
            &serde_json::json!(0),
            &serde_json::json!(0),
            &serde_json::json!(0)
        ),
        "{health}"
    );
    assert_eq!(health["scope_gone"], 1, "{health}");
    assert!(health.get("error").is_none(), "{health}");
    assert_eq!(store.withdrawal_state("tools", "lineage:4").unwrap(), None);
}

/// A withdrawal the provider refused for good stays failed only while its session lives, since
/// the next start asks again with the call's identity read from the session. Once the session is
/// deleted nothing can ask again, so recovery settles the row with its scope ended rather than
/// leaving health degraded at every start with no ledger to retry from.
#[tokio::test]
async fn a_failed_withdrawal_of_a_deleted_session_is_settled_on_recovery() {
    let bind = session("failed-then-deleted");
    let root = rig_root("failed-withdrawal-deleted-session");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    // No root keeps the session: it was deleted after its withdrawal failed.
    let runtime = session_store(&root).await;
    let store = Arc::new(review_store(&root));
    let call = registration("lineage:4", "tools", &bind);
    store.register_calls(std::slice::from_ref(&call)).unwrap();
    store
        .record_withdrawal(
            &call,
            "failed",
            1,
            &json!({"code": "scope_not_carrier", "message": "withdraw route refused"}),
        )
        .unwrap();
    let route = Arc::new(WithdrawalRoute {
        seen: Mutex::default(),
        mode: "ok",
    });
    let service = withdrawals(store.clone(), &runtime, route.clone(), Default::default());
    service.recover().await.unwrap();
    assert!(route.seen.lock().unwrap().is_empty(), "nothing is asked");
    let health = service.health();
    assert_eq!(health["failed"], 0, "{health}");
    assert_eq!(health["scope_gone"], 1, "{health}");
    assert!(health.get("error").is_none(), "{health}");
    assert_eq!(
        store.withdrawal_state("tools", "lineage:4").unwrap(),
        Some(("failed".into(), 1)),
        "the row stays, settled with its scope ended"
    );
}

/// Answers the first withdrawal that the provider is unreachable, and every later one that the
/// call was withdrawn, recording each call key with whether it named the session.
#[derive(Default)]
struct FlakyWithdrawalRoute {
    seen: Mutex<Vec<(String, bool)>>,
}

#[async_trait]
impl crate::held_withdrawals::WithdrawTransport for FlakyWithdrawalRoute {
    async fn withdraw(
        &self,
        call: &LateResultRegistration,
        session_identity: bool,
    ) -> (
        cortexkit_role_tool_provider::withdraw::WithdrawVerdict,
        Value,
    ) {
        use cortexkit_role_tool_provider::withdraw::caller_verdict;
        let mut seen = self.seen.lock().unwrap();
        seen.push((call.call_key.clone(), session_identity));
        if seen.len() == 1 {
            let error = serde_json::from_value(
                json!({"code": "target_unavailable", "message": "withdraw route refused"}),
            )
            .unwrap();
            (
                caller_verdict(Err(&error)),
                serde_json::to_value(error).unwrap(),
            )
        } else {
            let raw = json!({"answer": "withdrawn"});
            (caller_verdict(Ok(&raw)), raw)
        }
    }
}

/// A deletion withdraws a held call with the identity read from the session's plan before the
/// session went. When the provider asks to be asked again, the retry comes after the session is
/// gone, so it must carry that identity: read again, the deleted session would say the call
/// owes nothing, and the provider would keep running it.
#[tokio::test]
async fn a_withdrawal_before_deletion_is_retried_after_the_session_is_gone() {
    let bind = session("deleted-retry");
    let root = rig_root("withdraw-before-delete-retry");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    // No root keeps the session any more.
    let runtime = session_store(&root).await;
    let store = Arc::new(review_store(&root));
    store
        .register_calls(&[registration("lineage:4", "tools", &bind)])
        .unwrap();
    let route = Arc::new(FlakyWithdrawalRoute::default());
    let service = withdrawals(store.clone(), &runtime, route.clone(), Default::default());
    crate::retention::WithdrawBeforeDelete::withdraw_before_delete(
        service.as_ref(),
        vec![crate::retention::HeldAtDeletion {
            module: "tools".into(),
            call_key: "lineage:4".into(),
            session_identity: true,
        }],
    )
    .await;
    assert_eq!(
        store.withdrawal_state("tools", "lineage:4").unwrap(),
        Some(("pending".into(), 1))
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while store.withdrawal_state("tools", "lineage:4").unwrap() != Some(("done".into(), 2)) {
        assert!(
            std::time::Instant::now() < deadline,
            "the retry never came: {:?}",
            route.seen.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        *route.seen.lock().unwrap(),
        [
            ("lineage:4".to_string(), true),
            ("lineage:4".to_string(), true)
        ]
    );
}

struct DrainWithdrawalRoute {
    seen: std::sync::atomic::AtomicUsize,
    stopping: tokio_util::sync::CancellationToken,
}

#[async_trait]
impl crate::held_withdrawals::WithdrawTransport for DrainWithdrawalRoute {
    async fn withdraw(
        &self,
        _: &LateResultRegistration,
        _: bool,
    ) -> (
        cortexkit_role_tool_provider::withdraw::WithdrawVerdict,
        Value,
    ) {
        self.seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.stopping.cancel();
        let error =
            serde_json::from_value(json!({"code": "target_unavailable", "message": "offline"}))
                .unwrap();
        (
            cortexkit_role_tool_provider::withdraw::caller_verdict(Err(&error)),
            serde_json::to_value(error).unwrap(),
        )
    }
}

async fn review_drain(stopped_before_recovery: bool) {
    let name = if stopped_before_recovery {
        "review-stop-before"
    } else {
        "review-stop-retry"
    };
    let bind = session("cancelled-before-drain");
    let mut records = held_records("lineage:4", "tools");
    cancelled_held(&mut records, &["a"]);
    let (root, runtime) = withdrawal_root(name, &bind, &records).await;
    let store = Arc::new(review_store(&root));
    store
        .register_calls(&[registration("lineage:4", "tools", &bind)])
        .unwrap();
    let stopping = tokio_util::sync::CancellationToken::new();
    let route = Arc::new(DrainWithdrawalRoute {
        seen: Default::default(),
        stopping: stopping.clone(),
    });
    let service = withdrawals(store, &runtime, route.clone(), stopping.clone());
    if stopped_before_recovery {
        stopping.cancel();
    }
    service.recover().await.unwrap();
    assert_eq!(
        route.seen.load(std::sync::atomic::Ordering::SeqCst),
        usize::from(!stopped_before_recovery),
        "drain must stop new withdrawal attempts, including retries"
    );
}

#[tokio::test]
async fn review_regression_drain_suppresses_recovered_withdrawals() {
    review_drain(true).await;
}
#[tokio::test]
async fn review_regression_drain_suppresses_withdrawal_retries() {
    review_drain(false).await;
}

#[tokio::test]
async fn held_withdrawal_cancel_seal_crash_cut_recovers_once_even_after_intake_prunes() {
    use crate::held_slots::HeldWithdrawal;
    let bind = session("cancel-seal-cut");
    let records = held_records("lineage:4", "tools");
    let (root, runtime) = withdrawal_root("withdraw-seal-cut", &bind, &records).await;
    let path = root.join("late-results.db");
    let store = Arc::new(LateResultsStore::open(&path).unwrap());
    store
        .register_calls(&[registration("lineage:4", "tools", &bind)])
        .unwrap();
    let route = Arc::new(WithdrawalRoute {
        seen: Mutex::default(),
        mode: "ok",
    });
    let service = withdrawals(store.clone(), &runtime, route.clone(), Default::default());
    let reply = held_reply("lineage:4");
    service.withdraw(&reply).await.unwrap();
    assert!(
        route.seen.lock().unwrap().is_empty(),
        "an open hold is not a cancel"
    );
    let mut cancel = Vec::new();
    cancelled_held(&mut cancel, &["a"]);
    seed_root(&root, &bind, &cancel).await;
    // The process stops after the durable cancel and before invoking the withdrawal hook.
    store.remove("tools", "lineage:4").unwrap();
    drop(service);
    drop(store);
    let runtime = session_store(&root).await;
    let store = Arc::new(LateResultsStore::open(&path).unwrap());
    store.recovery_snapshot().unwrap();
    let service = withdrawals(store, &runtime, route.clone(), Default::default());
    service.recover().await.unwrap();
    service.withdraw(&reply).await.unwrap();
    service.recover().await.unwrap();
    assert_eq!(*route.seen.lock().unwrap(), ["lineage:4"]);
    assert_eq!(service.health()["failed"], 0);
}

/// A withdrawal its provider refused stays failed, and keeps the module's health degraded, only
/// until the provider answers it: the next start asks again with a fresh retry budget, and a
/// provider that now withdraws the call settles it done.
#[tokio::test]
async fn a_failed_withdrawal_is_asked_again_at_the_next_start_and_settles() {
    use crate::held_slots::HeldWithdrawal;
    let bind = session("failed-then-withdrawn");
    let mut records = held_records("lineage:4", "tools");
    cancelled_held(&mut records, &["a"]);
    let (root, runtime) = withdrawal_root("withdraw-failed-then-ok", &bind, &records).await;
    let path = root.join("late-results.db");
    let store = Arc::new(LateResultsStore::open(&path).unwrap());
    store
        .register_calls(&[registration("lineage:4", "tools", &bind)])
        .unwrap();
    let refusing = Arc::new(WithdrawalRoute {
        seen: Mutex::default(),
        mode: "scope_not_carrier",
    });
    let service = withdrawals(store, &runtime, refusing.clone(), Default::default());
    service.withdraw(&held_reply("lineage:4")).await.unwrap();
    assert_eq!(service.health()["failed"], 1);
    // A failed withdrawal is left alone while the process runs.
    service.withdraw(&held_reply("lineage:4")).await.unwrap();
    assert_eq!(refusing.seen.lock().unwrap().len(), 1);
    drop(service);
    // The next start finds the provider answering again.
    let store = Arc::new(LateResultsStore::open(&path).unwrap());
    store.recovery_snapshot().unwrap();
    let answering = Arc::new(WithdrawalRoute {
        seen: Mutex::default(),
        mode: "ok",
    });
    let service = withdrawals(
        store.clone(),
        &runtime,
        answering.clone(),
        Default::default(),
    );
    service.recover().await.unwrap();
    assert_eq!(*answering.seen.lock().unwrap(), ["lineage:4"]);
    assert_eq!(
        store.withdrawal_state("tools", "lineage:4").unwrap(),
        Some(("done".into(), 1)),
        "a fresh budget counts from one"
    );
    let health = service.health();
    assert_eq!(health["failed"], 0, "{health}");
    assert!(health.get("error").is_none(), "{health}");
}

#[tokio::test]
async fn held_withdrawal_scope_gone_refusals_are_reported_without_failure_or_resend() {
    use crate::held_slots::HeldWithdrawal;
    for (code, failed, scope_gone) in [
        ("scope_not_live", 0, 1),
        ("scope_ended", 0, 1),
        ("scope_not_carrier", 1, 0),
    ] {
        let bind = session(code);
        let mut records = held_records("lineage:4", "tools");
        cancelled_held(&mut records, &["a"]);
        let (root, runtime) = withdrawal_root(&format!("withdraw-{code}"), &bind, &records).await;
        let seeded = rig_records(&runtime, &bind).await.len();
        let path = root.join("late-results.db");
        let store = Arc::new(LateResultsStore::open(&path).unwrap());
        store
            .register_calls(&[registration("lineage:4", "tools", &bind)])
            .unwrap();
        let route = Arc::new(WithdrawalRoute {
            seen: Mutex::default(),
            mode: code,
        });
        let service = withdrawals(store, &runtime, route.clone(), Default::default());
        let reply = held_reply("lineage:4");
        service.withdraw(&reply).await.unwrap();
        service.withdraw(&reply).await.unwrap();
        let health = service.health();
        assert_eq!(health["failed"], failed, "{code}: {health}");
        assert_eq!(health["scope_gone"], scope_gone, "{code}: {health}");
        assert_eq!(health["pending"], 0);
        let details_key = if scope_gone > 0 {
            "scope_gone_details"
        } else {
            "failure_details"
        };
        assert_eq!(
            health[details_key],
            json!([{"code":code, "answer":null, "reason":null}])
        );
        drop(service);
        let store = Arc::new(LateResultsStore::open(&path).unwrap());
        store.recovery_snapshot().unwrap();
        let service = withdrawals(store, &runtime, route.clone(), Default::default());
        service.recover().await.unwrap();
        assert_eq!(service.health()["failed"], failed);
        assert_eq!(service.health()["scope_gone"], scope_gone);
        // A start asks a failed withdrawal once more, since what made its provider refuse may be
        // gone; a withdrawal whose scope is gone is never sent again.
        assert_eq!(
            route.seen.lock().unwrap().len(),
            1 + failed,
            "{code}: only a failed withdrawal is sent again, once per start"
        );
        assert_eq!(
            rig_records(&runtime, &bind).await.len(),
            seeded,
            "withdraw never writes to a cancelled run"
        );
    }
}

#[tokio::test]
async fn held_withdrawal_shutdown_and_restart_send_none() {
    let bind = session("shutdown");
    let records = held_records("lineage:4", "tools");
    let (root, runtime) = withdrawal_root("withdraw-shutdown", &bind, &records).await;
    let store = Arc::new(LateResultsStore::open(&root.join("late-results.db")).unwrap());
    store
        .register_calls(&[registration("lineage:4", "tools", &bind)])
        .unwrap();
    let route = Arc::new(WithdrawalRoute {
        seen: Mutex::default(),
        mode: "ok",
    });
    let service = withdrawals(store, &runtime, route.clone(), Default::default());
    service.recover().await.unwrap();
    // A shutdown interrupts the run with the call still held: that is not a cancel.
    let mut interrupted = run_end("run", RunFinishReason::Interrupted);
    if let Record::RunEnd(end) = &mut interrupted {
        end.interrupt_reason = Some("runner_shutdown".into());
    }
    seed_root(&root, &bind, &[interrupted]).await;
    service.recover().await.unwrap();
    assert!(route.seen.lock().unwrap().is_empty());
}

/// Waits until `service` sends nothing more: no withdrawal is in flight or waits to be retried.
async fn withdrawals_settled(service: &crate::held_withdrawals::HeldWithdrawals) {
    while service.busy() {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// A failing withdrawal is tried a bounded number of times, and its failure is durable: after a
/// restart it is asked again once with a fresh budget of the same size, never more, and nothing
/// is written to the cancelled run.
#[tokio::test(start_paused = true)]
async fn held_withdrawal_failures_are_bounded_durable_and_do_not_change_cancel() {
    for (mode, expected) in [("offline", 3), ("refused", 1)] {
        let bind = session(mode);
        let mut records = held_records("lineage:4", "tools");
        cancelled_held(&mut records, &["a"]);
        let (root, runtime) = withdrawal_root(&format!("withdraw-{mode}"), &bind, &records).await;
        let seeded = rig_records(&runtime, &bind).await.len();
        let path = root.join("late-results.db");
        let store = Arc::new(LateResultsStore::open(&path).unwrap());
        store
            .register_calls(&[registration("lineage:4", "tools", &bind)])
            .unwrap();
        let route = Arc::new(WithdrawalRoute {
            seen: Mutex::default(),
            mode,
        });
        let service = withdrawals(store, &runtime, route.clone(), Default::default());
        let slots = crate::held_slots::HeldSlots::new();
        slots.with_withdrawals(service.clone());
        broca_core::tool_plane::ProviderHeldLifecycle::withdraw_held(
            &*slots,
            &held_reply("lineage:4"),
        )
        .await
        .unwrap();
        withdrawals_settled(&service).await;
        assert_eq!(service.health()["failed"], 1);
        drop(slots);
        drop(service);
        let store = Arc::new(LateResultsStore::open(&path).unwrap());
        let service = withdrawals(store, &runtime, route.clone(), Default::default());
        service.recover().await.unwrap();
        withdrawals_settled(&service).await;
        assert_eq!(route.seen.lock().unwrap().len(), 2 * expected, "{mode}");
        assert_eq!(service.health()["failed"], 1, "{mode}");
        assert_eq!(
            rig_records(&runtime, &bind).await.len(),
            seeded,
            "withdraw never writes to a cancelled run"
        );
    }
}

/// While the store is being rebuilt, no missing registration proves anything: every entry is
/// Blocked, nothing is delivered and nothing is acked.
#[tokio::test]
async fn while_the_store_is_rebuilding_nothing_is_delivered_or_acked() {
    let call_key = "lineage:4";
    let bind = session("held");
    let rig = Rig::new(
        "rebuilding",
        Some((call_key, "tools", bind.clone())),
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    rig.intake.set_rebuilding(true);
    let _puller = rig.puller("tools");
    rig.feed.replies(vec![reply(
        vec![
            entry(kinds::RESULT, call_key, "ev-1"),
            entry(kinds::RESULT, "lineage:9", "ev-2"),
        ],
        2,
        false,
    )]);
    rig.intake.tick("tools").await.unwrap();
    assert!(rig.delivery.delivered.lock().unwrap().is_empty());
    assert!(rig.feed.acks().is_empty(), "{:?}", rig.feed.acks());
}

/// An entry whose held call already has its terminal (the entry was delivered and its ack
/// lost, or the provider re-served it after a restart) is acked as a no-op: the result is
/// never handed to the run a second time.
#[tokio::test]
async fn an_entry_for_a_call_that_already_has_its_terminal_is_acked_and_not_delivered() {
    let call_key = "lineage:4";
    let bind = session("answered");
    let rig = Rig::new(
        "answered",
        None,
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    rig.seed(&bind, &delivered_records(call_key, "tools")).await;
    let _puller = rig.puller("tools");
    rig.feed.replies(vec![reply(
        vec![entry(kinds::RESULT, call_key, "ev-1")],
        1,
        false,
    )]);
    rig.intake.tick("tools").await.unwrap();
    assert!(rig.delivery.delivered.lock().unwrap().is_empty());
    assert_eq!(rig.feed.acks(), vec![cursor(1)]);
}

/// The same entry pulled twice before its ack lands is handed over once: the second read
/// finds the first delivery under its `(provider, custodian, call_key, event_id)` key.
#[tokio::test]
async fn the_same_entry_read_twice_is_delivered_once() {
    let call_key = "lineage:4";
    let bind = session("held");
    let rig = Rig::new(
        "dedupe",
        Some((call_key, "tools", bind.clone())),
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    let _puller = rig.puller("tools");
    // The ack of the first read is lost, so the provider serves the entry again.
    rig.feed.fail_ack_once();
    rig.feed.replies(vec![
        reply(vec![entry(kinds::RESULT, call_key, "ev-1")], 1, false),
        reply(vec![entry(kinds::RESULT, call_key, "ev-1")], 1, false),
    ]);
    let _ = rig.intake.tick("tools").await;
    rig.intake.tick("tools").await.unwrap();
    assert_eq!(
        rig.delivery.delivered.lock().unwrap().clone(),
        vec![(call_key.to_owned(), "ev-1".to_owned())]
    );
    assert_eq!(rig.feed.acks(), vec![cursor(1)]);
}

/// An `expired` entry is the call's terminal answer: it is handed to the run like a result,
/// never dropped as a no-op and never left pending.
#[tokio::test]
async fn an_expired_entry_is_delivered_as_the_calls_terminal() {
    let call_key = "lineage:4";
    let bind = session("held");
    let rig = Rig::new(
        "expired",
        Some((call_key, "tools", bind.clone())),
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    let _puller = rig.puller("tools");
    let mut expired = entry(kinds::EXPIRED, call_key, "ev-1");
    expired.result = None;
    rig.feed.replies(vec![reply(vec![expired], 1, false)]);
    rig.intake.tick("tools").await.unwrap();
    assert_eq!(
        rig.delivery.delivered.lock().unwrap().clone(),
        vec![(call_key.to_owned(), "ev-1".to_owned())]
    );
    assert_eq!(rig.feed.acks(), vec![cursor(1)]);
}

/// A `progress` entry settles nothing: it is acked and the call stays open.
#[tokio::test]
async fn a_progress_entry_is_acked_and_settles_nothing() {
    let call_key = "lineage:4";
    let bind = session("held");
    let rig = Rig::new(
        "progress",
        Some((call_key, "tools", bind.clone())),
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    let _puller = rig.puller("tools");
    rig.feed.replies(vec![reply(
        vec![entry(kinds::PROGRESS, call_key, "ev-1")],
        1,
        false,
    )]);
    rig.intake.tick("tools").await.unwrap();
    assert!(rig.delivery.delivered.lock().unwrap().is_empty());
    assert_eq!(rig.feed.acks(), vec![cursor(1)]);
    assert!(rig.store.registration("tools", call_key).unwrap().is_some());
}

/// A registration whose call has its terminal and whose batch committed is pruned by the
/// puller's pass; a call still held keeps its registration and its feed.
#[tokio::test]
async fn a_settled_registration_is_pruned_and_a_held_one_is_kept() {
    let held_key = "lineage:4";
    let done_key = "lineage:7";
    let held = session("held");
    let done = session("done");
    let rig = Rig::new(
        "prune",
        Some((held_key, "tools", held.clone())),
        vec![
            registration(held_key, "tools", &held),
            registration(done_key, "tools", &done),
        ],
    )
    .await;
    rig.seed(&done, &delivered_records(done_key, "tools")).await;
    assert!(rig.intake.module_needs_feed("tools").await.unwrap());
    let kept: Vec<String> = rig
        .store
        .registrations()
        .unwrap()
        .into_iter()
        .map(|call| call.call_key)
        .collect();
    assert_eq!(kept, [held_key]);
}

/// An idle poll (a page with no entry after the acked position) sends no ack: it would
/// cover nothing. An ack the store still owed, as after a rebuild, is settled by reaching
/// the feed's head without one.
#[tokio::test]
async fn an_empty_page_sends_no_ack_and_settles_an_owed_one() {
    let call_key = "lineage:4";
    let bind = session("held");
    let rig = Rig::new(
        "empty-page",
        Some((call_key, "tools", bind.clone())),
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    rig.store.set_ack_owed("tools", true).unwrap();
    let _puller = rig.puller("tools");
    rig.feed.replies(vec![reply(Vec::new(), 0, false)]);
    rig.intake.tick("tools").await.unwrap();
    assert_eq!(rig.feed.pulls().len(), 1);
    assert!(rig.feed.acks().is_empty(), "{:?}", rig.feed.acks());
    assert!(!rig.store.feed("tools").unwrap().unwrap().ack_owed);
}

/// An entry whose first delivery failed is pinned. Once a later round settles it, the pin
/// is cleared and the entry is acked, so the feed does not stay unacked for good.
#[tokio::test]
async fn a_pinned_entry_that_later_settles_is_acked_and_unpinned() {
    let call_key = "lineage:4";
    let bind = session("held");
    let rig = Rig::new(
        "pin-settles",
        Some((call_key, "tools", bind.clone())),
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    let puller = rig.puller("tools");
    let page = || reply(vec![entry(kinds::RESULT, call_key, "ev-1")], 1, false);
    rig.feed.replies(vec![page(), page()]);
    // The first delivery fails the way a delivery that waited too long for the run fails.
    *rig.delivery.refuse.lock().unwrap() =
        Some("the run did not sync the held call's terminal in time".into());
    rig.intake.tick("tools").await.unwrap();
    assert!(rig.feed.acks().is_empty());
    assert_eq!(
        puller.pinned(),
        vec![entry(kinds::RESULT, call_key, "ev-1").intake_key("tools")]
    );

    *rig.delivery.refuse.lock().unwrap() = None;
    rig.intake.tick("tools").await.unwrap();
    assert_eq!(
        rig.delivery.delivered.lock().unwrap().clone(),
        vec![(call_key.to_owned(), "ev-1".to_owned())]
    );
    assert_eq!(
        rig.feed.acks(),
        vec![cursor(1)],
        "the pinned entry settled, so its page is acked; pinned={:?}",
        puller.pinned()
    );
    assert!(puller.pinned().is_empty(), "{:?}", puller.pinned());
    let feed = rig.store.feed("tools").unwrap().unwrap();
    assert_eq!(feed.position.map(|position| position.seq), Some(1));
    assert!(!feed.ack_owed);
}

#[tokio::test]
async fn repeated_delivery_failure_surfaces_health_without_blocking_the_page() {
    let bind = session("stuck");
    let later = session("later");
    let rig = Rig::new(
        "stuck-page-progress",
        Some(("lineage:4", "tools", bind.clone())),
        vec![
            registration("lineage:4", "tools", &bind),
            registration("later:4", "tools", &later),
        ],
    )
    .await;
    rig.seed(&later, &held_records("later:4", "tools")).await;
    let puller = rig.puller("tools");
    let page = || {
        reply(
            vec![
                entry(kinds::RESULT, "lineage:4", "stuck"),
                entry(kinds::RESULT, "later:4", "later"),
            ],
            2,
            false,
        )
    };
    *rig.delivery.refuse.lock().unwrap() = Some("terminal not synced".into());
    *rig.delivery.refuse_only.lock().unwrap() = Some("lineage:4".into());
    for attempt in 1..=DELIVERY_STUCK_AFTER {
        rig.feed.replies(vec![page()]);
        rig.intake.tick("tools").await.unwrap();
        assert_eq!(
            rig.intake.stuck_deliveries().len(),
            usize::from(attempt == DELIVERY_STUCK_AFTER)
        );
        assert!(
            rig.feed.acks().is_empty(),
            "never acknowledge past the failed entry"
        );
    }
    assert_eq!(
        *rig.delivery.delivered.lock().unwrap(),
        [("later:4".into(), "later".into())],
        "the later entry is delivered exactly once despite the pin"
    );
    assert!(rig
        .feed
        .pulls()
        .iter()
        .all(|(_, limit)| *limit == Some(FEED_PAGE_SIZE)));
    assert_eq!(
        rig.intake.stuck_deliveries()[0]["attempts"],
        DELIVERY_STUCK_AFTER
    );
    *rig.delivery.refuse.lock().unwrap() = None;
    rig.feed.replies(vec![page()]);
    rig.intake.tick("tools").await.unwrap();
    assert!(rig.intake.stuck_deliveries().is_empty());
    assert!(puller.pinned().is_empty());
    assert_eq!(rig.feed.acks(), [cursor(2)]);
}

#[tokio::test]
async fn later_pages_deliver_without_acking_past_an_earlier_pin() {
    let bind = session("stuck-page-one");
    let later = session("page-two");
    let rig = Rig::new(
        "stuck-multiple-pages",
        Some(("lineage:4", "tools", bind.clone())),
        vec![
            registration("lineage:4", "tools", &bind),
            registration("later:4", "tools", &later),
        ],
    )
    .await;
    rig.seed(&later, &held_records("later:4", "tools")).await;
    let _puller = rig.puller("tools");
    *rig.delivery.refuse.lock().unwrap() = Some("terminal not synced".into());
    *rig.delivery.refuse_only.lock().unwrap() = Some("lineage:4".into());
    let later_page = || reply(vec![entry(kinds::RESULT, "later:4", "later")], 2, false);
    rig.feed.replies(vec![
        reply(vec![entry(kinds::RESULT, "lineage:4", "stuck")], 1, true),
        later_page(),
        later_page(),
    ]);
    rig.intake.tick("tools").await.unwrap();
    assert_eq!(
        *rig.delivery.delivered.lock().unwrap(),
        [("later:4".into(), "later".into())]
    );
    assert!(
        rig.feed.acks().is_empty(),
        "a later page cannot acknowledge the earlier failed entry"
    );
    assert_eq!(
        rig.feed.pulls().len(),
        2,
        "no prefix ack is attempted after a prior-page pin"
    );
}

/// A provider feed that keeps what it publishes: a pull answers every entry after `since`
/// (or after the acked position, whichever is later), and the cursor is the entry count.
#[derive(Default)]
struct KeepingFeed {
    entries: Mutex<Vec<LateEntry>>,
    acked: Mutex<u64>,
    acks: Mutex<Vec<(u64, tokio::time::Instant)>>,
    pulls: Mutex<Vec<(Option<u64>, tokio::time::Instant)>>,
    changed: tokio::sync::Notify,
}

impl KeepingFeed {
    /// Wait, on the test's clock, until `done` holds for this feed.
    async fn wait_until(&self, done: impl Fn(&Self) -> bool) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if done(self) {
                return;
            }
            changed.await;
        }
    }
}

#[async_trait]
impl LateResultsFeed for KeepingFeed {
    async fn pull(
        &self,
        _module: &str,
        since: Option<&Cursor>,
        _limit: Option<u32>,
    ) -> Result<FeedReply, FeedError> {
        self.pulls
            .lock()
            .unwrap()
            .push((since.map(|cursor| cursor.seq), tokio::time::Instant::now()));
        let entries = self.entries.lock().unwrap().clone();
        let from = since.map_or(0, |cursor| cursor.seq);
        let start = from.max(*self.acked.lock().unwrap()) as usize;
        self.changed.notify_waiters();
        Ok(FeedReply {
            entries: entries.iter().skip(start).cloned().collect(),
            cursor: cursor(entries.len() as u64),
            more: false,
        })
    }

    async fn ack(&self, _module: &str, through: &Cursor) -> Result<(), FeedError> {
        {
            let mut acked = self.acked.lock().unwrap();
            *acked = (*acked).max(through.seq);
        }
        self.acks
            .lock()
            .unwrap()
            .push((through.seq, tokio::time::Instant::now()));
        self.changed.notify_waiters();
        Ok(())
    }
}

/// A session actor that loses the first resume request, as an actor that does not keep a
/// request arriving while its run is still pausing would. Every later request resumes the
/// paused batch, whose held call waits in its slot again.
struct LosesFirstResume {
    slots: Arc<crate::held_slots::HeldSlots>,
    lose_next: std::sync::atomic::AtomicBool,
    resumes: Mutex<Vec<(bool, tokio::time::Instant)>>,
    taken_at: Arc<Mutex<Option<tokio::time::Instant>>>,
    call_key: String,
}

#[async_trait]
impl crate::held_slots::HeldRunResumer for LosesFirstResume {
    async fn resume_for_late_result(&self, session: &BindIdentity) -> Result<(), String> {
        use broca_core::tool_plane::ProviderHeldLifecycle as _;
        let lost = self
            .lose_next
            .swap(false, std::sync::atomic::Ordering::SeqCst);
        self.resumes
            .lock()
            .unwrap()
            .push((lost, tokio::time::Instant::now()));
        if lost {
            return Ok(());
        }
        let slots = Arc::clone(&self.slots);
        let taken_at = Arc::clone(&self.taken_at);
        let bind = session.clone();
        let call_key = self.call_key.clone();
        tokio::spawn(async move {
            let ctx = broca_core::tool_plane::BatchCtx {
                batch_id: broca_protocol::BatchId("b".into()),
                bind,
                run_id: broca_protocol::RunId("run".into()),
                scope: None,
                media_capture: None,
            };
            let call = broca_protocol::ToolCall {
                tool_call_id: ToolCallId("a".into()),
                tool_name: "ask".into(),
                input: json!({}).into(),
                thought_signature: None,
                provider_state: None,
            };
            let reply = broca_core::tool_plane::ProviderHeldReply {
                tool_call_id: ToolCallId("a".into()),
                call_key,
                module: "tools".into(),
                reason: "running".into(),
                detail_digest: None,
            };
            let _ = slots.wait(&ctx, &call, &reply).await;
            *taken_at.lock().unwrap() = Some(tokio::time::Instant::now());
            slots.terminal_synced(&ctx, &call.tool_call_id).await;
        });
        Ok(())
    }
}

/// The whole delivery chain on a paused clock: the spawned puller loop, `HeldSlots` and
/// `SlotDelivery`. The entry is published before the first pull, and the first resume
/// request is lost, so the first delivery times out and pins the entry. The next pull after
/// the timeout delivers it from the pin and acks it once; later pulls read from the acked
/// position instead of re-reading the feed from its start.
#[tokio::test(start_paused = true)]
async fn a_delivery_that_times_out_is_delivered_from_its_pin_and_acked() {
    let call_key = "lineage:4";
    let bind = session("held");
    let root = rig_root("timed-out-delivery");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let runtime = session_store(&root).await;
    seed_root(&root, &bind, &held_records(call_key, "tools")).await;
    let store = Arc::new(LateResultsStore::open(&root.join("late-results.db")).unwrap());
    register_more(&store, &[registration(call_key, "tools", &bind)]).await;
    let slots = crate::held_slots::HeldSlots::new();
    let taken_at = Arc::new(Mutex::new(None));
    let actor = Arc::new(LosesFirstResume {
        slots: Arc::clone(&slots),
        lose_next: std::sync::atomic::AtomicBool::new(true),
        resumes: Mutex::new(Vec::new()),
        taken_at: Arc::clone(&taken_at),
        call_key: call_key.into(),
    });
    let mapper: Arc<crate::held_slots::EntryMapper> = Arc::new(|id, _entry| {
        Some(broca_protocol::ToolResult {
            tool_call_id: id.clone(),
            output: broca_protocol::ToolOutput::Text {
                text: "late".into(),
            },
            is_error: false,
        })
    });
    let delivery = Arc::new(crate::held_slots::SlotDelivery::new(
        Arc::clone(&slots),
        Arc::clone(&actor) as Arc<dyn crate::held_slots::HeldRunResumer>,
        mapper,
    ));
    let feed = Arc::new(KeepingFeed::default());
    feed.entries
        .lock()
        .unwrap()
        .push(entry(kinds::RESULT, call_key, "event-1"));
    let intake = LateResultsIntake::new(
        Arc::clone(&store),
        "broca",
        delivery as Arc<dyn HeldDelivery>,
        Arc::clone(&feed) as Arc<dyn LateResultsFeed>,
    );
    intake.set_session_store(runtime);
    let start = tokio::time::Instant::now();
    assert_eq!(
        intake.ensure_puller("tools").await.unwrap(),
        StartOutcome::Started
    );
    // Both waits run on the paused clock, so the bound costs no real time. It only turns
    // an ack that never comes into a failure instead of a hang.
    let acked = tokio::time::timeout(
        Duration::from_secs(600),
        feed.wait_until(|feed| !feed.acks.lock().unwrap().is_empty()),
    )
    .await;
    let since = |at: tokio::time::Instant| (at - start).as_millis();
    assert!(
        acked.is_ok(),
        "the entry is never acked; pinned={:?} feed_row={:?}",
        intake.puller("tools").map(|puller| puller.pinned()),
        store.feed("tools").unwrap()
    );
    let fallback = (crate::held_slots::SlotDelivery::DEFAULT_TIMEOUT + PULL_INTERVAL).as_millis();
    assert_eq!(taken_at.lock().unwrap().map(since), Some(fallback));
    let resumes: Vec<_> = actor
        .resumes
        .lock()
        .unwrap()
        .iter()
        .map(|(lost, at)| (*lost, since(*at)))
        .collect();
    assert_eq!(resumes, vec![(true, 0), (false, fallback)]);
    let acks: Vec<_> = feed
        .acks
        .lock()
        .unwrap()
        .iter()
        .map(|(seq, at)| (*seq, since(*at)))
        .collect();
    assert_eq!(acks, vec![(1, fallback)]);
    assert!(intake
        .puller("tools")
        .is_some_and(|puller| puller.pinned().is_empty()));
    let row = store.feed("tools").unwrap().unwrap();
    assert_eq!(row.position.map(|position| position.seq), Some(1));
    assert!(!row.ack_owed);

    // Two more rounds: both read from the acked position, and nothing is acked again.
    let pulls_at_ack = feed.pulls.lock().unwrap().len();
    tokio::time::timeout(
        Duration::from_secs(600),
        feed.wait_until(|feed| feed.pulls.lock().unwrap().len() >= pulls_at_ack + 2),
    )
    .await
    .expect("the puller keeps pulling while the call is registered as held");
    let later: Vec<_> = feed.pulls.lock().unwrap()[pulls_at_ack..]
        .iter()
        .map(|(since, _)| *since)
        .collect();
    assert!(later.iter().all(|since| *since == Some(1)), "{later:?}");
    assert_eq!(feed.acks.lock().unwrap().len(), 1);
    if let Some(puller) = intake.puller("tools") {
        puller.stop();
    }
}

/// A call handed back for the person's steer is terminal in the session, its outcome naming its
/// provider. The provider's later terminal for it is no held call's answer: it is delivered as a
/// notice, even though the call's run has ended since, and then acked.
#[tokio::test]
async fn a_handed_back_calls_terminal_is_delivered_as_a_notice() {
    let call_key = "lineage:7";
    let bind = session("handed-back");
    let rig = Rig::new(
        "handed-back",
        None,
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    let mut handed_back = outcome("run", "b", "a", CallOutcome::Completed);
    if let Record::ToolOutcome(record) = &mut handed_back {
        record.yielded_by = Some("tools".into());
    }
    rig.seed(
        &bind,
        &[
            run_start("run"),
            batch_start("run", "b", &["a"]),
            intent("run", "b", "a", "tools", Some(call_key)),
            handed_back,
            batch_end("run", "b"),
            run_end("run", RunFinishReason::Completed),
        ],
    )
    .await;
    // No batch waits on the call, yet its provider's terminal is still owed, so the provider's
    // feed must be read.
    assert!(rig.intake.module_needs_feed("tools").await.unwrap());
    let _puller = rig.puller("tools");
    rig.feed.replies(vec![reply(
        vec![entry(kinds::RESULT, call_key, "ev-1")],
        1,
        false,
    )]);
    rig.intake.tick("tools").await.unwrap();
    let noticed = rig.delivery.noticed.lock().unwrap().clone();
    assert_eq!(noticed.len(), 1, "{noticed:?}");
    assert_eq!(noticed[0].0, call_key);
    assert_eq!(noticed[0].1.tool_call_id, ToolCallId("a".into()));
    assert!(
        rig.delivery.delivered.lock().unwrap().is_empty(),
        "no batch waits for it"
    );
    assert_eq!(rig.feed.acks(), vec![cursor(1)]);
}

/// A session deleted while a call of it was registered, then created again at the same triple
/// before the intake saw it gone, holds none of that call's records: the call key names the
/// deleted session's lineage. The registration is retired as the deleted session's, so it keeps
/// no feed running, and the call's late entry settles as a no-op instead of pinning the ack
/// while the intake looks for its intent in the new session's ledger every round.
#[tokio::test]
async fn a_registration_of_a_session_deleted_and_created_again_is_retired() {
    let call_key = "lineage:4";
    let bind = session("created-again");
    let rig = Rig::new(
        "created-again",
        None,
        vec![registration(call_key, "tools", &bind)],
    )
    .await;
    // The session at the triple now is a new one, whose finished run never made the call.
    rig.seed(
        &bind,
        &[run_start("run"), run_end("run", RunFinishReason::Completed)],
    )
    .await;
    let _puller = rig.puller("tools");
    rig.feed.replies(vec![reply(
        vec![entry(kinds::RESULT, call_key, "ev-1")],
        1,
        false,
    )]);
    rig.intake.tick("tools").await.unwrap();
    assert!(rig.delivery.delivered.lock().unwrap().is_empty());
    assert_eq!(rig.feed.acks(), vec![cursor(1)], "the entry pins nothing");
    assert!(
        !rig.intake.module_needs_feed("tools").await.unwrap(),
        "the deleted session's call keeps no feed"
    );
    assert!(rig.store.registration("tools", call_key).unwrap().is_none());
}

/// A round reads each registered session once, for both feed liveness and classification, without
/// loading whole session views for closed sessions.
#[cfg(feature = "test-clock")]
#[tokio::test]
async fn a_puller_round_reads_closed_sessions_once_without_loading_views() {
    use broca_core::tool_plane::LateResultsIntake as _;
    use std::sync::atomic::Ordering;

    let calls = [
        ("lineage:4", session("one-read-a-round-a"), "ev-1"),
        ("lineage:5", session("one-read-a-round-b"), "ev-2"),
        ("lineage:6", session("one-read-a-round-c"), "ev-3"),
    ];
    let root = rig_root("one-read-a-round");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    // A view cap no view fits, so every read of the session loads its view again.
    let runtime = Arc::new(
        crate::store_open::StoreRuntime::open(
            &root,
            broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
            Arc::new(NoRuns),
            Duration::from_secs(600),
        )
        .await
        .unwrap()
        .with_view_bytes_for_test(1),
    );
    let mut registrations = Vec::new();
    for (key, bind, _) in &calls {
        seed_root(&root, bind, &held_records(key, "tools")).await;
        registrations.push(registration(key, "tools", bind));
    }
    let store = Arc::new(LateResultsStore::open(&root.join("late-results.db")).unwrap());
    store.register_group(&registrations).await.unwrap();
    let feed = Arc::new(ScriptedFeed::default());
    let delivery = Arc::new(RecordedDelivery::default());
    let intake = LateResultsIntake::new(
        Arc::clone(&store),
        "broca",
        Arc::clone(&delivery) as Arc<dyn HeldDelivery>,
        Arc::clone(&feed) as Arc<dyn LateResultsFeed>,
    );
    intake.set_session_store(Arc::clone(&runtime));
    let loads = || {
        runtime
            .views()
            .memory(|_| 0)
            .loads_by_reason
            .and_then(|reasons| reasons.get("held_calls").map(|loads| loads.loads))
            .unwrap_or(0)
    };
    feed.replies(vec![reply(
        calls
            .iter()
            .map(|(key, _, event)| entry(kinds::RESULT, key, event))
            .collect(),
        3,
        false,
    )]);
    let before = loads();
    let probes = runtime.class_probes.load(Ordering::Relaxed);
    assert!(
        intake.round("tools", &Puller::new()).await,
        "the held call needs the feed"
    );
    assert_eq!(
        *delivery.delivered.lock().unwrap(),
        calls.map(|(key, _, event)| (key.to_owned(), event.to_owned())),
        "the round classified and delivered each held entry"
    );
    assert_eq!(
        loads() - before,
        0,
        "intake should not load a whole view for any closed session"
    );
    assert_eq!(
        runtime.class_probes.load(Ordering::Relaxed) - probes,
        3,
        "one class probe per session for the whole round"
    );
}

#[path = "closed_read_tests.rs"]
mod closed_read_tests;

/// A provider that answers `withdraw_pending` for each of `pending` attempts, then withdraws the
/// call, recording when each attempt arrived.
struct PendingWithdrawalRoute {
    hint_ms: Option<u64>,
    pending: usize,
    seen: Mutex<Vec<tokio::time::Instant>>,
}

#[async_trait]
impl crate::held_withdrawals::WithdrawTransport for PendingWithdrawalRoute {
    async fn withdraw(
        &self,
        _: &LateResultRegistration,
        _: bool,
    ) -> (
        cortexkit_role_tool_provider::withdraw::WithdrawVerdict,
        Value,
    ) {
        use cortexkit_role_tool_provider::withdraw::caller_verdict;
        let mut seen = self.seen.lock().unwrap();
        seen.push(tokio::time::Instant::now());
        if seen.len() <= self.pending {
            let error = cortexkit_role_tool_provider::errors::withdraw_pending(self.hint_ms);
            (
                caller_verdict(Err(&error)),
                serde_json::to_value(error).unwrap(),
            )
        } else {
            let raw = json!({"answer": "withdrawn"});
            (caller_verdict(Ok(&raw)), raw)
        }
    }
}

/// `withdraw_pending` means the provider accepted the withdrawal but can't yet confirm the call
/// stopped (tool-provider 0.15.12 §5). The withdrawal stays owed, never failed, however many
/// attempts it takes: each retry waits the provider's hint clamped to 1 to 60 seconds, or a
/// backoff in the same bounds, and the run's end doesn't wait for any of them.
#[tokio::test(start_paused = true)]
async fn a_pending_withdrawal_stays_owed_and_is_asked_again_within_the_contract_delays() {
    use crate::held_slots::HeldWithdrawal;
    // (hint, the shortest and longest gap between two attempts)
    for (hint_ms, shortest, longest) in [
        (None, 1_000, 60_000),
        (Some(200), 1_000, 1_000),
        (Some(5_000), 5_000, 5_000),
        (Some(600_000), 60_000, 60_000),
    ] {
        let name = format!(
            "pending-{}",
            hint_ms.map_or("none".into(), |ms: u64| ms.to_string())
        );
        let bind = session(&name);
        let mut records = held_records("lineage:4", "tools");
        cancelled_held(&mut records, &["a"]);
        let (root, runtime) = withdrawal_root(&format!("withdraw-{name}"), &bind, &records).await;
        let store = Arc::new(LateResultsStore::open(&root.join("late-results.db")).unwrap());
        store
            .register_calls(&[registration("lineage:4", "tools", &bind)])
            .unwrap();
        // More pending answers than the route-refusal budget allows.
        let route = Arc::new(PendingWithdrawalRoute {
            hint_ms,
            pending: 5,
            seen: Mutex::default(),
        });
        let service = withdrawals(store.clone(), &runtime, route.clone(), Default::default());
        service.withdraw(&held_reply("lineage:4")).await.unwrap();
        assert_eq!(
            route.seen.lock().unwrap().len(),
            1,
            "the caller waits for one attempt only"
        );
        let health = service.health();
        assert_eq!(
            (&health["pending"], &health["failed"]),
            (&json!(1), &json!(0)),
            "{health}"
        );
        withdrawals_settled(&service).await;
        let seen = route.seen.lock().unwrap().clone();
        assert_eq!(
            seen.len(),
            6,
            "{hint_ms:?}: asked until the provider answered"
        );
        let gaps: Vec<u128> = seen.windows(2).map(|w| (w[1] - w[0]).as_millis()).collect();
        assert!(
            gaps.iter().all(|gap| (shortest..=longest).contains(gap)),
            "{hint_ms:?}: gaps {gaps:?}"
        );
        assert_eq!(
            store.withdrawal_state("tools", "lineage:4").unwrap(),
            Some(("done".into(), 6))
        );
        assert_eq!(service.health()["failed"], 0);
    }
}

/// A withdrawal still pending when the module stops is asked again at the next start, and a
/// stopping module sends no further retry.
#[tokio::test(start_paused = true)]
async fn a_pending_withdrawal_left_at_a_stop_is_asked_again_at_the_next_start() {
    use crate::held_slots::HeldWithdrawal;
    let bind = session("pending-across-stop");
    let mut records = held_records("lineage:4", "tools");
    cancelled_held(&mut records, &["a"]);
    let (root, runtime) = withdrawal_root("withdraw-pending-stop", &bind, &records).await;
    let path = root.join("late-results.db");
    let store = Arc::new(LateResultsStore::open(&path).unwrap());
    store
        .register_calls(&[registration("lineage:4", "tools", &bind)])
        .unwrap();
    let route = Arc::new(PendingWithdrawalRoute {
        hint_ms: Some(30_000),
        pending: 1,
        seen: Mutex::default(),
    });
    let stopping = tokio_util::sync::CancellationToken::new();
    let service = withdrawals(store, &runtime, route.clone(), stopping.clone());
    service.withdraw(&held_reply("lineage:4")).await.unwrap();
    stopping.cancel();
    withdrawals_settled(&service).await;
    assert_eq!(
        route.seen.lock().unwrap().len(),
        1,
        "a stopping module retries nothing"
    );
    drop(service);
    let store = Arc::new(LateResultsStore::open(&path).unwrap());
    store.recovery_snapshot().unwrap();
    let service = withdrawals(store.clone(), &runtime, route.clone(), Default::default());
    service.recover().await.unwrap();
    withdrawals_settled(&service).await;
    assert_eq!(route.seen.lock().unwrap().len(), 2);
    assert_eq!(
        store.withdrawal_state("tools", "lineage:4").unwrap(),
        Some(("done".into(), 2))
    );
}

/// `run_start(run)`, written `age` before now.
fn run_start_aged(run: &str, age: Duration) -> Record {
    let mut record = run_start(run);
    if let Record::RunStart(start) = &mut record {
        let at = crate::actor::epoch_ms() - age.as_millis() as u64;
        start.ts_ms = Some(at);
        start.written_at_ms = Some(at);
    }
    record
}

const DAY: Duration = Duration::from_secs(24 * 60 * 60);

/// A run started `age` ago whose one call, `key` in batch `batch`, was handed back to the person
/// for their steer, then finished: the provider still owes the call its terminal.
fn handed_back_run(run: &str, batch: &str, key: &str, age: Duration) -> Vec<Record> {
    let mut handed_back = outcome(run, batch, "a", CallOutcome::Completed);
    if let Record::ToolOutcome(record) = &mut handed_back {
        record.yielded_by = Some("tools".into());
    }
    vec![
        run_start_aged(run, age),
        batch_start(run, batch, &["a"]),
        intent(run, batch, "a", "tools", Some(key)),
        handed_back,
        batch_end(run, batch),
        run_end(run, RunFinishReason::Completed),
    ]
}

/// A run started `age` ago whose provider held its one call `key`, then was cancelled with the
/// call still held: the provider is owed a withdrawal.
fn cancelled_while_held(key: &str, age: Duration) -> Vec<Record> {
    let mut records = held_records(key, "tools");
    records[0] = run_start_aged("run", age);
    cancelled_held(&mut records, &["a"]);
    records
}

/// The call keys a database lost from `root` is rebuilt with from its sessions.
async fn rebuilt_keys(
    store: &LateResultsStore,
    root: &Path,
    runtime: &crate::store_open::StoreRuntime,
) -> Vec<String> {
    let mut report = RebuildReport::default();
    rebuild_store_from_sessions_with_progress(store, root, Some(runtime), &mut report, |_| {})
        .await
        .unwrap();
    assert!(report.unreadable.is_empty(), "{report:?}");
    let mut keys: Vec<String> = store
        .registrations()
        .unwrap()
        .into_iter()
        .map(|call| call.call_key)
        .collect();
    keys.sort();
    keys
}

/// A call handed back for the person's steer keeps its registration after its run ends, until
/// its provider's terminal arrives as a notice. A database rebuilt from the sessions' records
/// keeps it too, for runs started within the rebuild's window, however many runs came after.
/// An open batch's held call is rebuilt whenever its run started.
#[tokio::test]
async fn a_rebuild_keeps_the_handed_back_calls_of_recent_finished_runs() {
    let root = rig_root("rebuild-handed-back");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let runtime = session_store(&root).await;
    let recent = session("handed-back-recent");
    seed_root(
        &root,
        &recent,
        &handed_back_run("run", "b", "recent:7", DAY),
    )
    .await;
    let old = session("handed-back-old");
    seed_root(&root, &old, &handed_back_run("run", "b", "old:7", 8 * DAY)).await;
    // A handed-back call two runs back, both runs within the window.
    let earlier = session("handed-back-earlier");
    let mut records = handed_back_run("r1", "b1", "earlier:7", 2 * DAY);
    records.extend([
        run_start_aged("r2", DAY),
        run_end("r2", RunFinishReason::Completed),
    ]);
    seed_root(&root, &earlier, &records).await;
    // A handed-back call of a run started before the window, behind a recent run.
    let behind = session("handed-back-behind");
    let mut records = handed_back_run("r1", "b1", "behind:7", 8 * DAY);
    records.extend([
        run_start_aged("r2", DAY),
        run_end("r2", RunFinishReason::Completed),
    ]);
    seed_root(&root, &behind, &records).await;
    // A held call still waiting in a run started long before the window.
    let waiting = session("held-long-ago");
    let mut records = held_records("waiting:4", "tools");
    records[0] = run_start_aged("run", 30 * DAY);
    seed_root(&root, &waiting, &records).await;
    let store = LateResultsStore::open(&root.join("rebuilt.db")).unwrap();
    assert_eq!(
        rebuilt_keys(&store, &root, &runtime).await,
        ["earlier:7", "recent:7", "waiting:4"]
    );
    let call = store
        .registration("tools", "recent:7")
        .unwrap()
        .unwrap()
        .call;
    assert_eq!((call.lineage.as_str(), &call.bind), ("recent", &recent));
    assert!(store.feed("tools").unwrap().unwrap().ack_owed);
}

/// A held call cancelled with its run is owed a withdrawal until its provider answers one. A
/// database rebuilt from the sessions' records keeps its watch for runs started within the
/// rebuild's window, and withdrawal recovery then sends the withdrawal.
#[tokio::test]
async fn a_rebuild_keeps_the_withdrawal_owed_for_a_held_call_cancelled_with_its_run() {
    let root = rig_root("rebuild-cancelled-held");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let runtime = session_store(&root).await;
    seed_root(
        &root,
        &session("cancelled-recent"),
        &cancelled_while_held("recent:4", DAY),
    )
    .await;
    seed_root(
        &root,
        &session("cancelled-old"),
        &cancelled_while_held("old:4", 8 * DAY),
    )
    .await;
    let store = Arc::new(LateResultsStore::open(&root.join("late-results.db")).unwrap());
    assert_eq!(rebuilt_keys(&store, &root, &runtime).await, ["recent:4"]);
    let route = Arc::new(WithdrawalRoute {
        seen: Mutex::default(),
        mode: "ok",
    });
    let service = withdrawals(store.clone(), &runtime, route.clone(), Default::default());
    service.recover().await.unwrap();
    assert_eq!(*route.seen.lock().unwrap(), ["recent:4"]);
    assert_eq!(
        store.withdrawal_state("tools", "recent:4").unwrap(),
        Some(("done".into(), 1))
    );
}
