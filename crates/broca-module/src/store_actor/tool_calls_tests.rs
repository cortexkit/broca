//! `run.tool_calls` of a session kept in the session store, through the module's registry, in both
//! of the session store's modes.
//!
//! Each page is pinned (see `broca_test_support::pins`) with what the storage mints for itself
//! taken out: each call's key (kept as whether it is present), and the `next` cursor, which names
//! the log's own positions (kept as whether it is present, and followed to read every page).

use std::sync::Arc;

use broca_core::FileLeaseStore;
use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason, ToolCall};
use broca_protocol::{
    BindIdentity, MessageId, NormalizedEvent, ProviderControl, RunId, ToolCallId, Usage,
};
use broca_session_store::keys::Cipher;
use cortexkit_role_llm_runner::tool_calls::{ToolCallsPage, ToolCallsRequest};
use serde_json::{json, Value};
use tokio::sync::Notify;

use super::differential::IDLE;
use super::tests::{executor, read_step, request, text_step, Planes};
use crate::actor::{LineageStatus, SendKey, SendOutcome};
use crate::executor::{RunExecutor, RunKind};
use crate::registry::LineageRegistry;
use crate::serve::{ServeError, ServeHandler};

/// One module over a scratch folder, whose sessions start in the session store when `store`.
struct Side {
    dir: std::path::PathBuf,
    registry: LineageRegistry,
    key: BindIdentity,
    /// The caller whose sends reach the side's session.
    caller: super::tests::Caller,
    /// Removes the folder when dropped.
    owns_dir: bool,
}

impl Drop for Side {
    fn drop(&mut self) {
        if self.owns_dir {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

fn scratch() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "store-tool-calls-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Each side's gate on its tool dispatches: a dispatch signals the first `Notify` and waits for
/// a release of the second.
type ToolGate = (Arc<Notify>, Arc<Notify>);

fn tool_gate() -> ToolGate {
    (Arc::new(Notify::new()), Arc::new(Notify::new()))
}

/// A module over `dir` whose scripted provider answers `steps`, and whose tool dispatches each
/// wait for a release of `tools` when it is given. With `store`, a session that exists nowhere
/// starts in the session store, in mode `cipher`.
async fn side_at(
    dir: std::path::PathBuf,
    steps: Vec<Vec<NormalizedEvent>>,
    tools: Option<ToolGate>,
    cipher: Cipher,
) -> Side {
    side_answering(dir, steps, tools, cipher, "file contents").await
}

/// [`side_at`], whose `read` tool answers `tool_text`.
async fn side_answering(
    dir: std::path::PathBuf,
    steps: Vec<Vec<NormalizedEvent>>,
    tools: Option<ToolGate>,
    cipher: Cipher,
    tool_text: &str,
) -> Side {
    let planes = Planes {
        tool_gate: tools,
        ..Planes::default()
    };
    let mut store_loop = executor(steps.clone(), None);
    store_loop.planes = planes.clone();
    store_loop.tool_text = tool_text.into();
    let executor: Arc<dyn RunExecutor> = Arc::new(store_loop);
    let runtime = crate::store_open::StoreRuntime::open(
        &dir,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32]).creating(cipher),
        Arc::clone(&executor),
        IDLE,
    )
    .await
    .unwrap();
    let registry = LineageRegistry::new(Arc::new(
        crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            executor,
            std::sync::Arc::new(crate::store_open::StoreSlot::loaded(Arc::new(runtime))),
        )
        .with_idle_ttl(IDLE),
    ));
    let key = BindIdentity::new(dir.join("project"), "store-actor", "s1");
    Side {
        dir,
        registry,
        key,
        caller: Default::default(),
        owns_dir: true,
    }
}

/// The session store's side over the script, and its gate when `gated`.
async fn pair(
    steps: impl Fn() -> Vec<Vec<NormalizedEvent>>,
    gated: bool,
    cipher: Cipher,
) -> (Side, Option<ToolGate>) {
    let store_gate = gated.then(tool_gate);
    (
        side_at(scratch(), steps(), store_gate.clone(), cipher).await,
        store_gate,
    )
}

/// A side in the session store, over the script.
async fn side(steps: Vec<Vec<NormalizedEvent>>, gate: Option<ToolGate>, cipher: Cipher) -> Side {
    side_at(scratch(), steps, gate, cipher).await
}

impl Side {
    /// Sends `text` as the side's caller sends it.
    async fn send(&self, run_id: &str, text: &str) -> SendOutcome {
        let request = self.caller.shape(run_id, &SendKey::Minted, request(text));
        self.registry
            .route_submission_with_order(
                &self.key,
                RunId(run_id.into()),
                request,
                RunKind::FreshOrResume,
                SendKey::Minted,
                false,
                broca_protocol::Delivery::Queue,
                None,
                None,
                None,
                None,
                None,
                false,
                None,
            )
            .await
            .expect("the session opens")
    }

    async fn idle(&self) {
        broca_test_support::wait_until("the session is idle", || async {
            match self.registry.route_status(&self.key).await {
                Ok(LineageStatus::Idle) => Ok(()),
                other => Err(format!("{other:?}")),
            }
        })
        .await;
    }

    /// Waits until the session's actor has closed and left the registry.
    async fn closed(&self) {
        broca_test_support::wait_until("the session's actor closed", || async {
            match self.registry.slot_state(&self.key) {
                None => Ok(()),
                Some(state) => Err(format!("{state:?}")),
            }
        })
        .await;
    }

    /// Whether the session is kept in the session store, in mode `cipher`.
    fn stored_in(&self, cipher: Cipher) -> bool {
        use broca_session_store::backend::{LocalBackend, SessionAddr};
        let stored = LocalBackend::new(self.dir.join(broca_records::SESSION_STORAGE_DIR))
            .read_stored_head(&SessionAddr(broca_core::session_addr(&self.key)))
            .unwrap();
        stored.is_some_and(|(head, _)| {
            broca_session_store::head::stored_cipher(&head).unwrap() == cipher
        })
    }

    async fn calls(
        &self,
        run_id: &str,
        after: Option<String>,
        limit: Option<u64>,
    ) -> Result<ToolCallsPage, ServeError> {
        let mut request = ToolCallsRequest::new(run_id);
        if let Some(after) = after {
            request = request.with_after(after);
        }
        if let Some(limit) = limit {
            request = request.with_limit(limit);
        }
        self.registry.run_tool_calls(&self.key, request).await
    }

    /// Every page of the run's calls, `limit` at a time, following each page's `next`.
    async fn pages(&self, run_id: &str, limit: Option<u64>) -> Vec<Value> {
        let mut pages = Vec::new();
        let mut after = None;
        loop {
            let page = self.calls(run_id, after, limit).await;
            let next = page.as_ref().ok().and_then(|page| page.next.clone());
            pages.push(reply(page));
            match next {
                Some(next) => after = Some(next),
                None => return pages,
            }
        }
    }
}

/// A page as JSON with each storage's own keys and cursors kept as whether they are present, or
/// the refusal's variant.
fn reply(page: Result<ToolCallsPage, ServeError>) -> Value {
    match page {
        Ok(page) => {
            let mut value = serde_json::to_value(page).unwrap();
            if let Some(next) = value.get_mut("next") {
                *next = Value::Bool(true);
            }
            for call in value["calls"].as_array_mut().unwrap() {
                if let Some(key) = call.get_mut("call_key") {
                    *key = Value::Bool(key.is_string());
                }
            }
            value
        }
        Err(error) => json!({ "refused": format!("{error:?}") }),
    }
}

/// A step that calls the `read` tool once for each of `ids`, in order.
fn reads(id: &str, ids: &[&str]) -> Vec<NormalizedEvent> {
    let calls: Vec<_> = ids
        .iter()
        .enumerate()
        .map(|(n, call)| (*call, json!({ "n": n })))
        .collect();
    reads_with(id, &calls)
}

/// A step that calls the `read` tool once for each `(id, input)` of `calls`, in order.
fn reads_with(id: &str, calls: &[(&str, Value)]) -> Vec<NormalizedEvent> {
    let calls = calls
        .iter()
        .map(|(call, input)| {
            ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                provider_state: None,
                tool_call_id: ToolCallId((*call).into()),
                tool_name: "read".into(),
                input: input.clone().into(),
            })
        })
        .collect();
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: calls,
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(100),
                output_tokens: Some(20),
                ..Usage::default()
            },
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

/// The outcome of each call a page lists.
fn outcomes(page: &Value) -> Vec<Value> {
    page["calls"]
        .as_array()
        .map(|calls| calls.iter().map(|call| call["outcome"].clone()).collect())
        .unwrap_or_default()
}

/// Two finished runs: the first made three calls over two batches, the second none. Each run's
/// calls, a page at a time and whole, read alike; so does a run id no run carries. A cursor one
/// storage returned is refused by the other, naming `after`.
#[tokio::test]
async fn finished_runs_list_their_calls_alike() {
    for cipher in Cipher::ALL {
        let steps = || {
            vec![
                reads("a1", &["c1", "c2"]),
                read_step("a2"),
                text_step("a3", "done"),
                text_step("a4", "again"),
            ]
        };
        let (store, _) = pair(steps, false, cipher).await;
        for side in [&store] {
            for (run, text) in [("r1", "first"), ("r2", "second")] {
                assert!(matches!(
                    side.send(run, text).await,
                    SendOutcome::Active { .. }
                ));
                side.idle().await;
            }
        }
        assert!(store.stored_in(cipher), "{}", cipher.label());
        for (run, limit) in [
            ("r1", None),
            ("r1", Some(1)),
            ("r1", Some(2)),
            ("r2", None),
            ("unknown", None),
        ] {
            let pages = store.pages(run, limit).await;
            broca_test_support::pin!(format!("{run} by {limit:?}, {}", cipher.label()), pages);
        }
        let whole = store.pages("r1", None).await;
        assert_eq!(
            outcomes(&whole[0]),
            [json!("succeeded"), json!("succeeded"), json!("succeeded")],
            "{whole:?}"
        );
        assert_eq!(store.pages("r1", Some(1)).await.len(), 3);
        // A session's cursors name its own log: another session's cursor is refused.
        let other = side(steps(), None, cipher).await;
        for (run, text) in [("r1", "first"), ("r2", "second")] {
            other.send(run, text).await;
            other.idle().await;
        }
        let cursor = |page: Result<ToolCallsPage, ServeError>| page.unwrap().next.unwrap();
        let from_other = cursor(other.calls("r1", None, Some(1)).await);
        match store.calls("r1", Some(from_other), None).await {
            Err(ServeError::InvalidParams { field, .. }) => assert_eq!(field, "after"),
            other => panic!("a cursor from another log: {other:?}"),
        }
    }
}

/// A step whose two calls share an id: the first copy is dispatched and the second refused, and
/// both list alike, each with a key of its own.
#[tokio::test]
async fn a_repeated_call_id_lists_alike() {
    for cipher in Cipher::ALL {
        let steps = || vec![reads("a1", &["c1", "c1"]), text_step("a2", "done")];
        let (store, _) = pair(steps, false, cipher).await;
        for side in [&store] {
            assert!(matches!(
                side.send("r1", "first").await,
                SendOutcome::Active { .. }
            ));
            side.idle().await;
        }
        for limit in [None, Some(1)] {
            let pages = store.pages("r1", limit).await;
            broca_test_support::pin!(format!("by {limit:?}, {}", cipher.label()), pages);
        }
        let page = store.calls("r1", None, None).await.unwrap();
        let outcomes: Vec<&str> = page
            .calls
            .iter()
            .map(|call| call.outcome.as_str())
            .collect();
        assert_eq!(outcomes, ["succeeded", "refused"], "{page:?}");
        let keys: Vec<_> = page
            .calls
            .iter()
            .map(|call| call.call_key.clone())
            .collect();
        assert!(
            keys.iter().all(Option::is_some) && keys[0] != keys[1],
            "{keys:?}"
        );
    }
}

/// A run whose second batch is in flight: the first batch's call succeeded and the second's,
/// dispatched with no result yet, is indeterminate, alike, read whole and a page at a time.
/// Once the call finishes, the run's calls list alike again.
#[tokio::test]
async fn a_batch_in_flight_lists_alike() {
    for cipher in Cipher::ALL {
        let steps = || vec![read_step("a1"), read_step("a2"), text_step("a3", "done")];
        let (store, store_gate) = pair(steps, true, cipher).await;
        for (side, gate) in [(&store, &store_gate)] {
            assert!(matches!(
                side.send("r1", "first").await,
                SendOutcome::Active { .. }
            ));
            // The first batch's dispatch goes through; the second's waits.
            gate.as_ref().unwrap().1.notify_one();
            broca_test_support::wait_until("the second call is dispatched", || async {
                let page = reply(side.calls("r1", None, None).await);
                match outcomes(&page).as_slice() {
                    [first, second] if first == "succeeded" && second == "indeterminate" => Ok(()),
                    _ => Err(page.to_string()),
                }
            })
            .await;
        }
        for limit in [None, Some(1)] {
            let pages = store.pages("r1", limit).await;
            broca_test_support::pin!(format!("in flight by {limit:?}, {}", cipher.label()), pages);
        }
        for (side, gate) in [(&store, &store_gate)] {
            gate.as_ref().unwrap().1.notify_one();
            side.idle().await;
        }
        let pages = store.pages("r1", None).await;
        broca_test_support::pin!(format!("finished, {}", cipher.label()), pages);
        assert_eq!(
            outcomes(&pages[0]),
            [json!("succeeded"), json!("succeeded")]
        );
    }
}

/// A call's result is written as soon as it comes back, before the rest of its batch. While the
/// batch is still open, that call is finished, so it lists the ordinal its result is read at,
/// as the contract wants of every finished call; a call still waiting lists none.
#[tokio::test]
async fn a_finished_call_in_an_open_batch_lists_its_result_ordinal() {
    let steps = || vec![reads("a1", &["c1", "c2"]), text_step("a2", "done")];
    let (side, gate) = pair(steps, true, Cipher::None).await;
    let gate = gate.unwrap();
    assert!(matches!(
        side.send("r1", "first").await,
        SendOutcome::Active { .. }
    ));
    gate.1.notify_one();
    let page = broca_test_support::wait_until("one call of the batch is answered", || async {
        let page = reply(side.calls("r1", None, None).await);
        if outcomes(&page).iter().any(|outcome| outcome == "succeeded") {
            Ok(page)
        } else {
            Err(page.to_string())
        }
    })
    .await;
    for call in page["calls"].as_array().unwrap() {
        if call["outcome"] == "succeeded" {
            let ordinal = call["ordinal"].as_u64().unwrap();
            assert_eq!(call["result_ordinal"], json!(ordinal + 1), "{page}");
        } else {
            assert!(call.get("result_ordinal").is_none(), "{page}");
        }
    }
    gate.1.notify_one();
    side.idle().await;
    let pages = side.pages("r1", None).await;
    assert_eq!(
        outcomes(&pages[0]),
        [json!("succeeded"), json!("succeeded")]
    );
}

/// Runs a session on `dir` until its run's call is dispatched and waiting for its tool, then
/// drops every task where it waits, as a crash does: nothing records the call's result.
async fn crash_mid_dispatch(dir: std::path::PathBuf, cipher: Cipher) {
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let side = runtime.block_on(async {
            let steps = vec![read_step("a1"), text_step("a2", "done")];
            let mut side = side_at(dir, steps, Some(tool_gate()), cipher).await;
            side.owns_dir = false;
            assert!(matches!(
                side.send("r1", "first").await,
                SendOutcome::Active { .. }
            ));
            broca_test_support::wait_until("the call is dispatched", || async {
                let page = reply(side.calls("r1", None, None).await);
                match outcomes(&page).as_slice() {
                    [only] if only == "indeterminate" => Ok(()),
                    _ => Err(page.to_string()),
                }
            })
            .await;
            side
        });
        // The runtime drops every task where it waits, the actor's included, so nothing seals
        // the run. The module itself is never dropped, so no actor is told to stop first.
        drop(runtime);
        std::mem::forget(side);
    })
    .await
    .unwrap();
}

/// A call a crash cut off after dispatch lists as indeterminate. A store read recovers the
/// session before answering, matching the log-backed actor after recovery, and subsequent reads
/// keep the recorded unknown-outcome result unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn a_call_a_crash_cut_off_lists_as_indeterminate_alike() {
    for cipher in Cipher::ALL {
        let store_dir = scratch();
        crash_mid_dispatch(store_dir.clone(), cipher).await;
        let store = side_at(store_dir, Vec::new(), None, cipher).await;
        assert!(store.stored_in(cipher), "{}", cipher.label());
        let before = store.pages("r1", None).await;
        assert!(store.registry.slot_state(&store.key).is_some());
        broca_test_support::pin!(format!("the recovering read, {}", cipher.label()), before);
        assert_eq!(outcomes(&before[0]), [json!("indeterminate")]);
        assert!(
            before[0]["calls"][0]["result_bytes"].is_u64(),
            "the read recorded the call's unknown-outcome result"
        );
        // Explicit status and actor eviction must not change what the recovering read answered.
        for side in [&store] {
            side.registry.status(&side.key).await.unwrap();
            side.closed().await;
        }
        let after = store.pages("r1", None).await;
        assert_eq!(after, before, "the recovered read is stable");
        assert_eq!(outcomes(&after[0]), [json!("indeterminate")]);
        assert!(
            after[0]["calls"][0]["result_bytes"].is_u64(),
            "the open recorded the call's unknown-outcome result"
        );
    }
}

/// A page stops at the byte target before a call that would cross it, and a call larger than the
/// target is served alone and whole: arguments are never cut to fit.
#[tokio::test]
async fn pages_keep_their_byte_target_and_serve_an_oversized_call_whole() {
    let large = "a".repeat(1_700_000);
    let oversized = "b".repeat(3_200_000);
    let steps = vec![
        reads_with(
            "a1",
            &[
                ("c1", json!({ "content": large })),
                ("c2", json!({ "content": large })),
            ],
        ),
        reads_with("a2", &[("big", json!({ "content": oversized }))]),
        text_step("a3", "done"),
    ];
    let store = side(steps, None, Cipher::None).await;
    assert!(matches!(
        store.send("r1", "first").await,
        SendOutcome::Active { .. }
    ));
    store.idle().await;
    let mut after = None;
    let mut listed = Vec::new();
    loop {
        let page = store.calls("r1", after, None).await.unwrap();
        let ids: Vec<_> = page
            .calls
            .iter()
            .map(|call| call.tool_call_id.clone())
            .collect();
        for call in &page.calls {
            listed.push((
                call.tool_call_id.clone(),
                call.args["content"].as_str().unwrap().len(),
            ));
        }
        assert_eq!(ids.len(), 1, "each call fills a page on its own: {ids:?}");
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert_eq!(
        listed,
        [
            ("c1".to_owned(), 1_700_000),
            ("c2".to_owned(), 1_700_000),
            ("big".to_owned(), 3_200_000),
        ]
    );
}

/// A cursor whose members were changed after the module issued it is refused, naming `after`,
/// even when it still names a real call of the run.
#[tokio::test]
async fn a_cursor_changed_after_issue_is_refused() {
    let steps = vec![reads("a1", &["c1", "c2", "c3"]), text_step("a2", "done")];
    let store = side(steps, None, Cipher::None).await;
    store.send("r1", "first").await;
    store.idle().await;
    let first = store.calls("r1", None, Some(1)).await.unwrap();
    let issued = first.next.unwrap();
    let mut cursor: Value = serde_json::from_str(&issued).expect("a cursor is a JSON object");
    let changed = cursor
        .as_object_mut()
        .unwrap()
        .values_mut()
        .find_map(|value| value.as_u64().map(|number| (value, number)))
        .map(|(value, number)| *value = json!(number + 1));
    assert!(
        changed.is_some(),
        "the cursor has a number to change: {issued}"
    );
    match store
        .calls("r1", Some(serde_json::to_string(&cursor).unwrap()), Some(1))
        .await
    {
        Err(ServeError::InvalidParams { field, .. }) => assert_eq!(field, "after"),
        other => panic!("a changed cursor was served: {other:?}"),
    }
}

/// A page names each call's result by its size only: the result's content never appears.
#[tokio::test]
async fn a_page_never_holds_a_results_content() {
    const SECRET: &str = "TOOL-RESULT-CONTENT-MUST-NOT-LEAK-7f52e3";
    let steps = vec![reads("a1", &["c1"]), text_step("a2", "done")];
    let store = side_answering(scratch(), steps, None, Cipher::None, SECRET).await;
    store.send("r1", "first").await;
    store.idle().await;
    let page = store.calls("r1", None, None).await.unwrap();
    let body = serde_json::to_string(&page).unwrap();
    assert!(!body.contains(SECRET), "{body}");
    assert!(page.calls[0].result_bytes.unwrap() >= SECRET.len() as u64);
}
