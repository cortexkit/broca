//! Memory budgets of a store actor, measured with the counting allocator on a synthetic session
//! shaped like production ones (`broca_session_store::synthetic`), and the memory report's
//! estimates for the same actor held to what the allocator measured.
//!
//! The budget is the one `.cortexkit/internal-docs/designs/session-storage/27-memory-shape.md`
//! sets: an idle open session holds at most `1.2 x W + 400 B x messages`, where `W` is the
//! window's messages as compact JSON; a session inside a provider call holds that plus the
//! request's bytes, plus its compaction entries until they share content with its messages; and
//! sessions on one plan hold its tool list once. Each limit below is today's measure plus 10%,
//! with today's measure and the design's target beside it. A change that lowers a holder lowers
//! its limit; none may raise one.
//!
//! Every measured actor runs on a current-thread runtime whose blocking pool joins the test's
//! counting group, so the live projection's seeding off the actor is counted too.

use std::sync::Arc;

use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::session::Session;
use broca_session_store::synthetic::{window_bytes, write, Shape};
use broca_test_support::alloc::{installed, thousandths, Budget, Counts, Group};
use tokio::sync::{mpsc, Notify};

use super::tests::{
    bind, deployment, executor, idle, plan_manifest, send_planned, send_with, text_step, triple,
    untooled, Commands, Hold, ADDR,
};
use super::StoreActor;
use crate::actor::{LineageCmd, SendKey, SendOutcome, SubscribeOutcome};
use crate::fanout::FromCursor;
use crate::memory_report::MemoryAnswer;

/// Window messages of the measured session.
const MESSAGES: usize = 1_000;

/// What an idle actor with one live subscriber holds, in thousandths of `W`. Today: 1.648.
/// Target: about 1,288 at this session's 4.5 KB per message (1.2 x W + 400 B x messages). The
/// failure names the holders by the memory report's estimate, which a test below holds to the
/// allocator's total.
const IDLE_TOTAL: u64 = 1_814;

// Limits on what an actor holds inside a provider call, beyond the idle actor, in thousandths of
// `W`.
/// Today: 4.281, of which the request's bytes are 1.000 (the test's provider holds the request
/// as JSON, standing in for a transport's body) and the request's options, which hold one copy of
/// the window, 1.115. Target: the request's bytes and nothing else on a session without
/// compaction entries, about 1,000.
const IN_CALL_ABOVE_IDLE: u64 = 4_710;

/// Decoded copies of one plan's tool list held by `PLAN_SESSIONS` idle sessions on it: each
/// session's first episode would decode its own, but the decoded setup parts are shared through
/// the process. Today: 1. Target: 1.
const PLAN_TOOL_COPIES: u64 = 1;
/// Sessions in the shared-setup measure.
const PLAN_SESSIONS: usize = 4;

/// How far an estimate may be from what the allocator measured, in percent.
const ESTIMATE_TOLERANCE: u64 = 15;

/// A synthetic session of `MESSAGES` messages at the actor tests' address, on a backend of its
/// own, with its `W`. Written before any counting starts.
fn synthetic() -> (&'static MemoryBackend, usize) {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let w = futures::executor::block_on(async {
        let mut session = Session::create(
            backend,
            SessionAddr(ADDR.into()),
            triple(),
            &deployment(),
            false,
        )
        .await
        .unwrap();
        write(&mut session, &Shape::production(MESSAGES))
            .await
            .unwrap();
        window_bytes(&session.conversation)
    });
    (backend, w)
}

/// A current-thread runtime whose blocking threads count with `group`.
fn counted_runtime(group: &'static Group) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .on_thread_start(group.joiner())
        .build()
        .unwrap()
}

/// The session at `backend` loaded into an actor that serves subscribers, with a live
/// subscriber attached and the live projection seeded.
async fn subscribed_actor(
    backend: &'static MemoryBackend,
    executor: Arc<dyn crate::executor::RunExecutor>,
) -> (
    StoreActor<&'static MemoryBackend>,
    mpsc::Receiver<crate::fanout::Delivery>,
) {
    let session = Session::load(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
        .await
        .unwrap();
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let mut actor = StoreActor::new(bind(), session, executor, Box::new(parts))
        .with_readers(Arc::new(move || backend));
    let (sink, deliveries) = mpsc::channel(4096);
    assert_eq!(
        actor.subscribe(FromCursor::Start, true, sink),
        SubscribeOutcome::Attached
    );
    broca_test_support::wait_until("the live projection is seeded", || {
        let seeded = actor.fanout.as_ref().unwrap().head().is_some();
        async move {
            if seeded {
                Ok(())
            } else {
                Err("not seeded yet".into())
            }
        }
    })
    .await;
    // The subscriber's catch-up task registers its live receiver once the projection answers.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    (actor, deliveries)
}

/// An idle actor as the allocator measured it, with its estimate.
struct IdleActor {
    w: usize,
    counts: Counts,
    memory: broca_wire::MemorySession,
}

fn idle_actor() -> IdleActor {
    assert!(
        installed(),
        "the crate's tests count with the counting allocator"
    );
    let (backend, w) = synthetic();
    let group = Group::new();
    let runtime = counted_runtime(group);
    let counting = group.start();
    let (actor, deliveries) = runtime.block_on(subscribed_actor(
        backend,
        Arc::new(executor(Vec::new(), None)),
    ));
    let counts = counting.read();
    let memory = actor.memory().session;
    drop(deliveries);
    drop(actor);
    drop(runtime);
    IdleActor { w, counts, memory }
}

/// An idle actor with one live subscriber holds no more than its budget.
#[test]
fn an_idle_actor_with_a_subscriber_holds_no_more_than_its_budget() {
    let idle = idle_actor();
    let w = idle.w;
    let budget = Budget::new(
        format!("an idle store actor of {MESSAGES} messages with a live subscriber"),
        "W",
        w,
    )
    .holder(
        "total",
        idle.counts.held.max(0) as u64,
        thousandths(w, IDLE_TOTAL),
    );
    eprintln!(
        "{}\n  estimated by holder: {:?}",
        budget.table(),
        idle.memory.holders
    );
    budget.check();
}

/// The memory report's estimate of an idle actor is within [`ESTIMATE_TOLERANCE`] of what the
/// allocator measured.
#[test]
fn the_estimate_of_an_idle_actor_matches_the_allocator() {
    let idle = idle_actor();
    let measured = idle.counts.held.max(0) as u64;
    let estimated = idle.memory.holders.total;
    eprintln!(
        "estimated {estimated}, measured {measured}, {:+.1}%\n  {:?}",
        (estimated as f64 - measured as f64) * 100.0 / measured as f64,
        idle.memory.holders
    );
    assert!(
        estimated.abs_diff(measured) * 100 <= measured * ESTIMATE_TOLERANCE,
        "an idle actor: estimated {estimated} bytes, measured {measured}, off by more than \
         {ESTIMATE_TOLERANCE}%: {:?}",
        idle.memory.holders
    );
}

/// The actor's answer to the memory report.
async fn report(commands: &Commands) -> broca_wire::MemorySession {
    let (reply, answer) = tokio::sync::oneshot::channel();
    commands.send(LineageCmd::MemoryReport { reply }).unwrap();
    match answer.await.unwrap() {
        MemoryAnswer::Store(memory) => memory.session,
        other => panic!("a store actor answered {other:?}"),
    }
}

/// An actor whose run is inside a provider call holds no more than the idle actor plus its
/// budget, and the report sizes the request in flight. Once the call has ended the report sees
/// no request, and the request's options are freed: the report never kept them alive.
#[test]
fn an_actor_inside_a_provider_call_holds_no_more_than_its_budget() {
    assert!(
        installed(),
        "the crate's tests count with the counting allocator"
    );
    let (backend, w) = synthetic();
    let group = Group::new();
    let runtime = counted_runtime(group);
    let gate = Arc::new(Notify::new());
    let hold = Arc::new(Hold::default());
    let mut gated = executor(vec![text_step("a1", "done")], Some(Arc::clone(&gate)));
    gated.provider.hold = Some(Arc::clone(&hold));
    let counting = group.start();
    let (commands, served, idle_held, deliveries) = runtime.block_on(async {
        let (actor, deliveries) = subscribed_actor(backend, Arc::new(gated)).await;
        let idle_held = counting.read().held;
        let (sender, receiver) = mpsc::unbounded_channel();
        let commands = Commands::over(sender, backend);
        let served = tokio::spawn(actor.serve(receiver));
        (commands, served, idle_held, deliveries)
    });
    let (in_call, memory) = runtime.block_on(async {
        // The synthetic session's first run froze no tools, so its sends name none.
        let sent = send_with(
            &commands,
            "r-measured",
            untooled("one more question"),
            SendKey::Minted,
        )
        .await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        hold.reached.notified().await;
        let in_call = counting.read();
        (in_call, report(&commands).await)
    });
    let request_bytes = hold.body_len.load(std::sync::atomic::Ordering::SeqCst);
    let above_idle = (in_call.held - idle_held).max(0) as u64;
    let budget = Budget::new(
        format!("a store actor of {MESSAGES} messages inside a provider call"),
        "W",
        w,
    )
    .holder(
        "above the idle actor",
        above_idle,
        thousandths(w, IN_CALL_ABOVE_IDLE),
    );
    eprintln!(
        "{}\n  request bytes {request_bytes} ({:.3} x W)\n  estimated by holder: {:?}",
        budget.table(),
        request_bytes as f64 / w as f64,
        memory.holders
    );
    assert!(memory.run_in_progress);
    assert!(memory.holders.request_in_flight > 0, "{memory:?}");
    assert_eq!(memory.request_body_bytes, None);
    assert_eq!(
        memory.request_body_untracked.as_deref(),
        Some(super::memory::BODY_UNTRACKED)
    );
    budget.check();
    let after = runtime.block_on(async {
        gate.notify_one();
        idle(&commands).await;
        report(&commands).await
    });
    assert!(!after.run_in_progress);
    assert_eq!(after.holders.request_in_flight, 0);
    assert_eq!(after.request_body_untracked, None);
    let options = hold.options.lock().unwrap().clone().unwrap();
    assert!(
        options.upgrade().is_none(),
        "the ended call's options are freed"
    );
    drop(commands);
    drop(deliveries);
    runtime.block_on(async { drop(served.await) });
}

/// Idle sessions on one plan share one decoded copy of its tool list: the report counts the
/// copies, and the count stays within its budget.
#[test]
fn sessions_on_one_plan_hold_no_more_copies_of_its_tool_list_than_the_budget() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let manifest = plan_manifest("plan-1", serde_json::json!({}));
    let (answers, _actors) = runtime.block_on(async {
        // Each actor stays open until every answer is in, so no two copies share an address.
        let (mut answers, mut actors) = (Vec::new(), Vec::new());
        for n in 0..PLAN_SESSIONS {
            let backend: &'static MemoryBackend = Box::leak(Box::default());
            let session = Session::create(
                backend,
                SessionAddr(ADDR.into()),
                triple(),
                &deployment(),
                false,
            )
            .await
            .unwrap();
            let executor = Arc::new(executor(vec![text_step(&format!("a{n}"), "hello")], None));
            let actor = StoreActor::new(bind(), session, executor.clone(), Box::new(parts.clone()));
            let (sender, receiver) = mpsc::unbounded_channel();
            let commands = Commands::over(sender, backend);
            let served = tokio::spawn(actor.serve(receiver));
            assert!(matches!(
                send_planned(
                    &commands,
                    &format!("r{n}"),
                    untooled("first"),
                    "plan-1",
                    Some(manifest.clone())
                )
                .await,
                SendOutcome::Active { .. }
            ));
            idle(&commands).await;
            drop(commands);
            drop(served.await.unwrap());
            // Opened again, as a module opens a session it serves: the actor learns the first
            // episode from the session's records.
            let session =
                Session::load(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
                    .await
                    .unwrap();
            let actor = StoreActor::new(bind(), session, executor, Box::new(parts.clone()));
            actor
                .admission
                .learn(&actor.session, &actor.parts, &actor.key, None)
                .unwrap();
            answers.push(actor.memory());
            actors.push(actor);
        }
        (answers, actors)
    });
    let report =
        crate::memory_report::assemble(answers, 0, 0, Default::default(), Default::default());
    let tools = report
        .setup_parts
        .iter()
        .find(|part| part.kind == "tools")
        .expect("the plan's tool list is held");
    eprintln!("{:?}", report.setup_parts);
    assert_eq!(tools.sessions, PLAN_SESSIONS as u64);
    assert!(
        tools.decoded_copies <= PLAN_TOOL_COPIES,
        "{PLAN_SESSIONS} sessions on one plan hold {} decoded copies of its tool list, over the \
         limit of {PLAN_TOOL_COPIES}: {:?}",
        tools.decoded_copies,
        report.setup_parts
    );
}

/// The estimates the memory report makes of a live projection, a media fold and a request in
/// flight are each within [`ESTIMATE_TOLERANCE`] of what the allocator measured for the same
/// value, built from the synthetic session.
#[test]
fn the_estimates_of_a_projection_a_media_fold_and_a_request_match_the_allocator() {
    use broca_test_support::alloc::measure;

    assert!(
        installed(),
        "the crate's tests count with the counting allocator"
    );
    let (backend, _) = synthetic();
    let (session, records) = futures::executor::block_on(async {
        let session = Session::load(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
            .await
            .unwrap();
        let records = session.open_log_records().await.unwrap();
        (session, records)
    });
    let (control, projected) = measure(|| {
        let mut control = Box::new(crate::store_control::StoreControl::default());
        for (at, (seq, record)) in records.iter().enumerate() {
            let next = records.get(at + 1).map(|(_, record)| record);
            control.project(*seq, record, next).unwrap();
        }
        control
    });
    let parts = MemoryParts::default();
    let (fold, folded) = measure(|| {
        let mut fold = Box::new(crate::store_media::MediaFold::new(bind()));
        fold.take(&records, &parts);
        fold
    });
    let (request, built) = measure(|| {
        Box::new(broca_protocol::request::CallOptions::new(
            session
                .conversation
                .messages
                .iter()
                .map(|message| broca_protocol::Message::clone(message))
                .collect(),
        ))
    });
    let rows = [
        ("live projection", control.held_bytes(), projected),
        ("media fold", fold.held_bytes(), folded),
        (
            "request in flight",
            super::memory::request_bytes(&request),
            built,
        ),
    ];
    let table: String = rows
        .iter()
        .map(|(holder, estimated, counts)| {
            format!(
                "\n  {holder:<20} estimated {estimated:>10}, measured {:>10}",
                counts.held
            )
        })
        .collect();
    eprintln!("{table}");
    for (holder, estimated, counts) in rows {
        let measured = counts.held.max(0) as u64;
        assert!(
            (estimated as u64).abs_diff(measured) * 100 <= measured * ESTIMATE_TOLERANCE,
            "{holder}: estimated {estimated} bytes, measured {measured}, off by more than \
             {ESTIMATE_TOLERANCE}%{table}"
        );
    }
}

/// The report over an idle synthetic session and one session of today's storage: sizes and
/// counts only, nothing from the transcript. Printed, as the serve layer serializes it, for
/// readers to check.
#[test]
fn the_report_of_a_synthetic_store_carries_sizes_and_counts_only() {
    let (backend, _) = synthetic();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (actor, deliveries) = runtime.block_on(subscribed_actor(
        backend,
        Arc::new(executor(Vec::new(), None)),
    ));
    let views = crate::store_reads::ViewCache::default();
    let settled = crate::store_status::SettledRuns::default();
    let report = crate::memory_report::assemble(
        vec![actor.memory()],
        1,
        0,
        views.memory(|view| view.held_bytes().total()),
        settled.memory(),
    );
    let json = serde_json::to_string_pretty(&report).unwrap();
    println!("{json}");
    assert_eq!(report.sessions.len(), 1);
    assert_eq!(report.legacy_sessions, 1);
    assert_eq!(report.largest, [ADDR]);
    assert_eq!(
        report.sessions[0].window_messages,
        actor.session.conversation.messages.len() as u64
    );
    assert_eq!(report.totals, report.sessions[0].holders);
    // No message id, no text and no tool input of the session is in the report.
    for content in [
        "m-syn-",
        "call-run-",
        "lorem",
        "vexa",
        "tumbral",
        "synthetic.tools",
    ] {
        assert!(
            !json.contains(content),
            "the report carries {content:?}: {json}"
        );
    }
    drop(deliveries);
    drop(actor);
}
