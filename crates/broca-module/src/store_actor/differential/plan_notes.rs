//! Plan notes (llm-runner `plan_notes`) on a session kept in the session store: a refresh
//! waiting under `notes_until_rebuild` keeps the applied plan rendering, its run delivers the
//! system note after its user turn and refuses the removed tool, and a flush applies the change
//! in full and ends the notes. A route without a system-role turn applies a change of system
//! text at once, recording why.

use std::sync::Arc;

use broca_core::FileLeaseStore;
use broca_protocol::{BindIdentity, Role, SendScope, SubmissionRequest};
use broca_session_store::record::Record;

use super::super::tests::{executor, read_step, request, text_step, LoopExecutor};
use super::head::{core, send_titled};
use super::refresh::{manifest, plan};
use super::{Module, IDLE};
use crate::actor::{SendOutcome, SendPlan};
use crate::executor::RunExecutor;
use crate::registry::LineageRegistry;
use crate::serve::ServeHandler;

const NOTE: &str = "Answer as a reviewer from now on.";

/// Answers, for every model, whether its route has a system-role turn mid-conversation (the
/// catalog fact that decides whether plan notes can be delivered at all).
struct Form(bool);

struct Admitter;

#[async_trait::async_trait]
impl broca_fetch_plan::PlanAdmitter for Admitter {
    async fn admit(
        &self,
        parsed: &broca_fetch_plan::Plan,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
    ) -> Result<broca_fetch_plan::Admitted, broca_fetch_plan::Refusal> {
        let plan = serde_json::from_str(&parsed.text).unwrap();
        Ok(broca_fetch_plan::Admitted {
            manifest: manifest(&plan, &parsed.identity),
        })
    }
}

impl crate::cache::CacheDeclarer for Form {
    fn declare(
        &self,
        _: &broca_records::record::RunConfig,
    ) -> Option<crate::cache::CacheDeclaration> {
        None
    }

    fn turn_system_form(&self, _: &broca_protocol::ModelHandle) -> bool {
        self.0
    }
}

/// A module over `steps` whose executor the test keeps, to read the requests its model saw.
async fn module(
    steps: Vec<Vec<broca_protocol::NormalizedEvent>>,
    form: bool,
) -> (Module, Arc<LoopExecutor>) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "store-plan-notes-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let executor = Arc::new(executor(steps, None));
    let runtime = crate::store_open::StoreRuntime::open(
        &dir,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        Arc::clone(&executor) as Arc<dyn RunExecutor>,
        IDLE,
    )
    .await
    .unwrap();
    let registry = LineageRegistry::new(Arc::new(
        crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            Arc::clone(&executor) as Arc<dyn RunExecutor>,
            Arc::new(crate::store_open::StoreSlot::loaded(Arc::new(runtime))),
        )
        .with_idle_ttl(IDLE),
    ));
    registry.set_cache_declarer(Arc::new(Form(form)));
    let key = BindIdentity::new(dir.join("project"), "store-actor", "s1");
    let module = Module {
        dir,
        registry,
        key,
        caller: Default::default(),
    };
    (module, executor)
}

async fn send(module: &Module, run_id: &str, first: Option<&broca_records::record::PlanManifest>) {
    let mut request: SubmissionRequest = request(run_id);
    request.principal = Some(core());
    request.tools = None;
    request.policy_allowed = Default::default();
    let plan = SendPlan {
        identity: "plan-1".into(),
        manifest: first.map(|first| Arc::new(first.clone())),
        model_check: None,
    };
    let outcome = send_titled(module, run_id, request, None, Some(plan)).await;
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    module.closed().await;
}

/// The text of a request's messages, with their roles and whether each is a turn-scoped system
/// message.
fn shown(request: &broca_protocol::CallOptions) -> Vec<(Role, bool, String)> {
    request
        .prompt
        .iter()
        .map(|message| {
            let text = message
                .content
                .iter()
                .filter_map(|block| match block {
                    broca_protocol::ContentBlock::Text { text } => Some(text.to_string()),
                    _ => None,
                })
                .collect::<String>();
            (message.role, message.turn_system, text)
        })
        .collect()
}

/// Writes the session's `session.baseline` reply, as its owner reads it, to `name` in the folder
/// `BROCA_PLAN_NOTES_SAMPLES` names, so readers can check the bytes before the writer ships.
async fn sample(module: &Module, name: &str) {
    let Some(dir) = std::env::var_os("BROCA_PLAN_NOTES_SAMPLES") else {
        return;
    };
    let reply = module
        .registry
        .baseline(&module.key, Some(&core()), &SendScope::default())
        .await
        .unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    let path = std::path::Path::new(&dir).join(name);
    std::fs::write(path, serde_json::to_vec(&reply).unwrap()).unwrap();
}

/// Reads the session's whole transcript as `session.read` serves it and checks the note isn't in
/// it, and writes it in the same folder as [`sample`] for readers to check the same.
async fn sample_read(module: &Module, name: &str) {
    let page = module
        .registry
        .read(crate::serve::SessionReadRequest::new(
            module.key.clone(),
            None,
            Some(100),
        ))
        .await
        .unwrap();
    assert!(
        !serde_json::to_string(&page).unwrap().contains(NOTE),
        "a note shows in the transcript"
    );
    let Some(dir) = std::env::var_os("BROCA_PLAN_NOTES_SAMPLES") else {
        return;
    };
    let path = std::path::Path::new(&dir).join(name);
    std::fs::write(path, serde_json::to_vec(&page).unwrap()).unwrap();
}

async fn records(module: &Module) -> Vec<Record> {
    let session = module.load().await;
    session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .map(|(_, record)| record)
        .collect()
}

#[tokio::test]
async fn a_waiting_change_is_told_in_notes_until_a_flush_applies_it() {
    let first = manifest(&plan("you are a worker", &["read"], None, None), "plan-1");
    let next = manifest(&plan("you are a reviewer", &[], None, None), "plan-2");
    let (module, executor) = module(
        vec![
            text_step("a1", "hi"),
            read_step("a2"),
            text_step("a3", "done"),
            text_step("a4", "after"),
        ],
        true,
    )
    .await;
    send(&module, "r1", Some(&first)).await;
    let accepted = module
        .registry
        .route_session_refresh(
            &module.key,
            1,
            "notes_until_rebuild".into(),
            Box::new(next.clone()),
            Vec::new(),
            Some(NOTE.into()),
        )
        .await
        .unwrap();
    assert!(
        matches!(
            accepted,
            crate::actor::SessionRefreshOutcome::Accepted {
                applies_from_next_run: false,
                ..
            }
        ),
        "{accepted:?}"
    );
    sample(&module, "baseline-pending.json").await;
    send(&module, "r2", None).await;
    sample(&module, "baseline-notes-delivered.json").await;
    sample_read(&module, "read-after-notes.json").await;
    let calls = executor.provider.inner.calls();
    assert_eq!(calls.len(), 3, "one request for r1, two for r2");
    // Both of r2's requests keep the applied plan's tools and carry the note right after r2's
    // input, at system authority; r1's doesn't.
    assert!(!shown(&calls[0]).iter().any(|(_, _, text)| text == NOTE));
    for request in &calls[1..] {
        assert_eq!(
            request
                .tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            vec!["read"]
        );
        let shown = shown(request);
        let at = shown
            .iter()
            .position(|(_, _, text)| text == NOTE)
            .expect("the note renders");
        assert_eq!(shown[at].0, Role::System);
        assert!(shown[at].1, "a persistent turn-scoped system message");
        assert_eq!(shown[at - 1].2, "r2");
        assert!(
            shown
                .iter()
                .any(|(_, _, text)| text == &broca_core::plan_notes::tool_removal("read")),
            "{shown:?}"
        );
    }
    let log = records(&module).await;
    let refusal = log
        .iter()
        .find_map(|record| match record {
            Record::ToolReply(reply) => Some(reply.result.output.clone()),
            _ => None,
        })
        .expect("the call to the removed tool is answered");
    assert!(
        matches!(&refusal, broca_protocol::ToolOutput::Error { message, .. }
            if *message == broca_core::plan_notes::tool_removal("read")),
        "{refusal:?}"
    );
    assert!(!log
        .iter()
        .any(|record| matches!(record, Record::ToolIntent(_))));
    assert!(!log
        .iter()
        .any(|record| matches!(record, Record::RefreshApplied(_))));
    // A flush applies it in full at the next run: the new tools, and no notes.
    module
        .registry
        .session_flush_prefix(
            &module.key,
            Some(&core()),
            &SendScope::default(),
            "f1".into(),
        )
        .await
        .unwrap();
    send(&module, "r3", None).await;
    sample(&module, "baseline-applied.json").await;
    let calls = executor.provider.inner.calls();
    let last = calls.last().unwrap();
    assert!(last.tools.is_empty());
    assert!(!shown(last).iter().any(|(_, _, text)| text == NOTE));
    let applied: Vec<_> = records(&module)
        .await
        .into_iter()
        .filter_map(|record| match record {
            Record::RefreshApplied(applied) => Some((applied.applied_by, applied.reason)),
            _ => None,
        })
        .collect();
    assert_eq!(applied, vec![("fold".to_owned(), None)]);
}

#[tokio::test]
async fn a_system_change_on_a_route_without_a_form_applies_at_once() {
    let first = manifest(&plan("you are a worker", &["read"], None, None), "plan-1");
    let next = manifest(&plan("you are a reviewer", &["read"], None, None), "plan-2");
    let (module, executor) =
        module(vec![text_step("a1", "hi"), text_step("a2", "hi")], false).await;
    send(&module, "r1", Some(&first)).await;
    module
        .registry
        .route_session_refresh(
            &module.key,
            1,
            "notes_until_rebuild".into(),
            Box::new(next),
            Vec::new(),
            Some(NOTE.into()),
        )
        .await
        .unwrap();
    send(&module, "r2", None).await;
    let calls = executor.provider.inner.calls();
    assert!(!shown(&calls[1]).iter().any(|(_, _, text)| text == NOTE));
    let applied: Vec<_> = records(&module)
        .await
        .into_iter()
        .filter_map(|record| match record {
            Record::RefreshApplied(applied) => Some((applied.applied_by, applied.reason)),
            Record::PlanNotes(_) => panic!("no notes without a form"),
            _ => None,
        })
        .collect();
    assert_eq!(
        applied,
        vec![("append".to_owned(), Some("no_note_form".to_owned()))]
    );
}

/// A note sent with a change that leaves the system text as it is (only the tools change) is
/// accepted and ignored (llm-runner 0.9.5 `plan_notes`): the refresh is recorded with no note,
/// its run shows none, and a repeat carrying the note again is answered as that refresh.
#[tokio::test]
async fn a_note_with_unchanged_system_text_is_accepted_and_ignored() {
    let first = manifest(&plan("you are a worker", &["read"], None, None), "plan-1");
    let next = manifest(&plan("you are a worker", &[], None, None), "plan-2");
    let (module, executor) = module(vec![text_step("a1", "hi"), text_step("a2", "hi")], true).await;
    send(&module, "r1", Some(&first)).await;
    for _ in 0..2 {
        let accepted = module
            .registry
            .route_session_refresh(
                &module.key,
                1,
                "notes_until_rebuild".into(),
                Box::new(next.clone()),
                Vec::new(),
                Some(NOTE.into()),
            )
            .await
            .unwrap();
        assert!(
            matches!(
                accepted,
                crate::actor::SessionRefreshOutcome::Accepted { .. }
            ),
            "{accepted:?}"
        );
    }
    // Read once the actor has closed: loading the session while it is open would fence the
    // actor's writer, and r2's start could not be written.
    module.closed().await;
    let refreshes: Vec<_> = records(&module)
        .await
        .into_iter()
        .filter_map(|record| match record {
            Record::SessionRefresh(refresh) => Some(refresh.system_note),
            _ => None,
        })
        .collect();
    assert_eq!(refreshes, vec![None], "one refresh, recorded with no note");
    send(&module, "r2", None).await;
    let calls = executor.provider.inner.calls();
    assert!(!shown(calls.last().unwrap())
        .iter()
        .any(|(_, _, text)| text == NOTE));
}

async fn note_refresh(
    module: &Module,
    generation: u64,
    manifest: &broca_records::record::PlanManifest,
    note: Option<&str>,
) -> crate::actor::SessionRefreshOutcome {
    let outcome = module
        .registry
        .route_session_refresh(
            &module.key,
            generation,
            "notes_until_rebuild".into(),
            Box::new(manifest.clone()),
            Vec::new(),
            note.map(str::to_owned),
        )
        .await
        .unwrap();
    module.closed().await;
    outcome
}

fn assert_accepts(outcome: crate::actor::SessionRefreshOutcome, applies: bool) {
    assert!(
        matches!(outcome, crate::actor::SessionRefreshOutcome::Accepted {
            applies_from_next_run, ..
        } if applies_from_next_run == applies),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn restoring_applied_system_text_adds_a_correction_until_flush_after_restart() {
    let original = manifest(&plan("You are a worker.", &["read"], None, None), "plan-1");
    let changed = manifest(
        &plan("You are a reviewer.", &["read"], None, None),
        "plan-2",
    );
    let correction = "Go back to answering as a worker.";
    let (module, executor) = module(
        (1..=4).map(|n| text_step(&format!("a{n}"), "ok")).collect(),
        true,
    )
    .await;
    send(&module, "r1", Some(&original)).await;
    assert_accepts(note_refresh(&module, 1, &changed, Some(NOTE)).await, false);
    send(&module, "r2", None).await;
    // Every send and refresh closes its actor before the next operation reopens the session.
    let refusal = note_refresh(&module, 2, &original, None).await;
    assert!(
        matches!(refusal, crate::actor::SessionRefreshOutcome::Refused(
        crate::serve::SessionChangeError::InvalidParams { field, .. }
    ) if field == "system_note")
    );
    assert_accepts(
        note_refresh(&module, 2, &original, Some(correction)).await,
        false,
    );
    send(&module, "r3", None).await;
    let calls = executor.provider.inner.calls();
    let visible = shown(&calls[2]);
    assert_eq!(
        visible
            .iter()
            .filter(|(_, turn_system, _)| *turn_system)
            .map(|(_, _, text)| text.as_str())
            .collect::<Vec<_>>(),
        [NOTE, correction]
    );
    assert!(!records(&module)
        .await
        .iter()
        .any(|r| matches!(r, Record::RefreshApplied(_))));
    module
        .registry
        .session_flush_prefix(
            &module.key,
            Some(&core()),
            &SendScope::default(),
            "flush".into(),
        )
        .await
        .unwrap();
    send(&module, "r4", None).await;
    let calls = executor.provider.inner.calls();
    assert!(!shown(&calls[3]).iter().any(|(_, turn, _)| *turn));
    assert!(records(&module)
        .await
        .iter()
        .any(|r| matches!(r, Record::RefreshApplied(applied) if applied.generation == 2)));
}

#[tokio::test]
async fn returning_to_the_applied_text_before_delivery_still_requires_a_note() {
    let original = manifest(&plan("You are a worker.", &[], None, None), "plan-1");
    let changed = manifest(&plan("You are a reviewer.", &[], None, None), "plan-2");
    let (module, executor) = module(vec![text_step("a1", "ok"), text_step("a2", "ok")], true).await;
    send(&module, "r1", Some(&original)).await;
    assert_accepts(note_refresh(&module, 1, &changed, Some(NOTE)).await, false);
    let refusal = note_refresh(&module, 2, &original, None).await;
    assert!(
        matches!(refusal, crate::actor::SessionRefreshOutcome::Refused(
        crate::serve::SessionChangeError::InvalidParams { field, .. }
    ) if field == "system_note")
    );
    assert_accepts(
        note_refresh(&module, 2, &original, Some("Keep working.")).await,
        true,
    );
    send(&module, "r2", None).await;
    assert!(!shown(&executor.provider.inner.calls()[1])
        .iter()
        .any(|(_, turn, _)| *turn));
    let log = records(&module).await;
    assert!(!log.iter().any(|r| matches!(r, Record::PlanNotes(_))));
    assert!(log
        .iter()
        .any(|r| matches!(r, Record::RefreshApplied(applied) if applied.generation == 2)));
}

#[tokio::test]
async fn resending_the_pending_text_without_a_note_preserves_the_undelivered_note() {
    let original = manifest(&plan("You are a worker.", &[], None, None), "plan-1");
    let changed = manifest(&plan("You are a reviewer.", &[], None, None), "plan-2");
    let (module, executor) = module(vec![text_step("a1", "ok"), text_step("a2", "ok")], true).await;
    send(&module, "r1", Some(&original)).await;
    assert_accepts(note_refresh(&module, 1, &changed, Some(NOTE)).await, false);
    assert_accepts(note_refresh(&module, 2, &changed, None).await, false);
    send(&module, "r2", None).await;
    let calls = executor.provider.inner.calls();
    assert!(shown(&calls[1])
        .iter()
        .any(|(_, turn, text)| *turn && text == NOTE));
    assert!(records(&module)
        .await
        .iter()
        .any(|r| matches!(r, Record::PlanNotes(delivery) if delivery.generation == 2)));
}

#[tokio::test]
async fn a_dangling_system_note_uses_the_applied_plan_after_restart() {
    use broca_session_store::record::{PlanNote, PlanNoteKind, PlanNotes};

    let original = manifest(&plan("You are a worker.", &[], None, None), "plan-1");
    let changed = manifest(&plan("You are a reviewer.", &[], None, None), "plan-2");
    let (module, executor) = module(vec![text_step("a1", "ok"), text_step("a2", "ok")], true).await;
    send(&module, "r1", Some(&original)).await;
    assert_accepts(note_refresh(&module, 1, &changed, Some(NOTE)).await, false);
    let orphan = "An earlier note without a recorded refresh.";
    let mut session = module.load().await;
    session
        .append(
            &[Record::PlanNotes(PlanNotes {
                generation: 999,
                anchor: 1,
                notes: vec![PlanNote {
                    kind: PlanNoteKind::System,
                    text: orphan.into(),
                    form: "turn_system".into(),
                }],
                refused_tools: Vec::new(),
                model_key: "p:m".into(),
            })],
            true,
        )
        .await
        .unwrap();
    session.close().await.unwrap();
    send(&module, "r2", None).await;
    let calls = executor.provider.inner.calls();
    let visible = shown(&calls[1]);
    let notes = visible
        .iter()
        .filter(|(_, turn, _)| *turn)
        .map(|(_, _, text)| text.as_str())
        .collect::<Vec<_>>();
    assert_eq!(notes, [orphan, NOTE]);
    assert!(visible
        .iter()
        .any(|(_, _, text)| text.contains("You are a worker.")));
    assert!(!visible
        .iter()
        .any(|(_, _, text)| text.contains("You are a reviewer.")));
}

#[tokio::test]
async fn a_refresh_equal_to_the_noted_state_preserves_the_rendered_prefix_after_restart() {
    let original = manifest(&plan("You are a worker.", &[], None, None), "plan-1");
    let changed = plan("You are a reviewer.", &[], None, None);
    let (module, executor) = module(
        (1..=4).map(|n| text_step(&format!("a{n}"), "ok")).collect(),
        true,
    )
    .await;
    module.registry.set_plan_admitter(Arc::new(Admitter));
    send(&module, "r1", Some(&original)).await;
    let owner = core();
    let scope = SendScope::default();
    let refresh = |generation, plan: &serde_json::Value, note: Option<String>| {
        module.registry.session_refresh(
            &module.key,
            Some(&owner),
            &scope,
            plan.as_object().unwrap().clone(),
            "notes_until_rebuild".into(),
            generation,
            Vec::new(),
            note,
        )
    };
    let first = refresh(1, &changed, Some(NOTE.into())).await.unwrap();
    assert!(!first.applies_from_next_run);
    send(&module, "r2", None).await;
    let original_plan = serde_json::from_str(original.plan.as_json_text()).unwrap();
    assert!(matches!(refresh(2, &original_plan, None).await, Err(
        crate::serve::SessionChangeError::InvalidParams { field, .. }
    ) if field == "system_note"));
    let repeat = refresh(2, &changed, None).await.unwrap();
    assert_eq!(repeat.pending.generation, 2);
    assert!(!repeat.applies_from_next_run);
    let baseline = module
        .registry
        .baseline(&module.key, Some(&core()), &SendScope::default())
        .await
        .unwrap();
    assert_eq!(baseline.baseline.unwrap().pending.unwrap().generation, 2);
    send(&module, "r3", None).await;
    let calls = executor.provider.inner.calls();
    let before = &calls[1];
    let after = &calls[2];
    assert_eq!(
        serde_json::to_vec(&before.prompt[..]).unwrap(),
        serde_json::to_vec(&after.prompt[..before.prompt.len()]).unwrap(),
        "only the new conversation suffix can extend the request"
    );
    let log = records(&module).await;
    assert_eq!(
        log.iter()
            .filter(|r| matches!(r, Record::PlanNotes(_)))
            .count(),
        1
    );
    assert!(!log.iter().any(|r| matches!(r, Record::RefreshApplied(_))));
    module
        .registry
        .session_flush_prefix(
            &module.key,
            Some(&core()),
            &SendScope::default(),
            "flush".into(),
        )
        .await
        .unwrap();
    send(&module, "r4", None).await;
    assert!(records(&module)
        .await
        .iter()
        .any(|r| matches!(r, Record::RefreshApplied(applied) if applied.generation == 2)));
}

#[tokio::test]
async fn undoing_a_tool_removal_only_restores_calls_when_the_writer_is_enabled() {
    use broca_session_store::record::PlanNoteKind;

    let original = manifest(&plan("You are a worker.", &["read"], None, None), "plan-1");
    let removed = manifest(&plan("You are a worker.", &[], None, None), "plan-2");
    let mut steps = vec![text_step("first", "ok")];
    for run in 2..=5 {
        steps.push(read_step(&format!("call-{run}")));
        steps.push(text_step(&format!("done-{run}"), "ok"));
    }
    let (module, executor) = module(steps, true).await;
    send(&module, "r1", Some(&original)).await;
    assert_accepts(note_refresh(&module, 1, &removed, None).await, false);
    send(&module, "r2", None).await;
    assert_accepts(note_refresh(&module, 2, &original, None).await, false);
    send(&module, "r3", None).await;
    // The next run reloads the notes, with no refresh or additional correction.
    send(&module, "r4", None).await;
    assert_accepts(note_refresh(&module, 3, &removed, None).await, false);
    send(&module, "r5", None).await;

    let enabled = crate::plan_notes::WRITE_TOOL_RESTORATIONS;
    let calls = executor.provider.inner.calls();
    let removal = broca_core::plan_notes::tool_removal("read");
    let restoration = "Tool \"read\" is now available.";
    for request in [&calls[3], &calls[5]] {
        assert_eq!(request.tools[0].name, "read");
        let visible = shown(request);
        let texts = visible
            .iter()
            .filter(|(_, turn_system, _)| *turn_system)
            .map(|(_, _, text)| text.as_str())
            .collect::<Vec<_>>();
        let expected = if enabled {
            vec![removal.as_str(), restoration]
        } else {
            vec![removal.as_str()]
        };
        assert_eq!(texts, expected);
    }
    let log = records(&module).await;
    let dispatches = log
        .iter()
        .filter_map(|record| match record {
            Record::ToolIntent(intent) => Some(intent.run_id.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(dispatches, if enabled { vec!["r3", "r4"] } else { vec![] });
    let deliveries = log
        .iter()
        .filter_map(|record| match record {
            Record::PlanNotes(delivery) => Some((delivery.generation, delivery.notes[0].kind)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let expected = if enabled {
        vec![
            (1, PlanNoteKind::ToolRemoval),
            (2, PlanNoteKind::ToolRestored),
            (3, PlanNoteKind::ToolRemoval),
        ]
    } else {
        vec![(1, PlanNoteKind::ToolRemoval)]
    };
    assert_eq!(deliveries, expected);
    assert!(!log
        .iter()
        .any(|record| matches!(record, Record::RefreshApplied(_))));
}
