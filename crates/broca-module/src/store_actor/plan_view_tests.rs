//! Plan inspection over the real actor and scripted model; expected text and tool order are
//! checked against captured model calls, not reconstructed by the read's joining code.

use std::sync::{Arc, Mutex};

use broca_protocol::{ContentBlock, NormalizedEvent, ProviderControl, RunId};
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::session::Session;
use broca_wire::plan_view::{PlanViewResponse, PlanViewSystemItem};
use serde_json::json;
use tokio::sync::{oneshot, Notify};

use super::tests::{
    actor_with, executor, idle, send_planned, text_step, untooled, Commands, LoopExecutor,
};
use crate::actor::{LineageCmd, SendOutcome, SessionRefreshOutcome};
use crate::store_plan_view::Error;
use crate::store_reads::{StoreReadOutcome, StoreReadReply, StoreReadRequest};

fn manifest(label: &str, extras: bool) -> broca_records::record::PlanManifest {
    let first = format!("{label}: You are π.");
    let second = "工具 café";
    let tool = |provider: &str, name: &str| {
        json!({
            "name": name, "description": format!("{label} {name}"),
            "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}},
            "module": provider,
        })
    };
    let tools = vec![tool("beta", "beta.read"), tool("alpha", "alpha.edit")];
    let mut items = vec![json!({"provider":"alpha", "preset":"persona", "params":{}})];
    if extras {
        items.extend([
            json!({"provider":"empty", "params":{}}),
            json!({"provider":"missing", "params":{}, "optional":true}),
        ]);
    }
    items.push(json!({"provider":"beta", "params":{}}));
    let mut fetches = vec![
        json!({"provider":"beta", "catalog_digest":"b", "text":second, "tools":[tools[0].clone()]}),
        json!({"provider":"alpha", "catalog_digest":"a", "text":first, "tools":[tools[1].clone()]}),
    ];
    if extras {
        fetches.extend([
            json!({"provider":"empty", "catalog_digest":"e", "text":"", "tools":[]}),
            json!({"provider":"missing", "absent":"timeout"}),
        ]);
    }
    let mut providers = vec![
        json!({"provider":"alpha", "tools":[{"name":"alpha.edit", "capabilities":[]}]}),
        json!({"provider":"beta", "tools":[{"name":"beta.read", "capabilities":[]}]}),
    ];
    if extras {
        providers.extend([
            json!({"provider":"empty", "tools":[]}),
            json!({"provider":"missing", "tools":[]}),
        ]);
    }
    let composition = json!({"providers":providers, "step_transform_items":[]});
    let composition_digest =
        cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap();
    broca_records::record::PlanManifest {
        rich_ui: None,
        plan: broca_records::record::OpaqueJson::from_json_text(
            json!({
            "tool_items":[{"provider":"beta", "params":{}},{"provider":"alpha", "params":{}}],
            "system_text_items":items, "step_transform_items":[],
            "composition":composition, "composition_digest":composition_digest,
            })
            .to_string(),
        )
        .unwrap(),
        plan_identity: label.into(),
        composition_digest,
        fetches: fetches
            .into_iter()
            .map(|value| serde_json::from_value(value).unwrap())
            .collect(),
        tools: Arc::new(
            tools
                .into_iter()
                .map(|value| serde_json::from_value(value).unwrap())
                .collect(),
        ),
        system_text: format!("{first}\n\n{second}"),
        system_digest: label.into(),
        join_version: 1,
        role_versions: Default::default(),
        declarations: vec![],
    }
}

struct Harness {
    commands: Commands,
    backend: &'static MemoryBackend,
    parts: SharedParts,
}

impl std::ops::Deref for Harness {
    type Target = Commands;
    fn deref(&self) -> &Commands {
        &self.commands
    }
}

async fn start(
    executor: LoopExecutor,
) -> (
    Harness,
    tokio::task::JoinHandle<Session<&'static MemoryBackend>>,
) {
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let (commands, served, backend) = actor_with(executor, parts.clone()).await;
    (
        Harness {
            commands,
            backend,
            parts,
        },
        served,
    )
}

async fn ask(commands: &Harness, request: StoreReadRequest) -> StoreReadReply {
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::StoreRead {
            request: Box::new(request.clone()),
            reply,
        })
        .unwrap();
    match answer.await.unwrap() {
        StoreReadOutcome::Served(reply) => *reply,
        StoreReadOutcome::Stored {
            position,
            live_step,
            entry_speaks_for,
        } => {
            let view = Session::view_at(
                commands.backend,
                SessionAddr(super::tests::ADDR.into()),
                &super::tests::triple(),
                &super::tests::deployment(),
                &position,
            )
            .await
            .unwrap();
            let context = crate::store_reads::ReadContext {
                labels: Default::default(),
                token_counts: None,
                parts: &commands.parts,
                held: true,
                open: true,
                settled: None,
                session_name: "s1",
                live_step: live_step.as_ref(),
                stream_head: None,
                entry_speaks_for,
                class: None,
            };
            crate::store_reads::execute(&view, &context, &request).await
        }
        _ => panic!("the actor refused its read"),
    }
}

async fn view(commands: &Harness) -> Result<PlanViewResponse, Error> {
    match ask(commands, StoreReadRequest::PlanView).await {
        StoreReadReply::PlanView(reply) => reply,
        _ => panic!("another read answered the plan view"),
    }
}

async fn send(commands: &Commands, id: &str, first: Option<broca_records::record::PlanManifest>) {
    let identity = first
        .as_ref()
        .map_or("", |manifest| manifest.plan_identity.as_str())
        .to_owned();
    // A plain later send inherits the frozen plan; only admission carries a fetched manifest.
    let outcome = if first.is_some() {
        send_planned(commands, id, untooled("hello"), &identity, first).await
    } else {
        super::tests::send_with(
            commands,
            id,
            untooled("hello"),
            crate::actor::SendKey::Minted,
        )
        .await
    };
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
}

async fn refresh(
    commands: &Commands,
    generation: u64,
    policy: &str,
    manifest: broca_records::record::PlanManifest,
) {
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::SessionRefresh {
            generation,
            policy: policy.into(),
            manifest: Box::new(manifest),
            refetch: Vec::new(),
            reply,
        })
        .unwrap();
    assert!(matches!(
        answer.await.unwrap(),
        SessionRefreshOutcome::Accepted { .. }
    ));
}

fn matches_call(reply: &PlanViewResponse, call: &broca_protocol::request::CallOptions) {
    assert_eq!(
        call.prompt[0].content,
        [ContentBlock::Text {
            text: reply.system.text.clone().into()
        }]
    );
    assert_eq!(reply.tools.len(), call.tools.len());
    for (served, sent) in reply.tools.iter().zip(call.tools.iter()) {
        assert_eq!(served.name, sent.name);
        assert_eq!(served.description, sent.description);
        assert_eq!(served.input_schema, sent.input_schema);
        assert_eq!(Some(&served.provider), sent.module.as_ref());
        if let Some(tokens) = &served.tokens {
            assert!(!tokens.exact);
            assert!(tokens.count > 0);
        }
    }
    let items = reply.system.items.as_ref().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].provider, "alpha");
    assert_eq!(items[0].preset.as_deref(), Some("persona"));
    assert_eq!(items[1].provider, "beta");
    assert_eq!(
        item_text(&reply.system.text, &items[0]),
        reply.system.text.split("\n\n").next().unwrap()
    );
    assert_eq!(item_text(&reply.system.text, &items[1]), "工具 café");
    assert_eq!(
        &reply.system.text
            [usize::try_from(items[0].end).unwrap()..usize::try_from(items[1].start).unwrap()],
        "\n\n"
    );
    assert!(items.iter().all(|item| item.start < item.end
        && reply
            .system
            .text
            .is_char_boundary(usize::try_from(item.start).unwrap())
        && reply
            .system
            .text
            .is_char_boundary(usize::try_from(item.end).unwrap())));
    assert!(reply.as_planned);
}

fn item_text<'a>(text: &'a str, item: &PlanViewSystemItem) -> &'a str {
    &text[usize::try_from(item.start).unwrap()..usize::try_from(item.end).unwrap()]
}

#[tokio::test]
async fn plan_view_fresh_and_empty_optional_items_match_model_bytes() {
    let executor = executor(vec![text_step("a", "answer")], None);
    let provider = executor.provider.inner.clone();
    let (commands, served) = start(executor).await;
    let admitted = manifest("fresh", true);
    let fetched = admitted.fetches.clone();
    send(&commands, "r1", Some(admitted)).await;
    idle(&commands).await;
    let reply = view(&commands).await.unwrap();
    assert_eq!(reply.generation, 0);
    assert_eq!(reply.applied_at_ms, Some(1));
    assert!(reply.pending.is_none());
    assert!(
        reply.newest_step.is_none(),
        "the scripted provider reported no send time"
    );
    matches_call(&reply, &provider.calls()[0]);
    for item in reply.system.items.as_ref().unwrap() {
        let text = fetched
            .iter()
            .find_map(|fetch| match &fetch.outcome {
                broca_records::record::FetchOutcome::Answered { text, .. }
                    if fetch.provider == item.provider =>
                {
                    text.as_deref()
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(item_text(&reply.system.text, item), text);
    }
    drop(commands);
    served.await.unwrap();
}

fn attempted_step(id: &str, text: Option<&str>, sent_at_ms: u64) -> Vec<NormalizedEvent> {
    let attempt = serde_json::from_value(json!({
        "attempt":0, "request_body_len":10, "request_body_sha256":"0".repeat(64),
        "http_status":200, "response_body_len":0, "response_body_sha256":"0".repeat(64),
        "outcome":"complete", "ts_ms":sent_at_ms + 7, "sent_at_ms":sent_at_ms,
    }))
    .unwrap();
    let mut step = vec![NormalizedEvent::Control(ProviderControl::AttemptFinished {
        attempt,
    })];
    let mut answer = text_step(id, text.unwrap_or(""));
    if text.is_none() {
        answer.remove(0);
    }
    step.extend(answer);
    step
}

#[tokio::test]
async fn plan_view_refresh_intact_prefix_and_pending_match_model_bytes() {
    let mut executor = executor(
        vec![
            attempted_step("a", Some("first"), 110),
            attempted_step("b", Some("second"), 210),
            attempted_step("c", Some("third"), 310),
        ],
        None,
    );
    let time = Arc::new(Mutex::new(Some(100)));
    executor.start_time = Some(time.clone());
    let provider = executor.provider.inner.clone();
    let (commands, served) = start(executor).await;
    send(&commands, "r1", Some(manifest("first", false))).await;
    idle(&commands).await;
    refresh(&commands, 1, "immediately", manifest("second", false)).await;
    *time.lock().unwrap() = Some(200);
    send(&commands, "r2", None).await;
    idle(&commands).await;
    let applied = view(&commands).await.unwrap();
    assert_eq!(applied.generation, 1);
    assert_eq!(applied.applied_at_ms, Some(200));
    assert_eq!(applied.newest_step.as_ref().unwrap().at_ms, 210);
    matches_call(&applied, &provider.calls()[1]);
    refresh(&commands, 2, "on_prefix_rebuild", manifest("third", false)).await;
    let pending = view(&commands).await.unwrap();
    assert_eq!(pending.system, applied.system);
    assert_eq!(pending.tools, applied.tools);
    assert_eq!(
        serde_json::to_value(&pending).unwrap()["pending"],
        json!({"generation":2})
    );
    *time.lock().unwrap() = Some(300);
    send(&commands, "r3", None).await;
    idle(&commands).await;
    let same_generation = view(&commands).await.unwrap();
    assert_eq!(same_generation.generation, 1);
    assert_eq!(
        same_generation.applied_at_ms,
        Some(200),
        "not the latest run's start"
    );
    matches_call(&same_generation, &provider.calls()[2]);
    drop(commands);
    served.await.unwrap();
}

#[tokio::test]
async fn plan_view_refresh_prefix_rebuild_matches_model_bytes() {
    let mut executor = executor(
        vec![text_step("a", "first"), text_step("b", "second")],
        None,
    );
    let time = Arc::new(Mutex::new(Some(100)));
    executor.start_time = Some(time.clone());
    let provider = executor.provider.inner.clone();
    let (commands, served) = start(executor).await;
    send(&commands, "r1", Some(manifest("first", false))).await;
    idle(&commands).await;
    refresh(
        &commands,
        1,
        "on_prefix_rebuild",
        manifest("rebuilt", false),
    )
    .await;
    *time.lock().unwrap() = Some(400);
    let mut request = untooled("switch models");
    request.model = broca_protocol::ModelHandle::new("stub", "m2");
    assert!(matches!(
        super::tests::send_with(&commands, "r2", request, crate::actor::SendKey::Minted).await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    let reply = view(&commands).await.unwrap();
    assert_eq!(reply.generation, 1);
    assert_eq!(reply.applied_at_ms, Some(400));
    assert!(reply.pending.is_none());
    matches_call(&reply, &provider.calls()[1]);
    drop(commands);
    served.await.unwrap();
}

#[tokio::test]
async fn plan_view_unknown_first_run_time_is_absent() {
    let mut executor = executor(vec![text_step("a", "answer")], None);
    executor.start_time = Some(Arc::new(Mutex::new(None)));
    let (commands, served) = start(executor).await;
    send(&commands, "r1", Some(manifest("unknown-time", false))).await;
    idle(&commands).await;
    let reply = view(&commands).await.unwrap();
    assert_eq!(reply.applied_at_ms, None);
    assert!(serde_json::to_value(&reply)
        .unwrap()
        .get("applied_at_ms")
        .is_none());
    drop(commands);
    served.await.unwrap();
}

async fn in_step(commands: &Harness, run_id: &str) {
    broca_test_support::wait_until("the gated model step is in flight", || async {
        match ask(commands, StoreReadRequest::RunStatus(RunId(run_id.into()))).await {
            StoreReadReply::RunStatus(Ok(status)) => {
                let value = serde_json::to_value(status).unwrap();
                if value["step"].is_object() {
                    Ok(())
                } else {
                    Err(value.to_string())
                }
            }
            _ => Err("the run has not started".into()),
        }
    })
    .await;
}

#[tokio::test]
async fn plan_view_in_flight_keeps_last_dated_request_but_not_across_generations() {
    let gate = Arc::new(Notify::new());
    let executor = executor(
        vec![
            attempted_step("a", Some("dated"), 500),
            text_step("b", "undated"),
            text_step("c", "new generation"),
        ],
        Some(gate.clone()),
    );
    let (commands, served) = start(executor).await;
    send(&commands, "r1", Some(manifest("first", false))).await;
    gate.notify_one();
    idle(&commands).await;
    let previous = view(&commands).await.unwrap().newest_step.unwrap();
    send(&commands, "r2", None).await;
    in_step(&commands, "r2").await;
    assert_eq!(
        view(&commands).await.unwrap().newest_step,
        Some(previous.clone())
    );
    gate.notify_one();
    idle(&commands).await;
    assert_eq!(view(&commands).await.unwrap().newest_step, Some(previous));
    refresh(&commands, 1, "immediately", manifest("next", false)).await;
    send(&commands, "r3", None).await;
    in_step(&commands, "r3").await;
    let reply = view(&commands).await.unwrap();
    assert_eq!(reply.generation, 1);
    assert!(
        reply.newest_step.is_none(),
        "an earlier generation's request is not attributed to this plan"
    );
    gate.notify_one();
    idle(&commands).await;
    drop(commands);
    served.await.unwrap();
}

#[tokio::test]
async fn plan_view_reply_mid_is_the_read_mid_and_absent_without_message() {
    let executor = executor(
        vec![
            attempted_step("provider-id", Some("reply"), 600),
            attempted_step("unused-provider-id", None, 700),
        ],
        None,
    );
    let (commands, served) = start(executor).await;
    send(&commands, "r1", Some(manifest("first", false))).await;
    idle(&commands).await;
    let reply = view(&commands).await.unwrap();
    let StoreReadReply::Page(Ok(read)) =
        ask(&commands, StoreReadRequest::Page(Default::default())).await
    else {
        panic!("transcript page");
    };
    let assistant = read
        .response
        .messages
        .iter()
        .find(|message| message.message.role == broca_protocol::Role::Assistant)
        .unwrap();
    let newest = reply.newest_step.unwrap();
    assert_eq!(
        newest.at_ms, 600,
        "send time, not attempt completion at 607"
    );
    assert_eq!(newest.reply_mid.as_deref(), Some(assistant.mid.as_str()));
    assert_ne!(assistant.mid, "provider-id");
    send(&commands, "r2", None).await;
    idle(&commands).await;
    let newest = view(&commands).await.unwrap().newest_step.unwrap();
    assert_eq!(newest.at_ms, 700);
    assert!(newest.reply_mid.is_none());
    assert!(serde_json::to_value(newest)
        .unwrap()
        .get("reply_mid")
        .is_none());
    drop(commands);
    served.await.unwrap();
}

#[tokio::test]
async fn plan_view_unverifiable_join_keeps_stored_text_without_offsets() {
    for unknown_version in [false, true] {
        let executor = executor(vec![text_step("a", "reply")], None);
        let provider = executor.provider.inner.clone();
        let (commands, served) = start(executor).await;
        let mut admitted = manifest("first", false);
        if unknown_version {
            admitted.join_version = 99;
        } else {
            admitted.system_text = "the authoritative text".into();
        }
        let stored = admitted.system_text.clone();
        send(&commands, "r1", Some(admitted)).await;
        idle(&commands).await;
        let reply = view(&commands).await.unwrap();
        assert_eq!(reply.system.text, stored);
        assert!(reply.system.items.is_none());
        assert_eq!(
            provider.calls()[0].prompt[0].content,
            [ContentBlock::Text {
                text: stored.into()
            }]
        );
        drop(commands);
        served.await.unwrap();
    }
}

/// A run takes a versioned tool's newer same-major definition only where its prompt is written
/// afresh anyway: when it applies a pending change, or at a prefix rebuild such as a model
/// switch, with or without a change waiting. The actor tells the run which it is; any other run
/// keeps the session's tools.
#[tokio::test]
async fn a_run_is_told_it_starts_at_a_prefix_rebuild_or_applies_a_change() {
    let executor = executor(
        vec![
            text_step("a", "first"),
            text_step("b", "second"),
            text_step("c", "third"),
            text_step("d", "fourth"),
            text_step("e", "fifth"),
        ],
        None,
    );
    let points = Arc::clone(&executor.rebuild_points);
    let (commands, served) = start(executor).await;
    send(&commands, "r1", Some(manifest("first", false))).await;
    idle(&commands).await;
    refresh(&commands, 1, "immediately", manifest("second", false)).await;
    send(&commands, "r2", None).await;
    idle(&commands).await;
    send(&commands, "r3", None).await;
    idle(&commands).await;
    refresh(&commands, 2, "on_prefix_rebuild", manifest("third", false)).await;
    let mut request = untooled("switch models");
    request.model = broca_protocol::ModelHandle::new("stub", "m2");
    assert!(matches!(
        super::tests::send_with(&commands, "r4", request, crate::actor::SendKey::Minted).await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    let mut request = untooled("switch back");
    request.model = broca_protocol::ModelHandle::new("stub", "m");
    assert!(matches!(
        super::tests::send_with(&commands, "r5", request, crate::actor::SendKey::Minted).await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    assert_eq!(
        *points.lock().unwrap(),
        [false, true, false, true, true],
        "first run, applied refresh, plain run, model switch applying a change, model switch"
    );
    drop(commands);
    served.await.unwrap();
}
