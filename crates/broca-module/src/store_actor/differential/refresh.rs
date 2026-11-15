//! Plan refreshes on a session kept in the new storage, against today's actor for the same
//! refreshes.
//!
//! Each scenario drives the same sends, refreshes, policy changes and prefix flushes through a
//! module on today's storage and one that starts the session in the new storage, and compares
//! every reply and the plan each run froze. Refreshes are handed to each session's actor as the
//! serve layer hands over a fetched manifest (`route_session_refresh`); policy changes and
//! flushes go through the serve layer's own preflight (`ServeHandler`).

use std::sync::Arc;

use broca_core::WalStore as _;
use broca_protocol::{SendScope, SubmissionRequest};
use broca_session_store::fold::operations::ManifestRef;
use broca_session_store::record::Record;
use serde_json::{json, Value};

use super::super::tests::{request, text_step};
use super::head::{both, core, head_json, occipital, send_titled};
use super::Module;
use crate::actor::{SendOutcome, SendPlan};
use crate::serve::ServeHandler;

cipher_tests!(
    a_refreshed_sender_policy_refuses_a_module_it_doesnt_name_alike,
    only_the_new_storage_takes_a_refresh_that_adds_a_compaction_provider,
    a_pending_stamp_on_the_same_text_moves_to_the_next_run_and_stays,
);

/// A plan whose system text is `text`, offering the tools `tools` from one provider. With
/// `pinned`, its system text item names the model it was written for, so a run on another
/// model is refused. With `chooses`, the plan chooses the session's next-run model.
fn plan(text: &str, tools: &[&str], pinned: Option<Value>, chooses: Option<Value>) -> Value {
    let composition = json!({
        "providers": [{"provider": "fixture", "tools": tools
            .iter()
            .map(|name| json!({"name": name, "capabilities": []}))
            .collect::<Vec<_>>()}],
        "step_transform_items": [],
    });
    let mut params = json!({"text": text});
    if let Some(pinned) = pinned {
        params["model"] = pinned;
    }
    let mut plan = json!({
        "tool_items": if tools.is_empty() { vec![] } else { vec![json!({"provider": "fixture", "params": {}})] },
        "system_text_items": [{"provider": "fixture", "params": params}],
        "step_transform_items": [],
        "composition_digest": cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
        "composition": composition,
        "senders": [{"module": "occipital", "idle_start": true}],
    });
    if let Some(chooses) = chooses {
        plan["model"] = chooses;
    }
    plan
}

/// The manifest an admitter fetches for `plan`, under the plan identity `identity`.
fn manifest(plan: &Value, identity: &str) -> broca_wal::PlanManifest {
    let tools = plan["composition"]["providers"][0]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| {
            serde_json::from_value::<broca_protocol::Tool>(json!({
                "name": tool["name"], "description": "fixture",
                "input_schema": {"type": "object"}, "module": "fixture",
            }))
            .unwrap()
        })
        .collect();
    let text = plan["system_text_items"][0]["params"]["text"]
        .as_str()
        .unwrap()
        .to_owned();
    broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap(),
        plan_identity: identity.into(),
        composition_digest: plan["composition_digest"].as_str().unwrap().into(),
        fetches: vec![],
        tools: Arc::new(tools),
        system_digest: text.clone(),
        system_text: text,
        join_version: 1,
        role_versions: Default::default(),
        declarations: vec![],
    }
}

/// One step of a scenario, run alike on both modules. A refresh carries its manifest whole, as
/// the serve layer hands it over; the steps are few, so their size doesn't matter.
#[allow(clippy::large_enum_variant)]
enum Step {
    /// A send under the plan with this identity; the session's first send freezes the
    /// scenario's first plan. `model` runs it on another model than the request's, and
    /// `omit_model` leaves its model to the session.
    Send {
        run_id: &'static str,
        plan: &'static str,
        model: Option<&'static str>,
        omit_model: bool,
    },
    Refresh {
        generation: u64,
        policy: &'static str,
        manifest: broca_wal::PlanManifest,
    },
    Policy {
        generation: u64,
        policy: &'static str,
        send_id: &'static str,
    },
    Flush {
        send_id: &'static str,
    },
}

fn send(run_id: &'static str, plan: &'static str) -> Step {
    Step::Send {
        run_id,
        plan,
        model: None,
        omit_model: false,
    }
}

/// Runs `steps` on `module`, whose first send freezes `first`, and answers what each step was
/// answered with, and the session's baseline after each step.
async fn drive(
    module: &Module,
    first: &broca_wal::PlanManifest,
    steps: &[Step],
) -> (Vec<String>, Vec<String>) {
    let mut answers = Vec::new();
    let mut baselines = Vec::new();
    let mut sent = false;
    for step in steps {
        let answer = match step {
            Step::Send {
                run_id,
                plan,
                model,
                omit_model,
            } => {
                let mut request: SubmissionRequest = request("hello");
                // Every session here is core's, so another module is a sender, never the owner.
                request.principal = Some(core());
                request.tools = None;
                request.policy_allowed = Default::default();
                if let Some(model) = model {
                    request.model = broca_protocol::ModelHandle::new("stub", *model);
                }
                if *omit_model {
                    request
                        .inherit_frozen
                        .insert(broca_protocol::request::InheritedParam::Model);
                }
                let plan = SendPlan {
                    identity: (*plan).into(),
                    manifest: (!sent).then(|| Arc::new(first.clone())),
                    model_check: None,
                };
                let outcome = send_titled(module, run_id, request, None, Some(plan)).await;
                let started = matches!(outcome, SendOutcome::Active { .. });
                sent |= started;
                module.closed().await;
                match outcome {
                    SendOutcome::Active { .. } => "active".to_owned(),
                    other => format!("{other:?}"),
                }
            }
            Step::Refresh {
                generation,
                policy,
                manifest,
            } => format!(
                "{:?}",
                module
                    .registry
                    .route_session_refresh(
                        &module.key,
                        *generation,
                        (*policy).into(),
                        Box::new(manifest.clone()),
                    )
                    .await
                    .expect("the refresh is routed")
            ),
            Step::Policy {
                generation,
                policy,
                send_id,
            } => format!(
                "{:?}",
                module
                    .registry
                    .session_refresh_policy(
                        &module.key,
                        Some(&core()),
                        &SendScope::default(),
                        *generation,
                        (*policy).into(),
                        (*send_id).into(),
                    )
                    .await
                    .map(|reply| serde_json::to_value(reply).unwrap())
            ),
            Step::Flush { send_id } => format!(
                "{:?}",
                module
                    .registry
                    .session_flush_prefix(
                        &module.key,
                        Some(&core()),
                        &SendScope::default(),
                        (*send_id).into(),
                    )
                    .await
                    .map(|reply| serde_json::to_value(reply).unwrap())
            ),
        };
        module.closed().await;
        answers.push(answer);
        baselines.push(baseline(module).await);
    }
    (answers, baselines)
}

/// `session.baseline` as the session's owner reads it, and as another module is refused it,
/// so every scenario compares the baseline after each of its steps.
async fn baseline(module: &Module) -> String {
    let mut read = Vec::new();
    for principal in [core(), occipital()] {
        read.push(
            match module
                .registry
                .baseline(&module.key, Some(&principal), &SendScope::default())
                .await
            {
                Ok(reply) => format!("Ok({})", serde_json::to_string(&reply).unwrap()),
                Err(error) => format!("Err({error:?})"),
            },
        );
    }
    module.closed().await;
    read.join(" | ")
}

/// Each run of today's session with the plan identity and model its start froze.
fn today_runs(module: &Module) -> Vec<(String, Option<String>, String)> {
    module
        .wal
        .read_records(&module.key)
        .unwrap()
        .into_iter()
        .filter_map(|frame| match frame.record {
            broca_wal::WalRecord::RunStarted {
                run_id,
                manifest,
                config,
                ..
            } => Some((
                run_id.0,
                manifest.map(|manifest| manifest.plan_identity.clone()),
                config.model.model_id,
            )),
            _ => None,
        })
        .collect()
}

/// Each run of the store's session, as [`today_runs`] reads today's, and the refresh each run
/// start's batch says it applied.
async fn store_runs(module: &Module) -> (Vec<(String, Option<String>, String)>, Vec<u64>) {
    let session = module.load().await;
    let mut runs = Vec::new();
    let mut applied = Vec::new();
    for (_, record) in session.open_log_records().await.unwrap() {
        match record {
            Record::RunStart(start) => runs.push((
                start.run_id.clone(),
                ManifestRef::of_start(&start)
                    .and_then(|manifest| manifest.plan_identity().map(str::to_owned)),
                start.model.model_id.clone(),
            )),
            Record::RefreshApplied(record) => applied.push(record.generation),
            _ => {}
        }
    }
    (runs, applied)
}

/// Runs `steps` on both storages and requires every answer and every run's frozen plan to be
/// equal, and each refresh today's fold says a run applied to be recorded as applied in the new
/// storage. Answers both modules, and the store's answers and runs for the scenario to check
/// what they say.
async fn compare(
    first: &broca_wal::PlanManifest,
    steps: &[Step],
) -> (
    Module,
    Module,
    Vec<String>,
    Vec<(String, Option<String>, String)>,
) {
    let (today, store, _, _) = both(
        (0..steps.len())
            .map(|n| text_step(&format!("a{n}"), "hello"))
            .collect(),
        false,
    )
    .await;
    compared(today, store, first, steps).await
}

/// [`compare`], with the store module's sessions in mode `cipher`.
async fn compare_in(
    cipher: broca_session_store::keys::Cipher,
    first: &broca_wal::PlanManifest,
    steps: &[Step],
) -> (
    Module,
    Module,
    Vec<String>,
    Vec<(String, Option<String>, String)>,
) {
    let (today, store) = super::head::both_in(
        (0..steps.len())
            .map(|n| text_step(&format!("a{n}"), "hello"))
            .collect(),
        cipher,
    )
    .await;
    compared(today, store, first, steps).await
}

/// [`compare`] on the modules `today` and `store`.
async fn compared(
    today: Module,
    store: Module,
    first: &broca_wal::PlanManifest,
    steps: &[Step],
) -> (
    Module,
    Module,
    Vec<String>,
    Vec<(String, Option<String>, String)>,
) {
    let (today_answers, today_baselines) = drive(&today, first, steps).await;
    let (store_answers, store_baselines) = drive(&store, first, steps).await;
    alike!(store_answers, today_answers);
    alike!(store_baselines, today_baselines);
    let (runs, applied) = store_runs(&store).await;
    alike!(runs, today_runs(&today));
    let frames = today.wal.read_records(&today.key).unwrap();
    let today_applied: Vec<u64> =
        broca_fetch_plan::session_change::SessionChangeState::fold(&frames)
            .applied
            .into_keys()
            .collect();
    alike!(applied, today_applied);
    (today, store, store_answers, runs)
}

pub(super) fn first_plan() -> broca_wal::PlanManifest {
    manifest(&plan("you are a worker", &["read"], None, None), "plan-1")
}

fn second_plan() -> broca_wal::PlanManifest {
    manifest(
        &plan("you are a reviewer", &["read", "write"], None, None),
        "plan-2",
    )
}

/// An `immediately` refresh applies from the next run, which freezes its plan, alike.
#[tokio::test]
async fn an_immediate_refresh_applies_at_the_next_run_alike() {
    let (_, _, answers, runs) = compare(
        &first_plan(),
        &[
            send("r1", "plan-1"),
            Step::Refresh {
                generation: 1,
                policy: "immediately",
                manifest: second_plan(),
            },
            send("r2", "plan-2"),
        ],
    )
    .await;
    assert_eq!(answers[0], "active");
    assert!(
        answers[1].contains("applies_from_next_run: true"),
        "{answers:?}"
    );
    assert_eq!(runs[1].1.as_deref(), Some("plan-2"));
}

/// `session.baseline` on the new storage reports what today's does after a refresh is accepted
/// and after it applies: the generations, the pending change, the effective plan and its tools,
/// and it refuses a module that isn't the session's owner. `compared` already requires the two
/// storages to agree after every step of every scenario here; this pins what they agree on, so
/// two equally wrong answers can't pass.
#[tokio::test]
async fn the_baseline_reports_accepted_pending_and_applied_changes_alike() {
    let steps = [
        send("r1", "plan-1"),
        Step::Refresh {
            generation: 1,
            policy: "on_prefix_rebuild",
            manifest: second_plan(),
        },
        Step::Flush { send_id: "flush-1" },
        send("r2", "plan-2"),
    ];
    let (today, store, _, _) = both(
        (0..steps.len())
            .map(|n| text_step(&format!("a{n}"), "hello"))
            .collect(),
        false,
    )
    .await;
    let (_, today_baselines) = drive(&today, &first_plan(), &steps).await;
    let (_, baselines) = drive(&store, &first_plan(), &steps).await;
    alike!(baselines, today_baselines);
    // The baseline the owner read, from its `Ok(<reply JSON>)`.
    let owner = |read: &str| -> Value {
        let owner = read.split(" | ").next().unwrap();
        let json = owner
            .strip_prefix("Ok(")
            .unwrap()
            .strip_suffix(')')
            .unwrap();
        let reply: Value = serde_json::from_str(json).unwrap();
        assert_eq!(reply["state"], "ready", "{reply}");
        reply["baseline"].clone()
    };
    // After the first run: the first plan, nothing accepted yet.
    let first = owner(&baselines[0]);
    assert_eq!(first["accepted_generation"], 0, "{first}");
    assert_eq!(first["tool_names"], json!(["read"]), "{first}");
    // Accepted and waiting for a prefix rebuild.
    let accepted = owner(&baselines[1]);
    assert_eq!(accepted["accepted_generation"], 1, "{accepted}");
    assert_eq!(accepted["applied_generation"], 0, "{accepted}");
    assert_eq!(accepted["pending"]["generation"], 1, "{accepted}");
    // Applied by the run after the flush: the refreshed plan's tools, nothing pending.
    let applied = owner(&baselines[3]);
    assert_eq!(applied["applied_generation"], 1, "{applied}");
    assert!(applied.get("pending").is_none(), "{applied}");
    assert_eq!(applied["effective"]["generation"], 1, "{applied}");
    assert_eq!(applied["tool_names"], json!(["read", "write"]), "{applied}");
    // Another module is refused as not the owner.
    let refused = baselines[3].split(" | ").nth(1).unwrap();
    assert!(refused.contains("OwnerMismatch"), "{refused}");
}

/// An `on_prefix_rebuild` refresh waits: a run without a rebuild keeps the session's plan, and
/// the run after a model switch or a prefix flush freezes the refreshed one, alike.
#[tokio::test]
async fn a_refresh_on_prefix_rebuild_waits_for_a_model_switch_or_a_flush_alike() {
    let (_, _, answers, runs) = compare(
        &first_plan(),
        &[
            send("r1", "plan-1"),
            Step::Refresh {
                generation: 1,
                policy: "on_prefix_rebuild",
                manifest: second_plan(),
            },
            send("r2", "plan-1"),
            Step::Send {
                run_id: "r3",
                plan: "plan-2",
                model: Some("m2"),
                omit_model: false,
            },
            Step::Refresh {
                generation: 2,
                policy: "on_prefix_rebuild",
                manifest: first_plan(),
            },
            Step::Flush { send_id: "f1" },
            send("r4", "plan-1"),
        ],
    )
    .await;
    assert!(
        answers[1].contains("applies_from_next_run: false"),
        "{answers:?}"
    );
    assert_eq!(
        answers[5],
        r#"Ok(Object {"pending_generation": Number(2)})"#
    );
    let plans: Vec<_> = runs.iter().map(|run| run.1.as_deref()).collect();
    assert_eq!(
        plans,
        [
            Some("plan-1"),
            Some("plan-1"),
            Some("plan-2"),
            Some("plan-1")
        ]
    );
}

/// A policy change and a flush, each repeated identically and reused with another body: a
/// repeat is answered as the first was, a reused policy `send_id` is refused naming the field,
/// and a flush's `send_id` keeps the pending generation it first reported, alike.
#[tokio::test]
async fn policy_changes_and_flushes_repeat_and_refuse_reuse_alike() {
    let (_, _, answers, runs) = compare(
        &first_plan(),
        &[
            send("r1", "plan-1"),
            Step::Refresh {
                generation: 1,
                policy: "on_prefix_rebuild",
                manifest: second_plan(),
            },
            Step::Policy {
                generation: 1,
                policy: "immediately",
                send_id: "p1",
            },
            Step::Policy {
                generation: 1,
                policy: "immediately",
                send_id: "p1",
            },
            Step::Policy {
                generation: 1,
                policy: "on_prefix_rebuild",
                send_id: "p1",
            },
            Step::Flush { send_id: "f1" },
            Step::Flush { send_id: "f1" },
            send("r2", "plan-2"),
            Step::Flush { send_id: "f1" },
            Step::Flush { send_id: "f2" },
            Step::Policy {
                generation: 1,
                policy: "immediately",
                send_id: "p2",
            },
        ],
    )
    .await;
    assert_eq!(answers[2], answers[3]);
    assert!(answers[4].contains("SendIdReuse"), "{answers:?}");
    assert_eq!(answers[5], answers[6]);
    assert_eq!(answers[5], answers[8], "a flush repeat after the run");
    assert_eq!(
        answers[9], "Ok(Object {})",
        "no refresh is pending after the run"
    );
    assert!(answers[10].contains("AlreadyApplied"), "{answers:?}");
    assert_eq!(runs[1].1.as_deref(), Some("plan-2"));
}

/// A refresh repeating an older generation identically is answered with that generation's
/// original flags; one naming that generation with another plan is `generation_stale`, alike.
#[tokio::test]
async fn an_old_generation_repeats_identically_and_is_otherwise_stale_alike() {
    let (_, _, answers, _) = compare(
        &first_plan(),
        &[
            send("r1", "plan-1"),
            Step::Refresh {
                generation: 1,
                policy: "on_prefix_rebuild",
                manifest: second_plan(),
            },
            Step::Refresh {
                generation: 2,
                policy: "immediately",
                manifest: first_plan(),
            },
            Step::Refresh {
                generation: 1,
                policy: "on_prefix_rebuild",
                manifest: second_plan(),
            },
            Step::Refresh {
                generation: 1,
                policy: "immediately",
                manifest: first_plan(),
            },
        ],
    )
    .await;
    assert_eq!(answers[3], answers[1]);
    assert!(answers[4].contains("GenerationStale"), "{answers:?}");
}

/// A pending refresh whose system text names another model refuses the run that would apply it,
/// and the session keeps running its plan on a model the refresh accepts, alike.
#[tokio::test]
async fn a_pending_plan_whose_model_mismatches_refuses_the_run_alike() {
    let pinned = manifest(
        &plan(
            "you write for m2",
            &["read"],
            Some(json!({"provider": "stub", "id": "m2"})),
            None,
        ),
        "plan-pinned",
    );
    let (_, _, answers, runs) = compare(
        &first_plan(),
        &[
            send("r1", "plan-1"),
            Step::Refresh {
                generation: 1,
                policy: "immediately",
                manifest: pinned,
            },
            send("r2", "plan-pinned"),
            Step::Send {
                run_id: "r3",
                plan: "plan-pinned",
                model: Some("m2"),
                omit_model: false,
            },
        ],
    )
    .await;
    assert!(
        answers[2].contains("PendingPlanModelMismatch"),
        "{answers:?}"
    );
    assert_eq!(answers[3], "active");
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[1].1.as_deref(), Some("plan-pinned"));
}

/// A refresh that changes the plan's model choice moves a send that leaves its model to the
/// session onto the new model, and a refresh that grants a module the right to interrupt lets it
/// cancel the session's run; the head serves both, alike.
#[tokio::test]
async fn a_refresh_changes_the_inherited_model_and_the_senders_alike() {
    let choosing = |identity: &str, model: &str, interrupt: bool| {
        let mut plan = plan(
            "you are a worker",
            &["read"],
            None,
            Some(json!({"provider": "stub", "model": model})),
        );
        plan["senders"] =
            json!([{"module": "occipital", "idle_start": true, "interrupt": interrupt}]);
        manifest(&plan, identity)
    };
    let (today, store, answers, runs) = compare(
        &choosing("plan-a", "m", false),
        &[
            send("r1", "plan-a"),
            Step::Refresh {
                generation: 1,
                policy: "on_prefix_rebuild",
                manifest: choosing("plan-b", "m2", true),
            },
            Step::Send {
                run_id: "r2",
                plan: "plan-a",
                model: None,
                omit_model: true,
            },
        ],
    )
    .await;
    assert_eq!(answers[2], "active", "{answers:?}");
    assert_eq!(
        runs[1].2, "m2",
        "the send ran on the refreshed plan's model"
    );
    for principal in [None, Some(core()), Some(occipital())] {
        alike!(
            head_json(&store, principal.as_ref()).await,
            head_json(&today, principal.as_ref()).await,
            "heads differ for {principal:?}"
        );
    }
    let head = head_json(&store, Some(&occipital())).await;
    assert_eq!(head["model"]["model"], "m2", "{head}");
    assert!(head["sender"].is_object(), "{head}");
    // The refreshed sender's interrupt grant decides a cancel it makes, as today's does.
    for module in [&today, &store] {
        let authority = module
            .registry
            .resolve_cancel_authority(
                &module.key,
                Some(&occipital()),
                &SendScope::default(),
                broca_wal::record::RunCancelAt::Now,
            )
            .await;
        assert!(
            matches!(authority, Ok(crate::actor::CancelAuthority::Sender)),
            "{authority:?}"
        );
    }
}

/// Once a refresh sets the session's sender policy, a module its plan doesn't name is refused
/// `scope_owner_mismatch`, even with a string `delivery`, alike: here `occipital`, which the
/// first plan named and the refresh's plan drops, and a module neither plan named.
async fn a_refreshed_sender_policy_refuses_a_module_it_doesnt_name_alike(
    cipher: broca_session_store::keys::Cipher,
) {
    let without_occipital = {
        let mut plan = plan("you are a worker", &["read"], None, None);
        plan["senders"] = json!([{"module": "magic-context"}]);
        manifest(&plan, "plan-2")
    };
    let (today, store, answers, _) = compare_in(
        cipher,
        &first_plan(),
        &[
            send("r1", "plan-1"),
            Step::Refresh {
                generation: 1,
                policy: "immediately",
                manifest: without_occipital,
            },
        ],
    )
    .await;
    assert_eq!(answers[0], "active", "{answers:?}");
    let stranger = broca_protocol::BindPrincipal::Reserved {
        module_id: "stranger".into(),
    };
    for (run_id, principal) in [("s1", occipital()), ("s2", stranger)] {
        let mut answers = Vec::new();
        for module in [&today, &store] {
            let mut request = request("hello");
            request.principal = Some(principal.clone());
            request.tools = None;
            request.policy_allowed = Default::default();
            answers.push(send_titled(module, run_id, request, None, None).await);
            module.closed().await;
        }
        alike!(answers[1], answers[0], "{principal:?}");
        assert!(
            matches!(
                &answers[0],
                SendOutcome::DeliveryRefused {
                    code: cortexkit_role_llm_runner::errors::SCOPE_OWNER_MISMATCH,
                    field: "delivery",
                    ..
                }
            ),
            "{principal:?}: {:?}",
            answers[0]
        );
    }
}

/// [`manifest`], with its provider's system text answer stamped `digit` repeated (a Rich UI
/// catalog digest).
fn stamped(plan: &Value, identity: &str, digit: char) -> broca_wal::PlanManifest {
    let mut manifest = manifest(plan, identity);
    manifest.fetches = serde_json::from_value(json!([{
        "provider": "fixture", "catalog_digest": "d", "tools": [],
        "text": manifest.system_text,
    }]))
    .unwrap();
    manifest.rich_ui =
        Some(broca_protocol::RichUiStamp::new("1.0", digit.to_string().repeat(64)).unwrap());
    manifest
}

/// llm-runner 9.17, "same text, newer stamp": a refresh waiting for a prefix rebuild whose
/// system text is byte-identical to the effective plan's lends the next run its newer Rich UI
/// stamp. The refresh itself doesn't apply (no applied record, generation still pending), the
/// run keeps the effective plan, and the stamp stays on later runs once the session's actor is
/// opened again, because the run's start recorded it.
async fn a_pending_stamp_on_the_same_text_moves_to_the_next_run_and_stays(
    cipher: broca_session_store::keys::Cipher,
) {
    let taught = plan("you are taught", &["read"], None, None);
    let first = stamped(&taught, "plan-1", 'b');
    let steps = [
        send("r1", "plan-1"),
        Step::Refresh {
            generation: 1,
            policy: "on_prefix_rebuild",
            manifest: stamped(&taught, "plan-1", '4'),
        },
        send("r2", "plan-1"),
        send("r3", "plan-1"),
    ];
    let (_, store) = super::head::both_in(
        (0..steps.len())
            .map(|n| text_step(&format!("a{n}"), "hello"))
            .collect(),
        cipher,
    )
    .await;
    let (answers, _) = drive(&store, &first, &steps).await;
    assert_eq!(answers[0], "active");
    assert_eq!(answers[2], "active");
    assert_eq!(answers[3], "active");
    let (runs, applied) = store_runs(&store).await;
    assert!(
        applied.is_empty(),
        "the refresh is still pending: {applied:?}"
    );
    assert_eq!(runs.len(), 3);
    let session = store.load().await;
    assert_eq!(
        session
            .operations
            .plan
            .as_ref()
            .and_then(|plan| plan.pending.as_ref())
            .map(|pending| pending.generation),
        Some(1)
    );
    // Each run's reply carries its run's stamp: the first run's, then the newer one twice, the
    // last after the actor reopened the session from storage.
    let stamps: Vec<char> = session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(_, record)| match record {
            Record::EntryFacts(facts) => facts
                .rich_ui
                .as_ref()
                .map(|stamp| stamp.catalog_digest.chars().next().unwrap()),
            _ => None,
        })
        .collect();
    assert_eq!(stamps, vec!['b', '4', '4'], "{stamps:?}");
}

/// A refresh that adds a compaction provider: today's storage refuses it with today's exact
/// refusal, and the new storage takes it, against the latest accepted plan. Accepting it
/// enrolls nothing: the session moves only at the run that applies it.
async fn only_the_new_storage_takes_a_refresh_that_adds_a_compaction_provider(
    cipher: broca_session_store::keys::Cipher,
) {
    let mut compacted = plan("you compact", &["read"], None, None);
    compacted["compaction_item"] = json!({"provider": "ck-mc", "preset": "head", "params": {}});
    let compacted = manifest(&compacted, "plan-compacted");
    let steps = [
        send("r1", "plan-1"),
        Step::Refresh {
            generation: 1,
            policy: "immediately",
            manifest: compacted,
        },
    ];
    let (today, store) = super::head::both_in(
        (0..steps.len())
            .map(|n| text_step(&format!("a{n}"), "hello"))
            .collect(),
        cipher,
    )
    .await;
    let (today_answers, _) = drive(&today, &first_plan(), &steps).await;
    let (store_answers, _) = drive(&store, &first_plan(), &steps).await;
    alike_today_first!(
        today_answers[1],
        format!(
            "{:?}",
            crate::actor::SessionRefreshOutcome::Refused(
                crate::serve::SessionChangeError::InvalidParams {
                    field: "plan.compaction_item".into(),
                    message: "the session's compaction provider, preset and params are fixed \
                              for its life"
                        .into(),
                }
            )
        )
    );
    assert!(
        store_answers[1].contains("applies_from_next_run: true"),
        "{store_answers:?}"
    );
    let session = store.load().await;
    assert!(session.operations.enrolled.is_none());
    assert!(!session.operations.on_contract());
}
