//! What a tool change does to earlier signed reasoning, seen in the raw bytes the scripted model
//! receives. Anthropic binds signed thinking to the request's tool set, so after a run whose
//! tools differ from the run before it, broca sends no signed thinking from earlier messages
//! (`broca_core::signed_floor`); reordering the same tools changes nothing. A session on the
//! Messages stand-in, whose model signs its reasoning, takes a new plan through the real
//! `session.refresh` route at a run boundary, and the conformance kit compares the request sent
//! before the change with the first one sent after it.
//!
//! broca renders no `defer_loading` tools and appends no tool notes, so the kit's deferred
//! addition and tool-note changes have no fixture here and stay inapplicable.

use super::*;
use cortexkit_role_llm_runner_conformance::{
    ScriptedTurn, SignedToolChange, SignedToolChangeObservation,
};

/// The preset prefix that makes the fixture's tool catalog answer with the signed-floor tools.
/// The rest of the preset names the plan's variant.
const PRESET: &str = "signed-floor-";
const ALPHA: &str = "signed_floor_alpha";
const BETA: &str = "signed_floor_beta";
/// Literal markers in the signed reasoning, in its signature and in ordinary answer text. The
/// kit looks for them in raw request bytes, so they are plain letters that JSON never escapes.
const THINKING_MARKER: &str = "signedfloorthinking";
const SIGNATURE_MARKER: &str = "signedfloorsignature";
const TEXT_MARKER: &str = "signedfloorplaintext";

/// The two tools in the order the variant's catalog answers them: `reorder` reverses them.
fn order(variant: &str) -> [&'static str; 2] {
    if variant == "reorder" {
        [BETA, ALPHA]
    } else {
        [ALPHA, BETA]
    }
}

/// A plan offering the signed-floor tools under `variant`'s preset. The composition lists the
/// tools in one fixed order for every variant; only the catalog's answer differs.
pub(super) fn plan(variant: &str) -> Value {
    let tools: Vec<_> = [ALPHA, BETA]
        .into_iter()
        .map(|name| json!({"name": name, "capabilities": []}))
        .collect();
    let composition = json!({
        "providers": [{"provider": TOOL_MODULE, "tools": tools}],
        "step_transform_items": [],
    });
    json!({
        "tool_items": [{"provider": TOOL_MODULE, "preset": format!("{PRESET}{variant}"), "params": {}}],
        "system_text_items": [],
        "step_transform_items": [],
        "composition_digest":
            cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
        "composition": composition,
    })
}

/// The tool catalog's answer to a signed-floor plan's `tool.catalog`, or `None` for any other
/// plan. `edit` revises the first tool's description; `reorder` answers the same two tools in
/// the other order.
pub(super) fn catalog(request: &Value) -> Option<Value> {
    let variant = request["arguments"]["preset"]
        .as_str()?
        .strip_prefix(PRESET)?;
    let schema = json!({"type": "object", "properties": {"note": {"type": "string"}}});
    let tools: Vec<_> = order(variant)
        .into_iter()
        .map(|name| {
            let description = if variant == "edit" && name == ALPHA {
                format!("Answers {name} calls, revised.")
            } else {
                format!("Answers {name} calls.")
            };
            json!({
                "name": name,
                "schema_digest": cortexkit_role_tool_provider::catalog::schema_digest(&schema).unwrap(),
                "semantics": 1,
                "result_format": "tool-result/v1",
                "capabilities": [],
                "description": description,
                "input_schema": schema,
            })
        })
        .collect();
    let composition = &request["arguments"]["composition"];
    Some(json!({
        "generation": "1",
        "catalog_digest": "signed-floor",
        "composition_digest":
            cortexkit_role_tool_provider::catalog::composition_digest(composition).unwrap(),
        "tools": tools,
    }))
}

/// The kit's fixture for `change`, on a rig of its own: two runs under the first plan, so the
/// second request carries the first answer's signed reasoning (`before`), then the changed
/// plan through `session.refresh` and a third run, whose request is `after`.
///
/// `Resume` has no fixture: broca never sends a request again after a restart. A run whose
/// request was out when the runner stopped ends Interrupted, and a run paused for a restart
/// pauses before its next request is sent, so no request is ever rendered twice.
pub(super) async fn observe(
    subject: &BrocaSubject,
    session: &str,
    change: SignedToolChange,
) -> Result<Option<SignedToolChangeObservation>, HarnessError> {
    let variant = match change {
        SignedToolChange::Edit => "edit",
        SignedToolChange::Reorder => "reorder",
        _ => return Ok(None),
    };
    let root = broca_test_support::scratch_root()
        .join(format!("signed-floor-{session}-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&root);
    std::fs::create_dir_all(&root).map_err(|e| HarnessError::new(e.to_string()))?;
    subject
        .shared
        .signed_reasoning
        .lock()
        .unwrap()
        .insert(session.into(), SIGNATURE_MARKER.into());
    let turn = |parts| ScriptedTurn::new(parts);
    subject
        .install_script(
            session,
            Script {
                turns: vec![
                    turn(vec![
                        ScriptedPart::Reasoning(format!("Weighing the {THINKING_MARKER} answer.")),
                        ScriptedPart::Text(format!("The answer is {TEXT_MARKER}.")),
                    ]),
                    turn(vec![ScriptedPart::Text("The second answer.".into())]),
                    turn(vec![ScriptedPart::Text("The third answer.".into())]),
                ],
            },
        )
        .await?;
    let observed = runs(subject, &root, session, variant).await;
    subject
        .shared
        .signed_reasoning
        .lock()
        .unwrap()
        .remove(session);
    broca_test_support::remove_rig_dir(&root);
    observed.map(Some)
}

async fn runs(
    subject: &BrocaSubject,
    root: &Path,
    session: &str,
    variant: &str,
) -> Result<SignedToolChangeObservation, HarnessError> {
    let fail = |what: &str| HarnessError::new(what);
    let bodies = || {
        subject
            .shared
            .raw_bodies
            .lock()
            .unwrap()
            .get(session)
            .cloned()
            .unwrap_or_default()
    };
    let live = subject.start(root).await?;
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await?;
    run_to_completion(&route, send(session, 1, Some(plan("before")))).await?;
    run_to_completion(&route, send(session, 2, None)).await?;
    let sent_before = bodies().len();
    let mut observation = SignedToolChangeObservation::default();
    observation.before = bodies()
        .last()
        .cloned()
        .ok_or_else(|| fail("the model received no request"))?;
    observation.signed_markers = vec![THINKING_MARKER.into(), SIGNATURE_MARKER.into()];
    observation.unsigned_marker = TEXT_MARKER.into();
    let refreshed = route
        .request(
            "session.refresh",
            json!({"plan": plan(variant), "generation": 1, "policy": "immediately"}),
        )
        .await
        .map_err(|e| HarnessError::new(format!("session.refresh: {e:?}")))?;
    if !matches!(refreshed, Reply::Response(_)) {
        return Err(HarnessError::new(format!(
            "session.refresh refused: {refreshed:?}"
        )));
    }
    run_to_completion(&route, send(session, 3, None)).await?;
    observation.after = bodies()
        .get(sent_before)
        .cloned()
        .ok_or_else(|| fail("the run after the refresh sent no request"))?;
    drop(route);
    drop(live);
    Ok(observation)
}

/// A send of the fixture's `n`th prompt, which names the session so the stand-in model can
/// tell whose request it is answering. Only the first carries the plan.
fn send(session: &str, n: u32, plan: Option<Value>) -> Value {
    let mut fields = json!({
        "prompt": format!("{SESSION_MARKER}{session} prompt {n}"),
        "send_id": format!("{session}-{n}"),
        "model": {"provider": output_refs::PROVIDER, "model": output_refs::MODEL},
    });
    if let Some(plan) = plan {
        fields["plan"] = plan;
    }
    fields
}

/// Sends `fields` until its run has an id: a send that arrives while the previous run is still
/// being released is queued and answers `pending` first. The send id keeps it to one run.
async fn started(route: &BrocaRoute, fields: Value) -> Result<Value, HarnessError> {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            match route.request("session.send", fields.clone()).await {
                Ok(Reply::Response(reply)) if reply["run_id"].is_string() => {
                    return Ok(reply["run_id"].clone())
                }
                Ok(Reply::Response(reply)) if reply["state"] == "pending" => {
                    sleep(Duration::from_millis(20)).await;
                }
                other => return Err(HarnessError::new(format!("session.send: {other:?}"))),
            }
        }
    })
    .await
    .map_err(|_| HarnessError::new("the send never started its run"))?
}

async fn run_to_completion(route: &BrocaRoute, fields: Value) -> Result<(), HarnessError> {
    let run_id = started(route, fields).await?;
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            match route.request("run.result", json!({"run_id": run_id})).await {
                Ok(Reply::Response(value)) if value["state"] == "completed" => return Ok(()),
                Ok(Reply::Response(value))
                    if matches!(value["state"].as_str(), Some("active" | "paused")) =>
                {
                    sleep(Duration::from_millis(20)).await;
                }
                other => return Err(HarnessError::new(format!("run.result: {other:?}"))),
            }
        }
    })
    .await
    .map_err(|_| HarnessError::new("the run did not complete"))?
}

/// The fixture on its own, as the kit reads it: the signed markers reach the request before
/// the change, an edit leaves them out of the next request and a reorder keeps them, and the
/// request after the change offers the changed tools.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_signed_floor_fixture_shows_what_each_tool_change_keeps() {
    assert!(
        sibling_bin("ck-subc", "SUBC_CORE_BIN").exists(),
        "real daemon required"
    );
    let scratch = broca_test_support::scratch_root()
        .join(format!("signed-floor-subject-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    let subject = BrocaSubject::new(&scratch).await;
    let has = |bytes: &[u8], marker: &str| {
        bytes
            .windows(marker.len())
            .any(|part| part == marker.as_bytes())
    };
    let tools = |bytes: &[u8]| -> Vec<(String, String)> {
        let body: Value = serde_json::from_slice(bytes).unwrap();
        body["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| {
                (
                    tool["name"].as_str().unwrap().to_owned(),
                    tool["description"].as_str().unwrap().to_owned(),
                )
            })
            .collect()
    };
    for (change, strips) in [
        (SignedToolChange::Edit, true),
        (SignedToolChange::Reorder, false),
    ] {
        let session = format!("signed-floor-{change:?}").to_lowercase();
        let observed = observe(&subject, &session, change)
            .await
            .unwrap()
            .expect("broca has this fixture");
        for marker in [THINKING_MARKER, SIGNATURE_MARKER] {
            assert!(
                has(&observed.before, marker),
                "{change:?}: {marker} not sent before"
            );
            assert_eq!(
                has(&observed.after, marker),
                !strips,
                "{change:?}: {marker} after the change"
            );
        }
        assert!(
            has(&observed.after, TEXT_MARKER),
            "{change:?}: answer text lost"
        );
        let (before, after) = (tools(&observed.before), tools(&observed.after));
        assert_ne!(before, after, "{change:?}: the tools did not change");
        if change == SignedToolChange::Reorder {
            assert_eq!(
                before.iter().rev().collect::<Vec<_>>(),
                after.iter().collect::<Vec<_>>()
            );
        }
    }
    for change in [
        SignedToolChange::Resume,
        SignedToolChange::DeferredAddition,
        SignedToolChange::ToolNote,
    ] {
        assert!(observe(&subject, "signed-floor-none", change)
            .await
            .unwrap()
            .is_none());
    }
    broca_test_support::remove_rig_dir(&scratch);
}
