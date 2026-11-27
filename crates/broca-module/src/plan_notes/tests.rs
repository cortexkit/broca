use super::*;
use broca_session_store::record::PlanNote;

mod additions;

fn manifest(tools: &[&str], system_text: &str) -> PlanManifest {
    serde_json::from_value(serde_json::json!({
        "plan": "{}",
        "plan_identity": "p",
        "composition_digest": "c",
        "fetches": [],
        "tools": tools.iter().map(|name| serde_json::json!({
            "name": name, "description": "", "input_schema": {"type": "object"},
            "module": "m", "execution_mode": "parallel"
        })).collect::<Vec<_>>(),
        "system_text": system_text,
        "system_digest": "s",
        "join_version": 1,
        "role_versions": {}
    }))
    .unwrap()
}

/// The applied plan offers `read` and `grep` under "old"; the pending one drops `grep`, adds
/// `write` and says "new".
fn facts<'a>(current: &'a PlanManifest, pending: &'a PlanManifest) -> Facts<'a> {
    Facts {
        current: Some(current),
        noted_system: None,
        pending,
        generation: 2,
        system_note: Some("Answer in French."),
        delivered: &[],
        rendering_anchors: None,
        redelivery_enabled: true,
        window_start: 1,
        pending_since_ms: Some(1_000),
        now_ms: 2_000,
        cache_lost: false,
        turn_system: true,
        tool_addition_form: None,
        scoped: false,
        model_key: "p:m".into(),
    }
}

fn delivered(anchor: u64, notes: &[(PlanNoteKind, &str)], refused: &[&str]) -> PlanNotes {
    PlanNotes {
        blocks: Some(true),
        generation: 1,
        anchor,
        notes: notes
            .iter()
            .map(|(kind, text)| PlanNote {
                kind: *kind,
                text: (*text).into(),
                form: NOTE_FORM_TURN_SYSTEM.into(),
                tool: None,
                tool_dispatch: None,
            })
            .collect(),
        refused_tools: refused.iter().map(|name| (*name).into()).collect(),
        model_key: "p:m".into(),
    }
}

fn texts(start: &AtStart) -> Vec<String> {
    match start {
        AtStart::Wait(Some(delivery)) => delivery.notes.iter().map(|n| n.text.clone()).collect(),
        other => panic!("not a delivery: {other:?}"),
    }
}

#[test]
fn a_waiting_change_delivers_the_system_note_and_each_removal_but_no_addition() {
    let (current, pending) = (
        manifest(&["read", "grep"], "old"),
        manifest(&["read", "write"], "new"),
    );
    let start = at_start(&facts(&current, &pending));
    assert_eq!(
        texts(&start),
        vec![
            "Answer in French.".to_owned(),
            broca_core::plan_notes::tool_removal("grep"),
        ]
    );
    let AtStart::Wait(Some(delivery)) = start else {
        unreachable!()
    };
    assert_eq!(delivery.refused_tools, vec!["grep".to_owned()]);
    assert_eq!(delivery.generation, 2);
}

#[test]
fn a_cache_loss_or_six_hours_apply_the_change_in_full() {
    let (current, pending) = (manifest(&["read"], "old"), manifest(&["read"], "new"));
    let mut lost = facts(&current, &pending);
    lost.cache_lost = true;
    assert_eq!(at_start(&lost), AtStart::Apply { reason: None });
    let mut aged = facts(&current, &pending);
    aged.now_ms = 1_000 + APPLY_AFTER_MS;
    assert_eq!(at_start(&aged), AtStart::Apply { reason: None });
    aged.now_ms -= 1;
    assert!(matches!(at_start(&aged), AtStart::Wait(Some(_))));
}

#[test]
fn a_system_change_without_a_form_applies_in_full_and_a_tools_change_waits_gated() {
    let current = manifest(&["read", "grep"], "old");
    let mut no_form = facts(&current, &current);
    no_form.turn_system = false;
    let system = manifest(&["read", "grep"], "new");
    no_form.pending = &system;
    assert_eq!(
        at_start(&no_form),
        AtStart::Apply {
            reason: Some("no_note_form")
        }
    );
    // Tools only: nothing can be said, but the removed tool is refused from this run on.
    let tools = manifest(&["read"], "old");
    no_form.pending = &tools;
    no_form.system_note = None;
    let AtStart::Wait(Some(delivery)) = at_start(&no_form) else {
        panic!("a tools-only change waits");
    };
    assert!(delivery.notes.is_empty());
    assert_eq!(delivery.refused_tools, vec!["grep".to_owned()]);
}

#[test]
fn a_rendering_note_is_not_delivered_again_and_a_sealed_one_is() {
    let (current, pending) = (
        manifest(&["read", "grep"], "old"),
        manifest(&["read"], "new"),
    );
    let removal = broca_core::plan_notes::tool_removal("grep");
    let earlier = [delivered(
        4,
        &[
            (PlanNoteKind::System, "Answer in French."),
            (PlanNoteKind::ToolRemoval, &removal),
        ],
        &["grep"],
    )];
    let mut again = facts(&current, &pending);
    again.delivered = &earlier;
    again.noted_system = Some(&pending);
    assert_eq!(at_start(&again), AtStart::Wait(None));
    // A cut sealed the history holding the anchor: the notes are delivered afresh.
    again.window_start = 5;
    assert_eq!(
        texts(&at_start(&again)),
        vec!["Answer in French.".to_owned(), removal]
    );
    // A new owner note on an unchanged noted surface is ignored.
    again.window_start = 1;
    again.system_note = Some("Answer in German.");
    assert_eq!(at_start(&again), AtStart::Wait(None));
    // A correction returning to the applied surface is a new note, not an early apply.
    again.pending = &current;
    assert_eq!(
        texts(&at_start(&again)),
        vec!["Answer in German.".to_owned()]
    );
    again.turn_system = false;
    assert_eq!(at_start(&again), AtStart::Wait(None));
}
