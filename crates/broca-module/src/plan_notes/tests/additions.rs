use super::*;
use broca_core::capability::AddedToolDispatch;
use broca_session_store::record::PlanNoteTool;

fn proposed<'a>(old: &'a PlanManifest, pending: &'a PlanManifest) -> Facts<'a> {
    let mut facts = facts(old, pending);
    facts.tool_addition_form = Some(ToolAdditionForm::ResponsesAdditionalTools);
    facts.system_note = None;
    facts
}

fn addition(start: AtStart) -> DeliveredNote {
    let AtStart::Wait(Some(delivery)) = start else {
        panic!("no delivery: {start:?}")
    };
    let [note]: [DeliveredNote; 1] = delivery.notes.try_into().expect("one note");
    assert_eq!(note.kind, NoteKind::ToolAddition);
    note
}

fn recorded(note: &DeliveredNote) -> PlanNotes {
    let mut delivery = delivered(3, &[], &[]);
    let tool = note.tool.as_ref().unwrap();
    delivery.notes.push(PlanNote {
        kind: PlanNoteKind::ToolAddition,
        text: note.text.clone(),
        form: note.form.clone(),
        tool: Some(PlanNoteTool {
            name: tool.name.clone(),
            description: tool.description.clone(),
            input_schema: tool.input_schema.text().to_owned(),
        }),
        tool_dispatch: note.tool_dispatch.clone(),
    });
    delivery
}

#[test]
fn an_exact_tool_addition_is_only_repeated_if_its_anchor_is_cut() {
    let applied = manifest(&["read"], "same");
    let pending = manifest(&["read", "write"], "same");
    let mut facts = proposed(&applied, &pending);
    let note = addition(at_start(&facts));
    assert_eq!(note.form, "responses_additional_tools");
    assert_eq!(note.text, "");
    let definition = note.tool.as_ref().unwrap();
    assert_eq!(definition.name, "write");
    assert_eq!(definition.input_schema.text(), r#"{"type":"object"}"#);

    facts.tool_addition_form = None;
    assert_eq!(at_start(&facts), AtStart::Wait(None));
    facts.tool_addition_form = Some(ToolAdditionForm::ResponsesAdditionalTools);
    let earlier = [recorded(&note)];
    facts.delivered = &earlier;
    assert_eq!(at_start(&facts), AtStart::Wait(None));
    facts.window_start = 4;
    assert_eq!(addition(at_start(&facts)), note);
}

#[test]
fn removal_after_an_inline_addition_refuses_the_tool_without_redeclaring_it() {
    let applied = manifest(&["read"], "same");
    let with_write = manifest(&["read", "write"], "same");
    let mut facts = proposed(&applied, &with_write);
    let previously_added = [recorded(&addition(at_start(&facts)))];
    facts.delivered = &previously_added;
    facts.pending = &applied;
    let AtStart::Wait(Some(removal)) = at_start(&facts) else {
        panic!("the previously offered name must be removed")
    };
    assert_eq!(removal.refused_tools, ["write"]);
    assert_eq!(removal.notes.len(), 1);
    assert_eq!(removal.notes[0].kind, NoteKind::ToolRemoval);
    assert_eq!(
        removal.notes[0].text,
        broca_core::plan_notes::tool_removal("write")
    );

    let removed_then_added_back = [
        previously_added[0].clone(),
        delivered(
            4,
            &[(PlanNoteKind::ToolRemoval, &removal.notes[0].text)],
            &["write"],
        ),
    ];
    facts.delivered = &removed_then_added_back;
    facts.pending = &with_write;
    assert_eq!(at_start(&facts), AtStart::Wait(None));
}

#[test]
fn changed_definitions_under_applied_or_inline_names_wait_for_full_apply() {
    let applied = manifest(&["read"], "same");
    let original = manifest(&["read", "write"], "same");
    let mut facts = proposed(&applied, &original);
    let old_note = addition(at_start(&facts));
    let earlier = [recorded(&old_note)];
    facts.delivered = &earlier;
    let mut changed = original.clone();
    for tool in std::sync::Arc::make_mut(&mut changed.tools) {
        tool.description = "a different declaration".into();
    }
    facts.pending = &changed;
    assert_eq!(at_start(&facts), AtStart::Wait(None));
    facts.window_start = 4;
    assert_eq!(at_start(&facts), AtStart::Wait(None));
}

#[test]
fn additions_merge_with_identical_provider_transport_but_wait_if_the_preset_changes() {
    fn preset(plan: &mut PlanManifest, name: &str) {
        plan.plan = broca_records::record::OpaqueJson::from_json_text(
            serde_json::json!({"tool_items":[{"provider":"m","preset":name}]}).to_string(),
        )
        .unwrap();
    }
    let mut applied = manifest(&["read"], "same");
    let mut pending = manifest(&["read", "write"], "same");
    preset(&mut applied, "work");
    preset(&mut pending, "work");
    let candidate = addition(at_start(&proposed(&applied, &pending)));
    let frozen: AddedToolDispatch =
        serde_json::from_str(candidate.tool_dispatch.as_deref().unwrap()).unwrap();
    assert_eq!(
        frozen.provider.as_ref().unwrap().preset.as_deref(),
        Some("work")
    );
    let mut manifest =
        broca_core::capability::ToolManifest::from_shared_tools(applied.tools.clone())
            .with_plan_manifest(&applied)
            .unwrap();
    manifest.add_inline_tool(candidate.tool.unwrap(), frozen);
    assert_eq!(manifest.provider_preset("m"), Some("work"));
    assert_eq!(manifest.get("read").unwrap().module.as_deref(), Some("m"));
    assert_eq!(manifest.get("write").unwrap().module.as_deref(), Some("m"));
    preset(&mut pending, "another");
    assert_eq!(at_start(&proposed(&applied, &pending)), AtStart::Wait(None));
}

#[test]
fn a_cut_declaration_removed_from_the_pending_plan_only_gets_a_removal_note() {
    let applied = manifest(&["read"], "same");
    let pending = manifest(&["read", "write"], "same");
    let mut facts = proposed(&applied, &pending);
    let old = [recorded(&addition(at_start(&facts)))];
    facts.delivered = &old;
    facts.pending = &applied;
    facts.window_start = 4;
    let AtStart::Wait(Some(delivery)) = at_start(&facts) else {
        panic!("the name was removed")
    };
    assert_eq!(
        delivery
            .notes
            .iter()
            .map(|note| note.kind)
            .collect::<Vec<_>>(),
        [NoteKind::ToolRemoval]
    );
    assert_eq!(delivery.refused_tools, ["write"]);
}
