use super::*;
use broca_protocol::CallOptions;

fn negotiated_setup(view: Option<&CompactionView>) -> WalRecord {
    let mut record = setup_record(None);
    let WalRecord::CompactionMessage {
        view_features,
        view_due,
        ..
    } = &mut record
    else {
        unreachable!()
    };
    *view_features = Some(vec!["truncate".into(), "tag_bytes_edits".into()]);
    *view_due = view.map(|view| broca_wal::SetupViewDue {
        version: view.view_version,
        digest: view.digest.clone(),
    });
    record
}

fn tagged_prompt() -> (Vec<Message>, Vec<Message>) {
    let mut stored = stamped(vec![Message::new(
        Role::User,
        vec![
            ContentBlock::Text {
                text: "first".into(),
            },
            ContentBlock::Text {
                text: "second".into(),
            },
        ],
    )]);
    broca_protocol::tags::number_message(
        &mut stored[0],
        broca_protocol::tags::ComputedRules {
            render: true,
            clean: true,
        },
        &mut 16,
    )
    .unwrap();
    let mut prompt = stored.clone();
    broca_protocol::tags::render_tags_into(
        &mut prompt,
        broca_protocol::tags::TagRendering::default(),
    );
    (stored, prompt)
}

#[test]
fn negotiated_replace_ranges_remain_final_and_never_inherit_part_tags() {
    let (_, prompt) = tagged_prompt();
    let (view, _) = make_view(
        2,
        0,
        vec![ViewRange::replace(
            0,
            1,
            r#"[{"role":"user","content":[{"type":"text","text":"provider-final"}]}]"#,
        )],
    );
    let mut state = CompactionState::default();
    state.fold(&negotiated_setup(None));
    state.fold(&WalRecord::CompactionViewApplied {
        provider: "provider".into(),
        view_version: 2,
        view: broca_wal::OpaqueJson::from_json_text(serde_json::to_string(&view).unwrap()).unwrap(),
        choice: CompactionViewChoice::Applied,
    });
    let rendered = render(&CallOptions::new(prompt), &state, &[])
        .unwrap()
        .unwrap();
    assert_eq!(
        rendered.prompt[0].content,
        vec![ContentBlock::Text {
            text: "provider-final".into()
        }]
    );
}
