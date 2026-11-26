use super::*;
use crate::serve::ServeHandler;
use cortexkit_role_llm_runner::fork_answer::ForkPrepareReply;

const FORK_CALLER: &str = "colliculus";

pub(super) struct Capture {
    pub(super) sent: Sent,
    pub(super) age_early: bool,
}

fn caller() -> broca_protocol::BindPrincipal {
    broca_protocol::BindPrincipal::Reserved {
        module_id: FORK_CALLER.into(),
    }
}

fn cut_off_step(id: &str) -> Vec<NormalizedEvent> {
    let mut step = text_step(id, "part of an answer");
    let Some(NormalizedEvent::Control(ProviderControl::Finished { finish_reason, .. })) =
        step.last_mut()
    else {
        panic!("the scripted answer finishes")
    };
    *finish_reason = FinishReason::Length;
    step
}

fn fork_plan(tool_image: bool) -> Arc<broca_records::record::PlanManifest> {
    let mut plan = super::super::controls::manifest(
        json!({ "fork_callers": [FORK_CALLER], "render_tags": true }),
        vec![],
    );
    let tools = if tool_image {
        with_photo_tool("look").tools.unwrap()
    } else {
        vec![]
    };
    Arc::make_mut(&mut plan).tools = Arc::new(tools);
    plan
}

async fn media_head(tool_image: bool, age_early: bool) -> (Rendered, Sent) {
    let mut steps = vec![cut_off_step("reply")];
    if tool_image {
        steps.insert(0, photo_step("tool-call"));
    } else {
        steps.insert(0, text_step("first", "ready"));
    }
    if age_early {
        steps.push(cut_off_step("later"));
    }
    let mut head = Rendered::new(true, vec![], false, Cipher::None).await;
    let fork_sent = Sent::default();
    head.side.registry = registry(
        &head.side.dir,
        Some(Capture {
            sent: fork_sent.clone(),
            age_early,
        }),
        steps,
        None,
        &head.sent,
        None,
        head.side.cipher,
    )
    .await;
    let plan = fork_plan(tool_image);
    let request = if tool_image {
        text("show me the screen")
    } else {
        let outcome = send_planned(&head, "setup", planned(text("hello")), &plan).await;
        assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
        head.side.idle().await;
        let upload = head.side.uploaded(&bytes_of(PNG)).await;
        photo("look at this", &[&upload])
    };
    let outcome = send_planned(&head, "r1", planned(request), &plan).await;
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    head.side.idle().await;
    let bytes = head.sent().pop().unwrap().unwrap();
    assert!(
        String::from_utf8_lossy(&bytes).contains(&base64(&bytes_of(PNG))),
        "the head sent the image"
    );
    (head, fork_sent)
}

async fn prepare(head: &Rendered) -> Result<ForkPrepareReply, crate::fork_answer::ForkError> {
    head.side
        .registry
        .fork_prepare(&head.side.key, Some(&caller()))
        .await
}

async fn check_fork_base(tool_image: bool) {
    let (head, fork_sent) = media_head(tool_image, false).await;
    let fork = prepare(&head)
        .await
        .unwrap_or_else(|error| panic!("{}: {}", error.code(), error.message()));
    ask(&head, fork).await;
    let sent = fork_sent.lock().unwrap();
    assert_eq!(
        sent[0],
        *head.sent().last().unwrap(),
        "the fork's base is the head's request byte for byte"
    );
    let request = String::from_utf8_lossy(sent[1].as_ref().unwrap());
    assert!(request.contains(&base64(&bytes_of(PNG))));
    assert!(request.contains("What do you see?"));
}

async fn ask(head: &Rendered, fork: ForkPrepareReply) {
    let ask = serde_json::from_value(
        json!({"fork_id": fork.fork_id, "ask_id": "ask", "question": "What do you see?"}),
    )
    .unwrap();
    head.side
        .registry
        .fork_ask(&head.side.key, Some(&caller()), ask)
        .await
        .unwrap();
}

fn remove_media(head: &Rendered) {
    let mut objects = Vec::new();
    media_files(&head.side.dir, &mut objects);
    assert!(!objects.is_empty(), "the head's image is stored");
    for object in objects {
        std::fs::remove_file(object).unwrap();
    }
}

#[tokio::test]
async fn a_fork_rebuilds_a_user_image_byte_for_byte() {
    check_fork_base(false).await;
}

#[tokio::test]
async fn a_fork_rebuilds_a_tool_result_image_byte_for_byte() {
    check_fork_base(true).await;
}

#[tokio::test]
async fn a_fork_preserves_its_base_when_the_head_ages_an_image() {
    let (head, fork_sent) = media_head(false, true).await;
    let young = head.sent().pop().unwrap();
    let before_aging = prepare(&head).await.unwrap();
    let outcome = send_planned(&head, "r2", planned(text("and now?")), &fork_plan(false)).await;
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    head.side.idle().await;
    let aged = head.sent().pop().unwrap();
    let aged_text = String::from_utf8_lossy(aged.as_ref().unwrap());
    assert!(
        aged_text.contains("reason=aged"),
        "the head sent the aged marker: {aged_text}"
    );
    assert!(!aged_text.contains(&base64(&bytes_of(PNG))));

    ask(&head, before_aging).await;
    assert_eq!(fork_sent.lock().unwrap()[0], young);
    // An aged marker has no payload, so it can still be reproduced without the object.
    remove_media(&head);
    let after_aging = prepare(&head).await.unwrap();
    ask(&head, after_aging).await;
    assert_eq!(fork_sent.lock().unwrap()[2], aged);
}

#[tokio::test]
async fn a_fork_refuses_when_the_base_needs_a_missing_media_object() {
    let (head, fork_sent) = media_head(false, false).await;
    remove_media(&head);
    let error = prepare(&head).await.unwrap_err();
    assert_eq!(error.code(), "fork_base_unavailable");
    assert!(
        error.message().contains("media_object_missing"),
        "{}",
        error.message()
    );
    assert!(fork_sent.lock().unwrap().is_empty());
}
