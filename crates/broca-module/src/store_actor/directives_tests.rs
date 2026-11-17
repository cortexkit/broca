//! Turn directives on `run.status` and `run.result`: a run whose send asked for `wake` reports
//! the directive its final answer ends with.

use cortexkit_role_llm_runner::directives::DirectiveName;

use super::*;

impl Side {
    /// Sends `text` asking for the `wake` directive when `wake`.
    async fn send_with_directives(&self, run_id: &str, text: &str, wake: bool) -> SendOutcome {
        let key = SendKey::Minted;
        let mut request = self.caller.shape(run_id, &key, request(text));
        request.directives = wake.then(|| vec![DirectiveName::Wake]);
        self.registry
            .route_submission_with_order(
                &self.key,
                RunId(run_id.into()),
                request,
                RunKind::FreshOrResume,
                key,
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
}

/// Every published directive vector parses to its expected directive, or to none.
#[test]
fn wake_directives_read_every_published_vector() {
    use cortexkit_role_llm_runner::vectors;
    let mut read = 0;
    for (name, raw) in vectors::FILES
        .iter()
        .filter(|(name, _)| name.starts_with("directives/") && !name.ends_with("wire.json"))
    {
        let case: Value = serde_json::from_str(raw).unwrap();
        let expected = if case["expected"].is_null() {
            Value::Null
        } else {
            json!([case["expected"].clone()])
        };
        let parsed = crate::serve::wake_directives(case["text"].as_str().unwrap());
        assert_eq!(serde_json::to_value(parsed).unwrap(), expected, "{name}");
        read += 1;
    }
    assert!(read > 10, "only {read} directive vectors were read");
}

/// The answer a `wake` run gives is pinned on its own, so a change to the published vectors
/// can't silently change what an existing run's reply says.
#[test]
fn a_wake_answer_is_pinned_independently_of_the_vectors() {
    assert_eq!(
        serde_json::to_string(&crate::serve::wake_directives(
            "answer<wake in=\"000000001m\"> café </wake>"
        ))
        .unwrap(),
        r#"[{"name":"wake","in_ms":60000,"note":"café"}]"#
    );
}

/// A run that asked for `wake` reports the directive its answer ends with on `run.status` and
/// `run.result`, read after its actor closed. A run that didn't ask reports none, and neither
/// does a run whose answer has no text.
#[tokio::test]
async fn a_run_that_asked_for_wake_reports_its_directive() {
    let steps = vec![
        text_step("a1", "done <wake in=\"90s\"/>"),
        text_step("a2", "done <wake in=\"90s\"/>"),
        text_step("a3", ""),
    ];
    let store = side(steps, None).await;
    for (run, wake) in [("asked", true), ("unasked", false), ("textless", true)] {
        assert!(matches!(
            store.send_with_directives(run, "go", wake).await,
            SendOutcome::Active { .. }
        ));
        store.closed().await;
    }
    // Read unmasked: the test replies mask every `_ms` member, and `in_ms` is a duration.
    let unmasked = |reply: Result<_, crate::serve::ServeError>| -> Value {
        serde_json::to_value(reply.expect("the run is read")).unwrap()
    };
    let expected = json!([{"name": "wake", "in_ms": 90000}]);
    let live = unmasked(
        store
            .registry
            .run_result(&store.key, RunId("asked".into()))
            .await,
    );
    assert_eq!(live["directives"], expected, "{live}");
    let status = store
        .registry
        .run_status_reply(&store.key, RunId("asked".into()))
        .await
        .map(|reply| serde_json::to_value(reply).unwrap());
    assert_eq!(status.unwrap()["directives"], expected);
    for run in ["unasked", "textless"] {
        let status = store.run_status(run).await;
        assert_eq!(status["state"], "completed", "{run}: {status}");
        assert!(status.get("directives").is_none(), "{run}: {status}");
        assert!(
            store.run_result(run).await.get("directives").is_none(),
            "{run}"
        );
    }
}
