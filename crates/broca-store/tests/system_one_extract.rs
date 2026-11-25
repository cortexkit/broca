use broca_store::system_one::{
    BatchClaim, BatchScope, Claimed, Outcome, StoredOutcome, RETENTION_MS,
};
use broca_store::SystemOneStore;

#[test]
fn extractor_and_classifier_batches_with_one_id_are_isolated_through_retention() {
    let store = SystemOneStore::open_in_memory().unwrap();
    let scope = BatchScope::Flow {
        flow_id: "sample".into(),
    };
    let id = "same-id";
    let owner = "test-process";
    let claim = |role, model, now_ms| {
        store
            .claim(&BatchClaim {
                role,
                batch_id: id,
                model,
                identity: role,
                body_digest: role,
                frozen_row: "null",
                scope: &scope,
                ceiling_nanos: None,
                owner,
                now_ms,
            })
            .unwrap()
    };
    assert_eq!(
        claim(
            "extractor",
            "cloudflare-workers-ai/@cf/cloudflare/clairic",
            1
        ),
        Claimed::Fresh
    );
    assert_eq!(
        claim("classifier", "cloudflare-workers-ai/@cf/cloudflare/clef", 2),
        Claimed::Fresh
    );

    for (role, body, now) in [
        ("extractor", "extracted value", 1),
        ("classifier", "decision", 2),
    ] {
        let outcome = StoredOutcome {
            index: 0,
            outcome: Outcome::Answered(body.into()),
            input_tokens: Some(10),
            output_tokens: None,
        };
        assert!(store.record(role, id, owner, &outcome, 5, now).unwrap());
        store.release(role, id, owner).unwrap();
        assert_eq!(store.outcomes(role, id).unwrap(), [outcome]);
    }
    let swept = store.sweep(RETENTION_MS + 1).unwrap();
    assert_eq!((swept.batches, swept.outcomes), (1, 1));
    assert!(store.lookup("extractor", id).unwrap().is_none());
    assert!(store.outcomes("extractor", id).unwrap().is_empty());
    let classifier = store.lookup("classifier", id).unwrap().unwrap();
    assert_eq!(
        classifier.outcomes[0].outcome,
        Outcome::Answered("decision".into())
    );
    assert_eq!(classifier.spent_nanos, 5);
}
