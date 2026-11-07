use broca_module_serve::account_routing::{
    account_for_params, AccountForRequest, Candidate, Candidates,
};
use broca_protocol::request::AccountRoute;

#[test]
fn account_route_body_uses_frozen_names_after_reload_and_readmission() {
    let candidates = Candidates {
        view_digest: "view".into(),
        accounts: vec![Candidate {
            account_id: "account".into(),
            credential_id: "credential".into(),
        }],
    };
    let mut request = AccountForRequest {
        account_route: None,
        pool: None,
        credential_id: None,
        decision_id: None,
        service_tier: None,
        provider: "anthropic",
        model: "fixture",
        conversation_key: "conversation",
        admission_id: "run",
        initial_input_bytes: 5,
        candidates: &candidates,
        hold_cap: Default::default(),
    };
    assert_eq!(
        serde_json::to_vec(&account_for_params(&request)).unwrap(),
        include_bytes!("fixtures/auth-hints/account-for.json")
    );
    let route = AccountRoute::new("routing-provider", "routing-model");
    request.account_route = Some(&route);
    let admitted = account_for_params(&request);
    assert_eq!(admitted["provider"], "routing-provider");
    assert_eq!(admitted["model"], "routing-model");
    assert_eq!(admitted["affinity_model"], "routing-model");
    let mut record: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/auth-hints/run-started.json")).unwrap();
    record["config"]["account_route"] = serde_json::to_value(&route).unwrap();
    let broca_wal::WalRecord::RunStarted { config, .. } = serde_json::from_value(record).unwrap()
    else {
        panic!("run fixture")
    };
    request.account_route = config.account_route.as_ref();
    request.provider = "changed-render-provider";
    request.model = "changed-render-model";
    assert_eq!(account_for_params(&request), admitted);
    request.admission_id = "readmission";
    let readmitted = account_for_params(&request);
    for field in ["provider", "model", "affinity_model"] {
        assert_eq!(readmitted[field], admitted[field]);
    }
    assert_eq!(config.model.provider_module_id, "p");
    assert_eq!(config.model.model_id, "m");
}
