//! Scripted optional account continuation for the routing stand-in.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
};

use broca_module_serve::routing_capabilities::ACCOUNT_CONTINUE_RESUME;
use cortexkit_role_routing::{
    account_continue::{parse_continue_request, ContinueAnswer, ContinueOffer, ContinueRequest},
    capabilities::ACCOUNT_CONTINUE,
    describe::RoleDescribe,
};
use serde_json::Value;

#[derive(Clone)]
pub enum ContinuePlan {
    Answer(ContinueAnswer),
    ErrorWithDetail {
        code: &'static str,
        message: &'static str,
        detail: Value,
    },
    NoAnswer,
}

#[derive(Default)]
struct Scripts {
    by_selection: HashMap<String, ContinuePlan>,
    next: VecDeque<ContinuePlan>,
    questions: HashMap<String, ContinueRequest>,
    offers: Vec<(ContinueRequest, ContinueOffer)>,
    used_offers: HashMap<String, ContinueOffer>,
}

#[derive(Default)]
struct Reports {
    bodies: Vec<Vec<u8>>,
    continue_at: Option<usize>,
}

#[derive(Default)]
pub struct Continuations {
    declared: AtomicBool,
    resume_declared: AtomicBool,
    success_declared: AtomicBool,
    scripts: Mutex<Scripts>,
    requests: Mutex<Vec<Vec<u8>>>,
    reports: Mutex<Reports>,
    malformed: Mutex<Vec<String>>,
}

impl Continuations {
    pub fn declare(&self, declared: bool) {
        self.declared.store(declared, Ordering::SeqCst);
    }

    pub fn declare_resume(&self, declared: bool) {
        self.resume_declared.store(declared, Ordering::SeqCst);
    }

    /// Declare `account_observe_success`, so broca's served attempts carry a typed success.
    pub fn declare_success(&self, declared: bool) {
        self.success_declared.store(declared, Ordering::SeqCst);
    }

    pub fn describe(&self) -> RoleDescribe {
        let mut describe = RoleDescribe::new(vec![], false);
        if self.declared.load(Ordering::SeqCst) {
            describe = describe.with_capability(ACCOUNT_CONTINUE);
        }
        if self.resume_declared.load(Ordering::SeqCst) {
            describe = describe.with_capability(ACCOUNT_CONTINUE_RESUME);
        }
        if self.success_declared.load(Ordering::SeqCst) {
            describe = describe
                .with_capability(cortexkit_role_routing::capabilities::ACCOUNT_OBSERVE_SUCCESS);
        }
        describe
    }

    pub fn script(&self, selection_id: &str, plan: ContinuePlan) {
        self.scripts
            .lock()
            .unwrap()
            .by_selection
            .insert(selection_id.into(), plan);
    }

    /// Bind the next fresh selection id to this answer. Transport retries replay that answer.
    pub fn script_next(&self, plan: ContinuePlan) {
        self.scripts.lock().unwrap().next.push_back(plan);
    }

    pub fn receive(&self, body: Vec<u8>) -> ContinuePlan {
        self.requests.lock().unwrap().push(body.clone());
        {
            let mut reports = self.reports.lock().unwrap();
            let index = reports.bodies.len();
            reports.continue_at.get_or_insert(index);
        }
        let envelope: Value = serde_json::from_slice(&body).expect("continue request JSON");
        let request =
            parse_continue_request(&envelope["params"], &self.describe()).unwrap_or_else(|error| {
                let message = format!("malformed continue request from broca: {error:?}");
                self.malformed.lock().unwrap().push(message.clone());
                panic!("{message}");
            });
        let mut scripts = self.scripts.lock().unwrap();
        let fresh = !scripts.questions.contains_key(&request.selection_id);
        if let Some(previous) = scripts.questions.get(&request.selection_id) {
            assert!(
                previous.same_question(&request),
                "selection id reused for a changed question"
            );
        }
        let key = request.selection_id.clone();
        scripts.questions.insert(key.clone(), request.clone());
        if !scripts.by_selection.contains_key(&key) {
            let plan = scripts
                .next
                .pop_front()
                .expect("unscripted continue selection id");
            scripts.by_selection.insert(key.clone(), plan);
        }
        let plan = scripts.by_selection[&key].clone();
        if fresh {
            if let ContinuePlan::Answer(ContinueAnswer::Offer(offer)) = &plan {
                scripts.offers.push((request, offer.clone()));
            }
        }
        plan
    }

    pub fn record_report(&self, body: Vec<u8>) {
        self.reports.lock().unwrap().bodies.push(body.clone());
        let request: Value = serde_json::from_slice(&body).expect("report JSON");
        if request["method"] == "route.account_observe"
            && request["params"]["kind"] == "admission_started"
        {
            let params = &request["params"];
            let mut scripts = self.scripts.lock().unwrap();
            let used = scripts
                .offers
                .iter()
                .rev()
                .find(|(question, offer)| {
                    params["admission_id"] == question.admission_id
                        && params["run_id"] == question.run_id
                        && params["credential_id"] == offer.credential_id
                        && params["served_account_id"] == offer.account
                })
                .map(|(_, offer)| offer.clone());
            if let Some(offer) = used {
                scripts
                    .used_offers
                    .insert(params["admission_id"].as_str().unwrap().into(), offer);
            }
        }
    }

    pub fn admit_replay(&self, admission_id: &str, original: &[u8]) -> Vec<u8> {
        if !self.declared.load(Ordering::SeqCst) || !self.resume_declared.load(Ordering::SeqCst) {
            return original.to_vec();
        }
        let scripts = self.scripts.lock().unwrap();
        let Some(offer) = scripts.used_offers.get(admission_id) else {
            return original.to_vec();
        };
        let mut reply: Value = serde_json::from_slice(original).expect("stored admission JSON");
        reply["result"]["account"]["credential_id"] = Value::String(offer.credential_id.clone());
        reply["result"]["account"]["account_id"] = Value::String(offer.account.clone());
        reply["result"]["reason"]["decision_id"] = Value::String(offer.decision_id.clone());
        serde_json::to_vec(&reply).unwrap()
    }

    pub async fn handle(&self, body: Vec<u8>) -> subc_client_rs::HandlerOutcome {
        match self.receive(body) {
            ContinuePlan::Answer(answer) => subc_client_rs::HandlerOutcome::Response(
                serde_json::to_vec(&serde_json::json!({"result": answer})).unwrap(),
            ),
            ContinuePlan::ErrorWithDetail {
                code,
                message,
                detail,
            } => subc_client_rs::HandlerOutcome::ErrorWithDetail {
                code: code.into(),
                message: message.into(),
                detail,
            },
            ContinuePlan::NoAnswer => std::future::pending().await,
        }
    }

    pub fn requests(&self) -> Vec<Vec<u8>> {
        let malformed = self.malformed.lock().unwrap();
        assert!(
            malformed.is_empty(),
            "malformed continue requests: {malformed:?}"
        );
        self.requests.lock().unwrap().clone()
    }

    /// Raw observations and decision reports in arrival order, including the failed attempt.
    pub fn reports(&self) -> Vec<Vec<u8>> {
        self.reports.lock().unwrap().bodies.clone()
    }

    pub fn reports_after_continue(&self) -> Vec<Vec<u8>> {
        let reports = self.reports.lock().unwrap();
        reports
            .continue_at
            .map_or_else(Vec::new, |start| reports.bodies[start..].to_vec())
    }

    pub fn started(&self) -> bool {
        self.reports.lock().unwrap().continue_at.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cortexkit_role_routing::account_continue::{
        ContinueHold, ContinueOffer, ContinueRefusal, ExhaustedAccount,
    };
    use serde_json::json;

    fn request(selection_id: &str) -> Vec<u8> {
        let json = serde_json::to_vec(&json!({"method": "route.account_for", "params": ContinueRequest::new(
            "run1", "admission1", "conversation1", "decision1",
            ExhaustedAccount::new("oauth:anthropic:acct1", "acct1", "seven_day", 100), selection_id,
        )})).unwrap();
        [b" \n".as_slice(), &json, b"\n"].concat()
    }

    #[test]
    fn continue_standin_declares_only_when_enabled() {
        let routing = Continuations::default();
        assert!(!routing.describe().serves(ACCOUNT_CONTINUE));
        assert!(!routing.describe().serves(ACCOUNT_CONTINUE_RESUME));
        routing.declare(true);
        assert!(routing.describe().serves(ACCOUNT_CONTINUE));
        assert!(!routing.describe().serves(ACCOUNT_CONTINUE_RESUME));
        routing.declare_resume(true);
        assert!(routing.describe().serves(ACCOUNT_CONTINUE_RESUME));
        routing.declare_resume(false);
        assert!(!routing.describe().serves(ACCOUNT_CONTINUE_RESUME));
        routing.declare(false);
        assert!(!routing.describe().serves(ACCOUNT_CONTINUE));
    }

    #[tokio::test]
    async fn continue_standin_scripts_all_answers_by_selection_and_preserves_bytes() {
        let routing = Continuations::default();
        routing.declare(true);
        let initial = br#"{"method":"route.account_observe","params":{"kind":"admission_started","admission_id":"admission1"}}"#.to_vec();
        routing.record_report(initial.clone());
        assert!(!routing.started());
        assert!(routing.reports_after_continue().is_empty());
        let answers = [
            ContinueAnswer::Offer(ContinueOffer::new(
                "oauth:anthropic:acct2",
                "acct2",
                "decision2",
                200,
            )),
            ContinueAnswer::Hold(ContinueHold::new("no_capacity", json!({})).with_until_ms(300)),
            ContinueAnswer::Refuse(ContinueRefusal::new(
                "account_pinned",
                json!({"account":"acct1"}),
            )),
        ];
        for (id, answer) in ["selection1", "selection2", "selection3"]
            .into_iter()
            .zip(&answers)
        {
            routing.script(id, ContinuePlan::Answer(answer.clone()));
        }
        let mut received = Vec::new();
        for id in ["selection3", "selection1", "selection2", "selection1"] {
            let body = request(id);
            received.push(body.clone());
            let subc_client_rs::HandlerOutcome::Response(bytes) = routing.handle(body).await else {
                panic!("expected a scripted answer");
            };
            let response: Value = serde_json::from_slice(&bytes).unwrap();
            let answer: ContinueAnswer =
                serde_json::from_value(response["result"].clone()).unwrap();
            assert_eq!(
                answer,
                answers[match id {
                    "selection1" => 0,
                    "selection2" => 1,
                    _ => 2,
                }]
            );
        }
        assert_eq!(routing.requests(), received);
        let report = br#"{ "method":"route.report", "params":{"decision_id":"decision2","outcome":"completed"}}"#.to_vec();
        routing.record_report(report.clone());
        let observation = br#"{ "method":"route.account_observe", "params":{"admission_id":"admission1","credential_id":"oauth:anthropic:acct2","served_account_id":"acct2","kind":"attempt"}}"#.to_vec();
        routing.record_report(observation.clone());
        assert!(routing.started());
        assert_eq!(
            routing.reports(),
            vec![initial, report.clone(), observation.clone()]
        );
        assert_eq!(routing.reports_after_continue(), vec![report, observation]);
    }

    #[tokio::test]
    async fn continue_standin_binds_next_answer_and_replays_on_transport_retry() {
        let routing = Continuations::default();
        routing.declare(true);
        routing.script_next(ContinuePlan::Answer(ContinueAnswer::Hold(
            ContinueHold::new("no_capacity", json!({})),
        )));
        for _ in 0..2 {
            let ContinuePlan::Answer(ContinueAnswer::Hold(_)) =
                routing.receive(request("selection1"))
            else {
                panic!("transport retry must replay the first answer");
            };
        }
    }

    #[tokio::test(start_paused = true)]
    async fn continue_standin_no_answer_stays_pending() {
        let routing = Continuations::default();
        routing.declare(true);
        routing.script("selection1", ContinuePlan::NoAnswer);
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(10),
            routing.handle(request("selection1"))
        )
        .await
        .is_err());
        assert_eq!(routing.requests(), vec![request("selection1")]);
    }

    #[test]
    #[should_panic(expected = "malformed continue request from broca")]
    fn continue_standin_contract_parser_refuses_malformed_requests() {
        let routing = Continuations::default();
        routing.declare(true);
        routing.receive(br#"{"method":"route.account_for","params":{"mode":"continue"}}"#.to_vec());
    }

    #[test]
    #[should_panic(expected = "malformed continue requests")]
    fn continue_standin_readback_surfaces_a_handler_validation_failure() {
        let routing = Continuations::default();
        let _ = std::panic::catch_unwind(|| routing.receive(request("selection1")));
        routing.requests();
    }

    fn started(credential: &str, account: &str) -> Vec<u8> {
        serde_json::to_vec(&json!({"method":"route.account_observe", "params": {
            "kind":"admission_started", "admission_id":"admission1", "run_id":"run1",
            "credential_id":credential, "served_account_id":account,
        }}))
        .unwrap()
    }

    fn exercise_admit_replay(resume: bool) {
        let routing = Continuations::default();
        routing.declare(true);
        routing.declare_resume(resume);
        let original = br#"{ "result": {"admission":{"id":"admission1","state":"reserved"},"account":{"provider":"anthropic","account_id":"acct1","credential_id":"oauth:anthropic:acct1","method":"oauth"},"reason":{"decision_id":"decision1"}} }"#;
        routing.record_report(started("oauth:anthropic:acct1", "acct1"));
        assert_eq!(routing.admit_replay("admission1", original), original);
        routing.script(
            "selection1",
            ContinuePlan::Answer(ContinueAnswer::Offer(ContinueOffer::new(
                "oauth:anthropic:acct2",
                "acct2",
                "decision2",
                200,
            ))),
        );
        routing.receive(request("selection1"));
        assert_eq!(
            routing.admit_replay("admission1", original),
            original,
            "an unused offer does not move admission replay"
        );
        routing.record_report(started("oauth:anthropic:acct2", "wrong-account"));
        assert_eq!(routing.admit_replay("admission1", original), original);
        routing.record_report(started("oauth:anthropic:acct2", "acct2"));
        routing.record_report(started("oauth:anthropic:acct2", "acct2"));
        let current = routing.admit_replay("admission1", original);
        if resume {
            let current: Value = serde_json::from_slice(&current).unwrap();
            assert_eq!(current["result"]["account"]["account_id"], "acct2");
            assert_eq!(
                current["result"]["account"]["credential_id"],
                "oauth:anthropic:acct2"
            );
            assert_eq!(current["result"]["reason"]["decision_id"], "decision2");
        } else {
            assert_eq!(
                current, original,
                "legacy admit replay is byte-for-byte, even after an offer was used"
            );
        }
        routing.script(
            "selection2",
            ContinuePlan::Answer(ContinueAnswer::Offer(ContinueOffer::new(
                "oauth:anthropic:acct3",
                "acct3",
                "decision3",
                300,
            ))),
        );
        let mut next: Value = serde_json::from_slice(&request("selection2")).unwrap();
        next["params"]["previous_decision_id"] = json!("decision2");
        next["params"]["exhausted"]["credential_id"] = json!("oauth:anthropic:acct2");
        next["params"]["exhausted"]["account"] = json!("acct2");
        routing.receive(serde_json::to_vec(&next).unwrap());
        assert_eq!(
            routing.admit_replay("admission1", original),
            current,
            "an unused later offer does not replace the used offer"
        );
        routing.record_report(started("oauth:anthropic:acct3", "acct3"));
        let latest = routing.admit_replay("admission1", original);
        if resume {
            let latest: Value = serde_json::from_slice(&latest).unwrap();
            assert_eq!(latest["result"]["account"]["account_id"], "acct3");
            assert_eq!(
                latest["result"]["account"]["credential_id"],
                "oauth:anthropic:acct3"
            );
            assert_eq!(latest["result"]["reason"]["decision_id"], "decision3");
            assert_eq!(latest["result"]["admission"]["id"], "admission1");
        } else {
            assert_eq!(latest, original);
        }
        assert_eq!(routing.admit_replay("other-admission", original), original);
    }

    #[test]
    fn continue_standin_resume_declared_admit_replay_returns_last_used_offer() {
        exercise_admit_replay(true);
    }

    #[test]
    fn continue_standin_resume_undeclared_admit_replay_keeps_original_bytes() {
        exercise_admit_replay(false);
    }
}
