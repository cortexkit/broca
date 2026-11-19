//! The settings gate before broca starts, against a scripted daemon on the other end of an
//! in-memory socket: what broca sends, how it answers what arrives while it waits, and which
//! view it starts from.

use serde_json::{json, Value};
use subc_protocol::config::{
    ConfigGetReply, ConfigLayer, ConfigLayerSnapshot, ConfigReviewRequest, ConfigStatus, ConfigView,
};
use subc_protocol::{Frame, FrameType};
use subc_transport::{read_frame, write_frame};
use tokio::io::{DuplexStream, ReadHalf, WriteHalf};

use super::{wait_for_settings, GateStart};

/// The daemon's end of the socket.
struct Daemon {
    read: ReadHalf<DuplexStream>,
    write: WriteHalf<DuplexStream>,
}

impl Daemon {
    async fn next(&mut self) -> Frame {
        read_frame(&mut self.read)
            .await
            .unwrap()
            .expect("broca closed the socket")
    }

    /// The next frame broca sends, which must be a channel-0 request: its correlation id and
    /// body.
    async fn request(&mut self, op: &str) -> (u64, Value) {
        let frame = self.next().await;
        assert_eq!(frame.header.ty, FrameType::Request);
        assert_eq!(frame.header.channel, 0);
        let body: Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(body["op"], op, "{body}");
        (frame.header.corr, body)
    }

    async fn send(&mut self, ty: FrameType, corr: u64, body: Value) {
        let flags = subc_protocol::Flags::new(false, subc_protocol::Priority::Passive, false);
        let frame =
            Frame::build(ty, flags, 0, 0, corr, serde_json::to_vec(&body).unwrap()).unwrap();
        write_frame(&mut self.write, &frame).await.unwrap();
    }

    /// Answers broca's declaration and returns the correlation id of its `config.get`.
    async fn accept_declaration(&mut self) -> u64 {
        let (corr, declare) = self.request("config.declare").await;
        assert_eq!(declare["user_file"], "/config/broca.jsonc");
        assert_eq!(declare["schema"]["title"], "broca");
        self.send(FrameType::Response, corr, json!({"op": "config.declare"}))
            .await;
        self.request("config.get").await.0
    }
}

fn connected() -> (Daemon, ReadHalf<DuplexStream>, WriteHalf<DuplexStream>) {
    let (daemon, broca) = tokio::io::duplex(1 << 20);
    let (read, write) = tokio::io::split(daemon);
    let (broca_read, broca_write) = tokio::io::split(broca);
    (Daemon { read, write }, broca_read, broca_write)
}

fn view(generation: u64, attempt: u64, effective: Value, status: ConfigStatus) -> ConfigView {
    let mut view = ConfigView::new(
        generation,
        effective,
        Default::default(),
        Vec::new(),
        status,
    );
    view.attempt = Some(attempt);
    view
}

fn get_reply(active: Option<ConfigView>, pending: Option<ConfigView>) -> Value {
    serde_json::to_value(ConfigGetReply::new(active, pending)).unwrap()
}

/// While the daemon has not answered the request for the view, a probe reads broca degraded
/// with the reason, a route bind is refused with the retryable `module_warming`, and a review
/// is answered. Then broca starts from the pending view, not the active one.
#[tokio::test]
async fn broca_answers_while_it_waits_and_starts_from_the_pending_view() {
    let (mut daemon, mut read, mut write) = connected();
    let broca = tokio::spawn(async move {
        wait_for_settings(
            &mut read,
            &mut write,
            subc_protocol::PROTOCOL_VERSION,
            "/config/broca.jsonc",
        )
        .await
    });
    let get = daemon.accept_declaration().await;

    daemon
        .send(FrameType::Request, 7, json!({"op": "health.check"}))
        .await;
    let probe = daemon.next().await;
    assert_eq!(
        (probe.header.ty, probe.header.corr),
        (FrameType::Response, 7)
    );
    let probe: Value = serde_json::from_slice(&probe.body).unwrap();
    assert_eq!(probe["status"], "degraded", "{probe}");
    assert_eq!(probe["detail"], "waiting for the daemon's settings");

    daemon
        .send(
            FrameType::Request,
            8,
            json!({"op": "route.bind", "route_channel": 3, "epoch": 1,
                   "identity": {"project_root": "/p", "harness": "h", "session": "s"}}),
        )
        .await;
    let bind = daemon.next().await;
    assert_eq!((bind.header.ty, bind.header.corr), (FrameType::Error, 8));
    let bind: Value = serde_json::from_slice(&bind.body).unwrap();
    assert_eq!(bind["code"], "module_warming");

    let proposed = ConfigLayerSnapshot::file(
        "broca",
        ConfigLayer::User,
        "/config/broca.jsonc",
        Some(br#"{"service_tier": {"enabled": true}}"#.to_vec()),
    );
    let review = ConfigReviewRequest::new(0, vec![proposed], None);
    daemon
        .send(FrameType::Request, 9, serde_json::to_value(review).unwrap())
        .await;
    let review = daemon.next().await;
    assert_eq!(
        (review.header.ty, review.header.corr),
        (FrameType::Response, 9)
    );
    let review: Value = serde_json::from_slice(&review.body).unwrap();
    assert_eq!(review["refusals"], json!([]), "{review}");
    // With no approved base every field is new; only the one the file sets comes from it.
    let from_user: Vec<&Value> = review["fields"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|field| field["new_source"] == "user")
        .collect();
    assert_eq!(from_user.len(), 1, "{review}");
    assert_eq!(from_user[0]["path"], "/service_tier/enabled");

    let active = view(
        1,
        1,
        json!({"service_tier": {"enabled": false}}),
        ConfigStatus::Active,
    );
    let pending = view(
        2,
        3,
        json!({"service_tier": {"enabled": true}}),
        ConfigStatus::Pending,
    );
    daemon
        .send(
            FrameType::Response,
            get,
            get_reply(Some(active), Some(pending)),
        )
        .await;
    let GateStart::Settings(settings, _gate) = broca.await.unwrap().unwrap() else {
        panic!("broca did not start");
    };
    assert_eq!(
        settings.get("service_tier"),
        Some(&json!({"enabled": true}))
    );
}

/// With no approved settings, broca stays unready and says so, and asks again when the daemon
/// says the settings changed.
#[tokio::test]
async fn broca_waits_for_approved_settings_and_asks_again_on_a_change() {
    let (mut daemon, mut read, mut write) = connected();
    let broca = tokio::spawn(async move {
        wait_for_settings(
            &mut read,
            &mut write,
            subc_protocol::PROTOCOL_VERSION,
            "/config/broca.jsonc",
        )
        .await
    });
    let get = daemon.accept_declaration().await;
    daemon
        .send(
            FrameType::Error,
            get,
            json!({"code": "config_gated", "message": "no approved configuration"}),
        )
        .await;
    daemon
        .send(FrameType::Request, 7, json!({"op": "health.check"}))
        .await;
    let probe: Value = serde_json::from_slice(&daemon.next().await.body).unwrap();
    assert_eq!(
        probe["detail"],
        "waiting for the daemon's settings: none are approved yet"
    );
    // The probe asks again too, so a refusal the daemon lifts without a push isn't stuck.
    let retry = daemon.request("config.get").await.0;
    daemon
        .send(FrameType::Response, retry, get_reply(None, None))
        .await;

    daemon
        .send(
            FrameType::Push,
            0,
            json!({"op": "config.changed", "generation": 1, "fields": ["/service_tier/enabled"]}),
        )
        .await;
    let get = daemon.request("config.get").await.0;
    let active = view(
        1,
        1,
        json!({"service_tier": {"enabled": true}}),
        ConfigStatus::Active,
    );
    daemon
        .send(FrameType::Response, get, get_reply(Some(active), None))
        .await;
    let GateStart::Settings(settings, _gate) = broca.await.unwrap().unwrap() else {
        panic!("broca did not start");
    };
    assert_eq!(
        settings.get("service_tier"),
        Some(&json!({"enabled": true}))
    );
}

/// A view broca could not build from is refused with a NACK that names the key, never the
/// value, and broca keeps waiting for a better one.
#[tokio::test]
async fn a_view_broca_cannot_build_from_is_refused_and_broca_keeps_waiting() {
    let (mut daemon, mut read, mut write) = connected();
    let broca = tokio::spawn(async move {
        wait_for_settings(
            &mut read,
            &mut write,
            subc_protocol::PROTOCOL_VERSION,
            "/config/broca.jsonc",
        )
        .await
    });
    let get = daemon.accept_declaration().await;
    let broken = view(
        4,
        2,
        json!({"session_storage": {"encryption": "sometimes"}}),
        ConfigStatus::Pending,
    );
    daemon
        .send(FrameType::Response, get, get_reply(None, Some(broken)))
        .await;
    let (_, nack) = daemon.request("config.applied").await;
    assert_eq!(nack["result"], "nack");
    assert_eq!(
        (nack["generation"].as_u64(), nack["attempt"].as_u64()),
        (Some(4), Some(2))
    );
    assert_eq!(nack["reasons"], json!(["refused at /session_storage"]));
    assert!(!nack.to_string().contains("sometimes"), "{nack}");
    assert!(!broca.is_finished(), "broca must keep waiting");

    daemon
        .send(
            FrameType::Push,
            0,
            json!({"op": "config.changed", "generation": 5, "fields": []}),
        )
        .await;
    let get = daemon.request("config.get").await.0;
    daemon
        .send(
            FrameType::Response,
            get,
            get_reply(None, Some(view(5, 1, json!({}), ConfigStatus::Pending))),
        )
        .await;
    assert!(matches!(
        broca.await.unwrap().unwrap(),
        GateStart::Settings(..)
    ));
}

/// A drain or GOODBYE before any view ends the wait with nothing built and nothing to stop.
#[tokio::test]
async fn a_drain_before_the_settings_arrive_stops_broca() {
    let (mut daemon, mut read, mut write) = connected();
    let broca = tokio::spawn(async move {
        wait_for_settings(
            &mut read,
            &mut write,
            subc_protocol::PROTOCOL_VERSION,
            "/config/broca.jsonc",
        )
        .await
    });
    daemon.accept_declaration().await;
    daemon
        .send(
            FrameType::Push,
            0,
            json!({"op": "module.draining", "reason": "restart", "deadline_ms": 1}),
        )
        .await;
    assert!(matches!(
        broca.await.unwrap().unwrap(),
        GateStart::Stopped(crate::ServeEnd::Goodbye)
    ));
}

/// The gate's frames once the serve loop runs: what it sends through the loop's queue, and the
/// daemon's answers fed back the way the loop hands them over.
struct Served {
    gate: std::sync::Arc<super::Gate>,
    sent: tokio::sync::mpsc::Receiver<Frame>,
}

impl Served {
    async fn request(&mut self, op: &str) -> (u64, Value) {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(5), self.sent.recv())
            .await
            .expect("the gate sent nothing")
            .unwrap();
        let body: Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(body["op"], op, "{body}");
        (frame.header.corr, body)
    }

    fn answer(&self, corr: u64, body: Value) {
        let flags = subc_protocol::Flags::new(false, subc_protocol::Priority::Passive, false);
        let frame = Frame::build(
            FrameType::Response,
            flags,
            0,
            0,
            corr,
            serde_json::to_vec(&body).unwrap(),
        )
        .unwrap();
        assert!(
            self.gate.on_frame(&frame),
            "the gate must take its own answer"
        );
    }
}

/// Once broca is built, the gate marks it ready with its real roles in one `catalog.update`,
/// then acknowledges the view it started from. A later view that changes only a field applied
/// in place is applied and acknowledged; one that changes a field read at start is left for the
/// restart the daemon makes, with nothing sent back.
#[tokio::test]
async fn the_gate_makes_broca_ready_then_applies_only_in_place_changes() {
    let (mut daemon, mut read, mut write) = connected();
    let broca = tokio::spawn(async move {
        wait_for_settings(
            &mut read,
            &mut write,
            subc_protocol::PROTOCOL_VERSION,
            "/config/broca.jsonc",
        )
        .await
    });
    let get = daemon.accept_declaration().await;
    let started = json!({"service_tier": {"enabled": true}});
    let pending = view(2, 3, started.clone(), ConfigStatus::Pending);
    daemon
        .send(FrameType::Response, get, get_reply(None, Some(pending)))
        .await;
    let GateStart::Settings(_, gate) = broca.await.unwrap().unwrap() else {
        panic!("broca did not start");
    };
    let (tx, sent) = tokio::sync::mpsc::channel(8);
    let mut served = Served { gate, sent };

    let ready = tokio::spawn({
        let gate = served.gate.clone();
        async move { gate.finish_start(tx.into(), Vec::new()).await }
    });
    let (corr, update) = served.request("catalog.update").await;
    assert_eq!(update["ready"], true, "{update}");
    served.answer(corr, json!({"op": "catalog.update"}));
    let (corr, applied) = served.request("config.applied").await;
    assert_eq!(
        (
            &applied["result"],
            &applied["generation"],
            &applied["attempt"]
        ),
        (&json!("ack"), &json!(2), &json!(3))
    );
    served.answer(corr, json!({"op": "config.applied"}));
    ready.await.unwrap().unwrap();

    // Only the live field changes: applied in place, and the next attempt reads it.
    let mut live = started.clone();
    live["chatgpt_force_https"] = json!(true);
    let push = serde_json::to_vec(&json!({"op": "config.changed", "generation": 3,
        "fields": ["/chatgpt_force_https"]}))
    .unwrap();
    let changed = tokio::spawn({
        let gate = served.gate.clone();
        let push = push.clone();
        async move { gate.on_changed(&push).await }
    });
    let (corr, _) = served.request("config.get").await;
    served.answer(
        corr,
        get_reply(None, Some(view(3, 1, live.clone(), ConfigStatus::Pending))),
    );
    let (corr, applied) = served.request("config.applied").await;
    assert_eq!(
        (&applied["result"], &applied["generation"]),
        (&json!("ack"), &json!(3))
    );
    served.answer(corr, json!({"op": "config.applied"}));
    changed.await.unwrap().unwrap();
    assert!(crate::transport_config::default_policy()());

    // A field read at start changes: nothing is applied or acknowledged.
    let mut restart = live.clone();
    restart["service_tier"]["enabled"] = json!(false);
    let push = serde_json::to_vec(&json!({"op": "config.changed", "generation": 4,
        "fields": ["/service_tier/enabled"]}))
    .unwrap();
    let changed = tokio::spawn({
        let gate = served.gate.clone();
        async move { gate.on_changed(&push).await }
    });
    let (corr, _) = served.request("config.get").await;
    served.answer(
        corr,
        get_reply(None, Some(view(4, 1, restart, ConfigStatus::Pending))),
    );
    changed.await.unwrap().unwrap();
    assert!(served.sent.try_recv().is_err(), "nothing is acknowledged");
}
