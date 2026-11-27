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
        assert_eq!(declare["schema"]["title"], "Broca");
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
    async fn next(&mut self) -> Frame {
        tokio::time::timeout(std::time::Duration::from_secs(5), self.sent.recv())
            .await
            .expect("the gate sent nothing")
            .unwrap()
    }

    async fn request(&mut self, op: &str) -> (u64, Value) {
        let frame = self.next().await;
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

#[tokio::test(start_paused = true)]
async fn resume_acknowledges_an_applied_pending_view_after_the_old_ack_was_cut() {
    let effective = json!({"chatgpt_force_https":true});
    let old = std::sync::Arc::new(super::Gate {
        calls: super::Calls::new(subc_protocol::PROTOCOL_VERSION),
        schema: crate::settings::schema(),
        user_file: Some("/config/broca.jsonc".into()),
        startup_at: std::sync::Mutex::new(None),
        applied: std::sync::Mutex::new(Some(super::Applied {
            effective: json!({"chatgpt_force_https":false}),
            generation: 6,
            attempt: None,
            started: json!({"chatgpt_force_https":false}),
            waiting_restart: vec![],
            refused_at: None,
        })),
    });
    let (tx, sent) = tokio::sync::mpsc::channel(8);
    old.attach(tx.into());
    let mut daemon = Served {
        gate: old.clone(),
        sent,
    };
    let applied = old.apply_view(view(7, 8, effective.clone(), ConfigStatus::Pending));
    tokio::pin!(applied);
    let (_, ack) = tokio::select! {
        sent = daemon.request("config.applied") => sent,
        result = &mut applied => panic!("old view did not wait for its acknowledgement: {result:?}"),
    };
    assert_eq!(ack["attempt"], 8);
    old.detach_for_upgrade();

    let replacement = old.resumed(subc_protocol::PROTOCOL_VERSION);
    let (tx, sent) = tokio::sync::mpsc::channel(8);
    let mut daemon = Served {
        gate: replacement.clone(),
        sent,
    };
    let started = replacement.finish_resume(tx.into(), Vec::new(), true);
    let check = async {
        let (corr, _) = daemon.request("catalog.update").await;
        daemon.answer(corr, json!({}));
        let (corr, _) = daemon.request("config.declare").await;
        daemon.answer(corr, json!({}));
        let (corr, _) = daemon.request("config.get").await;
        daemon.answer(
            corr,
            get_reply(None, Some(view(7, 9, effective, ConfigStatus::Pending))),
        );
        let (corr, ack) = daemon.request("config.applied").await;
        assert_eq!(
            (ack["generation"].as_u64(), ack["attempt"].as_u64()),
            (Some(7), Some(9))
        );
        daemon.answer(corr, json!({}));
    };
    let (started, ()) = tokio::join!(started, check);
    started.unwrap();
}

#[tokio::test(start_paused = true)]
async fn resume_restarts_if_settings_became_daemon_managed() {
    let gate = super::Gate::ungated(subc_protocol::PROTOCOL_VERSION);
    let (tx, sent) = tokio::sync::mpsc::channel(8);
    let mut daemon = Served {
        gate: gate.clone(),
        sent,
    };
    let resume = gate.finish_resume(tx.into(), Vec::new(), true);
    tokio::pin!(resume);
    tokio::select! {
        result = &mut resume => assert!(matches!(result, Err(error) if error.0.contains("settings management changed"))),
        sent = daemon.next() => panic!("an ungated module published readiness under a managed gate: {:?}", sent.header),
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

    let startup_at = std::time::Instant::now();
    served.gate.record_startup(startup_at);
    let ready = tokio::spawn({
        let gate = served.gate.clone();
        async move { gate.finish_start(tx.into(), Vec::new()).await }
    });
    let (corr, update) = served.request("catalog.update").await;
    assert_eq!(update["ready"], true, "{update}");
    assert_eq!(*served.gate.startup_at.lock().unwrap(), Some(startup_at));
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
    assert_eq!(*served.gate.startup_at.lock().unwrap(), None);

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

/// On an install with no approval stamp every view is generation 0 with no attempt, and the
/// daemon restarts nothing. A change (always pushed as generation 0) makes broca read the view
/// again: a field read at start that changed is named in broca's health as waiting for its next
/// start, a view with the same settings changes nothing, and a view broca's checks refuse is
/// kept out and named. Nothing is ever acknowledged.
#[tokio::test]
async fn an_unstamped_change_is_compared_by_its_settings_and_never_acknowledged() {
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
    // `chatgpt_force_https` stays true throughout: the policy it sets is process-wide, and the
    // test above asserts it true.
    let started = json!({"service_tier": {"enabled": true}, "chatgpt_force_https": true});
    let unstamped = |effective: Value| {
        ConfigView::new(
            0,
            effective,
            Default::default(),
            Vec::new(),
            ConfigStatus::Unstamped,
        )
    };
    daemon
        .send(
            FrameType::Response,
            get,
            get_reply(Some(unstamped(started.clone())), None),
        )
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
    let (corr, _) = served.request("catalog.update").await;
    served.answer(corr, json!({"op": "catalog.update"}));
    ready.await.unwrap().unwrap();
    assert!(served.sent.try_recv().is_err(), "nothing to acknowledge");

    let push = serde_json::to_vec(&json!({"op": "config.changed", "generation": 0, "fields": []}))
        .unwrap();
    let change = |served: &Served| {
        let gate = served.gate.clone();
        let push = push.clone();
        tokio::spawn(async move { gate.on_changed(&push).await })
    };
    let mut edited = started.clone();
    edited["service_tier"]["enabled"] = json!(false);
    for (offered, waiting, refused_at) in [
        // A field read at start changes: it waits for the next start.
        (
            edited.clone(),
            json!(["/service_tier/enabled"]),
            json!(null),
        ),
        // The same settings again: nothing changes.
        (
            edited.clone(),
            json!(["/service_tier/enabled"]),
            json!(null),
        ),
        // A view broca refuses is kept out, and broca keeps the settings it runs on.
        (
            json!({"providers": {"deepseek": {"base_url": "not-a-url"}}}),
            json!(["/service_tier/enabled"]),
            json!("/providers"),
        ),
        // Back to the settings it started from: nothing waits any more.
        (started.clone(), json!([]), json!(null)),
    ] {
        let changed = change(&served);
        let (corr, _) = served.request("config.get").await;
        served.answer(corr, get_reply(Some(unstamped(offered)), None));
        changed.await.unwrap().unwrap();
        let health = served.gate.health().unwrap();
        assert_eq!(
            (
                &health["generation"],
                &health["waiting_restart"],
                &health["refused_at"]
            ),
            (&json!(0), &waiting, &refused_at),
            "{health}"
        );
        assert!(served.sent.try_recv().is_err(), "nothing is acknowledged");
    }
}

/// What broca sends the daemon through the gate, byte for byte, for the daemon's readers to
/// check: the declaration, the request for the view, three reviews (no base yet, a change on a
/// base, and a value the schema refuses) and the update that makes broca ready. The view is an
/// unstamped install's, which has no attempt, so broca acknowledges nothing. The requests the
/// daemon sends are built with the protocol crate's own types. With
/// `BROCA_SETTINGS_SAMPLES_DIR` set, every frame body is written there under its step and op.
#[tokio::test]
async fn the_frames_the_gate_exchanges_are_what_readers_were_shown() {
    const USER_FILE: &str = "/home/user/.config/cortexkit/broca.jsonc";
    let out = std::env::var_os("BROCA_SETTINGS_SAMPLES_DIR").map(std::path::PathBuf::from);
    let save = |name: &str, body: &[u8]| {
        if let Some(dir) = &out {
            std::fs::create_dir_all(dir).unwrap();
            std::fs::write(dir.join(name), body).unwrap();
        }
    };
    let (mut daemon, mut read, mut write) = connected();
    let broca = tokio::spawn(async move {
        wait_for_settings(
            &mut read,
            &mut write,
            subc_protocol::PROTOCOL_VERSION,
            USER_FILE,
        )
        .await
    });

    let declare = daemon.next().await;
    let body: Value = serde_json::from_slice(&declare.body).unwrap();
    assert_eq!(
        (&body["op"], &body["user_file"]),
        (&json!("config.declare"), &json!(USER_FILE))
    );
    assert!(body["schema"]["properties"].get("wal_recovery").is_none());
    save("01-config.declare.request.json", &declare.body);
    daemon
        .send(
            FrameType::Response,
            declare.header.corr,
            json!({"op": "config.declare"}),
        )
        .await;
    let get = daemon.next().await;
    save("02-config.get.request.json", &get.body);

    let user = br#"{
  // broca.jsonc after the one-time conversion
  "service_tier": {"enabled": true},
  "chatgpt_force_https": true,
}
"#;
    let user_layer = |bytes: &[u8]| {
        vec![ConfigLayerSnapshot::file(
            "broca",
            ConfigLayer::User,
            USER_FILE,
            Some(bytes.to_vec()),
        )]
    };
    // The view the daemon serves from that file, built as the daemon builds it: the reviewed
    // tuple's effective settings and sources, and the file it came from.
    let user_value: Value =
        serde_json::from_str(r#"{"service_tier": {"enabled": true}, "chatgpt_force_https": true}"#)
            .unwrap();
    let tuple = super::super::schema()
        .review_tuple(
            &[subc_config::LayerValue::new(
                subc_config::Layer::User,
                user_value,
            )],
            &subc_config::ValidationContext::new("broca", None),
        )
        .unwrap();
    // A retained base as the daemon sends it: generation 1, pending until activated, and with
    // no delivery attempt.
    let base = ConfigView::new(
        1,
        tuple.effective.clone(),
        tuple.sources.clone(),
        user_layer(user),
        ConfigStatus::Pending,
    );
    let reviews = [
        (
            "03-config.review.first",
            ConfigReviewRequest::new(0, user_layer(user), None),
        ),
        (
            "04-config.review.on-a-base",
            ConfigReviewRequest::new(
                1,
                user_layer(
                    br#"{"service_tier": {"enabled": false}, "chatgpt_force_https": false}"#,
                ),
                Some(base.clone()),
            ),
        ),
        (
            "05-config.review.refused",
            ConfigReviewRequest::new(
                0,
                // Well formed, so the daemon sends it, but not a URL broca can use.
                user_layer(br#"{"providers": {"deepseek": {"base_url": "not-a-url"}}}"#),
                None,
            ),
        ),
    ];
    let mut replies = Vec::new();
    for (corr, (step, request)) in (20u64..).zip(reviews) {
        let body = serde_json::to_vec(&request).unwrap();
        save(&format!("{step}.request.json"), &body);
        let flags = subc_protocol::Flags::new(false, subc_protocol::Priority::Passive, false);
        let frame = Frame::build(FrameType::Request, flags, 0, 0, corr, body).unwrap();
        write_frame(&mut daemon.write, &frame).await.unwrap();
        let reply = daemon.next().await;
        assert_eq!(
            (reply.header.ty, reply.header.corr),
            (FrameType::Response, corr)
        );
        save(&format!("{step}.reply.json"), &reply.body);
        replies.push(serde_json::from_slice::<Value>(&reply.body).unwrap());
    }
    assert_eq!(replies[0]["refusals"], json!([]), "{}", replies[0]);
    assert_eq!(replies[1]["refusals"], json!([]), "{}", replies[1]);
    assert_eq!(
        replies[2]["refusals"][0]["paths"],
        json!(["/providers"]),
        "{}",
        replies[2]
    );

    // On an install with no approval stamp the daemon serves the file's view as the active one,
    // at generation 0 (no generation assigned yet) and with no delivery attempt; broca builds
    // from it, and has nothing to acknowledge.
    let unstamped = ConfigView::new(
        0,
        tuple.effective,
        tuple.sources,
        user_layer(user),
        ConfigStatus::Unstamped,
    );
    let get_reply = get_reply(Some(unstamped), None);
    save(
        "02-config.get.reply.json",
        &serde_json::to_vec(&get_reply).unwrap(),
    );
    daemon
        .send(FrameType::Response, get.header.corr, get_reply)
        .await;
    let GateStart::Settings(_, gate) = broca.await.unwrap().unwrap() else {
        panic!("broca did not start");
    };
    let (tx, mut sent) = tokio::sync::mpsc::channel(8);
    // The roles broca really registers with: the daemon refuses an update that empties them.
    let provides = crate::serve::hello_manifest(
        "broca",
        &crate::consumer_routes::ConsumerRoutes::default(),
        false,
    )
    .provides;
    assert!(!provides.is_empty());
    let ready = tokio::spawn({
        let gate = gate.clone();
        async move { gate.finish_start(tx.into(), provides).await }
    });
    let update = tokio::time::timeout(std::time::Duration::from_secs(5), sent.recv())
        .await
        .expect("the gate sent nothing")
        .unwrap();
    save("06-catalog.update.request.json", &update.body);
    let served = Served { gate, sent };
    served.answer(update.header.corr, json!({"op": "catalog.update"}));
    ready.await.unwrap().unwrap();
    let mut sent = served.sent;
    assert!(
        sent.try_recv().is_err(),
        "an unstamped view has no attempt to acknowledge"
    );
}

thread_local! {
    /// Runs once, on this thread, right after a changed view's freshness check: it stands in
    /// for another push's task running while this one waits for the CPU.
    static AFTER_FRESHNESS_CHECK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

pub(super) fn after_freshness_check() {
    if let Some(hook) = AFTER_FRESHNESS_CHECK.with(|hook| hook.borrow_mut().take()) {
        hook();
    }
}

/// Each `config.changed` push runs as its own task. The older view's task loses the CPU right
/// after it finds its view newer than the one broca runs on, and the newer view is taken
/// meanwhile: broca must still end on the newer view, its live value and its generation.
#[test]
fn an_older_view_never_overwrites_a_newer_one_taken_after_its_freshness_check() {
    let started = json!({"service_tier": {"enabled": true}});
    let gate = std::sync::Arc::new(super::Gate {
        calls: super::Calls::new(subc_protocol::PROTOCOL_VERSION),
        schema: crate::settings::schema(),
        user_file: None,
        startup_at: std::sync::Mutex::new(None),
        applied: std::sync::Mutex::new(Some(super::Applied {
            effective: started.clone(),
            generation: 2,
            attempt: Some(1),
            started: started.clone(),
            waiting_restart: Vec::new(),
            refused_at: None,
        })),
    });
    let mut older = started.clone();
    older["chatgpt_force_https"] = json!(false);
    let mut newer = started;
    newer["chatgpt_force_https"] = json!(true);
    let older = view(3, 1, older, ConfigStatus::Pending);
    let newer = view(4, 1, newer, ConfigStatus::Pending);
    AFTER_FRESHNESS_CHECK.with(|hook| {
        let gate = gate.clone();
        *hook.borrow_mut() = Some(Box::new(move || {
            assert!(matches!(gate.take_in_place(&newer), super::InPlace::Taken));
        }));
    });
    // What the older push makes of its view doesn't matter, only where broca ends.
    gate.take_in_place(&older);
    let applied = gate.applied.lock().unwrap();
    let applied = applied.as_ref().unwrap();
    assert_eq!(
        (
            applied.generation,
            &applied.effective["chatgpt_force_https"]
        ),
        (4, &json!(true)),
        "an older view overwrote the newer one broca took"
    );
}
