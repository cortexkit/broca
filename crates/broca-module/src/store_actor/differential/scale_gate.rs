//! The scale gate: a real OpenCode 1 head imported through `session.import`, its first view
//! installed through `session.install_view`, then each operation a head serves timed against
//! hard limits, on disk and encrypted as in production.
//!
//! Ignored: it needs an extracted OpenCode 1 database (`ck-import opencode1-extract`) named by
//! `BROCA_SCALE_GATE_SOURCE`, with `BROCA_SCALE_GATE_SESSION` its session id, and a large one
//! takes minutes and gigabytes. `BROCA_SCALE_GATE_KEEP` sets how many newest entries the
//! installed view keeps (1,000 by default), and `BROCA_SCALE_GATE_SENDS` how many sends it times
//! (10 by default). Run it built optimized:
//!
//! ```text
//! cargo nextest run --cargo-profile release -p broca-module --run-ignored only \
//!     -E 'test(scale_gate)' --no-capture
//! ```

use std::time::{Duration, Instant};

use broca_core::compaction_v2::protocol::view::{Edit, EditOp, View};

use super::*;
use crate::serve::ServeHandler as _;
use crate::store_import::{
    ApplyViewCommand, ImportCommand, ImportRoots, InstallViewCommand, StoreCommand, StoreOutcome,
};

/// The most an operation's median may take once the view is installed. Import and install are
/// one-time steps and are reported, not limited.
const LIMIT: Duration = Duration::from_millis(20);

/// The most the first read of the closed session may take: it builds the session's cached view.
const COLD_LIMIT: Duration = Duration::from_millis(50);

/// Each repeated read runs this many times.
const ROUNDS: usize = 5;

/// How many later views the compaction provider applies after the sends.
const APPLIES: usize = 3;

/// The most an applied view's median may take. An apply on an idle head opens its actor,
/// restores its compaction state from the open log and checks the view against the window's
/// entries, each projected and hashed; about 46 ms on a 1,000-entry window. Before it checked
/// against every entry it took tens of seconds on a 400k-entry head.
const APPLY_LIMIT: Duration = Duration::from_millis(100);

fn sha256_of(path: &std::path::Path) -> String {
    use sha2::Digest as _;
    use std::io::Read as _;
    let mut file = std::fs::File::open(path).unwrap();
    let mut hasher = sha2::Sha256::new();
    let mut buffer = vec![0; 1 << 20];
    loop {
        let read = file.read(&mut buffer).unwrap();
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    format!("{:x}", hasher.finalize())
}

/// The slowest of [`ROUNDS`] runs of `operation`, and their median.
async fn timed<F, Fut, T>(mut operation: F) -> (Duration, Duration)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let mut times = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let started = Instant::now();
        let _ = operation().await;
        times.push(started.elapsed());
    }
    times.sort();
    (times[ROUNDS / 2], times[ROUNDS - 1])
}

/// A head plan with one tool, as core sends with an imported head's first send.
fn plan() -> crate::session_cmd::SendPlan {
    let plan = serde_json::json!({"senders": []});
    let manifest = broca_records::record::PlanManifest {
        rich_ui: None,
        plan: broca_records::record::OpaqueJson::from_json_text(plan.to_string()).unwrap(),
        plan_identity: "scale-gate-plan".into(),
        composition_digest: "composition".into(),
        tools: Arc::new(vec![serde_json::from_value(serde_json::json!({
            "name": "read",
            "description": "reads a file",
            "input_schema": {"type": "object"},
        }))
        .unwrap()]),
        system_text: "you are a head".into(),
        system_digest: "system".into(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations: vec![],
    };
    crate::session_cmd::SendPlan {
        identity: "scale-gate-plan".into(),
        manifest: Some(Arc::new(manifest)),
        model_check: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs BROCA_SCALE_GATE_SOURCE and BROCA_SCALE_GATE_SESSION"]
async fn scale_gate_imported_head() {
    let source = std::path::PathBuf::from(std::env::var("BROCA_SCALE_GATE_SOURCE").unwrap());
    let session_id = std::env::var("BROCA_SCALE_GATE_SESSION").unwrap();
    let keep: u64 = std::env::var("BROCA_SCALE_GATE_KEEP")
        .map(|keep| keep.parse().unwrap())
        .unwrap_or(1_000);
    let rounds: usize = std::env::var("BROCA_SCALE_GATE_SENDS")
        .map(|sends| sends.parse().unwrap())
        .unwrap_or(10);
    let mut steps = Vec::new();
    // Room for warm-ups, which take scripted answers too.
    for _ in 0..3 * rounds + 5 {
        steps.extend(hello());
    }
    let module = module_options(steps, None, None, IDLE, None).await;
    let mut report = Vec::new();

    let params = serde_json::from_value(serde_json::json!({
        "import_id": "scale-gate",
        "source": {
            "harness": "opencode1",
            "path": source.display().to_string(),
            "session": session_id,
            "sha256": sha256_of(&source),
        },
        "origin": {"kind": "import", "source_ref": format!("opencode1:{session_id}")},
    }))
    .unwrap();
    // No folder the import deletes its source from: the extract stays for the next run.
    let roots = Arc::new(ImportRoots::new(
        vec![source.parent().unwrap().to_path_buf()],
        None,
    ));
    let started = Instant::now();
    let outcome = module
        .registry
        .route_store_command(
            &module.key,
            StoreCommand::Import(Box::new(ImportCommand {
                params,
                principal: core(),
                scope: Some(match head_scope(core()) {
                    broca_protocol::SendScope::Stamped(stamped) => stamped.scope,
                    _ => unreachable!("a stamped scope"),
                }),
                roots,
                now_ms: 9_000,
            })),
        )
        .await
        .unwrap();
    assert!(matches!(outcome, StoreOutcome::Imported(_)), "{outcome:?}");
    report.push(format!("import: {:.1} s", started.elapsed().as_secs_f64()));
    module.closed().await;

    // The entry the view cuts through: everything up to it leaves the open log.
    let addr = broca_session_store::backend::SessionAddr(broca_core::session_addr(&module.key));
    // A head is kept in the class root its backup class names; find the one holding it.
    let root = [
        broca_records::features::SESSION_STORAGE_DIR,
        broca_records::session_storage::SESSION_STORAGE_DURABLE_DIR,
        broca_records::session_storage::SESSION_STORAGE_EPHEMERAL_DIR,
    ]
    .into_iter()
    .map(|name| module.dir.join(name))
    .find(|root| root.join("logs").join(&addr.0).exists())
    .expect("the imported session's root");
    let backend = broca_session_store::backend::LocalBackend::new(root);
    let triple = broca_session_store::head::SessionTriple {
        project_root: module.key.project_root.display().to_string(),
        harness: module.key.harness.clone(),
        session: module.key.session.clone(),
    };
    let deployment = broca_session_store::keys::DeploymentKey::new(1, &[7; 32]);
    let view = Session::view(backend, addr, &triple, &deployment)
        .await
        .unwrap();
    let length = crate::store_read::transcript_len(&view);
    assert!(length > keep, "{length} entries, keeping {keep}");
    let mid_at = |ordinal: u64| {
        let view = &view;
        async move {
            view.entries(ordinal, 1).await.unwrap()[0]
                .state
                .facts
                .as_ref()
                .unwrap()
                .mid
                .clone()
        }
    };
    let through = mid_at(length - keep).await;
    // The later views the compaction provider applies each cut a few entries further.
    let mut later_cuts = Vec::new();
    for step in 1..=APPLIES as u64 {
        later_cuts.push(mid_at(length - keep + 5 * step).await);
    }
    drop(view);
    report.push(format!(
        "entries: {length}, the view keeps the newest {keep}"
    ));

    let installed = View::seal(1, vec![Edit::new(1, EditOp::Cut { through })]);
    eprintln!("scale gate: installing the view");
    let started = Instant::now();
    let outcome = module
        .registry
        .route_store_command(
            &module.key,
            StoreCommand::InstallView(Box::new(InstallViewCommand {
                view: serde_json::value::to_raw_value(&installed).unwrap(),
                principal: core(),
            })),
        )
        .await
        .unwrap();
    assert!(
        matches!(outcome, StoreOutcome::ViewInstalled(_)),
        "{outcome:?}"
    );
    report.push(format!(
        "install view (seals the history before its cut): {:.1} s",
        started.elapsed().as_secs_f64()
    ));
    module.closed().await;

    let mut over = Vec::new();
    let mut measure =
        |name: &str, (median, slowest): (Duration, Duration), limit: Option<Duration>| {
            let line = format!(
                "{name}: median {:.2} ms, slowest {:.2} ms",
                median.as_secs_f64() * 1e3,
                slowest.as_secs_f64() * 1e3
            );
            eprintln!("scale gate: {line}");
            report.push(line);
            if limit.is_some_and(|limit| median > limit) {
                over.push(name.to_owned());
            }
        };

    // Closed: no actor holds the session.
    let newest = || {
        module.registry.read(opted_request(
            &module.key,
            (None, Some(100)),
            (true, false, false),
        ))
    };
    let (first, _) = {
        let started = Instant::now();
        newest().await.unwrap();
        (started.elapsed(), ())
    };
    measure(
        "closed: first read of the newest 100 messages",
        (first, first),
        Some(COLD_LIMIT),
    );
    measure(
        "closed: newest 100 messages",
        timed(&newest).await,
        Some(LIMIT),
    );
    let sealed = || {
        module.registry.read(opted_request(
            &module.key,
            (Some(10), Some(100)),
            (true, false, false),
        ))
    };
    measure(
        "closed: 100 sealed messages from the start",
        timed(sealed).await,
        Some(LIMIT),
    );
    let middle = length / 2;
    let halfway = || {
        module.registry.read(opted_request(
            &module.key,
            (Some(middle), Some(100)),
            (true, false, false),
        ))
    };
    measure(
        "closed: 100 sealed messages halfway",
        timed(halfway).await,
        Some(LIMIT),
    );
    measure(
        "closed: session.head",
        timed(|| module.registry.head(&module.key)).await,
        Some(LIMIT),
    );

    // A send opens the actor and starts a run; each later send reopens it once it closed.
    let (mut starts, mut ends) = (Vec::new(), Vec::new());
    for round in 1..=rounds {
        let mut ask = request(&format!("scale gate question {round}"));
        from_core(&mut ask);
        ask.scope = head_scope(core());
        ask.tools = None;
        ask.policy_allowed = Default::default();
        // A head's run freezes its model's context window; the compaction provider's views are
        // checked under it. The test's executor admits the send's own.
        ask.context_limit = Some(1_000_000);
        let run = format!("scale-gate-{round}");
        let phases_before = broca_session_store::metrics::snapshot();
        let started = Instant::now();
        let outcome = module
            .registry
            .route_submission_with_order(
                &module.key,
                RunId(run.clone()),
                module.caller.shape(&run, &SendKey::Minted, ask),
                RunKind::FreshOrResume,
                SendKey::Minted,
                false,
                broca_protocol::Delivery::Queue,
                Some(plan()),
                None,
                None,
                None,
                None,
                false,
                None,
            )
            .await
            .expect("the session opens");
        let answered = started.elapsed();
        // Where the send's time went, by storage phase.
        let phases: Vec<String> = broca_session_store::metrics::snapshot()
            .into_iter()
            .filter_map(|(name, after)| {
                let before = phases_before.get(name);
                let count = after.count - before.map_or(0, |before| before.count);
                let us = after.total_us - before.map_or(0, |before| before.total_us);
                (count > 0).then(|| format!("{name} {count}x {:.2} ms", us as f64 / 1e3))
            })
            .collect();
        eprintln!(
            "scale gate: send {round} storage phases: {}",
            phases.join(", ")
        );
        assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
        starts.push(answered);
        // The run's first request goes out and the scripted answer ends it: the time from the
        // send to the run's end is the time to its first model call, plus a scripted step.
        loop {
            let head =
                serde_json::to_value(module.registry.head(&module.key).await.unwrap()).unwrap();
            let last = &head["last_run_state"];
            if last["run_id"] == run.as_str() && last["state"] == "completed" {
                break;
            }
            assert!(started.elapsed() < Duration::from_secs(120), "{last}");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        ends.push(started.elapsed());
        // Reads while the actor is open.
        if round == 1 {
            measure(
                "open: newest 100 messages",
                timed(&newest).await,
                Some(LIMIT),
            );
            measure(
                "open: session.head",
                timed(|| module.registry.head(&module.key)).await,
                Some(LIMIT),
            );
        }
        module.closed().await;
    }
    // The compaction provider applies each later view through the session's actor, which
    // checks it against the session's entries under the latest run's rules.
    let mut applies = Vec::new();
    for (index, cut) in later_cuts.into_iter().enumerate() {
        let version = 2 + index as u64;
        let view = View::seal(
            version,
            vec![Edit::new(version, EditOp::Cut { through: cut })],
        );
        eprintln!("scale gate: applying view {version}");
        let started = Instant::now();
        let outcome = module
            .registry
            .route_store_command(
                &module.key,
                StoreCommand::ApplyView(Box::new(ApplyViewCommand {
                    view: serde_json::value::to_raw_value(&view).unwrap(),
                    caller: Some("magic-context".into()),
                })),
            )
            .await
            .unwrap();
        applies.push(started.elapsed());
        match &outcome {
            StoreOutcome::ViewApplied(answer) => {
                assert!(answer.contains("accepted"), "view {version}: {answer}")
            }
            other => panic!("view {version}: {other:?}"),
        }
    }
    let spread = |mut times: Vec<Duration>| {
        times.sort();
        (times[times.len() / 2], times[times.len() - 1])
    };
    measure(
        &format!("{rounds} sends: open the actor and start the run"),
        spread(starts),
        Some(LIMIT),
    );
    // Through the run's first request and its scripted answer, with the test's stand-in
    // provider and compaction provider in the time: reported, not limited.
    measure(&format!("{rounds} sends: the run ends"), spread(ends), None);
    measure(
        &format!("{APPLIES} applied views"),
        spread(applies),
        Some(APPLY_LIMIT),
    );

    eprintln!(
        "\nscale gate, {}:\n  {}",
        source.display(),
        report.join("\n  ")
    );
    assert!(over.is_empty(), "over their limits: {over:?}");
}
