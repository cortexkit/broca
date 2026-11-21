// Included in executor::tests: the daemon's deployment-key loader against the vault stand-in
// above, through a real `VaultClient` that reads by grant, over a registry whose opener holds
// the session store as the daemon's does.

/// The deployment key the held session was created under. Not one repeated byte, so a partial
/// copy of it is as recognisable as the whole.
fn deployment_key() -> [u8; 32] {
    std::array::from_fn(|i| (i as u8).wrapping_mul(37).wrapping_add(11))
}

/// The record's value: the key as 64 lowercase hex characters.
fn deployment_key_hex() -> String {
    deployment_key()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

const DEPLOYMENT_KEY_RECORD: &str = "opaque:broca-deployment:1";

/// An executor for a module whose test never runs anything.
struct NeverRuns;

#[async_trait::async_trait]
impl broca_module::executor::RunExecutor for NeverRuns {
}

/// A state root holding one session in the session store (`held`), created under
/// [`deployment_key`], and a registry over it whose new storage awaits its key, wired as `main`
/// wires it.
struct KeyRig {
    root: std::path::PathBuf,
    registry: broca_module::LineageRegistry,
    slot: Arc<broca_module::store_open::StoreSlot>,
    held: BindIdentity,
}

impl Drop for KeyRig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

async fn key_rig(name: &str) -> KeyRig {
    let root = broca_test_support::scratch_root()
        .join(format!("store-key-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let held = BindIdentity::new(root.join("project"), "runner", "held");
    broca_session_store::session::Session::create(
        broca_session_store::backend::LocalBackend::new(root.join(broca_wal::SESSION_STORAGE_DIR)),
        broca_session_store::backend::SessionAddr(broca_core::session_addr(&held)),
        broca_session_store::head::SessionTriple {
            project_root: held.project_root.display().to_string(),
            harness: held.harness.clone(),
            session: held.session.clone(),
        },
        &broca_session_store::keys::DeploymentKey::new(1, &deployment_key()),
        false,
    )
    .await
    .unwrap();
    let slot = Arc::new(broca_module::store_open::StoreSlot::awaiting_key(&root, 1));
    let registry = broca_module::LineageRegistry::new(Arc::new(
        broca_module::DefaultOpener::new(
            Arc::new(broca_core::FileLeaseStore::new(root.join("leases"))),
            Arc::new(NeverRuns),
        )
        .with_idle_ttl(Duration::from_secs(600))
        .with_store_slot(Arc::clone(&slot)),
    ));
    KeyRig {
        root,
        registry,
        slot,
        held,
    }
}

impl KeyRig {
    /// The loader `main` runs, reading from `daemon` on a connection that presents the broca
    /// module's consumer identity, retrying quickly.
    async fn loader(
        &self,
        daemon: &CapturingVaultDaemon,
    ) -> broca_module_serve::store_key::KeyLoader {
        broca_module_serve::store_key::KeyLoader {
            source: Some(key_source(&self.root, daemon).await),
            slot: Arc::clone(&self.slot),
            setup: broca_module::store_open::StoreRuntimeSetup {
                state_root: self.root.clone(),
                executor: Arc::new(NeverRuns),
                idle_ttl: Duration::from_secs(600),
                import_roots: None,
                billing: None,
                run_events: None,
                send_states: None,
            },
            backoff: broca_module_serve::store_key::Backoff {
                first: Duration::from_millis(20),
                max: Duration::from_millis(100),
            },
        }
    }

    /// Whether the held session opens; the refusal when it doesn't.
    async fn open_held(&self) -> Result<(), broca_module::registry::OpenError> {
        self.registry.actor_for(&self.held).await.map(drop)
    }
}

async fn key_source(
    root: &std::path::Path,
    daemon: &CapturingVaultDaemon,
) -> Arc<dyn broca_module_serve::store_key::DeploymentKeySource> {
    let conn = broca_subc::SubcConnection::connect_with_consumer_identity(
        &daemon.connection_file,
        supervised_broca_identity(),
    )
    .await
    .unwrap();
    Arc::new(broca_subc::VaultClient::new(
        Arc::new(conn),
        broca_module_serve::consumer_routes::CREDENTIALS_MODULE_ID,
        &BindIdentity::new(root, "broca", "vault"),
        Duration::from_secs(2),
    ))
}

fn assert_key_unavailable(refused: Result<(), broca_module::registry::OpenError>) {
    match refused {
        Err(broca_module::registry::OpenError::Classified { reason, class }) => {
            assert!(reason.starts_with("key_unavailable:"), "{reason}");
            assert_eq!(class, broca_protocol::ErrorClass::Transient);
        }
        other => panic!("expected the transient key_unavailable refusal, got {other:?}"),
    }
}

/// Every log event, as its level, target and fields (the message included).
#[derive(Clone, Default)]
struct KeyLogs(Arc<std::sync::Mutex<Vec<(tracing::Level, String, String)>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for KeyLogs {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        struct Fields(String);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.push_str(&format!("{}={value:?} ", field.name()));
            }
        }
        let mut fields = Fields(String::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push((
            *event.metadata().level(),
            event.metadata().target().to_string(),
            fields.0,
        ));
    }
}

impl KeyLogs {
    /// Captures every event emitted on this thread until the guard drops. The test runtime is
    /// single-threaded, so the loader's events are among them.
    fn capture(&self) -> tracing::subscriber::DefaultGuard {
        use tracing_subscriber::prelude::*;
        tracing::subscriber::set_default(tracing_subscriber::registry().with(self.clone()))
    }

    fn events(&self) -> Vec<(tracing::Level, String, String)> {
        self.0.lock().unwrap().clone()
    }

    /// The session-storage events at `level`, without the slow-phase warnings a disk sync on a
    /// loaded machine logs under the same target: those say how fast the machine is, not
    /// anything about the key.
    fn at(&self, level: tracing::Level) -> Vec<String> {
        self.events()
            .into_iter()
            .filter(|(at, target, fields)| {
                *at == level
                    && target == "session_storage"
                    && !fields.contains("slow session storage phase")
            })
            .map(|(_, _, fields)| fields)
            .collect()
    }
}

/// The spellings of the key a leak would show: its hex in either case, a prefix of it, its raw
/// bytes, and their `Debug` form.
fn key_spellings() -> Vec<Vec<u8>> {
    let hex = deployment_key_hex();
    let bytes = format!("{:?}", &deployment_key()[..8]);
    vec![
        hex.clone().into_bytes(),
        hex.to_uppercase().into_bytes(),
        hex.as_bytes()[..16].to_vec(),
        deployment_key().to_vec(),
        bytes.trim_end_matches(']').as_bytes().to_vec(),
    ]
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|window| window == needle)
}

/// Every file under `dir` that holds any spelling of the key.
fn files_holding_the_key(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                pending.push(path);
            } else if let Ok(bytes) = std::fs::read(&path) {
                if key_spellings().iter().any(|key| contains(&bytes, key)) {
                    found.push(path);
                }
            }
        }
    }
    found
}

/// THE DAEMON READS ITS DEPLOYMENT KEY BY GRANT AND SERVES A SESSION KEPT IN THE NEW STORAGE.
///
/// The read is exactly one `credential.get_scoped` naming `opaque:broca-deployment:1`, with no
/// lifetime demand, as the stand-in captured it off the socket, and nothing else is asked of the
/// vault. Health reports the key loaded.
#[tokio::test]
async fn a_vault_serving_the_deployment_key_loads_it_and_a_held_session_opens() {
    let rig = key_rig("served").await;
    let daemon = CapturingVaultDaemon::start(&rig.root).await;
    daemon.serve_deployment_key(DEPLOYMENT_KEY_RECORD, Some(deployment_key_hex().into_bytes()));
    let mut health = broca_module_serve::store_key::health_of(&rig.slot);
    let phases = health.as_object_mut().unwrap().remove("phases").unwrap();
    assert!(
        phases.is_object(),
        "health includes phase counters before the key loads"
    );
    assert_eq!(
        health,
        serde_json::json!({"key_id": 1, "loaded": false, "new_sessions": "encrypted"})
    );
    let loading = tokio::spawn(rig.loader(&daemon).await.run(rig.registry.clone()));
    broca_test_support::wait_until("the deployment key loaded", || async {
        if rig.slot.key_loaded() {
            Ok(())
        } else {
            Err("not loaded".to_string())
        }
    })
    .await;
    loading.await.unwrap();
    rig.open_held()
        .await
        .expect("the held session opens under the vault's key");
    let mut health = broca_module_serve::store_key::health_of(&rig.slot);
    let phases = health.as_object_mut().unwrap().remove("phases").unwrap();
    assert!(
        phases["open"]["count"].as_u64().is_some_and(|count| count > 0),
        "health includes the held session's real open"
    );
    assert_eq!(
        health,
        serde_json::json!({"key_id": 1, "loaded": true, "new_sessions": "encrypted"})
    );
    assert_eq!(
        daemon.requests(),
        vec![(
            "credential.get_scoped".to_string(),
            serde_json::json!({"credential_id": DEPLOYMENT_KEY_RECORD}),
        )]
    );
    daemon.assert_healthy();
}

/// WITHOUT THE RECORD, A HELD SESSION IS REFUSED UNTIL THE KEY ARRIVES.
///
/// The vault answers `not_found`, as it does for a missing record or a missing grant. The
/// module refuses the held session `key_unavailable` (transient), never reports the read to the
/// vault as an auth failure, and keeps reading. Once the record exists the key loads and the
/// held session is served, with no restart; the loader opens the session refused meanwhile, so
/// the recovery its open runs isn't left for its next command.
#[tokio::test]
async fn with_the_record_missing_a_held_session_is_refused_until_the_key_arrives() {
    let rig = key_rig("later").await;
    let daemon = CapturingVaultDaemon::start(&rig.root).await;
    let loading = tokio::spawn(rig.loader(&daemon).await.run(rig.registry.clone()));
    broca_test_support::wait_until("the loader to read again after not_found", || async {
        match daemon.scoped_reads().len() {
            reads if reads >= 2 => Ok(()),
            reads => Err(format!("{reads} reads")),
        }
    })
    .await;
    assert!(!rig.slot.key_loaded());
    assert_key_unavailable(rig.open_held().await);
    assert!(
        daemon
            .requests()
            .iter()
            .all(|(method, _)| method == "credential.get_scoped"),
        "a missing deployment key is never reported to the vault: {:?}",
        daemon.requests()
    );

    daemon.serve_deployment_key(DEPLOYMENT_KEY_RECORD, Some(deployment_key_hex().into_bytes()));
    loading.await.unwrap();
    assert!(rig.slot.key_loaded());
    assert_eq!(
        rig.registry.slot_state(&rig.held),
        Some("open"),
        "the loader opened the session refused before the key loaded"
    );
    rig.open_held()
        .await
        .expect("the held session is served once the key has loaded");
    daemon.assert_healthy();
}

/// A RECORD WHOSE VALUE ISN'T 64 LOWERCASE HEX CHARACTERS LOADS NO KEY.
///
/// Each malformed value is refused as malformed, the module keeps refusing the held session,
/// and no log repeats the value.
#[tokio::test]
async fn a_malformed_deployment_key_is_refused_and_no_key_loads() {
    let logs = KeyLogs::default();
    let _capture = logs.capture();
    let rig = key_rig("malformed").await;
    let daemon = CapturingVaultDaemon::start(&rig.root).await;
    let source = key_source(&rig.root, &daemon).await;
    let hex = deployment_key_hex();
    let malformed: Vec<Vec<u8>> = vec![
        hex.to_uppercase().into_bytes(),
        hex.as_bytes()[..62].to_vec(),
        format!("{hex}\n").into_bytes(),
        hex.replacen(|c: char| c.is_ascii_digit(), "z", 1).into_bytes(),
        deployment_key().to_vec(),
        Vec::new(),
    ];
    for value in &malformed {
        daemon.serve_deployment_key(DEPLOYMENT_KEY_RECORD, Some(value.clone()));
        assert!(
            matches!(
                broca_module_serve::store_key::read_key(source.as_ref(), 1).await,
                Err(broca_module_serve::store_key::KeyUnavailable::Malformed)
            ),
            "{:?} is malformed",
            String::from_utf8_lossy(value)
        );
    }
    let loading = tokio::spawn(rig.loader(&daemon).await.run(rig.registry.clone()));
    let reads = daemon.scoped_reads().len();
    broca_test_support::wait_until("the loader to read the malformed record twice", || async {
        match daemon.scoped_reads().len() - reads {
            loader_reads if loader_reads >= 2 => Ok(()),
            loader_reads => Err(format!("{loader_reads} reads")),
        }
    })
    .await;
    assert!(!rig.slot.key_loaded());
    assert_key_unavailable(rig.open_held().await);
    loading.abort();
    let warnings = logs.at(tracing::Level::WARN);
    assert_eq!(warnings.len(), 1, "one WARN while the key is missing: {warnings:?}");
    assert!(warnings[0].contains("reason=\"malformed\""), "{}", warnings[0]);
    for (_, _, fields) in logs.events() {
        for value in &malformed {
            let value = String::from_utf8_lossy(value);
            assert!(
                value.trim().is_empty() || !fields.contains(value.trim()),
                "a log repeated the record's value: {fields}"
            );
        }
    }
    daemon.assert_healthy();
}

/// THE DEPLOYMENT KEY NEVER REACHES DISK OR A LOG.
///
/// Through a missing record, its arrival, and a held session opened and served under it, no
/// file under the state root holds any spelling of the key, and no log event does: the one
/// WARN and the one INFO name the key id only.
#[tokio::test]
async fn the_deployment_key_never_reaches_disk_or_a_log() {
    let logs = KeyLogs::default();
    let _capture = logs.capture();
    let rig = key_rig("no-leak").await;
    let daemon = CapturingVaultDaemon::start(&rig.root).await;
    let loading = tokio::spawn(rig.loader(&daemon).await.run(rig.registry.clone()));
    broca_test_support::wait_until("the loader to find no record", || async {
        if daemon.scoped_reads().is_empty() {
            Err("no read yet".to_string())
        } else {
            Ok(())
        }
    })
    .await;
    assert_key_unavailable(rig.open_held().await);
    daemon.serve_deployment_key(DEPLOYMENT_KEY_RECORD, Some(deployment_key_hex().into_bytes()));
    loading.await.unwrap();
    rig.open_held().await.expect("the held session opens");
    let runtime = rig.slot.runtime().unwrap();
    runtime
        .current_view(&rig.held)
        .await
        .expect("the held session reads")
        .expect("the held session holds its creation");
    drop(_capture);

    assert_eq!(
        files_holding_the_key(&rig.root),
        Vec::<std::path::PathBuf>::new(),
        "no file under the state root holds the key"
    );
    for (level, target, fields) in logs.events() {
        for key in key_spellings() {
            assert!(
                !contains(fields.as_bytes(), &key),
                "a {level} log at {target} holds the key: {fields}"
            );
        }
    }
    let warnings = logs.at(tracing::Level::WARN);
    assert_eq!(warnings.len(), 1, "one WARN while the key is missing: {warnings:?}");
    assert!(warnings[0].contains("key_id=1"), "{}", warnings[0]);
    assert!(warnings[0].contains("reason=\"not_found\""), "{}", warnings[0]);
    let loaded: Vec<_> = logs
        .at(tracing::Level::INFO)
        .into_iter()
        .filter(|fields| fields.contains("has loaded"))
        .collect();
    assert_eq!(loaded.len(), 1, "one INFO when the key arrives: {loaded:?}");
    assert!(loaded[0].contains("key_id=1"), "{}", loaded[0]);
    daemon.assert_healthy();
}
