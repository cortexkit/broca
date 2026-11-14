use super::*;
use broca_core::{AllowAll, FileLeaseStore, FileWalStore, OutputEvent, RunError, RunSummary};
use broca_module::actor::{SendKey, SendOutcome, SendPlan};
use broca_module::executor::{
    RunExecutor, RunJob, RunKind, StoreRunEnd, StoreRunError, StoreRunJob, StoreRunMode,
};
use broca_module::{DefaultOpener, LineageRegistry};
use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason};
use broca_protocol::{MessageId, NormalizedEvent, ProviderControl, SubmissionRequest, Usage};
use broca_session_loop::writer::Writer;
use broca_session_store::record::RunStart;

// These fixtures intentionally use AFT's real provider/preset cache key. Keep their separate
// daemon peers from invalidating one another when cargo test runs tests in one process.
static AFT_SESSION_TESTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Holding;
struct LiveScope;
#[async_trait]
impl ScopeDescriber for LiveScope {
    async fn describe(&self, owner: &Principal, scope_ref: &str) -> Result<DescribedScope, String> {
        Ok(DescribedScope {
            status: ScopeStatus::Live,
            scope_epoch: Some(1),
            owner_synced: true,
            owner_configured: true,
            stamp: Some(
                ScopeStamp::new(
                    owner.clone(),
                    scope_ref,
                    1,
                    subc_protocol::scope::ScopeKind::Head,
                )
                .with_owner_authorized(true),
            ),
        })
    }
}
#[async_trait]
impl LateResultsIntake for Holding {
    async fn register_group(&self, _: &[LateResultRegistration]) -> Result<(), String> {
        Ok(())
    }
    async fn prune(&self, _: &[(String, String)]) -> Result<(), String> {
        Ok(())
    }
}
#[async_trait]
impl ProviderHeldLifecycle for Holding {
    async fn wait(&self, _: &BatchCtx, _: &ToolCall, _: &ProviderHeldReply) -> DispatchResult {
        std::future::pending().await
    }
}

struct Executor {
    plane: SubcToolPlane,
    provider: broca_core::provider::StubProvider,
    starts: Mutex<Vec<RunStart>>,
    pause_once: AtomicBool,
    resumed_transport: Mutex<Vec<BTreeMap<String, FrozenProviderDeclarations>>>,
    resume_gate: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
}

#[async_trait]
impl RunExecutor for Executor {
    async fn execute(
        &self,
        _: RunJob,
        _: tokio::sync::mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        unreachable!("this fixture only opens new storage")
    }
    async fn execute_on_store(
        &self,
        job: StoreRunJob,
        out: tokio::sync::mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        let StoreRunJob {
            mut req,
            control,
            mut writer,
            mut parts,
            mode,
            ..
        } = job;
        let result = async {
            let now = || 1;
            let holding = Holding;
            let mut seams = broca_session_loop::run::Seams {
                provider: &self.provider,
                tools: &self.plane,
                bind: req.session.clone(),
                out: Some(&out),
                cancel: control.cancel.clone().unwrap_or_default(),
                stop: control.stop_requested.clone().unwrap_or_default(),
                held: Some(&holding),
                approval: &AllowAll,
                hook_plane: None,
                now_ms: &now,
                session_address: None,
                compaction_plane: None,
                compaction_session: None,
                compaction_changes: false,
                keep_warm: None,
                account_reports: None,
                max_same_account_wait: Default::default(),
                auth_selection: None,
                step_progress: control.step_progress.clone(),
                model_call_ready: None,
                pause_for_auth_at_start: self.pause_once.swap(false, Ordering::SeqCst).then(|| {
                    serde_json::from_value(json!({"class":"auth_required","message":"sign in"}))
                        .unwrap()
                }),
                account_continue: None,
            };
            let mut result = match mode {
                StoreRunMode::Fresh => {
                    let admission = broca_session_loop::admit::admission(
                        &mut req,
                        &control,
                        &self.provider,
                        writer.operations(),
                        Some(1),
                    )
                    .map_err(StoreRunError::Admission)?;
                    broca_session_loop::run(&mut writer, parts.as_mut(), admission, &seams).await
                }
                StoreRunMode::Seal(cause) => {
                    broca_session_loop::seal(&mut writer, parts.as_ref(), &seams, cause).await
                }
                _ => broca_session_loop::resume(&mut writer, parts.as_ref(), &seams).await,
            };
            let resume_gate = self.resume_gate.lock().unwrap().clone();
            if let Some((reached, release)) = &resume_gate {
                // Public sign-in continuation is not served on new storage yet. Exercise the
                // real durable loop's resume entry on the actor's writer instead.
                self.starts.lock().unwrap().push(
                    writer
                        .operations()
                        .run
                        .as_ref()
                        .unwrap()
                        .start
                        .as_ref()
                        .unwrap()
                        .clone(),
                );
                reached.notify_one();
                release.notified().await;
                seams.pause_for_auth_at_start = None;
                result = broca_session_loop::resume(&mut writer, parts.as_ref(), &seams).await;
            }
            let start = writer
                .operations()
                .run
                .as_ref()
                .unwrap()
                .start
                .as_ref()
                .unwrap();
            self.starts.lock().unwrap().push(start.clone());
            if mode != StoreRunMode::Fresh || resume_gate.is_some() {
                let admission =
                    broca_session_loop::run::admission_of_start(start, parts.as_ref()).unwrap();
                self.resumed_transport
                    .lock()
                    .unwrap()
                    .push(admission.transport);
            }
            result.map_err(StoreRunError::Loop)
        }
        .await;
        StoreRunEnd { parts, result }
    }
}

fn step(tool: bool) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(if tool { "tool-step" } else { "done-step" }.into()),
                content: if tool {
                    vec![ContentBlock::ToolCall(ToolCall {
                        thought_signature: None,
                        tool_call_id: ToolCallId("bash-call".into()),
                        tool_name: "bash".into(),
                        input: json!({}).into(),
                    })]
                } else {
                    vec![ContentBlock::Text {
                        text: "done".into(),
                    }]
                },
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            finish_reason: if tool {
                FinishReason::ToolCalls
            } else {
                FinishReason::Stop
            },
            usage: Usage::default(),
            retries_used: 0,
        }),
    ]
}

async fn send(
    registry: &LineageRegistry,
    bind: &BindIdentity,
    run: &str,
    plan: Option<broca_wal::PlanManifest>,
) {
    let mut request: SubmissionRequest = serde_json::from_value(json!({
        "model":{"provider_module_id":"stub","model_id":"stub"},
        "input":[{"role":"user","content":[{"type":"text","text":"work"}]}]
    }))
    .unwrap();
    let owner = broca_protocol::BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    };
    request.principal = Some(owner.clone());
    request.scope = broca_protocol::SendScope::Stamped(broca_protocol::BindScope {
        scope: SessionScope {
            owner,
            agent_id_frozen: false,
            scope_ref: "head-transport".into(),
            scope_epoch: 1,
            flow_id: None,
            sender: None,
            agent_id: None,
            run_id: None,
        },
        owner_authorized: true,
        kind: broca_protocol::ScopeKind::Head,
        parent: None,
        parent_state: None,
    });
    let outcome = registry
        .route_submission_with_order(
            bind,
            RunId(run.into()),
            request,
            RunKind::FreshOrResume,
            SendKey::Minted,
            false,
            broca_protocol::Delivery::Queue,
            plan.map(|manifest| SendPlan {
                identity: manifest.plan_identity.clone(),
                manifest: Some(Arc::new(manifest)),
                model_check: None,
            }),
            None,
            None,
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
}

struct Session {
    executor: Arc<Executor>,
    registry: LineageRegistry,
    bind: BindIdentity,
    wire: Arc<Wire>,
    provider: InProcessModuleEnd,
}

async fn session(tag: &str, steps: Vec<Vec<NormalizedEvent>>, tagged: bool) -> Session {
    let (wire, provider) = wire();
    *wire.catalog.lock().unwrap() = Some(crate::subc_plane::tests::run_transport_tests::catalog(
        tagged, true,
    ));
    let holding = Arc::new(Holding);
    let plane = SubcToolPlane::with_connection(wire.clone(), "aft")
        .with_provider_held_support(holding.clone(), holding);
    plane.set_scope_describer(Arc::new(LiveScope));
    let executor = Arc::new(Executor {
        plane,
        provider: broca_core::provider::StubProvider::new(steps),
        starts: Mutex::default(),
        pause_once: AtomicBool::new(false),
        resumed_transport: Mutex::default(),
        resume_gate: Mutex::default(),
    });
    let dir = broca_test_support::scratch_root()
        .join(format!("run-transport-{tag}-{}", std::process::id()));
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let runtime = broca_module::store_open::StoreRuntime::open(
        &dir,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        executor.clone(),
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    let opener = DefaultOpener::new(
        Arc::new(FileLeaseStore::new(dir.join("leases"))),
        wal.clone(),
        executor.clone(),
    )
    .with_store_runtime(Arc::new(runtime));
    let registry =
        LineageRegistry::new_with_reader(Arc::new(opener), wal, None).with_store_new_sessions();
    let bind = BindIdentity::new(dir.join("project"), "runner", "transport");
    executor.plane.invalidate_route(&bind);
    Session {
        executor,
        registry,
        bind,
        wire,
        provider,
    }
}

async fn starts(executor: &Executor, count: usize) {
    broca_test_support::wait_until("store run reaches a pause or end", || {
        std::future::ready(if executor.starts.lock().unwrap().len() >= count {
            Ok(())
        } else {
            Err("run still active".into())
        })
    })
    .await;
}

#[tokio::test]
async fn fresh_store_run_learns_aft_yield_tags_and_restart_holds_without_cancel() {
    let _serial = AFT_SESSION_TESTS.lock().await;
    let logs = broca_test_support::LogCapture::observe(|_| true);
    let result = tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
        let Session {
            executor,
            registry,
            bind,
            wire,
            mut provider,
        } = session("store", vec![step(false), step(true)], false).await;
        let fixtures = crate::subc_plane::tests::run_transport_tests::catalog;
        let frozen = crate::subc_plane::tests::run_transport_tests::plan("aft", true);
        send(&registry, &bind, "first-run", Some(frozen)).await;
        starts(&executor, 1).await;
        assert!(executor.starts.lock().unwrap()[0]
            .run_choices
            .as_ref()
            .unwrap()
            .get("transport")
            .is_none());
        *wire.catalog.lock().unwrap() = Some(fixtures(true, true));
        // A learned-dead route invalidates the provider's cached facts, as a provider restart
        // does in production. The minute-bound cache is tested separately with a paused clock.
        executor.plane.invalidate_route(&bind);
        send(&registry, &bind, "second-run", None).await;
        let original = broca_test_support::wait_until("second run dispatches bash", || {
            std::future::ready(provider.try_frame().ok_or_else(|| {
                format!(
                    "no frame; starts={:?}; calls={:?}",
                    executor.starts.lock().unwrap(),
                    executor.provider.calls()
                )
            }))
        })
        .await;
        let original_body: serde_json::Value = serde_json::from_slice(&original.body).unwrap();
        assert_eq!(original_body["name"], "bash");
        assert_eq!(original_body["preset"], "worker");
        let observe = async {
            let frame = broca_test_support::wait_until("yield or cancel after runner stop", || {
                std::future::ready(
                    provider
                        .try_frame()
                        .ok_or_else(|| "no shutdown frame yet".into()),
                )
            })
            .await;
            assert_ne!(
                frame.header.ty,
                FrameType::Cancel,
                "inherited declarations send CANCEL instead of yielding the later-tagged bash"
            );
            let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
            assert_eq!(body["name"], "tool.yield");
            assert_eq!(body["arguments"]["call_key"], original_body["call_key"]);
            assert_eq!(
                (frame.header.channel, frame.header.epoch),
                (original.header.channel, original.header.epoch)
            );
            assert!(provider.reply(&frame, br#"{"answer":"accepted"}"#.to_vec()));
            assert!(provider.reply(
                &original,
                serde_json::to_vec(
                    &json!({"content":[],"held":{"reason":"running","yielded":true}})
                )
                .unwrap()
            ));
        };
        let (sealed, ()) = tokio::join!(registry.stop_and_seal(), observe);
        assert!(sealed);
        assert!(
            provider.try_frame().is_none(),
            "exactly one yield and no CANCEL"
        );
        let starts = executor.starts.lock().unwrap();
        let second = starts
            .iter()
            .find(|start| start.run_id == "second-run")
            .unwrap();
        assert_eq!(
            second.run_choices.as_ref().unwrap()["transport"]["aft"]["yield_tools"],
            json!(["bash", "bash_watch"])
        );
        assert_eq!(wire.catalog_reads.load(Ordering::SeqCst), 2);
        assert_eq!(
            starts[0].setup, second.setup,
            "transport does not change setup-part interning"
        );
    })
    .await;
    assert!(
        result.is_ok(),
        "store restart fixture timed out; logs={:?}",
        logs.events()
    );
}

async fn paused_transport_resume(assert_recorded: bool) {
    let _serial = AFT_SESSION_TESTS.lock().await;
    tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
        let Session {
            executor,
            registry,
            bind,
            wire,
            mut provider,
        } = session(
            if assert_recorded {
                "resume-facts"
            } else {
                "resume-rollback"
            },
            vec![step(false)],
            true,
        )
        .await;
        let reached = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *executor.resume_gate.lock().unwrap() = Some((reached.clone(), release.clone()));
        executor.pause_once.store(true, Ordering::SeqCst);
        send(
            &registry,
            &bind,
            "frozen-run",
            Some(crate::subc_plane::tests::run_transport_tests::plan(
                "aft", true,
            )),
        )
        .await;
        reached.notified().await;
        let start = executor.starts.lock().unwrap()[0].clone();
        assert_eq!(
            start.run_choices.as_ref().unwrap()["transport"]["aft"]["yield_tools"],
            json!(["bash", "bash_watch"])
        );
        // Even invalidation and a contradictory live catalog cannot change an existing run.
        *wire.catalog.lock().unwrap() = Some(
            crate::subc_plane::tests::run_transport_tests::catalog(false, true),
        );
        executor.plane.invalidate_route(&bind);
        release.notify_one();
        starts(&executor, 2).await;
        assert_eq!(
            wire.catalog_reads.load(Ordering::SeqCst),
            1,
            "a resume never re-reads transport"
        );
        assert_eq!(
            executor.starts.lock().unwrap()[1],
            start,
            "resume writes no replacement RunStart"
        );
        if assert_recorded {
            let resumed = executor.resumed_transport.lock().unwrap();
            assert_eq!(
                resumed[0]["aft"].yield_tools.as_ref().unwrap(),
                &BTreeSet::from(["bash".into(), "bash_watch".into()])
            );
        }
        assert_eq!(
            executor.provider.calls().len(),
            1,
            "resume reaches the model and completes"
        );
        assert!(registry.stop_and_seal().await);
        assert!(provider.try_frame().is_none());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn run_transport_resume_keeps_its_recorded_facts_after_catalog_changes() {
    paused_transport_resume(true).await;
}

#[tokio::test]
async fn run_transport_rollback_reader_can_ignore_the_member_and_resume() {
    // This compatibility case is also run with the transport reader disabled, matching a
    // rollback build: an unrecognized run_choices.transport member must neither refuse
    // continuation nor replace the existing run with a new one.
    paused_transport_resume(false).await;
}

#[tokio::test]
async fn run_transport_reread_leaves_rendered_request_bytes_identical() {
    let _serial = AFT_SESSION_TESTS.lock().await;
    use broca_provider::families::{openai_chat_frozen_config, OpenAiChat};
    use broca_provider::wire::WireFamily;
    let mut requests = Vec::new();
    for tagged in [false, true] {
        let tag = if tagged {
            "bytes-updated"
        } else {
            "bytes-frozen"
        };
        let Session {
            executor,
            registry,
            bind,
            ..
        } = session(tag, vec![step(false)], tagged).await;
        send(
            &registry,
            &bind,
            "request-run",
            Some(crate::subc_plane::tests::run_transport_tests::plan(
                "aft", true,
            )),
        )
        .await;
        starts(&executor, 1).await;
        assert_eq!(
            executor.starts.lock().unwrap()[0]
                .run_choices
                .as_ref()
                .unwrap()
                .get("transport")
                .is_some(),
            tagged
        );
        let calls = executor.provider.calls();
        assert_eq!(calls.len(), 1);
        requests.push(OpenAiChat.render(&openai_chat_frozen_config("test-model"), &calls[0]));
        assert!(registry.stop_and_seal().await);
    }
    assert_eq!(
        requests[0], requests[1],
        "re-read changes no rendered request bytes"
    );
}
