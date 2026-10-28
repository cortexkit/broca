use super::*;
use broca_core::tool_plane::{
    DispatchKey, DispatchRegistration, LateResultRegistration, LateResultsIntake,
    ProviderHeldLifecycle, ProviderHeldRecorder, ProviderHeldReply,
};
use broca_wal::{
    FetchOutcome, FrozenProviderDeclarations, FrozenResultFormat, OpaqueJson, PlanManifest,
    ProviderFetch,
};
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[path = "tests/dropped_media_tests.rs"]
mod dropped_media_tests;

struct Rig {
    identity: bool,
    declared_route: bool,
    reply: Mutex<serde_json::Value>,
    sent: Mutex<Vec<(Option<String>, Option<String>)>>,
    groups: Mutex<Vec<Vec<LateResultRegistration>>>,
    held_synced: AtomicBool,
    waits: AtomicUsize,
    fail_sync: AtomicBool,
    block_registration: AtomicBool,
    registering: tokio::sync::Notify,
    release_registration: tokio::sync::Notify,
    /// Whether each tool route this rig opened was asked to declare the held-reply capabilities.
    opened_declaring: Mutex<Vec<bool>>,
    opened_result: Mutex<Vec<bool>>,
}

impl Rig {
    /// The declarations each test needs come with its batch's frozen plan
    /// (`manifest_declaring`), so the fixture keeps no catalog of its own.
    fn new(identity: bool, declared_route: bool, _opt_in: bool, _late_results: bool) -> Arc<Self> {
        Arc::new(Self {
            identity,
            declared_route,
            reply: Mutex::new(json!({"content":[{"type":"text","text":"terminal"}]})),
            sent: Mutex::new(Vec::new()),
            groups: Mutex::new(Vec::new()),
            held_synced: AtomicBool::new(false),
            waits: AtomicUsize::new(0),
            fail_sync: AtomicBool::new(false),
            block_registration: AtomicBool::new(false),
            registering: tokio::sync::Notify::new(),
            opened_declaring: Mutex::new(Vec::new()),
            opened_result: Mutex::new(Vec::new()),
            release_registration: tokio::sync::Notify::new(),
        })
    }
    fn plane(self: &Arc<Self>, installed: bool) -> SubcToolPlane {
        let plane = SubcToolPlane::with_connection(self.clone(), "provider");
        if installed {
            plane
                .with_provider_held_support(self.clone(), self.clone())
                .declaring_held_reply_for_tests()
        } else {
            plane
        }
    }
    fn held(&self) {
        *self.reply.lock().unwrap() = json!({"content":[{"type":"text","text":"SECRET INTERIM"}],"held":{"reason":"running","detail":{"id":"opaque"}}});
    }
}

#[async_trait]
impl ToolPlaneConnection for Rig {
    fn consumer_module_id(&self) -> Option<&str> {
        self.identity.then_some("broca")
    }
    async fn catalog_list(&self) -> Result<Vec<CatalogEntry>, ConnError> {
        Ok(Vec::new())
    }
    async fn route_open(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&ScopeSelector>,
    ) -> Result<RouteHandle, ConnError> {
        Ok(RouteHandle::synthetic(7))
    }
    async fn route_open_dispatch(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&ScopeSelector>,
        tool_result_v1: bool,
        installed: bool,
    ) -> Result<RouteHandle, ConnError> {
        self.opened_declaring.lock().unwrap().push(installed);
        self.opened_result.lock().unwrap().push(tool_result_v1);
        let mut route = RouteHandle::synthetic(7);
        route.reads_tool_result = (tool_result_v1 || installed) && self.declared_route;
        route.reads_held_reply = installed && self.declared_route;
        Ok(route)
    }
    async fn route_close(&self, _: &RouteHandle) -> Result<(), ConnError> {
        Ok(())
    }
    fn route_is_open(&self, _: &RouteHandle) -> bool {
        true
    }
    async fn server_capabilities(&self) -> Result<Vec<String>, ConnError> {
        Ok(Vec::new())
    }
    async fn tool_call(
        &self,
        _: &RouteHandle,
        _: &str,
        _: &serde_json::Value,
        _: Option<&str>,
        _: Duration,
        _: Option<&str>,
    ) -> Result<serde_json::Value, ConnError> {
        panic!("dispatch must retain send certainty through the staged seam")
    }
    #[allow(clippy::too_many_arguments)]
    async fn tool_call_staged(
        &self,
        _: &RouteHandle,
        _: &ToolCall,
        _: Duration,
        _: Option<&str>,
        key: Option<&str>,
        _schema_pin: Option<&str>,
        _: &broca_core::tool_plane::DispatchSignals,
    ) -> Result<serde_json::Value, crate::connection::ApprovedCallError> {
        self.sent
            .lock()
            .unwrap()
            .push((key.map(str::to_owned), None));
        Ok(self.reply.lock().unwrap().clone())
    }
    async fn tool_call_approved(
        &self,
        _: &RouteHandle,
        _: &ToolCall,
        _: Duration,
        _: Option<&str>,
        args: &ApprovedArgs,
        _schema_pin: Option<&str>,
    ) -> Result<serde_json::Value, crate::connection::ApprovedCallError> {
        self.sent
            .lock()
            .unwrap()
            .push((Some(args.key.clone()), Some(args.bytes.clone())));
        Ok(self.reply.lock().unwrap().clone())
    }
}

#[async_trait]
impl LateResultsIntake for Rig {
    async fn register_group(&self, calls: &[LateResultRegistration]) -> Result<(), String> {
        self.groups.lock().unwrap().push(calls.to_vec());
        if self.block_registration.load(Ordering::SeqCst) {
            self.registering.notify_one();
            self.release_registration.notified().await;
        }
        Ok(())
    }
}
#[async_trait]
impl ProviderHeldRecorder for Rig {
    async fn sync_held(&self, held: &ProviderHeldReply) -> Result<(), String> {
        assert_eq!(held.call_key, "lineage:1");
        assert_eq!(held.module, "provider");
        assert_eq!(held.reason, "running");
        assert_eq!(held.detail_digest.as_ref().unwrap().len(), 64);
        if self.fail_sync.load(Ordering::SeqCst) {
            return Err("injected sync failure".into());
        }
        self.held_synced.store(true, Ordering::SeqCst);
        Ok(())
    }
}
#[async_trait]
impl ProviderHeldLifecycle for Rig {
    async fn wait(&self, _: &BatchCtx, call: &ToolCall, _: &ProviderHeldReply) -> DispatchResult {
        assert!(
            self.held_synced.load(Ordering::SeqCst),
            "lifecycle acted before the held record sync"
        );
        self.waits.fetch_add(1, Ordering::SeqCst);
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::Text {
                text: "late terminal".into(),
            },
            is_error: false,
        })
    }
}

fn ctx() -> BatchCtx {
    BatchCtx {
        media_capture: None,
        batch_id: BatchId("batch".into()),
        bind: BindIdentity {
            project_root: "/project".into(),
            harness: "broca".into(),
            session: "session".into(),
        },
        run_id: broca_protocol::RunId("run".into()),
        scope: None,
    }
}
fn call(id: &str) -> ToolCall {
    ToolCall {
        tool_call_id: ToolCallId(id.into()),
        tool_name: "tool".into(),
        input: json!({}),
        thought_signature: None,
    }
}
/// The batch's frozen plan as the run freezes it: a module in the plan, the tool it offers,
/// and whatever declarations the provider answered at admission.
fn manifest_declaring(opt_in: bool, late_results: bool) -> ToolManifest {
    manifest_format(opt_in.then_some("content/v1"), late_results)
}

fn manifest_format(format: Option<&str>, late_results: bool) -> ToolManifest {
    let declarations = FrozenProviderDeclarations {
        result_formats: format.map(|format| {
            std::collections::BTreeMap::from([(
                "tool".to_owned(),
                FrozenResultFormat(format.to_owned()),
            )])
        }),
        late_results: late_results.then_some(true),
        schema_pins: None,
    };
    let plan = PlanManifest {
        declarations: Vec::new(),
        plan: OpaqueJson::from_json_text(
            json!({"tool_items":[{"provider":"provider"}]}).to_string(),
        )
        .unwrap(),
        plan_identity: String::new(),
        composition_digest: String::new(),
        fetches: vec![ProviderFetch {
            provider: "provider".into(),
            outcome: FetchOutcome::Answered {
                catalog_digest: "digest".into(),
                tools: Vec::new().into(),
                text: None,
                result_ops: None,
                reply: None,
                declarations_frozen: Some(declarations),
                reference_slots: None,
            },
        }],
        tools: Vec::new().into(),
        system_text: String::new(),
        system_digest: String::new(),
        join_version: 1,
        role_versions: Default::default(),
    };
    ToolManifest::from_tools(vec![serde_json::from_value(json!({"name":"tool","description":"","input_schema":{},"module":"provider","execution_mode":"parallel"})).unwrap()])
        .with_plan_manifest(&plan)
        .unwrap()
}

fn manifest() -> ToolManifest {
    manifest_declaring(false, false)
}
fn registration(call: &ToolCall, key: &str, approved: bool) -> DispatchRegistration {
    DispatchRegistration {
        tool_call_id: call.tool_call_id.clone(),
        call_key: key.into(),
        approved,
    }
}
fn completed(result: DispatchResult) -> ToolResult {
    let DispatchResult::Completed(result) = result else {
        panic!("expected completed result, got {result:?}")
    };
    result
}

#[tokio::test]
async fn plain_keys_require_late_results_identity_and_installed_seams() {
    for installed in [false, true] {
        for identity in [false, true] {
            for late in [false, true] {
                let rig = Rig::new(identity, true, true, late);
                let plane = rig.plane(installed);
                let ctx = ctx();
                let call = call("call");
                plane
                    .batch_started(
                        &ctx,
                        std::slice::from_ref(&call),
                        &manifest_declaring(true, late),
                    )
                    .await
                    .unwrap();
                plane
                    .register_dispatch_group(&ctx, &[registration(&call, "lineage:1", false)])
                    .await
                    .unwrap();
                completed(
                    plane
                        .dispatch_keyed(
                            &ctx,
                            &call,
                            DispatchKey {
                                call_key: "lineage:1",
                                held_recorder: Some(rig.as_ref()),
                            },
                        )
                        .await,
                );
                let expected = (installed && identity && late).then(|| "lineage:1".to_owned());
                assert_eq!(rig.sent.lock().unwrap()[0].0, expected);
                assert_eq!(
                    rig.groups.lock().unwrap().len(),
                    usize::from(installed && identity && late)
                );
                assert_eq!(
                    rig.waits.load(Ordering::SeqCst),
                    0,
                    "ordinary registration must not start intake"
                );
            }
        }
    }
}

/// Production installs the held runtime without the debug-only route declaration. Every call
/// must then leave exactly as it did before the runtime existed, even to a provider whose frozen
/// plan declares `late_results` and a `content/v1` tool: the route declares no capability, a
/// plain call carries no key, nothing is registered with the intake, and the reply is read as
/// the legacy body.
#[tokio::test]
async fn an_installed_runtime_without_the_route_declaration_leaves_every_call_unchanged() {
    for installed in [false, true] {
        let rig = Rig::new(true, true, true, true);
        let plane = SubcToolPlane::with_connection(rig.clone(), "provider");
        let plane = if installed {
            plane.with_provider_held_support(rig.clone(), rig.clone())
        } else {
            plane
        };
        let ctx = ctx();
        let call = call("call");
        plane
            .batch_started(
                &ctx,
                std::slice::from_ref(&call),
                &manifest_declaring(true, true),
            )
            .await
            .unwrap();
        plane
            .register_dispatch_group(&ctx, &[registration(&call, "lineage:1", false)])
            .await
            .unwrap();
        let result = completed(
            plane
                .dispatch_keyed(
                    &ctx,
                    &call,
                    DispatchKey {
                        call_key: "lineage:1",
                        held_recorder: Some(rig.as_ref()),
                    },
                )
                .await,
        );
        assert_eq!(
            *rig.opened_declaring.lock().unwrap(),
            [false],
            "installed={installed}: the route must declare no held-reply capability"
        );
        assert_eq!(
            *rig.sent.lock().unwrap(),
            [(None, None)],
            "installed={installed}: a plain call must carry no key"
        );
        assert!(
            rig.groups.lock().unwrap().is_empty(),
            "installed={installed}: nothing may be registered"
        );
        assert_eq!(
            result,
            map_call_tool_result(
                &call.tool_call_id,
                &json!({"content":[{"type":"text","text":"terminal"}]})
            )
            .0,
            "installed={installed}: the reply is read as the legacy body"
        );
    }
}

#[tokio::test]
async fn no_key_is_sent_until_the_whole_dispatch_group_registration_syncs() {
    let rig = Rig::new(true, true, true, true);
    rig.block_registration.store(true, Ordering::SeqCst);
    let plane = Arc::new(rig.plane(true));
    let ctx = ctx();
    let calls = [call("first"), call("second")];
    plane
        .batch_started(&ctx, &calls, &manifest_declaring(true, true))
        .await
        .unwrap();
    let registrations = vec![
        registration(&calls[0], "lineage:1", false),
        registration(&calls[1], "lineage:2", true),
    ];
    let registering = {
        let plane = plane.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move { plane.register_dispatch_group(&ctx, &registrations).await })
    };
    tokio::time::timeout(Duration::from_secs(2), rig.registering.notified())
        .await
        .unwrap();
    let early = plane
        .dispatch_keyed(
            &ctx,
            &calls[0],
            DispatchKey {
                call_key: "lineage:1",
                held_recorder: None,
            },
        )
        .await;
    assert!(matches!(early, DispatchResult::NotSent { .. }));
    assert!(rig.sent.lock().unwrap().is_empty());
    rig.release_registration.notify_one();
    registering.await.unwrap().unwrap();
    completed(
        plane
            .dispatch_keyed(
                &ctx,
                &calls[0],
                DispatchKey {
                    call_key: "lineage:1",
                    held_recorder: None,
                },
            )
            .await,
    );
    completed(
        plane
            .dispatch_approved(
                &ctx,
                &calls[1],
                "{ \"z\":2, \"a\":1 }",
                "lineage:2",
                &Default::default(),
            )
            .await,
    );
    let groups = rig.groups.lock().unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].len(), 2);
    assert_eq!(groups[0][0].lineage, "lineage");
    assert_eq!(rig.sent.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn approved_keys_and_exact_argument_bytes_do_not_depend_on_late_results() {
    for installed in [false, true] {
        let rig = Rig::new(false, true, false, false);
        let plane = rig.plane(installed);
        let ctx = ctx();
        let call = call("approved");
        plane
            .batch_started(&ctx, std::slice::from_ref(&call), &manifest())
            .await
            .unwrap();
        let bytes = "{ \"z\":2, \"a\":1 }";
        completed(
            plane
                .dispatch_approved(&ctx, &call, bytes, "deferred:17", &Default::default())
                .await,
        );
        assert_eq!(
            rig.sent.lock().unwrap().as_slice(),
            &[(Some("deferred:17".into()), Some(bytes.into()))]
        );
        assert!(rig.groups.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn held_reply_requires_every_prerequisite_and_never_exposes_interim_content() {
    for missing in [
        "none",
        "seams",
        "route",
        "opt_in",
        "identity",
        "late_results",
        "key",
        "recorder",
        "is_error",
        "invalid_reason",
        "invalid_detail",
        "null_held",
    ] {
        let rig = Rig::new(
            missing != "identity",
            missing != "route",
            missing != "opt_in",
            missing != "late_results",
        );
        rig.held();
        match missing {
            "is_error" => {
                rig.reply.lock().unwrap()["isError"] = json!(true);
                rig.reply.lock().unwrap()["errorCode"] = json!("failed");
            }
            "invalid_reason" => rig.reply.lock().unwrap()["held"]["reason"] = json!("bad reason"),
            "invalid_detail" => rig.reply.lock().unwrap()["held"]["detail"] = json!(null),
            "null_held" => rig.reply.lock().unwrap()["held"] = json!(null),
            _ => {}
        }
        let plane = rig.plane(missing != "seams");
        let ctx = ctx();
        let call = call("call");
        plane
            .batch_started(
                &ctx,
                std::slice::from_ref(&call),
                &manifest_declaring(missing != "opt_in", missing != "late_results"),
            )
            .await
            .unwrap();
        plane
            .register_dispatch_group(&ctx, &[registration(&call, "lineage:1", false)])
            .await
            .unwrap();
        let result = completed(if missing == "key" {
            plane.dispatch(&ctx, &call).await
        } else {
            plane
                .dispatch_keyed(
                    &ctx,
                    &call,
                    DispatchKey {
                        call_key: "lineage:1",
                        held_recorder: (missing != "recorder").then_some(rig.as_ref()),
                    },
                )
                .await
        });
        assert!(
            !serde_json::to_string(&result)
                .unwrap()
                .contains("SECRET INTERIM"),
            "{missing}"
        );
        assert_eq!(result.is_error, missing != "none", "{missing}");
        assert_eq!(
            rig.waits.load(Ordering::SeqCst),
            usize::from(missing == "none"),
            "{missing}"
        );
        if missing == "none" {
            assert_eq!(
                result.output,
                ToolOutput::Text {
                    text: "late terminal".into()
                }
            );
        }
    }
}

#[tokio::test]
async fn undeclared_held_reply_is_a_protocol_refusal_not_a_final_answer() {
    let rig = Rig::new(true, true, true, true);
    rig.held();
    let plane = rig.plane(false);
    let ctx = ctx();
    let call = call("call");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&call),
            &manifest_declaring(true, true),
        )
        .await
        .unwrap();
    let result = completed(plane.dispatch(&ctx, &call).await);
    assert_eq!(
        result.output,
        ToolOutput::error("malformed provider-held reply: prerequisites not met")
    );
    assert!(result.is_error);
    assert_eq!(rig.waits.load(Ordering::SeqCst), 0);
    assert!(!rig.held_synced.load(Ordering::SeqCst));
}

#[tokio::test]
async fn held_record_sync_failure_cannot_join_a_waiting_slot() {
    let rig = Rig::new(true, true, true, true);
    rig.held();
    rig.fail_sync.store(true, Ordering::SeqCst);
    let plane = rig.plane(true);
    let ctx = ctx();
    let call = call("call");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&call),
            &manifest_declaring(true, true),
        )
        .await
        .unwrap();
    plane
        .register_dispatch_group(&ctx, &[registration(&call, "lineage:1", false)])
        .await
        .unwrap();
    assert!(matches!(
        plane
            .dispatch_keyed(
                &ctx,
                &call,
                DispatchKey {
                    call_key: "lineage:1",
                    held_recorder: Some(rig.as_ref())
                }
            )
            .await,
        DispatchResult::SentButOutcomeUnknown { .. }
    ));
    assert_eq!(rig.waits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn negotiated_final_content_uses_only_the_frozen_opt_in() {
    // A live catalog that no longer lists the opt-in cannot change how this batch reads a
    // reply: the session froze the tool's format when it was admitted. A missing errorCode
    // distinguishes strict negotiated validation from legacy decoding even though valid text
    // now has the same stored shape on both paths.
    let rig = Rig::new(true, true, true, true);
    *rig.reply.lock().unwrap() = json!({"content":[{"type":"text","text":"model text"}],"isError":true,"structuredContent":{"private":"program data"}});
    let plane = rig.plane(true);
    let ctx = ctx();
    let call = call("call");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&call),
            &manifest_declaring(true, true),
        )
        .await
        .unwrap();
    let result = completed(plane.dispatch(&ctx, &call).await);
    assert!(result.is_error);
    let ToolOutput::Error { message, .. } = result.output else {
        panic!("invalid negotiated errors must be typed tool errors")
    };
    assert!(message.contains("errorCode"), "{message}");
    plane.batch_finished(&ctx).await.unwrap();
    let ctx = BatchCtx {
        batch_id: BatchId("next".into()),
        ..ctx
    };
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&call),
            &manifest_declaring(false, false),
        )
        .await
        .unwrap();
    let result = completed(plane.dispatch(&ctx, &call).await);
    assert_eq!(result.output, ToolOutput::error("model text"));
}

#[tokio::test]
async fn staged_plain_key_reaches_the_real_guarded_wire_envelope() {
    let (conn, route, mut server) = SubcConnection::in_process_for_test(7, 1);
    let call = call("call");
    let signals = broca_core::tool_plane::DispatchSignals::default();
    let dispatch = ToolPlaneConnection::tool_call_staged(
        &conn,
        &route,
        &call,
        Duration::from_secs(2),
        Some("preset"),
        Some("lineage:1"),
        None,
        &signals,
    );
    tokio::pin!(dispatch);
    let request = tokio::select! {
        result = &mut dispatch => panic!("premature reply: {result:?}"),
        request = server.next_request() => request,
    };
    let raw: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(raw["call_key"], "lineage:1");
    assert_eq!(raw["tool_call_id"], "call");
    assert_eq!(raw["preset"], "preset");
    assert!(server.reply(
        &request,
        serde_json::to_vec(&json!({"content":[{"type":"text","text":"terminal"}]})).unwrap()
    ));
    dispatch.await.unwrap();
}

#[tokio::test]
async fn approved_provider_held_reply_keeps_its_deferred_anchor() {
    let rig = Rig::new(true, true, true, true);
    rig.held();
    let plane = rig.plane(true);
    let ctx = ctx();
    let call = call("call");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&call),
            &manifest_declaring(true, true),
        )
        .await
        .unwrap();
    plane
        .register_dispatch_group(&ctx, &[registration(&call, "lineage:1", true)])
        .await
        .unwrap();
    let bytes = "{ \"z\":2, \"a\":1 }";
    let result = completed(
        plane
            .dispatch_approved_keyed(
                &ctx,
                &call,
                bytes,
                DispatchKey {
                    call_key: "lineage:1",
                    held_recorder: Some(rig.as_ref()),
                },
                &Default::default(),
            )
            .await,
    );
    assert_eq!(
        result.output,
        ToolOutput::Text {
            text: "late terminal".into()
        }
    );
    assert_eq!(
        rig.sent.lock().unwrap().as_slice(),
        &[(Some("lineage:1".into()), Some(bytes.into()))]
    );
    assert_eq!(rig.waits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[cfg(feature = "test-attachments")]
async fn negotiated_media_dispatch_captures_with_nullable_error_code() {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use broca_core::{
        media_store::{MediaStore, MediaStoreClass},
        tool_media::ToolMediaCapture,
        wal_writer::WalStore,
    };
    use broca_protocol::ToolOutputPart;
    let data = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC";
    let rig = Rig::new(true, true, true, true);
    *rig.reply.lock().unwrap() = json!({"content":[
        {"type":"text","text":"before"},
        {"type":"image","mimeType":"image/png","data":data,"width":1,"height":1},
        {"type":"text","text":"after"}
    ],"errorCode":null});
    let plane = rig.plane(true);
    let mut ctx = ctx();
    let root =
        broca_test_support::scratch_root().join(format!("negotiated-media-{}", std::process::id()));
    let wal = broca_core::FileWalStore::new(root.join("wal"));
    drop(wal.open(&ctx.bind).unwrap());
    ctx.media_capture = Some(ToolMediaCapture::new(
        wal.uploads().unwrap(),
        ctx.bind.clone(),
    ));
    let call = call("captured");
    plane
        .batch_started(&ctx, std::slice::from_ref(&call), &manifest())
        .await
        .unwrap();
    let result = completed(plane.dispatch(&ctx, &call).await);
    assert_eq!(result.tool_call_id, call.tool_call_id);
    assert!(!result.is_error);
    let ToolOutput::Parts { parts } = &result.output else {
        panic!("negotiated image lost its parts")
    };
    assert_eq!(parts.len(), 3);
    assert_eq!(
        parts[0],
        ToolOutputPart::Text {
            text: "before".into()
        }
    );
    assert_eq!(
        parts[2],
        ToolOutputPart::Text {
            text: "after".into()
        }
    );
    let ToolOutputPart::Media { media } = &parts[1] else {
        panic!("negotiated image was not captured")
    };
    let store = MediaStore::open(&root, MediaStoreClass::NonDurable).unwrap();
    assert_eq!(
        store.read_verified(media).unwrap(),
        STANDARD.decode(data).unwrap()
    );
    assert_eq!(completed(plane.dispatch(&ctx, &call).await), result);
    assert_eq!(rig.sent.lock().unwrap().len(), 1);
}

/// The batch's frozen plan with the catalog schema pins a later build freezes at admission,
/// and no other declaration.
fn manifest_pinning(
    schema_pins: Option<std::collections::BTreeMap<String, broca_wal::DispatchSchemaPin>>,
) -> ToolManifest {
    let plan = PlanManifest {
        declarations: Vec::new(),
        plan: OpaqueJson::from_json_text(
            json!({"tool_items":[{"provider":"provider"}]}).to_string(),
        )
        .unwrap(),
        plan_identity: String::new(),
        composition_digest: String::new(),
        fetches: vec![ProviderFetch {
            provider: "provider".into(),
            outcome: FetchOutcome::Answered {
                catalog_digest: "digest".into(),
                tools: Vec::new().into(),
                text: None,
                result_ops: None,
                reply: None,
                declarations_frozen: schema_pins.map(|schema_pins| FrozenProviderDeclarations {
                    schema_pins: Some(schema_pins),
                    ..Default::default()
                }),
                reference_slots: None,
            },
        }],
        tools: Vec::new().into(),
        system_text: String::new(),
        system_digest: String::new(),
        join_version: 1,
        role_versions: Default::default(),
    };
    ToolManifest::from_tools(vec![serde_json::from_value(json!({"name":"tool","description":"","input_schema":{},"module":"provider","execution_mode":"parallel"})).unwrap()])
        .with_plan_manifest(&plan)
        .unwrap()
}

/// A run admitted by a build that freezes catalog schema pins, resumed here, dispatches each
/// call under the pin its plan recorded for that tool, never one from a later catalog. A plan
/// without pins, which is every plan this build admits itself, leaves the pin unknown.
#[tokio::test]
async fn a_resumed_batch_dispatches_under_the_schema_pin_its_plan_recorded() {
    let pin = broca_wal::DispatchSchemaPin {
        schema_digest: "634bdb1fe768fce0974f9c89e6f15f889470a8288277c6433e4711ab6a5b9335".into(),
        semantics: 7,
    };
    let call = call("call");
    for (pins, expected) in [
        (
            Some(std::collections::BTreeMap::from([(
                "tool".to_owned(),
                pin.clone(),
            )])),
            Some(pin.clone()),
        ),
        (None, None),
    ] {
        let rig = Rig::new(true, true, false, false);
        let plane = rig.plane(false);
        let ctx = ctx();
        plane
            .batch_started(&ctx, std::slice::from_ref(&call), &manifest_pinning(pins))
            .await
            .unwrap();
        assert_eq!(
            plane.dispatch_schema_pin(&ctx, &call.tool_call_id, &call.tool_name),
            expected
        );
        // A pin belongs to the tool it was recorded for.
        assert_eq!(
            plane.dispatch_schema_pin(&ctx, &call.tool_call_id, "another_tool"),
            None
        );
    }
}
