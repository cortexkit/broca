//! Agent-run scope admission through real actors, with refusals checked against file WAL bytes.

use super::tests::{min_config, text_req, SeamOpener};
use super::*;
use crate::serve::{SendRequest, ServeError};
use crate::{LineageRegistry, ServeHandler};
use broca_core::{FileLeaseStore, FileWalStore};
use broca_protocol::{BindPrincipal, BindScope, SendScope, SessionScope};

struct UnexpectedAdmission;

#[async_trait::async_trait]
impl crate::executor::RunExecutor for UnexpectedAdmission {
    async fn execute(
        &self,
        _job: crate::executor::RunJob,
        _output: tokio::sync::mpsc::Sender<broca_core::OutputEvent>,
    ) -> Result<broca_core::RunSummary, broca_core::RunError> {
        Err(broca_core::RunError::Config(
            "unexpected admission in refusal test".into(),
        ))
    }
}

fn basal() -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: "basal".into(),
    }
}
fn scope() -> SessionScope {
    serde_json::from_str(r#"{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"parent-run","scope_epoch":1,"sender":{"kind":"reserved","module_id":"basal"},"agent_id":"agent-1","run_id":"parent-run"}"#).unwrap()
}

#[tokio::test]
async fn enabled_agent_run_refusals_write_no_wal_bytes() {
    for case in [
        "owner",
        "carrier",
        "missing-agent",
        "changed-run",
        "changed-agent",
        "foreign",
        "tools",
        "planned-tools",
        "adoption",
    ] {
        let root = broca_test_support::scratch_root()
            .join(format!("agent-run-refusal-{}-{case}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let wal_store = Arc::new(FileWalStore::new(root.join("wal")));
        let key = broca_protocol::BindIdentity::new(
            root.join("project"),
            "runner",
            format!("basal:codemode-parent-run:{case}"),
        );
        let frozen = matches!(
            case,
            "changed-run" | "changed-agent" | "foreign" | "adoption"
        );
        let mut wal = wal_store.open(&key).unwrap();
        if frozen {
            let principal = if case == "foreign" {
                scope().owner
            } else {
                basal()
            };
            let scope = if matches!(case, "foreign" | "adoption") {
                None
            } else {
                Some(scope())
            };
            let record: WalRecord = serde_json::from_value(serde_json::json!({
                "type": "run_started", "run_id": "seed", "session": key,
                "principal": principal, "scope": scope, "config": min_config(),
                "input": [], "origin": {"kind": "fresh"}
            }))
            .unwrap();
            wal.append(1, &record, true).unwrap();
            wal.append(
                1,
                &WalRecord::RunFinished {
                    reason: broca_protocol::caller::RunFinishReason::Completed,
                    ts_ms: None,
                    usage: Default::default(),
                    error: None,
                    retries_used: 0,
                },
                true,
            )
            .unwrap();
        }
        drop(wal);
        let before = std::fs::read(wal_store.wal_path(&key)).unwrap();
        let registry = LineageRegistry::new(Arc::new(SeamOpener {
            lease_store: Arc::new(FileLeaseStore::new(root.join("leases"))),
            wal_store: wal_store.clone(),
            idle_ttl: Duration::from_secs(3600),
            executor: Arc::new(UnexpectedAdmission),
        }));
        let mut stamp = scope();
        if case == "missing-agent" {
            stamp.agent_id = None;
        }
        if case == "changed-run" {
            stamp.run_id = Some("other-run".into());
        }
        if case == "changed-agent" {
            stamp.agent_id = Some("other-agent".into());
        }
        let principal = match case {
            "owner" => stamp.owner.clone(),
            "carrier" => BindPrincipal::Reserved {
                module_id: "other".into(),
            },
            _ => basal(),
        };
        let mut request = text_req(&key, "refused").to_submission();
        request.principal = Some(principal);
        request.scope = SendScope::Stamped(BindScope {
            scope: stamp,
            owner_authorized: true,
        });
        let tool: broca_protocol::request::Tool = serde_json::from_value(serde_json::json!({
            "name": "edit", "description": "Edit", "input_schema": {"type": "object"},
            "execution_mode": "parallel"
        }))
        .unwrap();
        if case == "tools" {
            request.tools = Some(vec![tool.clone()]);
        }
        let plan = if case == "planned-tools" {
            let manifest: broca_wal::PlanManifest = serde_json::from_value(serde_json::json!({
                "plan": "{\"tool_items\":[{}]}", "plan_identity": "test-plan", "composition_digest": "digest",
                "fetches": [], "tools": [tool], "system_text": "", "system_digest": "digest",
                "join_version": 1, "role_versions": {}
            })).unwrap();
            Some(SendPlan {
                identity: "test-plan".into(),
                manifest: Some(Arc::new(manifest)),
            })
        } else {
            None
        };
        // Enable agent-run writing before the send opens the actor: actor construction captures
        // this test-only flag's value. The current-thread runtime keeps construction here.
        AGENT_RUN_WRITER_FOR_TEST.set(true);
        let response = registry
            .send(SendRequest {
                retention: None,
                stop_sequences_named: false,
                title: None,
                conversation_key: None,
                session: key.clone(),
                run_id: RunId("refused".into()),
                send_id: Some("refused".into()),
                delivery: Default::default(),
                request: Box::new(request),
                skip_materialization: false,
                kind: RunKind::FreshOrResume,
                plan,
            })
            .await;
        AGENT_RUN_WRITER_FOR_TEST.set(false);
        match case {
            "changed-run" => assert!(
                matches!(
                    response,
                    Err(ServeError::ScopeRefused(
                        broca_core::ScopeRefusal::RunMismatch {
                            field: broca_core::scope_admission::RunMismatchField::RunId
                        }
                    ))
                ),
                "{case}: {response:?}"
            ),
            "changed-agent" => assert!(
                matches!(
                    response,
                    Err(ServeError::ScopeRefused(
                        broca_core::ScopeRefusal::RunMismatch {
                            field: broca_core::scope_admission::RunMismatchField::AgentId
                        }
                    ))
                ),
                "{case}: {response:?}"
            ),
            "foreign" => assert!(
                matches!(
                    response,
                    Err(ServeError::ScopeRefused(
                        broca_core::ScopeRefusal::SenderNotOwner
                    ))
                ),
                "{case}: {response:?}"
            ),
            "missing-agent" => assert!(
                matches!(
                    response,
                    Err(ServeError::ScopeRefused(
                        broca_core::ScopeRefusal::FlowScopeRequired
                    ))
                ),
                "{case}: {response:?}"
            ),
            "tools" | "planned-tools" => {
                let error = response.expect_err("tool-bearing agent-run send must be refused");
                assert!(
                    error.to_string().contains("agent-run sends cannot carry"),
                    "{case}: {error}"
                );
            }
            _ => assert!(
                matches!(
                    response,
                    Err(ServeError::ScopeRefused(
                        broca_core::ScopeRefusal::OwnerMismatch(_)
                    ))
                ),
                "{case}: {response:?}"
            ),
        }
        assert_eq!(
            std::fs::read(wal_store.wal_path(&key)).unwrap(),
            before,
            "{case} appended WAL bytes"
        );
        assert!(registry.stop_and_seal().await);
        let _ = std::fs::remove_dir_all(root);
    }
}

#[tokio::test]
async fn writer_refuses_paused_retries_without_the_frozen_run_scope() {
    let root =
        broca_test_support::scratch_root().join(format!("agent-run-paused-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let wal_store = Arc::new(FileWalStore::new(root.join("wal")));
    let key = broca_protocol::BindIdentity::new(
        root.join("project"),
        "runner",
        "basal:codemode-parent-run:0",
    );
    let mut wal = wal_store.open(&key).unwrap();
    let started: WalRecord = serde_json::from_value(serde_json::json!({
        "type": "run_started", "run_id": "paused", "session": key,
        "principal": basal(), "scope": scope(), "config": min_config(),
        "input": [], "origin": {"kind": "fresh"}
    }))
    .unwrap();
    wal.append(1, &started, true).unwrap();
    let paused: WalRecord = serde_json::from_value(serde_json::json!({
        "type": "run_paused", "run_id": "paused", "reason": "auth_required"
    }))
    .unwrap();
    wal.append(1, &paused, true).unwrap();
    drop(wal);
    let before = std::fs::read(wal_store.wal_path(&key)).unwrap();
    let registry = LineageRegistry::new(Arc::new(SeamOpener {
        lease_store: Arc::new(FileLeaseStore::new(root.join("leases"))),
        wal_store: wal_store.clone(),
        idle_ttl: Duration::from_secs(3600),
        executor: Arc::new(UnexpectedAdmission),
    }));
    for send in [SendScope::Unstamped, SendScope::DaemonWithoutScopes] {
        let mut request = text_req(&key, "paused").to_submission();
        request.scope = send;
        request.principal = Some(basal());
        let result = registry
            .send(SendRequest {
                retention: None,
                stop_sequences_named: false,
                title: None,
                conversation_key: None,
                session: key.clone(),
                run_id: RunId("paused".into()),
                send_id: Some("paused".into()),
                delivery: Default::default(),
                request: Box::new(request),
                skip_materialization: false,
                kind: RunKind::FreshOrResume,
                plan: None,
            })
            .await;
        assert!(
            matches!(
                result,
                Err(ServeError::ScopeRefused(
                    broca_core::ScopeRefusal::RunMismatch {
                        field: broca_core::scope_admission::RunMismatchField::RunId
                    }
                ))
            ),
            "{result:?}"
        );
        assert_eq!(std::fs::read(wal_store.wal_path(&key)).unwrap(), before);
    }
    assert!(registry.stop_and_seal().await);
    let _ = std::fs::remove_dir_all(root);
}
