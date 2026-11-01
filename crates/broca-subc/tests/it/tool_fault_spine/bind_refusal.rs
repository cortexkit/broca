//! A module's bind refusal crosses the daemon as a route.open Error, before tool dispatch.

use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct BindRefusal {
    code: &'static str,
    refusals: usize,
    binds: AtomicUsize,
    calls: AtomicUsize,
}

struct Handler(Arc<BindRefusal>);

const BIND_MESSAGE: &str =
    "job illustrative-inspect holds a root reader; retry once it finishes or is cancelled";

#[async_trait::async_trait]
impl subc_client_rs::ModuleHandler for Handler {
    async fn on_bind(&self, _: &subc_client_rs::RouteBindRequest) -> subc_client_rs::BindDecision {
        let attempt = self.0.binds.fetch_add(1, Ordering::SeqCst);
        if attempt < self.0.refusals {
            subc_client_rs::BindDecision::reject(self.0.code, BIND_MESSAGE)
        } else {
            subc_client_rs::BindDecision::accept()
        }
    }

    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        _: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        subc_client_rs::HandlerOutcome::Response(
            serde_json::to_vec(&json!({"content": [{"type": "text", "text": "served"}]})).unwrap(),
        )
    }
}

async fn check_bind_refusal(code: &'static str, retryable: bool) {
    use broca_core::{no_dispatch_result, WalStore};
    use subc_protocol::manifest::{
        Concurrency, ExecutionMode, IdentityScope, ModuleManifest, ProviderRole, Tool,
    };
    let tag = format!("bind-refusal-{code}");
    let Some(rig) = Rig::start_with_modules(&tag, vec![]).await else {
        return;
    };
    let stub = Arc::new(BindRefusal {
        code,
        refusals: 2,
        binds: AtomicUsize::new(0),
        calls: AtomicUsize::new(0),
    });
    let manifest = ModuleManifest::builder("aft", env!("CARGO_PKG_VERSION"))
        .provides(vec![ProviderRole::ToolProvider {
            tools: vec![Tool {
                name: "echo".into(),
                description: None,
                execution_mode: ExecutionMode::Pure,
                schema: json!({"type": "object"}),
            }],
            identity_scope: vec![IdentityScope::Project, IdentityScope::Session],
            concurrency: Concurrency::ModuleManaged,
            emits_push: false,
            sub_supervises: false,
        }])
        .build();
    // Connect first: it waits for the daemon to publish its connection file, which the
    // in-process module server below reads once and does not wait for.
    let plane = SubcToolPlane::new(rig.connect().await, "aft");
    let (_, serve) =
        subc_client_rs::serve_with_handle(&rig.connection_file, manifest, Handler(stub.clone()))
            .await
            .unwrap();
    let serving = tokio::spawn(serve);
    wait_for_aft(&plane, "echo").await;
    let provider = StubProvider::new(vec![
        tool_step(&[("first", "echo", json!({})), ("second", "echo", json!({}))]),
        text_step("done"),
    ]);
    let (summary, events) = drive_run(&rig, &plane, &tag, "echo", &provider).await;
    assert_eq!(summary.reason, RunFinishReason::Completed);
    let results: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            OutputEvent::Control(ControlUnit::ToolResult { result }) => Some(result.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 2);
    if retryable {
        assert_eq!(stub.binds.load(Ordering::SeqCst), 3);
        assert_eq!(stub.calls.load(Ordering::SeqCst), 2, "each call sent once");
        assert!(results.iter().all(|result| !result.is_error));
    } else {
        assert_eq!(
            stub.binds.load(Ordering::SeqCst),
            1,
            "unknown codes are terminal"
        );
        assert_eq!(stub.calls.load(Ordering::SeqCst), 0);
        for result in &results {
            let output = serde_json::to_value(&result.output).unwrap();
            assert_eq!(output["module"], "aft");
            assert_eq!(output["code"], code);
            assert_eq!(output["detail"], BIND_MESSAGE);
            assert_eq!(output["nothing_sent"], true);
        }
        let bind = BindIdentity::new(&rig.dir, "tool-fault-spine", &tag);
        let records = FileWalStore::new(rig.dir.join(format!("wal-{tag}")))
            .read_records(&bind)
            .unwrap();
        let replayed: Vec<_> = records
            .iter()
            .filter_map(|frame| match &frame.record {
                broca_wal::WalRecord::ToolNoDispatch {
                    tool_call_id,
                    reason,
                    ..
                } => Some(no_dispatch_result(tool_call_id, reason)),
                _ => None,
            })
            .collect();
        assert_eq!(replayed, results);
        assert!(!records.iter().any(|frame| matches!(
            frame.record,
            broca_wal::WalRecord::ToolDispatchIntent { .. }
        )));
    }
    serving.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_bind_refusal_is_attributed_without_dispatch() {
    check_bind_refusal("bind_blocked_by_reader", false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protocol_retryable_bind_refusal_clears_before_dispatch() {
    check_bind_refusal("module_warming", true).await;
}
