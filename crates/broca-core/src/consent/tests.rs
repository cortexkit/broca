use crate::tool_plane::*;
use broca_protocol::{BatchId, BindIdentity, RunId};

fn ctx() -> BatchCtx {
    BatchCtx {
        media_capture: None,
        batch_id: BatchId("b".into()),
        bind: BindIdentity::new("/p", "test", "s"),
        run_id: RunId("r".into()),
        scope: None,
    }
}
struct Absent;
#[async_trait::async_trait]
impl ElicitationPlane for Absent {}
fn unavailable<T: std::fmt::Debug>(result: Result<T, ApprovalRouteError>) {
    let error = result.unwrap_err();
    assert_eq!(error.code, "elicitation_unavailable");
    assert!(!error.retryable);
}
#[tokio::test]
async fn await_default_refuses_nonretryably() {
    unavailable(Absent.r#await(&ctx(), "id", 120_000).await);
}
#[tokio::test]
async fn ack_default_refuses_nonretryably() {
    unavailable(
        Absent
            .ack(
                &ctx(),
                &ApprovalCursor {
                    provider_incarnation: "i".into(),
                    seq: 1,
                },
            )
            .await,
    );
}
#[tokio::test]
async fn report_execution_default_refuses_nonretryably() {
    unavailable(
        Absent
            .report_execution(&ctx(), "id", ApprovalExecution::Executed)
            .await,
    );
}
#[tokio::test]
async fn detach_default_refuses_nonretryably() {
    unavailable(Absent.detach(&ctx(), "id").await);
}
