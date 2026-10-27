use async_trait::async_trait;
use broca_module::registry::OpenError;
use broca_module::serve::{
    CancelResponse, RetractResponse, SendRequest, SendResponse, SessionReadRequest, StatusResponse,
    SubscribeFrom,
};
use broca_module::{Delivery, ServeError, ServeHandler};
use broca_protocol::{BindIdentity, ErrorClass, RunId, SubmissionId};
use tokio::sync::mpsc;

/// A serve handler that implements only the required methods, so every other method answers
/// with the trait's default refusal. Its required methods are never reached by these tests.
struct DefaultsOnly;

#[async_trait]
impl ServeHandler for DefaultsOnly {
    async fn send(&self, _: SendRequest) -> Result<SendResponse, ServeError> {
        unreachable!("not exercised")
    }
    async fn retract(
        &self,
        _: &BindIdentity,
        _: SubmissionId,
    ) -> Result<RetractResponse, ServeError> {
        unreachable!("not exercised")
    }
    async fn subscribe(
        &self,
        _: &BindIdentity,
        _: SubscribeFrom,
        _: mpsc::Sender<Delivery>,
    ) -> Result<(), ServeError> {
        unreachable!("not exercised")
    }
    async fn read(
        &self,
        _: SessionReadRequest,
    ) -> Result<broca_wire::SessionReadResponse, ServeError> {
        unreachable!("not exercised")
    }
    async fn cancel(&self, _: &BindIdentity, _: RunId) -> Result<CancelResponse, ServeError> {
        unreachable!("not exercised")
    }
    async fn status(&self, _: &BindIdentity) -> Result<StatusResponse, ServeError> {
        unreachable!("not exercised")
    }
    async fn run_status(
        &self,
        _: &BindIdentity,
        _: RunId,
    ) -> Result<broca_wire::RunStatusResponse, ServeError> {
        unreachable!("not exercised")
    }
}

/// The class an `Open` refusal carries, or a panic naming what was returned instead.
fn open_class<T: std::fmt::Debug>(site: &str, result: Result<T, ServeError>) -> Option<ErrorClass> {
    match result {
        Err(ServeError::Open(_, class)) => class,
        other => panic!("{site}: expected an Open refusal, got {other:?}"),
    }
}

/// A serve implementation without a capability refuses it with a class a caller can act on:
/// a missing method is Permanent (a retry reaches the same implementation), while a send that
/// cannot tell whether the session was written is Transient (the classifier may answer later).
#[tokio::test]
async fn default_refusals_carry_their_retry_class() {
    let handler = DefaultsOnly;
    let session = BindIdentity::new(std::path::PathBuf::from("/tmp/project"), "runner", "s");
    let run = || RunId("r".into());
    let permanent = Some(ErrorClass::Permanent);
    let cases = [
        (
            "cancel_at: boundary runtime not installed",
            open_class(
                "cancel_at",
                handler
                    .cancel_at(
                        &session,
                        run(),
                        broca_wal::record::RunCancelAt::StepBoundary,
                    )
                    .await,
            ),
            permanent,
        ),
        (
            "head: unsupported",
            open_class("head", handler.head(&session).await),
            permanent,
        ),
        (
            "warm: unsupported",
            open_class("warm", handler.warm(&session, None).await),
            permanent,
        ),
        (
            "run_result: unsupported",
            open_class("run_result", handler.run_result(&session, run()).await),
            permanent,
        ),
        (
            "run_tool_calls: unsupported",
            open_class(
                "run_tool_calls",
                handler
                    .run_tool_calls(
                        &session,
                        cortexkit_role_llm_runner::tool_calls::ToolCallsRequest::new("r"),
                    )
                    .await,
            ),
            permanent,
        ),
        (
            "baseline: unsupported",
            open_class(
                "baseline",
                handler.baseline(&session, None, &Default::default()).await,
            ),
            permanent,
        ),
        (
            "session_written: classifier unsupported",
            open_class("session_written", handler.session_written(&session)),
            Some(ErrorClass::Transient),
        ),
    ];
    for (site, actual, expected) in cases {
        assert_eq!(actual, expected, "{site}");
    }
}

/// An open failure with only a text cause has no class to give a caller; a typed cause keeps
/// its class (see `typed_open_errors_preserve_all_shared_classes`).
#[test]
fn an_erased_open_error_carries_no_class() {
    let error = ServeError::from(OpenError::Open("lost cause".into()));
    assert!(
        matches!(&error, ServeError::Open(reason, None) if reason == "lost cause"),
        "{error:?}"
    );
}

#[test]
fn typed_open_errors_preserve_all_shared_classes() {
    for class in [
        ErrorClass::Transient,
        ErrorClass::Permanent,
        ErrorClass::Auth,
        ErrorClass::ContextOverflow,
    ] {
        let error = broca_module::ServeError::from(broca_module::registry::OpenError::Classified {
            reason: "known cause".into(),
            class,
        });
        assert!(
            matches!(error, broca_module::ServeError::Open(_, Some(actual)) if actual == class)
        );
        assert_eq!(error.to_string(), "open failed: known cause");
    }
}
