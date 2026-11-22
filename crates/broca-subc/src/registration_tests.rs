use super::*;
use std::collections::VecDeque;
use std::sync::Mutex;

/// Answers each `claim.mint` with the next scripted answer, counting the requests.
#[derive(Default)]
struct Scripted {
    answers: Mutex<VecDeque<MintAnswer>>,
    requests: Mutex<Vec<serde_json::Value>>,
}

#[async_trait]
impl ClaimMinter for Scripted {
    async fn call(&self, request: &ClaimMintRequest) -> Result<MintAnswer, String> {
        self.requests
            .lock()
            .unwrap()
            .push(serde_json::to_value(request).unwrap());
        self.answers
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| "no scripted answer".into())
    }
}

fn minted(claim: &str) -> MintAnswer {
    MintAnswer::Done(
        serde_json::to_vec(&serde_json::json!({
            "op": "claim.mint", "claim": claim, "expires_at_ms": 1u64
        }))
        .unwrap(),
    )
}

fn refused(code: &str, detail: Option<serde_json::Value>) -> MintAnswer {
    let mut error = ErrorBody::new(code, "refused");
    if let Some(detail) = detail {
        error = error.with_detail(detail);
    }
    MintAnswer::Refused(error)
}

/// A rate-limited mint waits the daemon's delay and asks again; each request asks for a claim
/// for broca's own connection.
#[tokio::test(start_paused = true)]
async fn a_rate_limited_mint_waits_and_asks_again() {
    let minter = Scripted::default();
    minter.answers.lock().unwrap().extend([
        refused(
            error_codes::CLAIM_RATE_LIMITED,
            Some(serde_json::json!({"retry_after_ms": 250})),
        ),
        minted("CLAIMCLAIMCLAIMCLAIM00"),
    ]);
    let started = tokio::time::Instant::now();
    let claim = mint_with(&minter).await.unwrap();
    assert_eq!(claim.expose_secret(), "CLAIMCLAIMCLAIMCLAIM00");
    assert!(started.elapsed() >= Duration::from_millis(250));
    let requests = minter.requests.lock().unwrap().clone();
    assert_eq!(
        requests,
        vec![serde_json::json!({"op": "claim.mint", "purpose": "self"}); 2]
    );
}

/// Any other refusal is final: no second request, and the error names the code.
#[tokio::test]
async fn a_refused_mint_is_final() {
    let minter = Scripted::default();
    minter
        .answers
        .lock()
        .unwrap()
        .push_back(refused(error_codes::CLAIM_MINT_NOT_ALLOWED, None));
    let error = mint_with(&minter).await.unwrap_err();
    assert!(error.contains("claim_mint_not_allowed"), "{error}");
    assert_eq!(minter.requests.lock().unwrap().len(), 1);
}

/// The daemon's side of a fresh connection: reads the first frame and answers it with `reply`.
async fn daemon_answers(
    mut daemon: tokio::io::DuplexStream,
    reply: impl FnOnce(&Frame) -> Frame,
) -> serde_json::Value {
    let frame = read_frame(&mut daemon).await.unwrap().unwrap();
    write_frame(&mut daemon, &reply(&frame)).await.unwrap();
    assert_eq!(
        (frame.header.channel, frame.header.corr),
        (0, 1),
        "the claim is the connection's first request"
    );
    serde_json::from_slice(&frame.body).unwrap()
}

/// A presented claim is the connection's first request, and the daemon's acceptance binds it.
#[tokio::test]
async fn a_presented_claim_is_accepted() {
    let (mut client, daemon) = tokio::io::duplex(4096);
    let daemon = tokio::spawn(daemon_answers(daemon, |_| {
        let body = serde_json::to_vec(&ClaimPresentReply::new()).unwrap();
        Frame::build(FrameType::Response, control_flags(), 0, 0, 1, body).unwrap()
    }));
    present(&mut client, Claim::new("CLAIMCLAIMCLAIMCLAIM01"))
        .await
        .unwrap();
    assert_eq!(
        daemon.await.unwrap(),
        serde_json::json!({"op": "claim.present", "claim": "CLAIMCLAIMCLAIMCLAIM01"})
    );
}

/// A refused claim fails the connection with the daemon's code, so it is never used unbound.
#[tokio::test]
async fn a_refused_claim_fails_the_connection() {
    let (mut client, daemon) = tokio::io::duplex(4096);
    let daemon = tokio::spawn(daemon_answers(daemon, |_| {
        let body = serde_json::to_vec(&ErrorBody::new(error_codes::CLAIM_INVALID, "no")).unwrap();
        Frame::build(FrameType::Error, control_flags(), 0, 0, 1, body).unwrap()
    }));
    let error = present(&mut client, Claim::new("CLAIMCLAIMCLAIMCLAIM02"))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("claim_invalid"), "{error}");
    daemon.await.unwrap();
}
