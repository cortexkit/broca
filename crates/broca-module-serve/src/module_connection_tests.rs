use super::*;
use subc_protocol::claim::{ClaimMintReply, ClaimPurpose};
use subc_protocol::{Flags, Frame, FrameType, Priority};

fn control_flags() -> Flags {
    Flags::new(false, Priority::Passive, false)
}

/// Before the serve loop runs, a mint reads the module connection itself: it takes the daemon's
/// answer to its own request, and keeps a frame meant for the serve loop (here a health probe that
/// arrived first), which the loop then reads before anything else on the connection.
#[tokio::test]
async fn a_startup_mint_takes_its_answer_and_keeps_other_frames_for_the_serve_loop() {
    let (module, mut daemon) = tokio::io::duplex(8192);
    let gate = crate::settings::gate::Gate::ungated(subc_protocol::PROTOCOL_VERSION);
    let connection = Arc::new(ModuleConnection::new(Box::new(module), gate));
    let mint = tokio::spawn({
        let connection = connection.clone();
        async move {
            connection
                .call(&ClaimMintRequest::new(ClaimPurpose::SelfConnection))
                .await
        }
    });

    let request = read_frame(&mut daemon).await.unwrap().unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
        serde_json::json!({"op": "claim.mint", "purpose": "self"})
    );
    let probe = Frame::build(
        FrameType::Request,
        control_flags(),
        0,
        0,
        7,
        br#"{"op":"health.check"}"#.to_vec(),
    )
    .unwrap();
    write_frame(&mut daemon, &probe).await.unwrap();
    let reply = ClaimMintReply::new(
        subc_protocol::claim::Claim::new("CLAIMCLAIMCLAIMCLAIM03"),
        1,
    );
    let answer = Frame::build(
        FrameType::Response,
        control_flags(),
        0,
        0,
        request.header.corr,
        serde_json::to_vec(&reply).unwrap(),
    )
    .unwrap();
    write_frame(&mut daemon, &answer).await.unwrap();

    match mint.await.unwrap().unwrap() {
        MintAnswer::Done(body) => {
            let minted: ClaimMintReply = serde_json::from_slice(&body).unwrap();
            assert_eq!(minted.claim.expose_secret(), "CLAIMCLAIMCLAIMCLAIM03");
        }
        MintAnswer::Refused(error) => panic!("the mint was refused: {error:?}"),
    }
    let (mut reader, _writer) = connection.hand_over().await;
    let kept = read_frame(&mut reader).await.unwrap().unwrap();
    assert_eq!((kept.header.ty, kept.header.corr), (FrameType::Request, 7));
    assert_eq!(kept.body, probe.body);
}
