use super::*;

// The agent-data contract publishes refusal vectors for cases broca never meets,
// such as a transcript its host keeps or the copy a receiver saves to undo an
// activation. Those generic refusals must still survive this serve ERROR-frame
// writer, without inventing broca operations that would produce them.
#[tokio::test]
async fn contract_refusal_vectors_use_the_serve_error_frame_writer() {
    use broca_wire::agent_data::{
        errors::{driver_action_for, may_refuse, DriverAction, ErrorBody as ContractError},
        Op,
    };
    let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
    for name in ["refusals.json", "digest-mismatch.json"] {
        let file: serde_json::Value =
            serde_json::from_str(cortexkit_role_agent_data::vectors::file(name).unwrap()).unwrap();
        for (index, case) in file["refusals"].as_array().unwrap().iter().enumerate() {
            let error: ContractError = serde_json::from_value(case["body"].clone()).unwrap();
            send_error_bytes(
                &tx,
                PROTOCOL_VERSION,
                7,
                3,
                index as u64,
                serde_json::to_vec(&error).unwrap(),
            )
            .await
            .unwrap();
            let frame = rx.recv().await.unwrap();
            assert_eq!(frame.header.ty, FrameType::Error);
            assert_eq!(frame.header.channel, 7);
            assert_eq!(frame.header.epoch, 3);
            assert_eq!(frame.header.corr, index as u64);
            let decoded: ContractError = serde_json::from_slice(&frame.body).unwrap();
            assert_eq!(
                decoded.code.as_str(),
                case["body"]["code"],
                "{}",
                case["name"]
            );
            assert_eq!(decoded.message, case["body"]["message"], "{}", case["name"]);
            assert_eq!(
                serde_json::to_value(&decoded).unwrap()["detail"],
                case["body"]["detail"],
                "{}",
                case["name"]
            );
            let action = match case["op"].as_str() {
                Some(op) => {
                    let op = Op::from_method(&format!("agent_data.{op}")).unwrap();
                    assert!(may_refuse(op, &decoded.code));
                    if let Some(detail) = decoded.digest_mismatch_detail().unwrap() {
                        assert_eq!(
                            detail.reason.as_deref(),
                            case["body"]["detail"]["reason"].as_str()
                        );
                    }
                    driver_action_for(op, &decoded.code)
                }
                None => decoded.code.driver_action(),
            };
            let expected = match case["driver"].as_str().unwrap() {
                "abort" => DriverAction::Abort,
                "retry_within_step" => DriverAction::RetryWithinStep,
                "retry_later" => DriverAction::RetryLater,
                "retry_after_operator" => DriverAction::RetryAfterOperator,
                "retry" => DriverAction::Retry,
                "terminal_for_step" => DriverAction::TerminalForStep,
                other => panic!("unknown driver-action vector {other}"),
            };
            assert_eq!(action, expected, "{}", case["name"]);
            assert_eq!(action.retryable(), case["retryable"], "{}", case["name"]);
            assert_eq!(decoded.code.is_known(), case["named"], "{}", case["name"]);
        }
    }
}
