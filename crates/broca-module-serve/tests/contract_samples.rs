//! Wire samples exercised through the same serve loop as session callers.
mod delivery_rig;

use delivery_rig::*;
use serde_json::json;

fn capture(name: &str, bytes: &[u8]) {
    if std::env::var_os("BROCA_CAPTURE_SAMPLES").is_some() {
        println!(
            "BROCA_SAMPLE {name} {}",
            std::str::from_utf8(bytes).unwrap()
        );
    }
}

#[tokio::test]
async fn describe_contract_sample() {
    let mut rig = Rig::start("contract-describe-sample").await;
    let frame = rig
        .request(7, json!({"method":"role.describe","params":{}}))
        .await;
    assert_eq!(frame.header.ty, subc_protocol::FrameType::Response);
    capture("role.describe.json", &frame.body);
    rig.finish().await;
}
