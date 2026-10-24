//! Default reply bytes captured through the session-bound management route.
mod delivery_rig;
use delivery_rig::*;
use serde_json::json;

fn assert_capture(frame: &subc_protocol::Frame, name: &str) {
    assert_eq!(frame.header.ty, subc_protocol::FrameType::Response);
    let path = golden_path(name);
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, [&frame.body[..], b"\n"].concat()).unwrap();
        println!("{name}: {}", String::from_utf8_lossy(&frame.body));
    }
    assert_eq!(
        frame.body.as_slice(),
        std::fs::read(path).unwrap().strip_suffix(b"\n").unwrap()
    );
}

#[tokio::test]
async fn unknown_facts_keep_pre_addition_default_reply_bytes() {
    let mut rig = Rig::start("unknown-head-facts").await;
    let read = rig
        .request(7, json!({"method":"session.read","params":{}}))
        .await;
    assert_capture(&read, "session_read.before_head_additions.json");
    let head = rig
        .request(7, json!({"method":"session.head","params":{}}))
        .await;
    assert_capture(&head, "session_head.before_head_additions.json");
    rig.finish().await;
}
