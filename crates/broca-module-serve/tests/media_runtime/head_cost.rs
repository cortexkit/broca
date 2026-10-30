use super::*;
use broca_core::{Persister, WalStore};
use broca_module::ServeHandler;
use broca_wal::{
    access_counter::{ReadClass, WalAccessCounter},
    WalRecord,
};

async fn prepare(messages: usize, paused: bool) -> Rig {
    let mut rig = Rig::start(&format!("head-cost-{messages}-{paused}")).await;
    broca_core::media_store::create_media_directories(&rig.dir).unwrap();
    *rig.script.media_frozen.lock().unwrap() = Some(frozen(WireFamilyId::AnthropicMessages));
    let wal = broca_core::FileWalStore::new(rig.dir.join("wal"));
    let mut writer = wal.open(&rig.session).unwrap();
    for episode in 0..messages / 2 - 1 {
        let input: Vec<Value> = (0..if episode == 0 { 2 } else { 1 })
            .map(|_| json!({"role":"user","content":[{"type":"text","text":"history"}]}))
            .collect();
        let start: WalRecord = serde_json::from_value(json!({"type":"run_started","run_id":format!("history-{episode}"),"session":rig.session,
            "config":{"model":{"provider_module_id":"fake","model_id":"fake"},"tools":[],"tool_choice":{"type":"auto"},
                "generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"frozen_render_config":{"render_config":"stateless"}},
            "input":input,"origin":{"kind":"fresh"}})).unwrap();
        writer.append(1, &start, false).unwrap();
        writer
            .append(
                1,
                &serde_json::from_value(json!({"type":"step_started","step_id":1})).unwrap(),
                false,
            )
            .unwrap();
        writer.append(1,&serde_json::from_value(json!({"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":format!("history-{episode}"),"content":[{"type":"text","text":"answer"}]},"usage":{},"retries_used":0})).unwrap(),false).unwrap();
        writer.append(1,&serde_json::from_value(json!({"type":"run_finished","run_id":format!("history-{episode}"),"reason":"completed","usage":{},"retries_used":0})).unwrap(),false).unwrap();
    }
    writer.sync().unwrap();
    drop(writer);
    let range = wal.read_records_range(&rig.session, 0, None, None).unwrap();
    let watermark = Persister::project_range(rig.store.as_ref(), &rig.session, &range).unwrap();
    let anchor = range.anchor_at(watermark);
    if paused {
        *rig.script.pause_next_run.lock().unwrap() = true;
    } else {
        rig.script.hold_model(true);
    }
    result(&rig.send("current", "current", None).await);
    if paused {
        rig.wait_idle().await;
    } else {
        rig.wait_entered().await;
    }
    let tail = wal
        .read_records_range(&rig.session, watermark, None, anchor.as_ref())
        .unwrap();
    Persister::project_range(rig.store.as_ref(), &rig.session, &tail).unwrap();
    rig
}

pub(super) async fn measure(sizes: &[usize], assert_bounded: bool) {
    for &messages in sizes {
        let rig = prepare(messages, false).await;
        let warm = rig.registry().head(&rig.session).await.unwrap();
        assert_eq!(warm.last_ordinal, Some(messages as u64 - 1));
        assert_eq!(warm.last_run_state.as_ref().unwrap().state, "active");
        let counter = WalAccessCounter::default();
        for sample in 0..3 {
            let operation = counter.operation(format!("active-head-{messages}-{sample}"));
            let before = std::time::Instant::now();
            let head = operation
                .run_future(rig.registry().head(&rig.session))
                .await
                .unwrap();
            let elapsed = before.elapsed().as_micros();
            assert_eq!(head.last_run_state.as_ref().unwrap().state, "active");
            let access = counter.snapshot().pop().unwrap();
            let origin_bytes: u64 = access
                .reads
                .iter()
                .filter(|read| read.class == ReadClass::Origin)
                .map(|read| read.length)
                .sum();
            let source_bytes: u64 = access
                .reads
                .iter()
                .filter(|read| read.class == ReadClass::Source)
                .map(|read| read.length)
                .sum();
            println!("ACTIVE_HEAD_COST messages={messages} sample={sample} elapsed_us={elapsed} origin_bytes={origin_bytes} source_bytes={source_bytes} frames_decoded={}",access.frames_decoded);
            if assert_bounded {
                assert_eq!(origin_bytes, 0, "active head repeated a whole-log scan");
                assert!(
                    source_bytes < 16384,
                    "active head read a growing source range"
                );
                assert_eq!(head.input.as_ref().unwrap().attachments, "yes");
            }
        }
        let mut rig = rig;
        rig.script.hold_model(false);
        rig.wait_idle().await;
        assert!(rig.registry().stop_and_seal().await);
        rig.finish().await;
        let rig = prepare(messages, true).await;
        assert!(rig.registry().stop_and_seal().await);
        let rig = rig.restart().await;
        let counter = WalAccessCounter::default();
        for sample in 0..4 {
            let operation = counter.operation(format!("resumable-head-{messages}-{sample}"));
            let before = std::time::Instant::now();
            let head = operation
                .run_future(rig.registry().head(&rig.session))
                .await
                .unwrap();
            let elapsed = before.elapsed().as_micros();
            assert_eq!(head.last_run_state.as_ref().unwrap().state, "paused");
            assert_eq!(head.last_ordinal, Some(messages as u64 - 1));
            let access = counter.snapshot().pop().unwrap();
            let origin_bytes: u64 = access
                .reads
                .iter()
                .filter(|read| read.class == ReadClass::Origin)
                .map(|read| read.length)
                .sum();
            let source_bytes: u64 = access
                .reads
                .iter()
                .filter(|read| read.class == ReadClass::Source)
                .map(|read| read.length)
                .sum();
            println!("RESUMABLE_HEAD_COST messages={messages} sample={sample} elapsed_us={elapsed} origin_bytes={origin_bytes} source_bytes={source_bytes} frames_decoded={}",access.frames_decoded);
            assert_eq!(rig.registry().slot_counts().open, 0);
            if assert_bounded {
                assert_eq!(head.input.as_ref().unwrap().attachments, "yes");
                if sample > 0 {
                    assert_eq!(origin_bytes, 0, "resumable head repeated a whole-log scan");
                    assert!(source_bytes < 16384);
                }
            }
        }
        rig.finish().await;
    }
}
