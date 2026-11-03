use broca_protocol::{CallOptions, Role};
use broca_provider::{
    families::AnthropicMessages,
    wire::{FrozenRenderConfig, WireFamily},
};
use broca_wal::{
    framing::{decode_next, DecodeStep},
    WalRecord,
};

#[test]
fn reader_only_replays_writer_log_with_identical_native_and_downgraded_bytes() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let bytes = std::fs::read(root.join("turn-system-writer.wal")).unwrap();
    let mut tail = bytes.as_slice();
    let mut records = Vec::new();
    while !tail.is_empty() {
        let DecodeStep::Record { framed, consumed } = decode_next(tail) else {
            panic!("reader refused writer's serialized log");
        };
        records.push(framed.record);
        tail = &tail[consumed..];
    }
    let WalRecord::RunStarted { config, .. } = &records[0] else {
        panic!("writer fixture starts a run");
    };
    let mut frozen: FrozenRenderConfig =
        serde_json::from_value(config.frozen_render_config.clone()).unwrap();
    assert!(frozen.anthropic_turn_system);
    let state = broca_core::replay::replay(&records).unwrap();
    assert_eq!(state.prompt.len(), 2);
    assert_eq!(state.prompt[1].role, Role::System);
    assert!(state.prompt[1].turn_system);
    let opts = CallOptions::new(state.prompt);
    let native = AnthropicMessages.render(&frozen, &opts);
    assert_eq!(
        native,
        std::fs::read(root.join("turn-system-writer-native.json")).unwrap()
    );
    frozen.anthropic_turn_system = false;
    let downgraded = AnthropicMessages.render(&frozen, &opts);
    assert_eq!(
        downgraded,
        std::fs::read(root.join("turn-system-writer-downgraded.json")).unwrap()
    );
    assert_eq!(opts.prompt[1].role, Role::System);
    assert!(opts.prompt[1].turn_system);
}
