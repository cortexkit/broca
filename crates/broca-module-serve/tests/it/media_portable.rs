//! Backup acceptance uses only the portable filetrees in the deployed descriptor.
//! Rendering is offline and explicit until the live executor hydrates media itself.

use std::path::{Path, PathBuf};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use broca_core::media_store::{MediaStore, MediaStoreClass};
use broca_protocol::{BindIdentity, ContentBlock, Message, Role};
use broca_provider::{
    media_profile::{select_media_profile, MediaQuirk},
    media_request::{prepare, render_prepared, MediaAging},
    spec::{InputModalities, WireFamilyId},
};
use broca_wal::{FileWal, Wal, WalRecord};
use serde_json::json;

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "media-portable-{}",
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        broca_owner_only::create_dir_all(&root).unwrap();
        Self(root)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        broca_test_support::remove_rig_dir(&self.0);
    }
}

fn files(root: &Path) -> Vec<PathBuf> {
    fn visit(root: &Path, path: &Path, out: &mut Vec<PathBuf>) {
        if path.is_file() {
            out.push(path.strip_prefix(root).unwrap().to_owned());
        } else {
            for entry in std::fs::read_dir(path).unwrap() {
                visit(root, &entry.unwrap().path(), out);
            }
        }
    }
    let mut out = Vec::new();
    visit(root, root, &mut out);
    out.sort();
    out
}

fn render(root: &Path, identity: &BindIdentity, shown: usize) -> Vec<u8> {
    let wal_path = root.join("wal-durable").join(format!(
        "{}.wal",
        broca_core::wal_writer::session_addr(identity)
    ));
    let frames = FileWal::open(wal_path).unwrap().replay().unwrap();
    let records: Vec<_> = frames.into_iter().map(|frame| frame.record).collect();
    let WalRecord::RunStarted { config, .. } = &records[0] else {
        panic!("missing admission")
    };
    let step = records
        .iter()
        .rev()
        .find_map(|record| match record {
            WalRecord::StepStarted { step_id } => Some(step_id.0),
            _ => None,
        })
        .unwrap();
    let (frozen, opts, _) =
        broca_module_serve::wal_render::boundary_options(&records, config, "portable", step)
            .unwrap();
    let profile = frozen.media.as_ref().unwrap();
    let store = MediaStore::open(root, MediaStoreClass::Durable).unwrap();
    let mut session = broca_core::media_request::SessionMedia::new(store);
    let aging = MediaAging::replay_wal(profile, &mut session, &records).unwrap();
    let mut bundle = prepare(&session, Some(profile), &opts.prompt, &aging).unwrap();
    assert_eq!(
        bundle.media.len(),
        shown,
        "check actual hydration, not just equality of marker-only bodies: {:?}",
        bundle.prompt
    );
    render_prepared(
        &*broca_provider::registry::wire_family(frozen.family_id).unwrap(),
        &frozen,
        &opts,
        Some(&mut bundle),
        &|prompt| prompt.to_vec(),
    )
    .unwrap()
}

#[test]
fn portable_entries_restore_media_requests_without_disposable_objects() {
    let source = Scratch::new();
    let restored = Scratch::new();
    let identity = BindIdentity::new(source.0.join("project"), "runner", "portable");
    let mut frozen = broca_provider::families::anthropic_frozen_config("claude-haiku-5-5");
    // Keep both document occurrences visible: PDF token estimates are independent
    // of decoded length, so two tiny PDFs can exceed a smaller context's aging budget.
    frozen.media = Some(select_media_profile(
        WireFamilyId::AnthropicMessages,
        Some(MediaQuirk::Anthropic),
        "claude-haiku-5-5",
        InputModalities::from_value(&json!(["text", "image", "pdf"])),
        Some(1_000_000),
    ));
    let mut private_digests = Vec::new();
    let mut durable_bytes = Vec::new();
    for (class, wal_tree, label) in [
        (MediaStoreClass::Durable, "wal-durable", "durable"),
        (MediaStoreClass::NonDurable, "wal", "disposable"),
        (MediaStoreClass::Ephemeral, "wal-ephemeral", "ephemeral"),
    ] {
        let bytes =
            format!("%PDF-1.7\n{label} media must keep its durability class\n%%EOF\n").into_bytes();
        let reference =
            broca_core::media_validation::validate_media(&bytes, "application/pdf").unwrap();
        if label == "durable" {
            durable_bytes = bytes.clone();
        } else {
            private_digests.push(reference.sha256.clone());
        }
        let store = MediaStore::open(&source.0, class).unwrap();
        let mut manifest = store.session(&identity).unwrap();
        let path = source.0.join(wal_tree).join(format!(
            "{}.wal",
            broca_core::wal_writer::session_addr(&identity)
        ));
        let mut wal = FileWal::open(path)
            .unwrap()
            .with_feature_floor(source.0.join(broca_wal::FEATURE_FLOOR_FILENAME));
        let input = vec![Message::new(
            Role::User,
            vec![
                ContentBlock::Text {
                    text: "Inspect the document.".into(),
                },
                ContentBlock::Media(reference.clone()),
            ],
        )];
        let started: WalRecord = serde_json::from_value(json!({
            "type":"run_started", "run_id":"portable", "session":identity, "input":input,
            "origin":{"kind":"fresh"},
            "config":{"model":{"provider_module_id":"anthropic","model_id":"claude-haiku-5-5"},
                "tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},
                "context_epoch":0,"renderer_version":frozen.renderer_version,"frozen_render_config":frozen}
        })).unwrap();
        manifest
            .register_and_append(&reference, &bytes, || {
                wal.append(1, &started, true).map_err(std::io::Error::other)
            })
            .unwrap();
        for record in [
            json!({"type":"step_started","step_id":1}),
            json!({"type":"model_step_finished","step_id":1,"usage":{},"assistant_message":{
                "message_id":"tool-call", "content":[{"type":"tool_call","tool_call_id":"read-doc",
                    "tool_name":"read","input":{}}]}}),
            json!({"type":"tool_batch_started","batch_id":"batch","step_id":1,"planned_calls":["read-doc"]}),
            json!({"type":"tool_dispatch_intent","batch_id":"batch","tool_call_id":"read-doc","args":{}}),
        ] {
            wal.append(1, &serde_json::from_value(record).unwrap(), true)
                .unwrap();
        }
        let result = serde_json::from_value(json!({
            "type":"tool_result", "batch_id":"batch", "outcome":"completed",
            "result":{"tool_call_id":"read-doc","is_error":true,"output":{"kind":"parts","parts":[
                {"kind":"text","text":"before"}, {"kind":"media","media":reference},
                {"kind":"text","text":"after"}
            ]}}
        }))
        .unwrap();
        manifest
            .register_and_append(&reference, &bytes, || {
                wal.append(1, &result, true).map_err(std::io::Error::other)
            })
            .unwrap();
        for record in [
            json!({"type":"tool_batch_finished","batch_id":"batch"}),
            json!({"type":"step_started","step_id":2}),
        ] {
            wal.append(1, &serde_json::from_value(record).unwrap(), true)
                .unwrap();
        }
    }
    let expected = render(&source.0, &identity, 2);
    assert!(String::from_utf8_lossy(&expected).contains(&STANDARD.encode(&durable_bytes)));
    let descriptor: serde_json::Value =
        serde_json::from_slice(include_bytes!("../../../../engram-catalog.json")).unwrap();
    for entry in descriptor["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["class"] == "portable")
    {
        assert_eq!(entry["mechanism"], "filetree");
        let relative = entry["path"].as_str().unwrap();
        let tree = source.0.join(relative);
        if !tree.exists() {
            continue;
        }
        for path in files(&tree) {
            let destination = restored.0.join(relative).join(&path);
            broca_owner_only::create_dir_all(destination.parent().unwrap()).unwrap();
            std::fs::copy(tree.join(path), destination).unwrap();
        }
    }
    for path in files(&restored.0) {
        let bytes = std::fs::read(restored.0.join(&path)).unwrap();
        for digest in &private_digests {
            assert!(
                !path.to_string_lossy().contains(digest),
                "private media object was backed up: {path:?}"
            );
            assert!(
                !bytes
                    .windows(digest.len())
                    .any(|window| window == digest.as_bytes()),
                "a portable file names private media: {path:?}"
            );
        }
    }
    // Remove every original entry before rendering so no read can fall back to the source store.
    broca_test_support::remove_rig_dir(&source.0);
    assert_eq!(render(&restored.0, &identity, 2), expected);
    for excluded in [
        "media",
        "media-ephemeral",
        "media-uploads",
        "wal",
        "wal-ephemeral",
    ] {
        assert!(
            !restored.0.join(excluded).exists(),
            "restored excluded tree {excluded}"
        );
    }
}

#[test]
fn protected_bundle_retry_and_reopened_first_step_render_identical_bytes() {
    let source = Scratch::new();
    let identity = BindIdentity::new(source.0.join("project"), "runner", "protected");
    let bytes = b"%PDF-1.7\nprotected input larger than the aging thresholds\n%%EOF\n";
    let reference = broca_core::media_validation::validate_media(bytes, "application/pdf").unwrap();
    let mut frozen = broca_provider::families::anthropic_frozen_config("claude-haiku-5-5");
    let mut profile = select_media_profile(
        WireFamilyId::AnthropicMessages,
        Some(MediaQuirk::Anthropic),
        "claude-haiku-5-5",
        InputModalities::from_value(&json!(["text", "pdf"])),
        Some(200000),
    );
    profile.aging = broca_provider::media_profile::AgingThresholds {
        byte_trigger: 2,
        byte_target: 1,
        token_trigger: 2,
        token_target: 1,
    };
    frozen.media = Some(profile);
    let started = serde_json::from_value(json!({
        "type":"run_started", "run_id":"portable", "session":identity,
        "input":[{"role":"user","content":[{"type":"media","kind":reference.kind,
            "mime":reference.mime,"sha256":reference.sha256,"byte_size":reference.byte_size}]}],
        "origin":{"kind":"fresh"},
        "config":{"model":{"provider_module_id":"anthropic","model_id":"claude-haiku-5-5"},
            "tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},
            "context_epoch":0,"renderer_version":frozen.renderer_version,"frozen_render_config":frozen}
    })).unwrap();
    let path = source.0.join("wal-durable").join(format!(
        "{}.wal",
        broca_core::wal_writer::session_addr(&identity)
    ));
    let store = MediaStore::open(&source.0, MediaStoreClass::Durable).unwrap();
    let mut wal = FileWal::open(&path)
        .unwrap()
        .with_feature_floor(source.0.join(broca_wal::FEATURE_FLOOR_FILENAME));
    store
        .session(&identity)
        .unwrap()
        .register_and_append(&reference, bytes, || {
            wal.append(1, &started, true).map_err(std::io::Error::other)
        })
        .unwrap();
    wal.append(
        1,
        &WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        },
        true,
    )
    .unwrap();
    drop(wal);
    let first = render(&source.0, &identity, 1);
    assert!(String::from_utf8_lossy(&first).contains(&STANDARD.encode(bytes)));
    // Transmitting a request adds no durable completion. Reopen the same WAL before
    // retrying, and keep the bundle even though it exceeds both aging thresholds.
    let mut wal = FileWal::open(path).unwrap();
    assert_eq!(render(&source.0, &identity, 1), first);
    wal.append(
        1,
        &WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        },
        true,
    )
    .unwrap();
    assert_eq!(render(&source.0, &identity, 1), first);
    wal.append(
        1,
        &WalRecord::StepStarted {
            step_id: broca_protocol::StepId(2),
        },
        true,
    )
    .unwrap();
    let following = String::from_utf8(render(&source.0, &identity, 0)).unwrap();
    assert!(following.contains("reason=aged"), "{following}");
    assert!(!following.contains(&STANDARD.encode(bytes)));
}
