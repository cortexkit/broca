//! Decision-model admission with an in-memory Cerebellum image source and provider.

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use broca_module_serve::{
    config::LlmToolsConfig,
    llm_tools::{LlmTools, NoPrivateContent},
    system_one::*,
};
use broca_protocol::{
    BindPrincipal, BindScope, ParentState, ScopeKind, ScopeParent, SendScope, SessionScope,
};
use broca_store::{SqliteStore, SystemOneStore};
use broca_subc::{ImageResolveError, ResolvedImage};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const PIXEL: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==";
const HANDLE: &str = "ih0123456789abcdef0123456789abcdef";

#[derive(Default)]
struct Cerebellum {
    replies: Mutex<HashMap<String, ImageReply>>,
    calls: Mutex<Vec<(String, String, Option<String>)>>,
}
type ImageReply = Result<(String, Vec<u8>), ImageResolveError>;

impl Cerebellum {
    fn image(&self, handle: &str, mime: &str, bytes: Vec<u8>) {
        self.replies
            .lock()
            .unwrap()
            .insert(handle.into(), Ok((mime.into(), bytes)));
    }
}

#[async_trait]
impl ImageResolver for Cerebellum {
    async fn resolve(
        &self,
        handle: &str,
        scope: &str,
        parent: Option<&str>,
    ) -> Result<ResolvedImage, ImageResolveError> {
        self.calls
            .lock()
            .unwrap()
            .push((handle.into(), scope.into(), parent.map(str::to_owned)));
        let (content_type, bytes) = self
            .replies
            .lock()
            .unwrap()
            .get(handle)
            .cloned()
            .unwrap_or(Err(ImageResolveError::Unavailable))?;
        // The test fake stands in for the wire decoder, which checks length and digest before
        // this boundary: bytes here are already verified, as `ResolvedImage` carries no digest.
        Ok(ResolvedImage {
            content_type,
            bytes,
        })
    }
}

#[derive(Default)]
struct Provider(Mutex<Vec<Value>>);

#[async_trait]
impl ClassifierProvider for Provider {
    fn egress_host(&self) -> String {
        "127.0.0.1".into()
    }
    fn credential_id(&self) -> Option<String> {
        None
    }
    async fn call(&self, body: String) -> ProviderReply {
        let body: Value = serde_json::from_str(&body).unwrap();
        let model = body["model"].clone();
        self.0.lock().unwrap().push(body);
        ProviderReply::Answered(serde_json::to_vec(&json!({"model": model, "answers":{"q":{"type":"noul","noul":0.7}},"usage":{"input_tokens":10}})).unwrap())
    }
}

struct Catalog;
impl BatchCatalog for Catalog {
    fn row(&self, model: &str) -> Option<broca_catalog::ModelBatchRow> {
        Some(broca_catalog::ModelBatchRow {
            kind: Some("decision".into()),
            context_tokens: match model {
                CLEF => Some(65_536),
                CLEF_FLASH => Some(24_576),
                _ => None,
            },
            attachment: model != JEV_1_13_0,
            input_rate_nanos: Some(match model {
                CLEF => 240_000_000,
                CLEF_FLASH => 38_000_000,
                _ => 42_000_000,
            }),
            output_rate_nanos: Some(0),
            model_name: None,
            provider_name: None,
        })
    }
}

struct Rig {
    system: Arc<SystemOne>,
    tools: Arc<LlmTools>,
    source: Arc<Cerebellum>,
    provider: Arc<Provider>,
    store: Arc<SystemOneStore>,
    export: Arc<SqliteStore>,
}

fn rig(enabled: bool) -> Rig {
    let source = Arc::new(Cerebellum::default());
    source.image(HANDLE, "image/png", STANDARD.decode(PIXEL).unwrap());
    let provider = Arc::new(Provider::default());
    let store = Arc::new(SystemOneStore::open_in_memory().unwrap());
    let export = Arc::new(SqliteStore::open_in_memory().unwrap());
    let mut system = SystemOne::new(
        store.clone(),
        "images",
        Arc::new(Catalog),
        vec![
            ModelPin::jev(provider.clone()),
            ModelPin::clef(provider.clone()),
            ModelPin::clef_flash(provider.clone()),
        ],
        Config::default(),
        Arc::new(AtomicUsize::new(0)),
    )
    .with_export(
        export.clone(),
        Arc::new(broca_module_serve::ChargeBasisTable::shipped_defaults()),
    );
    if enabled {
        system = system.with_image_resolver(source.clone());
    }
    let system = Arc::new(system);
    let tools = Arc::new(LlmTools::new(
        Arc::new(system.fork_for_tools()),
        LlmToolsConfig {
            image_references: enabled,
            private_content_providers: vec!["cloudflare-workers-ai".into()],
            ..Default::default()
        },
        Arc::new(NoPrivateContent),
    ));
    Rig {
        system,
        tools,
        source,
        provider,
        store,
        export,
    }
}

fn principal() -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: "owner".into(),
    }
}
fn scope(flow: bool) -> SendScope {
    SendScope::Stamped(BindScope {
        scope: SessionScope {
            owner: principal(),
            scope_ref: "stamped-scope".into(),
            scope_epoch: 1,
            flow_id: flow.then(|| "flow".into()),
            sender: None,
            agent_id: Some("agent".into()),
            run_id: Some("run".into()),
        },
        owner_authorized: true,
        kind: ScopeKind::Ephemeral,
        parent: Some(Box::new(ScopeParent {
            owner: principal(),
            scope_ref: "stamped-parent".into(),
            scope_epoch: 1,
        })),
        parent_state: Some(ParentState::Linked),
    })
}
fn arguments(model: &str, images: Value) -> Value {
    json!({"model":model,"questions":{"q":{"type":"noul","instructions":"Is it urgent?"}},
        "item":{"state":"picture","images":images},"max_cost_usd":1})
}
async fn tool(rig: &Rig, args: Value, send: &SendScope) -> Result<String, Value> {
    rig.tools
        .answer(
            &serde_json::to_vec(
                &json!({"name":"llm_classify","call_key":"image-call","arguments":args}),
            )
            .unwrap(),
            Some(&principal()),
            send,
        )
        .await
        .map(|raw| raw.get().into())
        .map_err(|bytes| serde_json::from_slice(&bytes).unwrap())
}
fn assert_nothing_written(rig: &Rig) {
    assert!(rig.provider.0.lock().unwrap().is_empty());
    let batch =
        format!("{:x}", Sha256::digest(serde_jcs::to_vec(&json!({
        "principal":"reserved:owner", "owner":"reserved:owner", "scope_ref":"stamped-scope",
        "scope_epoch":1,"call_key":"image-call"
    })).unwrap()));
    assert!(rig
        .store
        .lookup(LLM_CLASSIFY_STORE_ROLE, &batch)
        .unwrap()
        .is_none());
    assert!(rig
        .export
        .read_export_page(None, 100)
        .unwrap()
        .facts
        .is_empty());
}

#[tokio::test]
async fn image_reference_resolves_for_clef_and_same_key_replays_after_handle_expires() {
    let rig = rig(true);
    let args = arguments(CLEF, json!([{"handle":HANDLE}]));
    let first = tool(&rig, args.clone(), &scope(false)).await.unwrap();
    let calls = rig.source.calls.lock().unwrap().clone();
    assert_eq!(
        calls,
        vec![(
            HANDLE.into(),
            "stamped-scope".into(),
            Some("stamped-parent".into())
        )]
    );
    let bodies = rig.provider.0.lock().unwrap().clone();
    assert_eq!(bodies.len(), 1);
    assert_eq!(
        bodies[0]["images"],
        json!([{"content_type":"image/png","base64":PIXEL}])
    );
    rig.source.replies.lock().unwrap().clear();
    assert_eq!(tool(&rig, args, &scope(false)).await.unwrap(), first);
    assert_eq!(rig.source.calls.lock().unwrap().len(), 1);
    assert_eq!(rig.provider.0.lock().unwrap().len(), 1);
    for fact in rig.export.read_export_page(None, 100).unwrap().facts {
        assert!(!format!("{fact:?}").contains(PIXEL));
    }
}

#[tokio::test]
async fn image_reference_refusals_are_fixed_class_and_write_nothing() {
    for (source_error, code, class) in [
        (
            ImageResolveError::Unavailable,
            "image_unavailable",
            "permanent",
        ),
        (
            ImageResolveError::SourceUnavailable,
            "image_source_unavailable",
            "transient",
        ),
        (
            ImageResolveError::InvalidReply,
            "image_unavailable",
            "permanent",
        ),
        // A reply the daemon could not deliver for its size is refused as an oversized inline
        // image is, not as a missing handle.
        (ImageResolveError::TooLarge, "invalid_params", "permanent"),
    ] {
        let rig = rig(true);
        rig.source
            .replies
            .lock()
            .unwrap()
            .insert(HANDLE.into(), Err(source_error));
        let refusal = tool(
            &rig,
            arguments(CLEF, json!([{"handle":HANDLE}])),
            &scope(false),
        )
        .await
        .unwrap_err();
        assert_eq!(refusal["code"], code);
        assert_eq!(refusal["detail"]["class"], class);
        assert_eq!(refusal["detail"]["field"], "items[0].images[0]");
        if class == "transient" {
            assert_eq!(refusal["detail"]["retry_after_ms"], 1000);
        }
        assert_nothing_written(&rig);
    }
}

#[tokio::test]
async fn image_reference_scope_and_attachment_admission_precede_resolution() {
    let rig = rig(true);
    assert_eq!(
        tool(
            &rig,
            arguments(CLEF, json!([{"handle":HANDLE}])),
            &SendScope::Internal
        )
        .await
        .unwrap_err()["code"],
        "scope_required"
    );
    assert_eq!(
        tool(
            &rig,
            arguments(JEV_1_13_0, json!([{"handle":HANDLE}])),
            &scope(false)
        )
        .await
        .unwrap_err()["code"],
        "invalid_params"
    );
    assert!(rig.source.calls.lock().unwrap().is_empty());
    assert_nothing_written(&rig);
    tool(
        &rig,
        arguments(CLEF_FLASH, json!([{"handle":HANDLE}])),
        &scope(true),
    )
    .await
    .unwrap();
    assert_eq!(rig.source.calls.lock().unwrap()[0].2, None);
}

#[tokio::test]
async fn image_reference_requires_catalog_attachment_support_and_a_classifier_stamp() {
    struct TextCatalog;
    impl BatchCatalog for TextCatalog {
        fn row(&self, model: &str) -> Option<broca_catalog::ModelBatchRow> {
            Catalog.row(model).map(|mut row| {
                row.attachment = false;
                row
            })
        }
    }
    let store = Arc::new(SystemOneStore::open_in_memory().unwrap());
    let provider = Arc::new(Provider::default());
    let source = Arc::new(Cerebellum::default());
    let service = Arc::new(
        SystemOne::new(
            store.clone(),
            "catalog",
            Arc::new(TextCatalog),
            vec![ModelPin::clef(provider.clone())],
            Config::default(),
            Arc::default(),
        )
        .with_image_resolver(source.clone()),
    );
    let body = serde_json::to_vec(&json!({"method":"classify.run","params":{
        "batch_id":"text-catalog","model":CLEF,"questions":{"q":{"type":"noul","instructions":"Is it urgent?"}},
        "items":[{"state":"picture","images":[{"handle":HANDLE}]}]
    }})).unwrap();
    let unscoped = service
        .clone()
        .answer(&body, Some(&principal()), &SendScope::Internal)
        .await
        .unwrap_err();
    assert_eq!(
        serde_json::from_slice::<Value>(&unscoped).unwrap()["code"],
        "scope_required"
    );
    let scoped = service
        .answer(&body, Some(&principal()), &scope(false))
        .await
        .unwrap_err();
    let scoped: Value = serde_json::from_slice(&scoped).unwrap();
    assert_eq!(scoped["code"], "invalid_params");
    assert_eq!(scoped["detail"]["limit"], 0);
    assert!(store
        .lookup("classifier", "text-catalog")
        .unwrap()
        .is_none());
    assert!(provider.0.lock().unwrap().is_empty());
    assert!(source.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn image_reference_resolved_bytes_get_inline_size_type_count_and_pixel_checks() {
    let mut too_many_pixels = STANDARD.decode(PIXEL).unwrap();
    too_many_pixels[16..20].copy_from_slice(&4001_u32.to_be_bytes());
    too_many_pixels[20..24].copy_from_slice(&4000_u32.to_be_bytes());
    let mut crc = !0_u32;
    for byte in &too_many_pixels[12..29] {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320_u32.wrapping_mul(crc & 1));
        }
    }
    too_many_pixels[29..33].copy_from_slice(&(!crc).to_be_bytes());
    for (mime, bytes, count) in [
        (
            "image/png",
            vec![0; cortexkit_model_batch::image::IMAGE_MAX_BYTES + 1],
            1,
        ),
        ("image/gif", STANDARD.decode(PIXEL).unwrap(), 1),
        ("image/jpeg", STANDARD.decode(PIXEL).unwrap(), 1),
        ("image/png", STANDARD.decode(PIXEL).unwrap(), 5),
        ("image/png", b"not an image".to_vec(), 1),
        ("image/png", vec![0; 3 * 1024 * 1024], 3),
    ] {
        let rig = rig(true);
        rig.source.image(HANDLE, mime, bytes);
        let refusal = tool(
            &rig,
            arguments(CLEF, json!(vec![json!({"handle":HANDLE}); count])),
            &scope(false),
        )
        .await
        .unwrap_err();
        assert_eq!(refusal["code"], "invalid_params");
        assert_nothing_written(&rig);
    }
    let rig = rig(true);
    let inline = json!([{"content_type":"image/png","base64":STANDARD.encode(&too_many_pixels)}]);
    rig.source.image(HANDLE, "image/png", too_many_pixels);
    for images in [json!([{"handle":HANDLE}]), inline] {
        let refusal = tool(&rig, arguments(CLEF, images), &scope(false))
            .await
            .unwrap_err();
        assert_eq!(
            refusal["message"],
            "image content type or pixel limit is invalid"
        );
        assert_nothing_written(&rig);
    }
}

#[tokio::test]
async fn image_reference_classifier_replay_keeps_handles_not_resolved_bytes() {
    let rig = rig(true);
    let params = json!({"batch_id":"classifier-image","model":CLEF,"questions":{"q":{"type":"noul","instructions":"Is it urgent?"}},"items":[{"state":"picture","images":[{"handle":HANDLE}]}]});
    let body = serde_json::to_vec(&json!({"method":"classify.run","params":params})).unwrap();
    let first = rig
        .system
        .clone()
        .answer(&body, Some(&principal()), &scope(false))
        .await
        .unwrap();
    let stored = rig
        .store
        .lookup("classifier", "classifier-image")
        .unwrap()
        .unwrap();
    let original = cortexkit_role_classifier::request::parse_request(&params.to_string()).unwrap();
    assert_eq!(
        stored.identity,
        serde_json::to_string(
            &cortexkit_role_classifier::identity::BatchIdentity::of(&original).unwrap()
        )
        .unwrap()
    );
    assert!(!stored.identity.contains(PIXEL));
    rig.source.replies.lock().unwrap().clear();
    let replay = rig
        .system
        .clone()
        .answer(&body, Some(&principal()), &scope(false))
        .await
        .unwrap();
    assert_eq!(replay.get(), first.get());
    assert_eq!(rig.source.calls.lock().unwrap().len(), 1);
    assert_eq!(rig.provider.0.lock().unwrap().len(), 1);
    let mut inline = params;
    inline["items"][0]["images"] = json!([{"content_type":"image/png","base64":PIXEL}]);
    let error = rig
        .system
        .clone()
        .answer(
            &serde_json::to_vec(&json!({"method":"classify.run","params":inline})).unwrap(),
            Some(&principal()),
            &scope(false),
        )
        .await
        .unwrap_err();
    assert_eq!(
        serde_json::from_slice::<Value>(&error).unwrap()["code"],
        "batch_id_reuse"
    );
}

#[tokio::test]
async fn image_reference_switch_off_preserves_catalog_schema_digest_and_describe_bytes() {
    let rig = rig(false);
    let catalog = rig
        .tools
        .answer(
            br#"{"name":"tool.catalog","arguments":{}}"#,
            None,
            &SendScope::Internal,
        )
        .await
        .unwrap();
    // Frozen from the previous released schema and the existing catalog envelope, not from
    // the new schema generator: changes to either hashes or schema bytes must be visible.
    assert_eq!(
        catalog.get(),
        include_str!("fixtures/llm-tools/catalog-before-image-references.json").trim_end()
    );
    let describe = rig
        .system
        .clone()
        .answer(
            br#"{"method":"role.describe","params":{}}"#,
            None,
            &SendScope::Internal,
        )
        .await
        .unwrap();
    assert_eq!(
        describe.get(),
        include_str!("../../broca-wire/tests/golden/classifier_role_describe.json").trim_end()
    );
    let refusal = tool(
        &rig,
        arguments(CLEF, json!([{"handle":HANDLE}])),
        &scope(false),
    )
    .await
    .unwrap_err();
    assert_eq!(refusal["code"], "invalid_params");
    assert_eq!(
        refusal["message"],
        cortexkit_model_batch::image::REFERENCE_UNSUPPORTED
    );
    assert!(rig.source.calls.lock().unwrap().is_empty());
    assert_nothing_written(&rig);
    let body = serde_json::to_vec(&json!({"method":"classify.run","params":{
        "batch_id":"unsupported","model":CLEF,"questions":{"q":{"type":"noul","instructions":"Is it urgent?"}},
        "items":[{"state":"picture","images":[{"handle":HANDLE}]}]
    }})).unwrap();
    let refused = rig
        .system
        .clone()
        .answer(&body, Some(&principal()), &scope(false))
        .await
        .unwrap_err();
    let refused: Value = serde_json::from_slice(&refused).unwrap();
    assert_eq!(
        refused["message"],
        cortexkit_model_batch::image::REFERENCE_UNSUPPORTED
    );
    assert_eq!(refused["detail"]["field"], "items[0].images[0]");
    assert!(rig
        .store
        .lookup("classifier", "unsupported")
        .unwrap()
        .is_none());
    let enabled = self::rig(true);
    let describe = serde_json::to_value(enabled.system.describe()).unwrap();
    assert_eq!(describe["image_reference"], json!({}));
    let catalog = enabled
        .tools
        .answer(
            br#"{"name":"tool.catalog","arguments":{}}"#,
            None,
            &SendScope::Internal,
        )
        .await
        .unwrap();
    let catalog: Value = serde_json::from_str(catalog.get()).unwrap();
    assert_eq!(
        catalog["tools"][0]["input_schema"],
        cortexkit_llm_tools::classify::input_schema_with_image_references()
    );
}
