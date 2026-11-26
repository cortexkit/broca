use super::*;
use broca_catalog::{CatalogCache, CatalogSource, FusiformCatalogResponse};
use broca_module_serve::serves::GatewayListing;
use broca_subc::vault::{ScopedCredential, ScopedList};
use cortexkit_role_llm_runner::serves::{check_serves, decode_serves, Serves, ServesModels};
use serde_json::{json, value::RawValue, Value};
use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Default)]
struct Vault(Mutex<Option<Arc<ScopedList>>>);

#[async_trait]
impl GatewayListing for Vault {
    async fn listing(self: Arc<Self>) -> Option<Arc<ScopedList>> {
        self.0.lock().unwrap().clone()
    }
}

impl Vault {
    fn set_gateway(&self, provider: &str) {
        *self.0.lock().unwrap() = Some(Arc::new(ScopedList {
            credentials: vec![ScopedCredential {
                id: "fixture-opaque-id".into(),
                categories: vec!["llm-gateway".into()],
                credential_type: "apikey".into(),
                serves: vec!["anthropic".into()],
                refresh_adapter: None,
                state: "active".into(),
                record_version: 1,
                operations: vec!["read".into()],
                account_id: None,
                email: None,
                org_name: None,
                provider_ids: vec![provider.into()],
                auth_method: Some("gateway".into()),
            }],
            grants: 1,
            grant_tuples: vec![],
            view: provider.into(),
        }));
    }
}

fn catalog(cache: &CatalogCache, version: i64, extra_provider: bool) {
    fn row(npm: &str, api: Option<&str>) -> BTreeMap<String, Value> {
        let mut row = BTreeMap::from([
            ("existence".into(), json!("present")),
            ("limit.context".into(), json!(32_000)),
            ("capability.attachment".into(), json!(false)),
            ("capability.reasoning".into(), json!(false)),
            ("capability.tool_call".into(), json!(true)),
            ("wire.npm".into(), json!({"value": npm, "from": "test"})),
        ]);
        if let Some(api) = api {
            row.insert("wire.api".into(), json!({"value": api, "from": "test"}));
        }
        row
    }
    let anthropic = || row("@ai-sdk/anthropic", Some("https://anthropic.example/v1"));
    let openai = || row("@ai-sdk/openai", Some("https://openai.example/v1"));
    let gemini = || row("@ai-sdk/google", Some("https://gemini.example"));
    let mut antigravity = row("@ai-sdk/google", None);
    antigravity.insert(
        "route.reasoning_wire_ids".into(),
        json!({"low":"fixture-low"}),
    );
    antigravity.insert("route.reasoning_default".into(), json!("low"));
    let mut batch = openai();
    batch.insert("model.kind".into(), json!("decision"));
    let mut models = BTreeMap::from([
        ("anthropic/claude-fixture".into(), anthropic()),
        ("openai/gpt-fixture".into(), openai()),
        ("google/gemini-fixture".into(), gemini()),
        ("google/antigravity-fixture".into(), antigravity),
        ("google/no-endpoint".into(), row("@ai-sdk/google", None)),
        ("not-a-runner/clef".into(), batch),
        (
            "not-supported/no-family".into(),
            row("other-sdk", Some("https://other.example")),
        ),
        ("opencode/hosted".into(), openai()),
        ("opencode-go/hosted".into(), anthropic()),
        ("meta/muse-fixture".into(), row("other-sdk", None)),
        ("mixed/fixture".into(), openai()),
        (
            "mixed/unsupported".into(),
            row("other-sdk", Some("https://other.example")),
        ),
    ]);
    if extra_provider {
        models.insert("added/model".into(), openai());
    }
    cache
        .install_for_test(
            FusiformCatalogResponse {
                source: "test".into(),
                resolved_at_ms: version,
                catalog_version: version,
                models,
                withheld: vec![],
                uncertain: vec![],
                wire_corrections: vec![],
            },
            CatalogSource::Fusiform,
        )
        .unwrap();
}

#[derive(serde::Deserialize)]
struct Reply<'a> {
    #[serde(borrow)]
    result: &'a RawValue,
}

async fn describe<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut R,
    writer: &mut W,
    params: Value,
    corr: u64,
) -> Vec<u8> {
    write_req(
        writer,
        7,
        corr,
        json!({"method":"role.describe","params":params}),
    )
    .await;
    let reply = read_one(reader).await;
    assert_eq!(reply.header.corr, corr);
    assert_eq!(reply.header.ty, FrameType::Response);
    reply.body
}

fn serves(body: &[u8]) -> Serves {
    let reply: Reply<'_> = serde_json::from_slice(body).unwrap();
    let raw: Value = serde_json::from_str(reply.result.get()).unwrap();
    let declared = broca_module_serve::manifest::BROCA_HARNESSES.map(String::from);
    let result = decode_serves(&raw["serves"], Some(&declared)).unwrap();
    check_serves(&result, Some(&declared)).unwrap();
    result
}

fn save_samples(hello: &[u8], describe: &[u8]) {
    let Some(dir) = std::env::var_os("BROCA_SERVES_SAMPLE_DIR") else {
        return;
    };
    #[derive(serde::Deserialize)]
    struct Hello<'a> {
        #[serde(borrow)]
        manifest: Manifest<'a>,
    }
    #[derive(serde::Deserialize)]
    struct Manifest<'a> {
        #[serde(borrow)]
        role_declarations: BTreeMap<String, &'a RawValue>,
    }
    let hello: Hello<'_> = serde_json::from_slice(hello).unwrap();
    let slot = hello.manifest.role_declarations[cortexkit_role_routing::ROLE_DECLARATIONS_KEY];
    let reply: Reply<'_> = serde_json::from_slice(describe).unwrap();
    let dir = std::path::PathBuf::from(dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("hello-routing-slot.json"), slot.get()).unwrap();
    std::fs::write(dir.join("role-describe-serves.json"), reply.result.get()).unwrap();
}

#[tokio::test]
async fn role_describe_serves_tracks_complete_catalog_and_gateway_snapshots() {
    let dir = tmpdir("describe-serves");
    let registry = registry(&dir).await;
    let catalog_cache = Arc::new(CatalogCache::empty());
    let vault = Arc::new(Vault::default());
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut reader, mut writer) = tokio::io::split(client);
    let (read_half, write_half) = tokio::io::split(server);
    let serve = tokio::spawn(broca_module_serve::serve::serve_connection_with_llm_tools(
        read_half,
        write_half,
        "broca",
        registry.clone(),
        registry,
        broca_module_serve::serve::VaultGauge::not_in_use(),
        Some(catalog_cache.clone()),
        Default::default(),
        None,
        None,
        None,
        None,
        None,
        None,
        Some(vault.clone()),
        None,
    ));
    let hello = read_one(&mut reader).await;
    assert_eq!(hello.header.ty, FrameType::Hello);
    let ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            resume_token: None,
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut writer, &ack).await.unwrap();
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new("broca", "describe"),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut writer, 0, 1, serde_json::to_value(bind).unwrap()).await;
    assert_eq!(read_one(&mut reader).await.header.ty, FrameType::Response);

    let opt_in = || json!({"serves": true});
    let not_ready = serves(&describe(&mut reader, &mut writer, opt_in(), 2).await);
    assert_eq!(not_ready.harnesses, Some(BTreeMap::new()));

    catalog(&catalog_cache, 1, false);
    let no_vault = serves(&describe(&mut reader, &mut writer, opt_in(), 3).await);
    assert!(no_vault.list_for(Some("broca")).is_none());
    assert_eq!(
        no_vault.list_for(Some("broca-uncapped")).unwrap()[0].provider,
        "openai"
    );
    vault.set_gateway("anthropic-gw-acme");
    let wire = describe(&mut reader, &mut writer, opt_in(), 4).await;
    let first = serves(&wire);
    let capped = first.list_for(Some("broca")).unwrap();
    assert_eq!(
        capped
            .iter()
            .map(|entry| entry.provider.as_str())
            .collect::<Vec<_>>(),
        [
            "anthropic",
            "anthropic-gw-acme",
            "anthropic-platform",
            "google",
            "meta",
            "mixed",
            "openai-platform",
        ]
    );
    let uncapped = first.list_for(Some("broca-uncapped")).unwrap();
    assert_eq!(uncapped.len(), 1);
    assert_eq!(uncapped[0].provider, "openai");
    assert_eq!(uncapped[0].models, ServesModels::All);
    let platform = capped
        .iter()
        .find(|entry| entry.provider == "openai-platform")
        .expect("routing can place OpenAI API keys on the capped harness");
    assert_eq!(platform.models, ServesModels::All);
    assert!(platform.includes(
        "gpt-fixture",
        &std::collections::BTreeSet::from(["gpt-fixture".into()])
    ));
    assert!(!capped.iter().any(|entry| entry.provider == "openai"));
    assert_eq!(capped[1].models, ServesModels::All);
    assert_eq!(capped[3].except.as_deref().unwrap(), ["no-endpoint"]);
    assert_eq!(capped[5].except.as_deref().unwrap(), ["unsupported"]);
    save_samples(&hello.body, &wire);

    catalog(&catalog_cache, 2, false);
    let same = serves(&describe(&mut reader, &mut writer, opt_in(), 5).await);
    assert_eq!(same, first, "only changes to the lists advance generation");
    catalog(&catalog_cache, 3, true);
    let next = serves(&describe(&mut reader, &mut writer, opt_in(), 6).await);
    assert_ne!(next.generation, same.generation);
    assert_eq!(next.list_for(Some("broca")).unwrap()[0].provider, "added");
    vault.set_gateway("anthropic-gw-other");
    let other = serves(&describe(&mut reader, &mut writer, opt_in(), 7).await);
    assert_ne!(other.generation, next.generation);
    let capped = other.list_for(Some("broca")).unwrap();
    assert!(capped
        .iter()
        .any(|entry| entry.provider == "anthropic-gw-other"));
    assert!(!capped
        .iter()
        .any(|entry| entry.provider == "anthropic-gw-acme"));

    drop(writer);
    drop(reader);
    assert_eq!(serve.await.unwrap().unwrap(), ServeEnd::Eof);
    std::fs::remove_dir_all(dir).unwrap();
}
