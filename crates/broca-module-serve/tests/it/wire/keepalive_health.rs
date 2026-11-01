use broca_fetch_plan::{CompactionSupport, DescribeAnswer, FetchError, Fetcher};
use cortexkit_role_step_transform_provider::subscription::DeclareRequest;
use cortexkit_role_tool_provider::catalog::CatalogRequest;

struct FailedPromise(serde_json::Value);

#[async_trait::async_trait]
impl Fetcher for FailedPromise {
    async fn call_keepalive_supported(&self, _: &str) -> Result<bool, FetchError> {
        Err(FetchError::Lost("optional discovery unavailable".into()))
    }
    async fn catalog(&self, _: &str, _: &CatalogRequest) -> Result<serde_json::Value, FetchError> {
        Ok(self.0.clone())
    }
    async fn declare(&self, _: &str, _: &DeclareRequest) -> Result<serde_json::Value, FetchError> {
        unreachable!("the fixture has no transform item")
    }
    async fn describe_compaction(&self, _: &str) -> Result<DescribeAnswer, FetchError> {
        unreachable!("the fixture has no compaction item")
    }
}

pub(super) async fn record_failed_lookup() {
    let composition = serde_json::json!({"providers":[{"provider":"health-probe","tools":[]}]});
    let digest = cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap();
    let plan = broca_fetch_plan::parse_plan(&serde_json::json!({
        "tool_items":[], "system_text_items":[{"provider":"health-probe","preset":"persona","params":{}}],
        "step_transform_items":[], "composition":composition, "composition_digest":digest,
    }), CompactionSupport::Unsupported).unwrap();
    let text = "teaching";
    let fetcher = FailedPromise(serde_json::json!({
        "generation":"1", "catalog_digest":"fixture", "composition_digest":digest, "tools":[],
        "system_text":{
            "text":text, "item_digest":cortexkit_role_tool_provider::catalog::system_text_digest(text),
            "preflight_digest":"preflight", "composition_digest":digest, "tool_names":[],
        },
    }));
    broca_fetch_plan::admit(
        &plan,
        &fetcher,
        &broca_protocol::BindIdentity::new("/health-probe", "test", "optional-lookup"),
        std::time::Duration::from_secs(1),
    )
    .await
    .expect("optional discovery must not refuse admission");
}
