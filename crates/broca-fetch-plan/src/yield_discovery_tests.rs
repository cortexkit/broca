use super::*;

struct Probe {
    unfinished: bool,
}

#[async_trait]
impl Fetcher for Probe {
    async fn call_yield_supported(&self, _: &str) -> Result<bool, FetchError> {
        if self.unfinished {
            std::future::pending().await
        } else {
            Err(FetchError::Lost("private provider error".into()))
        }
    }
    async fn catalog(&self, _: &str, _: &CatalogRequest) -> Result<Value, FetchError> {
        Ok(Value::Null)
    }
    async fn declare(&self, _: &str, _: &DeclareRequest) -> Result<Value, FetchError> {
        unreachable!()
    }
    async fn describe_compaction(&self, _: &str) -> Result<DescribeAnswer, FetchError> {
        unreachable!()
    }
}

#[tokio::test]
async fn first_admission_warns_for_failed_and_unfinished_yield_discovery() {
    let capture = broca_test_support::LogCapture::new();
    for unfinished in [false, true] {
        let provider = if unfinished {
            "yield-unfinished"
        } else {
            "yield-failed"
        };
        for _ in 0..2 {
            let (_, _, yieldable) = capture
                .scope_future(catalog_with_optional_keepalive(
                    &Probe { unfinished },
                    provider,
                    &CatalogRequest::new(Default::default()),
                    Instant::now() + ADMISSION_DEADLINE,
                ))
                .await
                .unwrap();
            assert!(!yieldable);
        }
    }
    let logs = capture.events();
    let warnings: Vec<_> = logs
        .iter()
        .filter(|event| event.target == "tool_yield")
        .collect();
    assert_eq!(warnings.len(), 2, "one warning per provider and cause");
    assert!(warnings
        .iter()
        .all(|event| !format!("{event:?}").contains("private provider error")));
}
