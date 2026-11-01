mod call_keepalive {
    use super::*;

    struct DeclaringFetcher {
        answer: Value,
        keepalive: bool,
    }

    #[async_trait]
    impl Fetcher for DeclaringFetcher {
        async fn call_keepalive_supported(&self, _: &str) -> Result<bool, FetchError> {
            Ok(self.keepalive)
        }
        async fn catalog(&self, _: &str, _: &CatalogRequest) -> Result<Value, FetchError> {
            Ok(self.answer.clone())
        }
        async fn declare(&self, _: &str, _: &DeclareRequest) -> Result<Value, FetchError> {
            panic!("the plan has no transforms")
        }
        async fn describe_compaction(&self, _: &str) -> Result<DescribeAnswer, FetchError> {
            panic!("the plan has no compaction provider")
        }
    }

    #[tokio::test]
    async fn manifest_promise_is_frozen_without_changing_plan_or_composition_digests() {
        let plan = plan_for("provider");
        let answer = catalog_answer(
            &plan,
            Some("teaching"),
            &cortexkit_role_tool_provider::catalog::system_text_digest("teaching"),
        );
        let session = broca_protocol::BindIdentity::new("/project", "test", "session");
        let mut manifests = Vec::new();
        for keepalive in [false, true] {
            let admitted = admit(
                &plan,
                &DeclaringFetcher {
                    answer: answer.clone(),
                    keepalive,
                },
                &session,
                Duration::from_secs(1),
            )
            .await
            .unwrap();
            assert_eq!(admitted.manifest.plan_identity, plan.identity);
            assert_eq!(
                admitted.manifest.composition_digest,
                plan.composition_digest
            );
            assert_eq!(
                frozen_provider_declarations_of(&admitted.manifest, "provider")
                    .and_then(|declarations| declarations.call_keepalive),
                keepalive.then_some(true),
            );
            let stored = serde_json::to_vec(&admitted.manifest).unwrap();
            let decoded: PlanManifest = serde_json::from_slice(&stored).unwrap();
            assert_eq!(serde_json::to_vec(&decoded).unwrap(), stored);
            let digest =
                crate::plan_identity(&serde_json::to_value(&admitted.manifest).unwrap()).unwrap();
            assert_eq!(
                crate::plan_identity(&serde_json::to_value(&decoded).unwrap()).unwrap(),
                digest
            );
            let value = serde_json::to_value(&decoded).unwrap();
            if !keepalive {
                assert!(value["fetches"][0].get("declarations_frozen").is_none());
                assert!(!String::from_utf8(stored)
                    .unwrap()
                    .contains("call_keepalive"));
            }
            manifests.push((decoded, digest));
        }
        assert_ne!(
            manifests[0].1, manifests[1].1,
            "the full-manifest digest includes the new promise"
        );
        let mut declaring = serde_json::to_value(&manifests[1].0).unwrap();
        declaring["fetches"][0]
            .as_object_mut()
            .unwrap()
            .remove("declarations_frozen");
        assert_eq!(declaring, serde_json::to_value(&manifests[0].0).unwrap());
    }
}
