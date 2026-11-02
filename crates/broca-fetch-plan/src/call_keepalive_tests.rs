mod call_keepalive {
    use super::*;

    #[test]
    fn pre_declaration_manifest_golden_keeps_its_bytes_and_no_keepalive_promise() {
        let historical =
            include_str!("../../broca-wal/tests/fixtures/plan_manifest_pre_declarations.json")
                .trim_end();
        let record: broca_wal::WalRecord = serde_json::from_str(historical).unwrap();
        let broca_wal::WalRecord::RunStarted {
            manifest: Some(ref manifest),
            ..
        } = record
        else {
            panic!("the historical fixture contains its frozen manifest");
        };
        assert!(!manifest.fetches.is_empty());
        for fetch in &manifest.fetches {
            assert!(frozen_provider_declarations_of(manifest, &fetch.provider)
                .and_then(|d| d.call_keepalive)
                .is_none());
        }
        assert_eq!(serde_json::to_string(&record).unwrap(), historical);
    }

    struct DeclaringFetcher {
        answer: Value,
        keepalive: bool,
    }

    struct UnavailablePromiseFetcher(Value);

    #[async_trait]
    impl Fetcher for UnavailablePromiseFetcher {
        async fn call_keepalive_supported(&self, _: &str) -> Result<bool, FetchError> {
            Err(FetchError::Lost("daemon catalog unavailable".into()))
        }
        async fn catalog(&self, _: &str, _: &CatalogRequest) -> Result<Value, FetchError> {
            Ok(self.0.clone())
        }
        async fn declare(&self, _: &str, _: &DeclareRequest) -> Result<Value, FetchError> {
            panic!("the plan has no transforms")
        }
        async fn describe_compaction(&self, _: &str) -> Result<DescribeAnswer, FetchError> {
            panic!("the plan has no compaction provider")
        }
    }

    #[tokio::test]
    async fn a_failed_keepalive_lookup_admits_the_send_with_no_frozen_promise() {
        let plan = plan_for("unavailable-keepalive-catalog");
        let answer = catalog_answer(
            &plan,
            Some("teaching"),
            &cortexkit_role_tool_provider::catalog::system_text_digest("teaching"),
        );
        let admitted = admit(
            &plan,
            &UnavailablePromiseFetcher(answer),
            &broca_protocol::BindIdentity::new("/project", "test", "lookup-unavailable"),
            Duration::from_secs(1),
        )
        .await
        .expect("optional liveness discovery cannot refuse a send");
        assert!(frozen_provider_declarations_of(
            &admitted.manifest,
            "unavailable-keepalive-catalog"
        )
        .and_then(|declarations| declarations.call_keepalive)
        .is_none());
        assert_eq!(admitted.manifest.system_text, "teaching");
    }

    struct TimedPromiseFetcher {
        answer: Value,
        promise_delay: Duration,
        fetch_delay: Duration,
        fail: bool,
    }

    #[async_trait]
    impl Fetcher for TimedPromiseFetcher {
        async fn call_keepalive_supported(&self, _: &str) -> Result<bool, FetchError> {
            tokio::time::sleep(self.promise_delay).await;
            if self.fail {
                Err(FetchError::Lost("catalog unavailable".into()))
            } else {
                Ok(true)
            }
        }
        async fn catalog(&self, _: &str, _: &CatalogRequest) -> Result<Value, FetchError> {
            tokio::time::sleep(self.fetch_delay).await;
            Ok(self.answer.clone())
        }
        async fn declare(&self, _: &str, _: &DeclareRequest) -> Result<Value, FetchError> {
            unreachable!("no transform item")
        }
        async fn describe_compaction(&self, _: &str) -> Result<DescribeAnswer, FetchError> {
            unreachable!("no compaction item")
        }
    }

    #[tokio::test(start_paused = true)]
    async fn optional_discovery_never_extends_the_required_fetch_fixture_latency() {
        for (delay_ms, fail, counter, expected) in [
            (200, false, "lookup_unfinished", None),
            (0, true, "lookup_failed", None),
            (20, false, "probes", Some(true)),
        ] {
            let plan = plan_for("fixture-latency");
            let fetcher = TimedPromiseFetcher {
                answer: catalog_answer(
                    &plan,
                    Some("teaching"),
                    &cortexkit_role_tool_provider::catalog::system_text_digest("teaching"),
                ),
                promise_delay: Duration::from_millis(delay_ms),
                fetch_delay: Duration::from_millis(50),
                fail,
            };
            let before = crate::keepalive_discovery_health()[counter]
                .as_u64()
                .unwrap();
            let started = Instant::now();
            let admitted = admit(
                &plan,
                &fetcher,
                &broca_protocol::BindIdentity::new("/project", "test", "fixture-latency"),
                Duration::from_secs(1),
            )
            .await
            .unwrap();
            assert_eq!(started.elapsed(), Duration::from_millis(50));
            assert_eq!(
                frozen_provider_declarations_of(&admitted.manifest, "fixture-latency")
                    .and_then(|d| d.call_keepalive),
                expected
            );
            assert!(
                crate::keepalive_discovery_health()[counter]
                    .as_u64()
                    .unwrap()
                    > before
            );
        }
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
