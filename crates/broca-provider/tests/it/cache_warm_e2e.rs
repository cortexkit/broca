//! Prompt-cache warms over the live Anthropic transport, against a local server that records
//! every request body it receives. The expected warm body is derived from the base request's own
//! bytes by a plain text edit, never by the code that builds warms.

#[cfg(feature = "live")]
mod gated {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use broca_core::{Provider, WarmFailure};
    use broca_protocol::{
        content::{ContentBlock, Message, Role},
        error::ACCOUNT_LIMIT_EXHAUSTED,
        request::Tool,
        CallOptions, GenerationConfig,
    };
    use broca_provider::{
        families::{anthropic_frozen_config, AnthropicMessages},
        variant::{FrozenVariant, VariantRequest},
        wire::{AuthHandle, CacheClass, CachePolicy, CacheTtl, FrozenRenderConfig, MarkerSyntax},
        LiveProvider,
    };
    use sha2::{Digest, Sha256};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    /// What the server answers one connection with.
    #[derive(Clone)]
    enum Reply {
        Full {
            status: u16,
            headers: Vec<(&'static str, &'static str)>,
            body: Vec<u8>,
        },
    }

    /// A server that answers each connection on its own task.
    struct Server {
        url: String,
        bodies: Arc<Mutex<Vec<Vec<u8>>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl Server {
        async fn start(script: Vec<Reply>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let bodies = Arc::new(Mutex::new(Vec::new()));
            let task = {
                let bodies = Arc::clone(&bodies);
                tokio::spawn(async move {
                    for reply in script {
                        let (mut socket, _) = listener.accept().await.unwrap();
                        let bodies = Arc::clone(&bodies);
                        tokio::spawn(async move {
                            let body = read_body(&mut socket).await;
                            bodies.lock().unwrap().push(body);
                            write_reply(&mut socket, reply).await;
                        });
                    }
                })
            };
            Self { url, bodies, task }
        }

        fn bodies(&self) -> Vec<Vec<u8>> {
            self.bodies.lock().unwrap().clone()
        }
    }

    async fn read_body(socket: &mut TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        let mut chunk = [0u8; 8192];
        let header_end = loop {
            let read = socket.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "client disconnected before sending the request");
            request.extend_from_slice(&chunk[..read]);
            if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                break end + 4;
            }
        };
        let headers = String::from_utf8_lossy(&request[..header_end]).to_ascii_lowercase();
        let length = headers
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .map(|value| value.trim().parse::<usize>().unwrap())
            .unwrap_or(0);
        while request.len() < header_end + length {
            let read = socket.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "client disconnected mid-body");
            request.extend_from_slice(&chunk[..read]);
        }
        request[header_end..header_end + length].to_vec()
    }

    async fn write_reply(socket: &mut TcpStream, reply: Reply) {
        match reply {
            Reply::Full {
                status,
                headers,
                body,
            } => {
                let mut head = format!(
                    "HTTP/1.1 {status} Scripted\r\nContent-Length: {}\r\nConnection: close\r\n",
                    body.len()
                );
                for (name, value) in headers {
                    head.push_str(&format!("{name}: {value}\r\n"));
                }
                head.push_str("\r\n");
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(&body).await;
                let _ = socket.flush().await;
            }
        }
    }

    /// The answer to a warm: a message with no content, the usage that warm read and wrote, and
    /// the unified rate-limit headers a subscription account reports.
    fn warm_answer() -> Reply {
        Reply::Full {
            status: 200,
            headers: vec![
                ("Content-Type", "application/json"),
                ("anthropic-ratelimit-unified-status", "allowed"),
                ("anthropic-ratelimit-unified-5h-utilization", "0.25"),
                ("x-unrelated", "dropped"),
            ],
            body: br#"{"id":"msg_w","type":"message","role":"assistant","content":[],"stop_reason":"max_tokens","usage":{"input_tokens":3,"cache_read_input_tokens":4000,"cache_creation_input_tokens":0,"output_tokens":0}}"#.to_vec(),
        }
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    /// A first-party Anthropic config frozen with 1h cache markers and adaptive thinking at high
    /// effort: the shape a warm must keep byte for byte.
    fn warmable_frozen(server: &Server) -> FrozenRenderConfig {
        let mut frozen = anthropic_frozen_config("claude-opus-4-8");
        frozen.base_url = Some(server.url.clone());
        frozen.cache = Some(CachePolicy {
            version: 1,
            class: CacheClass::Markers {
                syntax: MarkerSyntax::AnthropicCacheControl,
                ttl: CacheTtl::OneHour,
                max_breakpoints: 4,
                lookback_blocks: 20,
                tail: None,
                omit_ttl: false,
            },
        });
        frozen.variant = Some(FrozenVariant {
            name: "high".into(),
            request: VariantRequest::Anthropic {
                thinking: Some(serde_json::json!({"type": "adaptive", "display": "summarized"})),
                effort: Some("high".into()),
            },
        });
        frozen
    }

    fn read_tool() -> Tool {
        Tool {
            name: "read".into(),
            description: "Read a file".into(),
            input_schema:
                serde_json::json!({"type": "object", "properties": {"path": {"type": "string"}}})
                    .into(),
            module: None,
            execution_mode: Default::default(),
            reply: None,
        }
    }

    fn message(role: Role, text: &str) -> Message {
        Message {
            turn_system: false,
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role,
            content: vec![ContentBlock::Text { text: text.into() }],
        }
    }

    /// The warm body a provider must send for `base`, derived by editing `base`'s text: the one
    /// `max_tokens` value becomes 0 and the one `"stream":true` member goes, nothing else.
    fn expected_warm_body(base: &[u8]) -> Vec<u8> {
        let text = String::from_utf8(base.to_vec()).unwrap();
        let value: serde_json::Value = serde_json::from_slice(base).unwrap();
        let max_tokens = format!(r#""max_tokens":{},"#, value["max_tokens"]);
        assert_eq!(text.matches(&max_tokens).count(), 1, "{text}");
        assert_eq!(text.matches(r#""stream":true,"#).count(), 1, "{text}");
        text.replacen(&max_tokens, r#""max_tokens":0,"#, 1)
            .replacen(r#""stream":true,"#, "", 1)
            .into_bytes()
    }

    /// The warm body differs from the base request in exactly `max_tokens` and `stream`: the
    /// system text, tools, messages, every 1h cache marker, adaptive thinking, effort and
    /// temperature are byte-identical. The provider's usage and rate-limit headers come back, and
    /// the digest it reports is of the bytes the server received.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_warm_body_changes_only_max_tokens_and_stream() {
        let server = Server::start(vec![warm_answer()]).await;
        let provider = LiveProvider::new(
            AnthropicMessages,
            warmable_frozen(&server),
            AuthHandle::Bearer("t".into()),
        );
        let mut opts = CallOptions::new(vec![
            message(Role::System, "You are terse."),
            message(Role::User, "first question"),
            message(Role::Assistant, "first answer"),
            message(Role::User, "second question"),
        ]);
        opts.tools = Arc::new(vec![read_tool()]);
        opts.generation = Arc::new(GenerationConfig {
            temperature: Some(0.5),
            ..GenerationConfig::default()
        });
        let base = provider.render_request(&opts);
        let base_text = String::from_utf8(base.clone()).unwrap();
        for kept in [
            r#""ttl":"1h""#,
            r#""type":"adaptive""#,
            r#""effort":"high""#,
            r#""temperature":0.5"#,
            r#""name":"read""#,
        ] {
            assert!(
                base_text.contains(kept),
                "the base must carry {kept}: {base_text}"
            );
        }

        let sent = provider.warm(&base).await.expect("the warm is answered");
        let bodies = server.bodies();
        assert_eq!(bodies.len(), 1);
        assert_eq!(
            String::from_utf8(bodies[0].clone()).unwrap(),
            String::from_utf8(expected_warm_body(&base)).unwrap()
        );
        assert_eq!(sent.warm_body_sha256, sha256_hex(&bodies[0]));
        assert_eq!(sent.usage.input_tokens, Some(3));
        assert_eq!(sent.usage.cached_input_tokens, Some(4000));
        assert_eq!(sent.usage.cache_write_tokens, Some(0));
        assert_eq!(
            sent.ratelimit_headers,
            vec![
                (
                    "anthropic-ratelimit-unified-status".to_string(),
                    "allowed".to_string()
                ),
                (
                    "anthropic-ratelimit-unified-5h-utilization".to_string(),
                    "0.25".to_string()
                ),
            ]
        );
    }

    #[tokio::test]
    async fn codex_warm_caps_only_output_and_reports_zero_read_once_without_retry() {
        use broca_provider::{
            families::{frozen_config_for_family, OpenAiResponsesChatgpt},
            spec::WireFamilyId,
        };
        let logs = broca_test_support::LogCapture::new();
        let server = Server::start(vec![Reply::Full {
            status: 200,
            headers: vec![("Content-Type", "text/event-stream")],
            body: b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"discard me\"}\n\ndata: {\"type\":\"response.incomplete\",\"response\":{\"usage\":{\"input_tokens\":5648,\"input_tokens_details\":{\"cached_tokens\":0,\"cache_write_tokens\":0},\"output_tokens\":16}}}\n\n".to_vec(),
        }]).await;
        let mut frozen = frozen_config_for_family(WireFamilyId::OpenAiResponsesChatgpt, "gpt-5");
        frozen.base_url = Some(server.url.clone());
        frozen.reasoning_effort = Some("high".into());
        let provider = LiveProvider::new(
            OpenAiResponsesChatgpt,
            frozen,
            AuthHandle::Bearer("t".into()),
        );
        let base =
            provider.render_request(&CallOptions::new(vec![message(Role::User, "question")]));
        let base_text = String::from_utf8(base.clone()).unwrap();
        assert!(base_text.contains(r#""effort":"high""#));
        assert!(base_text.contains(r#""stream":true"#));
        assert!(!base_text.contains("max_output_tokens"));
        let expected = base_text
            .replacen(r#""model":"#, r#""max_output_tokens":16,"model":"#, 1)
            .into_bytes();
        let sent = logs.scope_future(provider.warm(&base)).await.unwrap();
        assert_eq!(server.bodies(), vec![expected]);
        assert_eq!(sent.usage.input_tokens, Some(5648));
        assert_eq!(sent.usage.cached_input_tokens, Some(0));
        assert_eq!(sent.usage.cache_write_tokens, Some(0));
        assert_eq!(sent.usage.output_tokens, Some(16));
        let zero_read: Vec<_> = logs
            .events()
            .into_iter()
            .filter(|event| {
                event
                    .fields
                    .values()
                    .any(|value| value.contains("Codex cache warm read zero cached tokens"))
            })
            .collect();
        assert_eq!(zero_read.len(), 1);
        assert_eq!(zero_read[0].level, tracing::Level::INFO);
    }

    #[tokio::test]
    async fn responses_warm_reads_cached_and_fresh_usage_from_json() {
        use broca_provider::{
            families::{frozen_config_for_family, OpenAiResponses},
            spec::WireFamilyId,
        };
        let server = Server::start(vec![Reply::Full {
            status: 200, headers: vec![],
            body: br#"{"usage":{"input_tokens":5648,"input_tokens_details":{"cached_tokens":4864,"cache_write_tokens":0},"output_tokens":16}}"#.to_vec(),
        }]).await;
        let mut frozen = frozen_config_for_family(WireFamilyId::OpenAiResponses, "gpt-5");
        frozen.base_url = Some(server.url.clone());
        let provider = LiveProvider::new(OpenAiResponses, frozen, AuthHandle::Bearer("t".into()));
        let base =
            provider.render_request(&CallOptions::new(vec![message(Role::User, "question")]));
        let mut expected: serde_json::Value = serde_json::from_slice(&base).unwrap();
        expected["max_output_tokens"] = 16.into();
        let sent = provider.warm(&base).await.unwrap();
        assert_eq!(
            server.bodies(),
            vec![broca_protocol::canonical_json_bytes(&expected)]
        );
        assert_eq!(sent.usage.input_tokens, Some(784));
        assert_eq!(sent.usage.cached_input_tokens, Some(4864));
        assert_eq!(sent.usage.cache_write_tokens, Some(0));
    }

    /// A credential that now belongs to another account sends nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changed_account_sends_nothing() {
        let server = Server::start(vec![warm_answer()]).await;
        let provider = LiveProvider::new(
            AnthropicMessages,
            warmable_frozen(&server),
            AuthHandle::Bearer("t".into()),
        )
        .with_account_refs(Some("acct-1".into()), Some("acct-2".into()));
        let base = provider.render_request(&CallOptions::new(vec![message(Role::User, "q")]));
        assert_eq!(
            provider.warm(&base).await,
            Err(WarmFailure::AccountChanged {
                frozen: Some("acct-1".into()),
                served: Some("acct-2".into()),
            })
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(server.bodies().is_empty(), "nothing may reach the provider");
    }

    /// A warm that meets an exhausted subscription comes back as the account-limit error, with
    /// its reset time, so the next warm is refused before sending.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_exhausted_account_is_reported_with_its_reset() {
        let server = Server::start(vec![Reply::Full {
            status: 429,
            headers: vec![
                ("Content-Type", "application/json"),
                ("anthropic-ratelimit-unified-status", "rejected"),
                ("anthropic-ratelimit-unified-reset", "1790740800"),
                ("retry-after", "352835"),
            ],
            body: br#"{"type":"error","error":{"type":"rate_limit_error","message":"limit"}}"#
                .to_vec(),
        }])
        .await;
        let provider = LiveProvider::new(
            AnthropicMessages,
            warmable_frozen(&server),
            AuthHandle::Bearer("t".into()),
        );
        let base = provider.render_request(&CallOptions::new(vec![message(Role::User, "q")]));
        let Err(WarmFailure::Provider(error)) = provider.warm(&base).await else {
            panic!("expected a provider refusal");
        };
        assert_eq!(
            error.provider_code.as_deref(),
            Some(ACCOUNT_LIMIT_EXHAUSTED)
        );
        assert_eq!(error.resets_at_ms, Some(1_790_740_800_000));
        assert_eq!(
            server.bodies().len(),
            1,
            "a warm is one attempt, never retried"
        );
    }
}
