//! Provider attempt records, end to end: a real `run_loop` step over the live HTTP transport,
//! against a local server that scripts each attempt's bytes, must leave exactly one
//! `model_attempt_finished` WAL record per HTTP attempt, and each record's response digest
//! must be the SHA-256 of exactly the bytes that server wrote (or the prefix it wrote before
//! cutting the stream). The expected digests are computed from the server's own bytes, never
//! from anything broca reports.

// A file-level `cfg` would delete this file when the feature is off and report "0 tests ok";
// gating a module keeps a visible notice instead.
#[cfg(not(feature = "live"))]
#[test]
fn this_suite_needs_its_feature_enabled() {
    eprintln!(
        "SKIP attempt_records: compiled without its feature, so none of its cases exist. \
         This reports `ok` -- re-run with `-F broca-provider/live` to execute them."
    );
}

#[cfg(feature = "live")]
mod gated {
    use std::{
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use broca_core::{
        project_control, replay, run_loop, run_loop_with_control, AdmissionCheckpointV1, AllowAll,
        FileLeaseStore, FileWalStore, OutputEvent, RunControl, RunRequest, StubToolPlane, WalStore,
    };
    use std::{
        collections::VecDeque,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use broca_protocol::{
        caller::{PauseReason, RunFinishReason},
        content::{ContentBlock, Message, Role},
        display::DisplayEvent,
        request::{ModelHandle, StopCondition, ToolChoice},
        AttemptOutcome, BindIdentity, CredentialRefresh, GenerationConfig, RunId,
    };
    use broca_provider::{
        credential_refresh::{CredentialRefresher, RefreshFuture, RefreshedCredential},
        families::{anthropic_frozen_config, AnthropicMessages},
        wire::AuthHandle,
        LiveProvider,
    };
    use broca_wal::{FramedRecord, WalRecord};
    use sha2::{Digest, Sha256};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::mpsc,
    };
    use tokio_util::sync::CancellationToken;

    /// The transport's cap on a non-2xx body read (`MAX_HTTP_ERROR_BODY_READ_BYTES`).
    const ERROR_BODY_CAP: usize = 1024 * 1024;

    /// One complete Anthropic Messages stream whose text is `text`.
    fn anthropic_sse(text: &str) -> Vec<u8> {
        let events = [
            (
                "message_start",
                r#"{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"m","usage":{"input_tokens":3,"output_tokens":0}}}"#.to_string(),
            ),
            (
                "content_block_start",
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#.to_string(),
            ),
            (
                "content_block_delta",
                serde_json::json!({
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "text_delta", "text": text},
                })
                .to_string(),
            ),
            (
                "content_block_stop",
                r#"{"type":"content_block_stop","index":0}"#.to_string(),
            ),
            (
                "message_delta",
                r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}"#.to_string(),
            ),
            ("message_stop", r#"{"type":"message_stop"}"#.to_string()),
        ];
        let mut body = String::new();
        for (event, data) in events {
            body.push_str(&format!("event: {event}\ndata: {data}\n\n"));
        }
        body.into_bytes()
    }

    /// What the server does with one connection.
    enum Reply {
        /// A complete response.
        Full {
            status: u16,
            headers: Vec<(&'static str, &'static str)>,
            body: Vec<u8>,
        },
        /// A 200 promising `body.len()` bytes that writes only the first `sent` and closes.
        Cut { body: Vec<u8>, sent: usize },
        /// A 200 promising more than `prefix`, writing `prefix`, then holding the connection
        /// open until the client drops it.
        Stall { prefix: Vec<u8> },
    }

    /// What one connection exchanged, as seen by the server.
    #[derive(Debug, Clone)]
    struct Served {
        request_body: Vec<u8>,
        /// The request's `authorization` header value. Test tokens only, and still compared
        /// as booleans and never printed, like a real credential would be.
        authorization: Option<String>,
        /// Every response body byte the server handed to its socket.
        response_body_written: Vec<u8>,
    }

    struct Server {
        url: String,
        served: Arc<Mutex<Vec<Served>>>,
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
            let served = Arc::new(Mutex::new(Vec::new()));
            let task = {
                let served = Arc::clone(&served);
                tokio::spawn(async move {
                    for reply in script {
                        let (mut socket, _) = listener.accept().await.unwrap();
                        let (authorization, request_body) = read_request(&mut socket).await;
                        let written = write_reply(&mut socket, reply).await;
                        served.lock().unwrap().push(Served {
                            request_body,
                            authorization,
                            response_body_written: written,
                        });
                        // Dropping the socket here closes the connection (or ends a cut).
                    }
                })
            };
            Self { url, served, task }
        }

        fn served(&self) -> Vec<Served> {
            self.served.lock().unwrap().clone()
        }
    }

    async fn read_request(socket: &mut TcpStream) -> (Option<String>, Vec<u8>) {
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
        let raw_headers = String::from_utf8_lossy(&request[..header_end]).into_owned();
        let authorization = raw_headers.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("authorization")
                .then(|| value.trim().to_string())
        });
        let headers = raw_headers.to_ascii_lowercase();
        let content_length = headers
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .map(|value| value.trim().parse::<usize>().unwrap())
            .unwrap_or(0);
        while request.len() < header_end + content_length {
            let read = socket.read(&mut chunk).await.unwrap();
            assert_ne!(
                read, 0,
                "client disconnected before the request body completed"
            );
            request.extend_from_slice(&chunk[..read]);
        }
        (
            authorization,
            request[header_end..header_end + content_length].to_vec(),
        )
    }

    fn head(status: u16, content_length: usize, headers: &[(&str, &str)]) -> Vec<u8> {
        let mut head = format!(
            "HTTP/1.1 {status} Scripted\r\nContent-Length: {content_length}\r\nConnection: close\r\n"
        );
        for (name, value) in headers {
            head.push_str(&format!("{name}: {value}\r\n"));
        }
        head.push_str("\r\n");
        head.into_bytes()
    }

    /// Write `reply`, returning exactly the body bytes handed to the socket. A client that
    /// stops reading (the capped error body) makes a write fail; the bytes the client did read
    /// are still a prefix of what is returned here.
    async fn write_reply(socket: &mut TcpStream, reply: Reply) -> Vec<u8> {
        match reply {
            Reply::Full {
                status,
                headers,
                body,
            } => {
                socket
                    .write_all(&head(status, body.len(), &headers))
                    .await
                    .unwrap();
                let _ = socket.write_all(&body).await;
                let _ = socket.flush().await;
                body
            }
            Reply::Cut { body, sent } => {
                socket
                    .write_all(&head(
                        200,
                        body.len(),
                        &[("Content-Type", "text/event-stream")],
                    ))
                    .await
                    .unwrap();
                socket.write_all(&body[..sent]).await.unwrap();
                socket.flush().await.unwrap();
                body[..sent].to_vec()
            }
            Reply::Stall { prefix } => {
                socket
                    .write_all(&head(
                        200,
                        prefix.len() + 4096,
                        &[("Content-Type", "text/event-stream")],
                    ))
                    .await
                    .unwrap();
                socket.write_all(&prefix).await.unwrap();
                socket.flush().await.unwrap();
                // Hold the connection until the client goes away.
                let mut byte = [0u8; 1];
                while matches!(socket.read(&mut byte).await, Ok(n) if n > 0) {}
                prefix
            }
        }
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn scratch(tag: &str) -> PathBuf {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-attempt-records-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        dir
    }

    fn request(dir: &Path) -> RunRequest {
        RunRequest {
            input_written_at_ms: None,
            directives: None,
            session_class: None,
            retention: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("r".into()),
            session: BindIdentity::new(dir.join("proj"), "attempts", "s1"),
            model: ModelHandle::new("anthropic", "m"),
            input: vec![Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                tags: Default::default(),
                role: Role::User,
                content: vec![ContentBlock::Text { text: "hi".into() }],
            }],
            tools: vec![],
            policy_allowed: Default::default(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            cache: None,
            stop_when: vec![StopCondition::StepCount { n: 5 }],
            submission_id: None,
            budget: None,
            context_limit: None,
            work_class: None,
            context_management: None,
            variant: None,
            service_tier: None,
            inherit_frozen_cache: false,
            structured_output: None,
        }
    }

    fn provider(server: &Server) -> LiveProvider<AnthropicMessages> {
        let mut frozen = anthropic_frozen_config("m");
        frozen.base_url = Some(server.url.clone());
        LiveProvider::new(AnthropicMessages, frozen, AuthHandle::Bearer("test".into()))
    }

    fn frames(wal: &FileWalStore, session: &BindIdentity) -> Vec<FramedRecord> {
        WalStore::read_records(wal, session).unwrap()
    }

    /// One reported attempt: its record's lineage and sequence number, and its headers.
    type ReportedAttempt = (
        Option<broca_wal::WalLineageId>,
        u64,
        std::collections::BTreeMap<String, String>,
    );

    /// What the run loop told an account reporter, in order.
    #[derive(Default)]
    struct CapturedReports {
        started: Mutex<Vec<(Option<broca_wal::WalLineageId>, u64)>>,
        attempts: Mutex<Vec<ReportedAttempt>>,
    }

    impl broca_core::AccountReporter for CapturedReports {
        fn run_started(&self, lineage: Option<broca_wal::WalLineageId>, wal_seq: u64) {
            self.started.lock().unwrap().push((lineage, wal_seq));
        }

        fn attempt_recorded(
            &self,
            lineage: Option<broca_wal::WalLineageId>,
            wal_seq: u64,
            attempt: &broca_protocol::ModelAttempt,
        ) {
            self.attempts.lock().unwrap().push((
                lineage,
                wal_seq,
                attempt.account_observation.ratelimit_headers.clone(),
            ));
        }
    }

    /// Every attempt the loop records is handed to the account reporter with the lineage and
    /// sequence number of ITS OWN WAL record, and with every `anthropic-ratelimit-unified-*`
    /// header the server sent, by lowercase name, value byte-for-byte as the HTTP layer delivers it
    /// (which drops only the whitespace around a value): unknown names and odd values included,
    /// other headers excluded. The headers never reach the WAL.
    #[tokio::test(flavor = "multi_thread")]
    async fn account_reports_name_each_attempt_record_and_carry_its_ratelimit_headers_verbatim() {
        let dir = scratch("account-reports");
        let server = Server::start(vec![
            Reply::Full {
                status: 503,
                headers: vec![
                    ("Content-Type", "text/plain"),
                    ("retry-after-ms", "0"),
                    ("Anthropic-Ratelimit-Unified-5h-Utilization", "0.9950"),
                    ("anthropic-ratelimit-unified-some-future-window", "x;q=1, 2"),
                    ("anthropic-ratelimit-unified-7d-reset", "1.79068680e9"),
                    ("anthropic-ratelimit-requests-remaining", "5"),
                    ("x-anthropic-ratelimit-unified-fake", "no"),
                ],
                body: b"busy".to_vec(),
            },
            Reply::Full {
                status: 200,
                headers: vec![
                    ("Content-Type", "text/event-stream"),
                    ("anthropic-ratelimit-unified-status", "allowed_warning"),
                ],
                body: anthropic_sse("hello"),
            },
        ])
        .await;
        let provider = provider(&server);
        let lease = FileLeaseStore::new(dir.join("lease"));
        let wal = FileWalStore::new(dir.join("wal"));
        let req = request(&dir);
        let session = req.session.clone();
        let reports = Arc::new(CapturedReports::default());
        let control = RunControl {
            account_reporter: Some(Arc::clone(&reports) as Arc<dyn broca_core::AccountReporter>),
            ..RunControl::default()
        };
        let (tx, mut rx) = mpsc::channel(256);
        let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let summary = run_loop_with_control(
            req,
            &provider,
            &StubToolPlane::new(),
            &lease,
            &AllowAll,
            &wal,
            None,
            control,
            &tx,
        )
        .await
        .expect("run ok");
        drop(tx);
        collector.await.unwrap();
        assert_eq!(summary.reason, RunFinishReason::Completed);

        let framed = frames(&wal, &session);
        let lineage = {
            use broca_wal::Wal;
            broca_wal::FileWal::open(wal.wal_path(&session))
                .unwrap()
                .lineage_id()
                .expect("the run's log has a lineage")
        };
        let started_seq = framed
            .iter()
            .find(|frame| matches!(frame.record, WalRecord::RunStarted { .. }))
            .map(|frame| frame.seq)
            .unwrap();
        let attempt_seqs: Vec<u64> = framed
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::ModelAttemptFinished { .. }))
            .map(|frame| frame.seq)
            .collect();
        assert_eq!(attempt_seqs.len(), 2);
        assert_eq!(
            *reports.started.lock().unwrap(),
            vec![(Some(lineage), started_seq)]
        );
        let attempts = reports.attempts.lock().unwrap().clone();
        assert_eq!(
            attempts
                .iter()
                .map(|(lineage, seq, _)| (*lineage, *seq))
                .collect::<Vec<_>>(),
            attempt_seqs
                .iter()
                .map(|seq| (Some(lineage), *seq))
                .collect::<Vec<_>>(),
            "each report names its own attempt record"
        );
        let expected = |pairs: &[(&str, &str)]| -> std::collections::BTreeMap<String, String> {
            pairs
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect()
        };
        assert_eq!(
            attempts[0].2,
            expected(&[
                ("anthropic-ratelimit-unified-5h-utilization", "0.9950"),
                ("anthropic-ratelimit-unified-7d-reset", "1.79068680e9"),
                ("anthropic-ratelimit-unified-some-future-window", "x;q=1, 2"),
            ])
        );
        assert_eq!(
            attempts[1].2,
            expected(&[("anthropic-ratelimit-unified-status", "allowed_warning")])
        );
        let wal_bytes = std::fs::read(wal.wal_path(&session)).unwrap();
        assert!(
            !wal_bytes
                .windows(b"ratelimit".len())
                .any(|window| window == b"ratelimit"),
            "the headers are reported, never recorded"
        );
    }

    /// The three chaos shapes in one step: a non-2xx whose body is longer than the read cap, a
    /// 200 cut mid-stream, and a clean 200. The transport retries through all three.
    #[tokio::test(flavor = "multi_thread")]
    async fn every_attempt_is_recorded_with_the_digest_of_the_bytes_the_server_sent() {
        let dir = scratch("three");
        let clean = anthropic_sse("hello there");
        let cut_body = anthropic_sse("this stream is cut");
        // Mid-way through the text delta event: the prefix holds complete events and a torn one.
        let cut_at = cut_body
            .windows(b"cut\"".len())
            .position(|w| w == b"cut\"")
            .unwrap();
        let oversized_error: Vec<u8> = (0..ERROR_BODY_CAP + 4096)
            .map(|i| b'a' + (i % 26) as u8)
            .collect();
        let server = Server::start(vec![
            Reply::Full {
                status: 503,
                // A zero retry delay keeps the test fast; the classification is unchanged.
                headers: vec![("Content-Type", "text/plain"), ("retry-after-ms", "0")],
                body: oversized_error.clone(),
            },
            Reply::Cut {
                body: cut_body.clone(),
                sent: cut_at,
            },
            Reply::Full {
                status: 200,
                headers: vec![("Content-Type", "text/event-stream")],
                body: clean.clone(),
            },
        ])
        .await;
        let provider = provider(&server);
        let lease = FileLeaseStore::new(dir.join("lease"));
        let wal = FileWalStore::new(dir.join("wal"));
        let req = request(&dir);
        let session = req.session.clone();
        let (tx, mut rx) = mpsc::channel(256);
        let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let summary = run_loop(
            req,
            &provider,
            &StubToolPlane::new(),
            &lease,
            &AllowAll,
            &wal,
            &tx,
        )
        .await
        .expect("run ok");
        drop(tx);
        collector.await.unwrap();
        assert_eq!(summary.reason, RunFinishReason::Completed);

        let served = server.served();
        assert_eq!(served.len(), 3, "the server saw three attempts");
        let framed = frames(&wal, &session);
        let records: Vec<&WalRecord> = framed.iter().map(|f| &f.record).collect();

        let attempts: Vec<&WalRecord> = records
            .iter()
            .copied()
            .filter(|r| matches!(r, WalRecord::ModelAttemptFinished { .. }))
            .collect();
        assert_eq!(attempts.len(), 3, "one record per attempt: {attempts:#?}");

        // Every attempt record precedes the step's ModelStepFinished.
        let step_finished_at = records
            .iter()
            .position(|r| matches!(r, WalRecord::ModelStepFinished { .. }))
            .expect("the step completed");
        let last_attempt_at = records
            .iter()
            .rposition(|r| matches!(r, WalRecord::ModelAttemptFinished { .. }))
            .unwrap();
        assert!(last_attempt_at < step_finished_at);
        let WalRecord::ModelStepFinished {
            step_id: finished_step,
            retries_used,
            ..
        } = records[step_finished_at]
        else {
            unreachable!()
        };
        assert_eq!(*retries_used, 2);

        // (outcome, status, the exact bytes the digest must cover, cap)
        type Expected<'a> = (AttemptOutcome, Option<u16>, &'a [u8], Option<u64>);
        let expected: [Expected<'_>; 3] = [
            (
                AttemptOutcome::HttpError,
                Some(503),
                &served[0].response_body_written[..ERROR_BODY_CAP],
                Some(ERROR_BODY_CAP as u64),
            ),
            (
                AttemptOutcome::ChunkError,
                Some(200),
                &served[1].response_body_written,
                None,
            ),
            (
                AttemptOutcome::Complete,
                Some(200),
                &served[2].response_body_written,
                None,
            ),
        ];
        // The server's bytes are what the script says: the digests below are over them.
        assert_eq!(served[0].response_body_written, oversized_error);
        assert_eq!(served[1].response_body_written, cut_body[..cut_at].to_vec());
        assert_eq!(served[2].response_body_written, clean);

        for (index, (record, (outcome, status, sent, cap))) in
            attempts.iter().zip(expected).enumerate()
        {
            let WalRecord::ModelAttemptFinished {
                step_id,
                attempt,
                request_body_len,
                request_body_sha256,
                http_status,
                content_type,
                response_body_len,
                response_body_sha256,
                response_body_cap,
                outcome: recorded_outcome,
                ts_ms,
                sent_at_ms,
                transport,
                websocket_request,
                websocket_close_code,
                input_transformations: _,
                input_transformations_unmeasured: _,
                credential_refresh,
                served_account_ref,
                content_progress: _,
            } = record
            else {
                unreachable!()
            };
            assert_eq!(step_id, finished_step, "attempt {index}");
            // Every attempt here reached the server, so each records when its request was handed
            // over, which is never after the attempt ended.
            assert!(
                sent_at_ms.is_some_and(|sent_at_ms| sent_at_ms <= *ts_ms),
                "attempt {index}: sent_at_ms {sent_at_ms:?}, ts_ms {ts_ms}"
            );
            assert_eq!(transport.as_deref(), Some("https"));
            assert!(websocket_request.is_none());
            assert!(websocket_close_code.is_none());
            assert_eq!(*credential_refresh, None, "no token was refreshed");
            assert_eq!(*served_account_ref, None, "accountless credential");
            assert_eq!(*attempt, index as u32, "gapless ordinals");
            assert_eq!(*recorded_outcome, outcome, "attempt {index}");
            assert_eq!(*http_status, status, "attempt {index}");
            assert_eq!(*response_body_cap, cap, "attempt {index}");
            assert_eq!(*response_body_len, sent.len() as u64, "attempt {index}");
            assert_eq!(
                *response_body_sha256,
                sha256_hex(sent),
                "attempt {index}: digest of the server's bytes"
            );
            let request = &served[index].request_body;
            assert_eq!(*request_body_len, request.len() as u64, "attempt {index}");
            assert_eq!(
                *request_body_sha256,
                sha256_hex(request),
                "attempt {index}: digest of the request the server received"
            );
            assert!(*ts_ms > 0);
            let expected_type = if index == 0 {
                "text/plain"
            } else {
                "text/event-stream"
            };
            assert_eq!(content_type.as_deref(), Some(expected_type));
        }
        eprintln!(
            "sample record: {}",
            serde_json::to_string(attempts[1]).unwrap()
        );

        // The same WAL with the attempt records removed replays to the same session.
        let stripped: Vec<WalRecord> = framed
            .iter()
            .map(|f| f.record.clone())
            .filter(|r| !matches!(r, WalRecord::ModelAttemptFinished { .. }))
            .collect();
        let all: Vec<WalRecord> = framed.iter().map(|f| f.record.clone()).collect();
        let with_state = replay(&all).unwrap();
        let without_state = replay(&stripped).unwrap();
        assert_eq!(
            serde_json::to_vec(&with_state.prompt).unwrap(),
            serde_json::to_vec(&without_state.prompt).unwrap(),
            "reconstructed prompt bytes"
        );
        assert_eq!(format!("{with_state:?}"), format!("{without_state:?}"));
        let units = |frames: &[FramedRecord]| -> Vec<_> {
            project_control(frames)
                .into_iter()
                .map(|(_, unit)| unit)
                .collect()
        };
        let stripped_frames: Vec<FramedRecord> = framed
            .iter()
            .filter(|f| !matches!(f.record, WalRecord::ModelAttemptFinished { .. }))
            .cloned()
            .collect();
        assert_eq!(units(&framed), units(&stripped_frames), "control units");
        let mut checkpoint = AdmissionCheckpointV1::from_frames(&framed).unwrap();
        let stripped_checkpoint = AdmissionCheckpointV1::from_frames(&stripped_frames).unwrap();
        checkpoint.at_seq = stripped_checkpoint.at_seq;
        assert_eq!(
            format!("{checkpoint:?}"),
            format!("{stripped_checkpoint:?}")
        );

        drop(server);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A caller cancel while the response is streaming records the in-flight attempt as
    /// `cancelled`, covering the bytes that had arrived, BEFORE the run's Cancelled terminal.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_caller_cancel_mid_stream_is_recorded_before_the_cancelled_terminal() {
        let dir = scratch("cancel");
        let full = anthropic_sse("streaming when cancelled");
        // Up to the end of the text delta event, so the client has display output to show it
        // has received the prefix.
        let delta_end = full
            .windows(b"content_block_stop".len())
            .position(|w| w == b"content_block_stop")
            .unwrap()
            - "event: ".len();
        let prefix = full[..delta_end].to_vec();
        let server = Server::start(vec![Reply::Stall {
            prefix: prefix.clone(),
        }])
        .await;
        let provider = provider(&server);
        let lease = FileLeaseStore::new(dir.join("lease"));
        let wal = FileWalStore::new(dir.join("wal"));
        let req = request(&dir);
        let session = req.session.clone();
        let cancel = CancellationToken::new();
        let control = RunControl {
            cancel: Some(cancel.clone()),
            ..Default::default()
        };
        let (tx, mut rx) = mpsc::channel(256);
        // Cancel once the streamed text has reached the caller: by then the prefix is read.
        let canceller = {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                let mut cancelled = false;
                while let Some(event) = rx.recv().await {
                    if !cancelled
                        && matches!(event, OutputEvent::Display(DisplayEvent::TextDelta { .. }))
                    {
                        cancel.cancel();
                        cancelled = true;
                    }
                }
                cancelled
            })
        };
        let summary = tokio::time::timeout(
            Duration::from_secs(30),
            run_loop_with_control(
                req,
                &provider,
                &StubToolPlane::new(),
                &lease,
                &AllowAll,
                &wal,
                None,
                control,
                &tx,
            ),
        )
        .await
        .expect("the cancelled run ends")
        .expect("run ok");
        drop(tx);
        assert!(canceller.await.unwrap(), "the stream reached the caller");
        assert_eq!(summary.reason, RunFinishReason::Cancelled);

        let records: Vec<WalRecord> = frames(&wal, &session)
            .into_iter()
            .map(|f| f.record)
            .collect();
        let attempt_at = records
            .iter()
            .position(|r| matches!(r, WalRecord::ModelAttemptFinished { .. }))
            .expect("the cancelled attempt is recorded");
        let terminal_at = records
            .iter()
            .position(|r| {
                matches!(
                    r,
                    WalRecord::RunFinished {
                        reason: RunFinishReason::Cancelled,
                        ..
                    }
                )
            })
            .expect("a Cancelled terminal");
        assert!(attempt_at < terminal_at, "{records:#?}");
        assert_eq!(
            records
                .iter()
                .filter(|r| matches!(r, WalRecord::ModelAttemptFinished { .. }))
                .count(),
            1
        );
        let WalRecord::ModelAttemptFinished {
            attempt,
            outcome,
            http_status,
            response_body_len,
            response_body_sha256,
            ..
        } = &records[attempt_at]
        else {
            unreachable!()
        };
        assert_eq!(*attempt, 0);
        assert_eq!(*outcome, AttemptOutcome::Cancelled);
        assert_eq!(*http_status, Some(200));
        assert_eq!(*response_body_len, prefix.len() as u64);
        assert_eq!(*response_body_sha256, sha256_hex(&prefix));

        drop(server);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // In-step token refresh after a provider 401. A fake credential source stands in for the
    // vault; the HTTP exchange, the transport, and the run loop are the real ones.

    /// The token every run below is admitted with, and that the server rejects first.
    const REJECTED_TOKEN: &str = "rejected-test-token";
    const FRESH_TOKEN: &str = "fresh-test-token";

    fn unauthorized() -> Reply {
        Reply::Full {
            status: 401,
            headers: vec![("Content-Type", "application/json")],
            body: br#"{"type":"error","error":{"type":"authentication_error","message":"invalid bearer token"}}"#
                .to_vec(),
        }
    }

    fn streamed(text: &str) -> Reply {
        Reply::Full {
            status: 200,
            headers: vec![("Content-Type", "text/event-stream")],
            body: anthropic_sse(text),
        }
    }

    fn bearer(token: &str) -> AuthHandle {
        AuthHandle::Bearer(token.to_string())
    }

    /// Whether a served request carried exactly `Bearer <token>`. A boolean, so a failing
    /// assertion never prints a token.
    fn sent_with(served: &Served, token: &str) -> bool {
        served.authorization.as_deref() == Some(format!("Bearer {token}").as_str())
    }

    /// What the fake credential source reports it asked for; the re-sent attempt must carry it.
    fn refresh_evidence() -> CredentialRefresh {
        CredentialRefresh {
            vault_op: "get".into(),
            min_ttl_ms: Some(600_000),
            force_refresh: true,
            record_version: Some(2),
            reason: Some(CredentialRefresh::REASON_REJECTED.into()),
        }
    }

    /// What the fake credential source reports for a token replaced before a send because it
    /// was about to expire.
    fn expiring_evidence() -> CredentialRefresh {
        CredentialRefresh {
            vault_op: "get_scoped".into(),
            min_ttl_ms: Some(600_000),
            force_refresh: false,
            record_version: Some(3),
            reason: Some(CredentialRefresh::REASON_EXPIRING.into()),
        }
    }

    /// Hands out scripted credentials in order (`None` is a refusal) and counts every request:
    /// `replies` after a 401, `expiring` before each attempt. An exhausted `expiring` script
    /// answers `None`, as a source does for a token with enough life left.
    struct ScriptedRefresher {
        account_ref: Option<String>,
        replies: Mutex<VecDeque<Option<AuthHandle>>>,
        calls: AtomicUsize,
        expiring: Mutex<VecDeque<Option<AuthHandle>>>,
        expiring_calls: AtomicUsize,
    }

    impl CredentialRefresher for ScriptedRefresher {
        fn refresh_rejected(&self, _status: u16) -> RefreshFuture<'_, RefreshedCredential> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let next = self.replies.lock().unwrap().pop_front().flatten();
            Box::pin(async move {
                next.map(|auth| RefreshedCredential {
                    account_ref: self.account_ref.clone(),
                    auth,
                    evidence: refresh_evidence(),
                })
            })
        }

        fn refresh_expiring(&self) -> RefreshFuture<'_, RefreshedCredential> {
            self.expiring_calls.fetch_add(1, Ordering::SeqCst);
            let next = self.expiring.lock().unwrap().pop_front().flatten();
            Box::pin(async move {
                next.map(|auth| RefreshedCredential {
                    account_ref: self.account_ref.clone(),
                    auth,
                    evidence: expiring_evidence(),
                })
            })
        }
    }

    struct RefreshRun {
        summary: broca_core::RunSummary,
        served: Vec<Served>,
        /// (outcome, http status, credential refresh) of every attempt record, in order.
        attempts: Vec<(AttemptOutcome, Option<u16>, Option<CredentialRefresh>)>,
        refresh_calls: usize,
        expiring_calls: usize,
        provider: LiveProvider<AnthropicMessages>,
        server: Server,
        dir: PathBuf,
    }

    /// One run admitted with [`REJECTED_TOKEN`] against `script`, with `replies` as the
    /// credential source's answers. Every script carries one reply more than the behavior
    /// under test should consume, so a build that re-sends too often completes instead of
    /// hanging on a connection nobody answers, and the assertions catch it.
    async fn run_with_refresher(
        tag: &str,
        script: Vec<Reply>,
        replies: Vec<Option<AuthHandle>>,
    ) -> RefreshRun {
        run_with_accounts(tag, script, replies, None, None).await
    }

    async fn run_with_accounts(
        tag: &str,
        script: Vec<Reply>,
        replies: Vec<Option<AuthHandle>>,
        frozen_account: Option<&str>,
        refreshed_account: Option<&str>,
    ) -> RefreshRun {
        run_with_expiring(
            tag,
            script,
            replies,
            Vec::new(),
            frozen_account,
            refreshed_account,
        )
        .await
    }

    /// As [`run_with_accounts`], with `expiring` as the source's answers to the check made
    /// before each attempt.
    async fn run_with_expiring(
        tag: &str,
        script: Vec<Reply>,
        replies: Vec<Option<AuthHandle>>,
        expiring: Vec<Option<AuthHandle>>,
        frozen_account: Option<&str>,
        refreshed_account: Option<&str>,
    ) -> RefreshRun {
        let dir = scratch(tag);
        let server = Server::start(script).await;
        let refresher = Arc::new(ScriptedRefresher {
            account_ref: refreshed_account.map(str::to_owned),
            replies: Mutex::new(replies.into()),
            calls: AtomicUsize::new(0),
            expiring: Mutex::new(expiring.into()),
            expiring_calls: AtomicUsize::new(0),
        });
        let mut frozen = anthropic_frozen_config("m");
        frozen.base_url = Some(server.url.clone());
        let provider = LiveProvider::new(AnthropicMessages, frozen, bearer(REJECTED_TOKEN))
            .with_credential_refresher(refresher.clone())
            .with_account_refs(
                frozen_account.map(str::to_owned),
                frozen_account.map(str::to_owned),
            );
        let summary = run_once(&provider, &dir, "s1").await;
        let session = BindIdentity::new(dir.join("proj"), "attempts", "s1");
        let wal = FileWalStore::new(dir.join("wal"));
        let attempts = frames(&wal, &session)
            .into_iter()
            .filter_map(|frame| match frame.record {
                WalRecord::ModelAttemptFinished {
                    outcome,
                    http_status,
                    credential_refresh,
                    ..
                } => Some((outcome, http_status, credential_refresh)),
                _ => None,
            })
            .collect();
        RefreshRun {
            summary,
            served: server.served(),
            attempts,
            refresh_calls: refresher.calls.load(Ordering::SeqCst),
            expiring_calls: refresher.expiring_calls.load(Ordering::SeqCst),
            provider,
            server,
            dir,
        }
    }

    async fn run_once(
        provider: &LiveProvider<AnthropicMessages>,
        dir: &Path,
        session: &str,
    ) -> broca_core::RunSummary {
        let lease = FileLeaseStore::new(dir.join("lease"));
        let wal = FileWalStore::new(dir.join("wal"));
        let mut req = request(dir);
        req.session = BindIdentity::new(dir.join("proj"), "attempts", session);
        req.run_id = RunId(format!("r-{session}"));
        let (tx, mut rx) = mpsc::channel(256);
        let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let summary = tokio::time::timeout(
            Duration::from_secs(60),
            run_loop(
                req,
                provider,
                &StubToolPlane::new(),
                &lease,
                &AllowAll,
                &wal,
                &tx,
            ),
        )
        .await
        .expect("the run ends")
        .expect("run ok");
        drop(tx);
        collector.await.unwrap();
        summary
    }

    fn paused_for_auth(summary: &broca_core::RunSummary) -> bool {
        summary.paused == Some(PauseReason::AuthRequired)
    }

    fn account_stamps(run: &RefreshRun) -> Vec<Option<String>> {
        let session = BindIdentity::new(run.dir.join("proj"), "attempts", "s1");
        frames(&FileWalStore::new(run.dir.join("wal")), &session)
            .into_iter()
            .filter_map(|frame| match frame.record {
                WalRecord::ModelAttemptFinished {
                    served_account_ref, ..
                } => Some(served_account_ref),
                _ => None,
            })
            .collect()
    }

    async fn resume_with_account(served_account: &str, resends: usize) {
        use broca_core::{
            run_loop_resume_after_auth_with_control, run_loop_with_control, AuthSelection,
            RunControl,
        };
        let dir = scratch(&format!("resume-{served_account}"));
        let server = Server::start(vec![streamed("resumed"), streamed("extra")]).await;
        let lease = FileLeaseStore::new(dir.join("lease"));
        let wal = FileWalStore::new(dir.join("wal"));
        let selection = AuthSelection {
            credential_id: "oauth:anthropic:resume".into(),
            account_ref: Some("account-a".into()),
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        };
        let mut frozen = anthropic_frozen_config("m");
        frozen.base_url = Some(server.url.clone());
        let initial = LiveProvider::new(AnthropicMessages, frozen.clone(), bearer(REJECTED_TOKEN));
        let (tx, mut rx) = mpsc::channel(256);
        let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let pause = broca_protocol::ProviderError {
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            class: broca_protocol::ErrorClass::Auth,
            message: "credential needs reauthentication".into(),
            status: None,
            retry_after_secs: None,
            provider_code: Some("needs_reauth".into()),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        };
        let admitted = run_loop_with_control(
            request(&dir),
            &initial,
            &StubToolPlane::new(),
            &lease,
            &AllowAll,
            &wal,
            None,
            RunControl {
                auth_selection: Some(selection.clone()),
                pause_for_auth_at_start: Some(pause),
                ..Default::default()
            },
            &tx,
        )
        .await
        .unwrap();
        assert!(paused_for_auth(&admitted));
        assert_eq!(server.served().len(), 0);
        let session = request(&dir).session;
        let frozen_account = frames(&wal, &session)
            .into_iter()
            .find_map(|frame| match frame.record {
                WalRecord::RunStarted { config, .. } => {
                    config.auth_selection.and_then(|s| s.account_ref)
                }
                _ => None,
            })
            .expect("account frozen in the admitted run");
        for resend in 0..resends {
            // Resume reconstructs the provider from a fresh vault token, but retains the
            // account in RunStarted even if the same credential now belongs to another account.
            let provider =
                LiveProvider::new(AnthropicMessages, frozen.clone(), bearer(FRESH_TOKEN))
                    .with_account_refs(Some(frozen_account.clone()), Some(served_account.into()));
            let summary = run_loop_resume_after_auth_with_control(
                request(&dir),
                &provider,
                &StubToolPlane::new(),
                &lease,
                &AllowAll,
                &wal,
                None,
                RunControl {
                    auth_selection: Some(AuthSelection {
                        account_ref: Some(served_account.into()),
                        ..selection.clone()
                    }),
                    ..Default::default()
                },
                &tx,
            )
            .await
            .unwrap();
            let records = frames(&wal, &session);
            assert_eq!(
                records
                    .iter()
                    .filter(|f| matches!(f.record, WalRecord::RunStarted { .. }))
                    .count(),
                1
            );
            if served_account == "account-a" {
                assert_eq!(summary.reason, RunFinishReason::Completed);
                assert_eq!(server.served().len(), 1);
            } else {
                assert_eq!(
                    server.served().len(),
                    0,
                    "resume must not send on another account"
                );
                assert!(paused_for_auth(&summary));
                let errors: Vec<_> = records
                    .iter()
                    .filter_map(|frame| match &frame.record {
                        WalRecord::RunPaused {
                            error: Some(error), ..
                        } if error.provider_code.as_deref() == Some("account_changed") => {
                            Some(error)
                        }
                        _ => None,
                    })
                    .collect();
                assert_eq!(errors.len(), resend + 1, "each resend durably pauses again");
                assert_eq!(
                    errors.last().unwrap().class,
                    broca_protocol::ErrorClass::Auth
                );
                assert!(!records
                    .iter()
                    .any(|f| matches!(f.record, WalRecord::ModelAttemptFinished { .. })));
            }
        }
        drop(tx);
        collector.await.unwrap();
        drop(server);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resumed_different_account_is_refused_on_every_resend_without_requests() {
        resume_with_account("account-b", 2).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resumed_same_account_proceeds() {
        resume_with_account("account-a", 1).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn same_account_refresh_proceeds_and_stamps_every_attempt() {
        let run = run_with_accounts(
            "same-account",
            vec![unauthorized(), streamed("ok"), streamed("extra")],
            vec![Some(bearer(FRESH_TOKEN))],
            Some("account-a"),
            Some("account-a"),
        )
        .await;
        assert_eq!(run.summary.reason, RunFinishReason::Completed);
        assert_eq!(run.served.len(), 2);
        assert_eq!(
            account_stamps(&run),
            vec![Some("account-a".into()), Some("account-a".into())]
        );
        drop(run.server);
        let _ = std::fs::remove_dir_all(run.dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn different_account_refresh_is_refused_without_a_provider_request() {
        let run = run_with_accounts(
            "different-account",
            vec![unauthorized(), streamed("must not send")],
            vec![Some(bearer(FRESH_TOKEN))],
            Some("account-a"),
            Some("account-b"),
        )
        .await;
        assert_eq!(
            run.served.len(),
            1,
            "no provider request on the other account"
        );
        assert!(paused_for_auth(&run.summary));
        assert_eq!(account_stamps(&run), vec![Some("account-a".into())]);
        let session = BindIdentity::new(run.dir.join("proj"), "attempts", "s1");
        let error = frames(&FileWalStore::new(run.dir.join("wal")), &session)
            .into_iter()
            .find_map(|frame| match frame.record {
                WalRecord::RunPaused { error, .. } => error,
                _ => None,
            })
            .expect("typed pause error");
        assert_eq!(error.provider_code.as_deref(), Some("account_changed"));
        assert_eq!(error.class, broca_protocol::ErrorClass::Auth);
        assert_eq!(error.status, None, "no provider rejected the unused token");
        assert!(error.message.contains("account-a") && error.message.contains("account-b"));
        drop(run.server);
        let _ = std::fs::remove_dir_all(run.dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_account_on_either_side_allows_refresh_and_stamps_only_known_accounts() {
        for (tag, frozen, refreshed) in [
            ("unknown-frozen", None, Some("account-b")),
            ("unknown-refreshed", Some("account-a"), None),
        ] {
            let run = run_with_accounts(
                tag,
                vec![unauthorized(), streamed("ok"), streamed("later")],
                vec![Some(bearer(FRESH_TOKEN))],
                frozen,
                refreshed,
            )
            .await;
            assert_eq!(run.summary.reason, RunFinishReason::Completed);
            assert_eq!(run.served.len(), 2);
            assert_eq!(
                account_stamps(&run),
                vec![frozen.map(str::to_owned), refreshed.map(str::to_owned)]
            );
            let later = run_once(&run.provider, &run.dir, "s2").await;
            assert_eq!(later.reason, RunFinishReason::Completed);
            let session = BindIdentity::new(run.dir.join("proj"), "attempts", "s2");
            let stamps: Vec<_> = frames(&FileWalStore::new(run.dir.join("wal")), &session)
                .into_iter()
                .filter_map(|frame| match frame.record {
                    WalRecord::ModelAttemptFinished {
                        served_account_ref, ..
                    } => Some(served_account_ref),
                    _ => None,
                })
                .collect();
            assert_eq!(stamps, vec![refreshed.map(str::to_owned)]);
            drop(run.server);
            let _ = std::fs::remove_dir_all(run.dir);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn apikey_refresh_stamps_no_account() {
        let run = run_with_refresher(
            "accountless",
            vec![unauthorized(), streamed("ok"), streamed("extra")],
            vec![Some(bearer(FRESH_TOKEN))],
        )
        .await;
        assert_eq!(run.summary.reason, RunFinishReason::Completed);
        assert_eq!(account_stamps(&run), vec![None, None]);
        drop(run.server);
        let _ = std::fs::remove_dir_all(run.dir);
    }

    /// A 401, a refresh that serves a different token, and the same request re-sent once with
    /// it: the run completes, both attempts are recorded, the retry names the refresh, and the
    /// provider keeps the fresh token for the steps after it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_401_is_recovered_with_one_forced_refresh_and_one_resend() {
        let run = run_with_refresher(
            "refresh-ok",
            vec![
                unauthorized(),
                streamed("after refresh"),
                streamed("next run"),
            ],
            vec![Some(bearer(FRESH_TOKEN))],
        )
        .await;
        assert_eq!(run.summary.reason, RunFinishReason::Completed);
        assert!(!paused_for_auth(&run.summary));
        assert_eq!(run.refresh_calls, 1, "exactly one forced refresh");
        assert_eq!(run.served.len(), 2, "the 401 and one re-send");
        assert!(
            sent_with(&run.served[0], REJECTED_TOKEN),
            "first attempt used the admitted token"
        );
        assert!(
            sent_with(&run.served[1], FRESH_TOKEN),
            "re-send used the fresh token: false"
        );
        assert!(
            run.served[0].request_body == run.served[1].request_body,
            "the re-send carries the same request bytes"
        );
        assert_eq!(
            run.attempts,
            vec![
                (AttemptOutcome::HttpError, Some(401), None),
                (
                    AttemptOutcome::Complete,
                    Some(200),
                    Some(refresh_evidence())
                ),
            ],
            "two attempt records; only the re-send names the refresh"
        );

        // The provider now holds the fresh token: a later step goes out with it and does not
        // ask for another refresh.
        let RefreshRun {
            provider,
            server,
            dir,
            ..
        } = run;
        let later = run_once(&provider, &dir, "s2").await;
        assert_eq!(later.reason, RunFinishReason::Completed);
        let served = server.served();
        assert_eq!(served.len(), 3);
        assert!(
            sent_with(&served[2], FRESH_TOKEN),
            "later step used the fresh token: false"
        );
        drop(server);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The credential source answers with the very token that was rejected: re-sending it
    /// cannot help, so the run pauses for re-auth without a second request.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_same_token_back_from_the_refresh_pauses_without_a_resend() {
        let run = run_with_refresher(
            "refresh-same",
            vec![unauthorized(), streamed("must not be reached")],
            vec![Some(bearer(REJECTED_TOKEN))],
        )
        .await;
        assert!(paused_for_auth(&run.summary), "{:?}", run.summary.reason);
        assert_eq!(run.refresh_calls, 1);
        assert_eq!(run.served.len(), 1, "no re-send with the rejected token");
        assert_eq!(
            run.attempts,
            vec![(AttemptOutcome::HttpError, Some(401), None)]
        );
        drop(run.server);
        let _ = std::fs::remove_dir_all(&run.dir);
    }

    /// The credential source has no fresh token (the vault's `needs_reauth`, a refusal, or no
    /// reply all look like this to the transport): the run pauses as before, with no re-send.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_refused_refresh_pauses_without_a_resend() {
        let run = run_with_refresher(
            "refresh-refused",
            vec![unauthorized(), streamed("must not be reached")],
            vec![None],
        )
        .await;
        assert!(paused_for_auth(&run.summary), "{:?}", run.summary.reason);
        assert_eq!(run.refresh_calls, 1);
        assert_eq!(run.served.len(), 1, "no re-send without a fresh token");
        assert_eq!(
            run.attempts,
            vec![(AttemptOutcome::HttpError, Some(401), None)]
        );
        drop(run.server);
        let _ = std::fs::remove_dir_all(&run.dir);
    }

    /// The refreshed token is rejected too: one recovery per step, so the run pauses after the
    /// second 401 instead of refreshing again.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_second_401_after_the_refresh_pauses() {
        let run = run_with_refresher(
            "refresh-twice",
            vec![
                unauthorized(),
                unauthorized(),
                streamed("must not be reached"),
            ],
            vec![
                Some(bearer(FRESH_TOKEN)),
                Some(bearer("another-test-token")),
            ],
        )
        .await;
        assert!(paused_for_auth(&run.summary), "{:?}", run.summary.reason);
        assert_eq!(run.refresh_calls, 1, "one recovery per step");
        assert_eq!(run.served.len(), 2);
        assert!(
            sent_with(&run.served[1], FRESH_TOKEN),
            "re-send used the fresh token: false"
        );
        assert_eq!(
            run.attempts,
            vec![
                (AttemptOutcome::HttpError, Some(401), None),
                (
                    AttemptOutcome::HttpError,
                    Some(401),
                    Some(refresh_evidence())
                ),
            ]
        );
        drop(run.server);
        let _ = std::fs::remove_dir_all(&run.dir);
    }

    /// The `request_body_sha256` of every attempt record of session `s1`, in order.
    fn attempt_request_digests(run: &RefreshRun) -> Vec<String> {
        let session = BindIdentity::new(run.dir.join("proj"), "attempts", "s1");
        frames(&FileWalStore::new(run.dir.join("wal")), &session)
            .into_iter()
            .filter_map(|frame| match frame.record {
                WalRecord::ModelAttemptFinished {
                    request_body_sha256,
                    ..
                } => Some(request_body_sha256),
                _ => None,
            })
            .collect()
    }

    /// The source says the held token is about to expire: the step's only attempt goes out
    /// with the fresh token, no 401 is spent on the old one, and the attempt record names the
    /// pre-send refresh. Later steps keep the fresh token.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_expiring_token_is_replaced_before_the_send() {
        let run = run_with_expiring(
            "expiring-ok",
            vec![streamed("ok"), streamed("next run")],
            Vec::new(),
            vec![Some(bearer(FRESH_TOKEN))],
            None,
            None,
        )
        .await;
        assert_eq!(run.summary.reason, RunFinishReason::Completed);
        assert_eq!(run.refresh_calls, 0, "nothing was rejected");
        assert_eq!(run.expiring_calls, 1, "one check before the one attempt");
        assert_eq!(run.served.len(), 1, "no 401 was spent on the old token");
        assert!(
            sent_with(&run.served[0], FRESH_TOKEN),
            "the attempt used the fresh token: false"
        );
        assert_eq!(
            run.attempts,
            vec![(
                AttemptOutcome::Complete,
                Some(200),
                Some(expiring_evidence())
            )]
        );
        let RefreshRun {
            provider,
            server,
            dir,
            ..
        } = run;
        let later = run_once(&provider, &dir, "s2").await;
        assert_eq!(later.reason, RunFinishReason::Completed);
        assert!(
            sent_with(&server.served()[1], FRESH_TOKEN),
            "later step used the fresh token: false"
        );
        drop(server);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The token is a header, attached after render: a pre-send refresh between two attempts
    /// changes the authorization and nothing else, so both attempts carry the same body bytes
    /// and the same request digest. Only the attempt sent with the fresh token names it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_pre_send_refresh_keeps_the_rendered_body_and_its_digest() {
        let unavailable = Reply::Full {
            status: 503,
            headers: vec![("Content-Type", "application/json")],
            body: br#"{"error":{"message":"overloaded"}}"#.to_vec(),
        };
        let run = run_with_expiring(
            "expiring-digest",
            vec![unavailable, streamed("ok"), streamed("extra")],
            Vec::new(),
            vec![None, Some(bearer(FRESH_TOKEN))],
            None,
            None,
        )
        .await;
        assert_eq!(run.summary.reason, RunFinishReason::Completed);
        assert_eq!(run.expiring_calls, 2, "one check before each attempt");
        assert_eq!(run.served.len(), 2);
        assert!(
            sent_with(&run.served[0], REJECTED_TOKEN),
            "the first attempt used the held token: false"
        );
        assert!(
            sent_with(&run.served[1], FRESH_TOKEN),
            "the second attempt used the fresh token: false"
        );
        assert!(
            run.served[0].request_body == run.served[1].request_body,
            "the refreshed attempt carries the same request bytes"
        );
        let digests = attempt_request_digests(&run);
        assert_eq!(digests.len(), 2);
        assert_eq!(digests[0], digests[1], "the request digest is unchanged");
        assert_eq!(
            digests[0],
            sha256_hex(&run.served[0].request_body),
            "the digest is of the bytes the server received"
        );
        assert_eq!(
            run.attempts,
            vec![
                (AttemptOutcome::HttpError, Some(503), None),
                (
                    AttemptOutcome::Complete,
                    Some(200),
                    Some(expiring_evidence())
                ),
            ]
        );
        drop(run.server);
        let _ = std::fs::remove_dir_all(&run.dir);
    }

    /// The source could not serve a fresh token before the send (unreachable, refused): the
    /// attempt goes out with the held token, and when the provider then refuses it, the 401
    /// recovery still runs exactly as it would have without the pre-send check.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_pre_send_refresh_sends_the_held_token_and_a_401_still_recovers() {
        let run = run_with_expiring(
            "expiring-failed",
            vec![unauthorized(), streamed("ok"), streamed("extra")],
            vec![Some(bearer(FRESH_TOKEN))],
            vec![None, None],
            None,
            None,
        )
        .await;
        assert_eq!(run.summary.reason, RunFinishReason::Completed);
        assert_eq!(run.expiring_calls, 2);
        assert_eq!(run.refresh_calls, 1);
        assert_eq!(run.served.len(), 2);
        assert!(
            sent_with(&run.served[0], REJECTED_TOKEN),
            "the attempt went out with the held token: false"
        );
        assert!(
            sent_with(&run.served[1], FRESH_TOKEN),
            "the 401 re-send used the fresh token: false"
        );
        assert_eq!(
            run.attempts,
            vec![
                (AttemptOutcome::HttpError, Some(401), None),
                (
                    AttemptOutcome::Complete,
                    Some(200),
                    Some(refresh_evidence())
                ),
            ]
        );
        drop(run.server);
        let _ = std::fs::remove_dir_all(&run.dir);
    }

    /// A token refreshed before the send that belongs to another account than the one the run
    /// was admitted with is never sent: the run pauses `account_changed`, as for a 401 refresh.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_pre_send_refresh_for_another_account_pauses_without_a_send() {
        let run = run_with_expiring(
            "expiring-other-account",
            vec![streamed("must not send")],
            Vec::new(),
            vec![Some(bearer(FRESH_TOKEN))],
            Some("account-a"),
            Some("account-b"),
        )
        .await;
        assert!(paused_for_auth(&run.summary), "{:?}", run.summary.reason);
        assert_eq!(
            run.served.len(),
            0,
            "no provider request on the other account"
        );
        assert!(run.attempts.is_empty(), "no attempt was sent");
        let session = BindIdentity::new(run.dir.join("proj"), "attempts", "s1");
        let error = frames(&FileWalStore::new(run.dir.join("wal")), &session)
            .into_iter()
            .find_map(|frame| match frame.record {
                WalRecord::RunPaused { error, .. } => error,
                _ => None,
            })
            .expect("typed pause error");
        assert_eq!(error.provider_code.as_deref(), Some("account_changed"));
        assert_eq!(error.class, broca_protocol::ErrorClass::Auth);
        drop(run.server);
        let _ = std::fs::remove_dir_all(run.dir);
    }
}
