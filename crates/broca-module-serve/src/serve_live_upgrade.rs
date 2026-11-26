//! Resume the module wire without restarting the process that owns the sessions.

use std::path::Path;
use std::time::Duration;

use broca_subc::{daemon_upgrade, registration};
use tokio::time::Instant;

use super::connection::{serve_generation, Services};
use super::*;
use crate::module_connection::ModuleConnection;

#[derive(Clone, Copy)]
pub(super) struct Notice {
    received: Instant,
    deadline: Instant,
    ready_at_receipt: bool,
}

pub(super) struct Generation {
    notice: OnceLock<Notice>,
    notice_arrived: CancellationToken,
    resumed_from: Option<Notice>,
    started: Arc<OnceLock<()>>,
    can_upgrade: bool,
}

// Give the credential no Debug or Display representation, and strip it from the ACK before
// the frame loop uses the rest of the connection's metadata.
#[derive(Default)]
struct LatestToken(Option<String>);

impl LatestToken {
    fn refresh(&mut self, ack: &mut ModuleHelloAckBody) {
        self.0 = ack.resume_token.take();
    }
}

impl Generation {
    fn new(resumed_from: Option<Notice>, can_upgrade: bool) -> Self {
        Self {
            notice: OnceLock::new(),
            notice_arrived: CancellationToken::new(),
            resumed_from,
            started: Arc::new(OnceLock::new()),
            can_upgrade,
        }
    }

    pub(super) fn on_started(&self) -> Option<impl FnOnce(usize) + Send + use<>> {
        let notice = self.resumed_from?;
        let started = Arc::clone(&self.started);
        Some(move |runs_in_flight| {
            daemon_upgrade::resumed(notice.deadline);
            let _ = started.set(());
            tracing::warn!(target: "lifecycle", elapsed_ms = notice.received.elapsed().as_millis(),
                runs_in_flight, "resumed after daemon upgrade");
        })
    }

    pub(super) fn notice(&self) -> Option<Notice> {
        self.notice.get().copied()
    }

    pub(super) fn interrupted_startup(&self) -> bool {
        self.notice().is_some_and(|notice| !notice.ready_at_receipt)
    }

    pub(super) async fn old_wire_deadline(&self) {
        self.notice_arrived.cancelled().await;
        tokio::time::sleep_until(
            self.notice()
                .expect("notice set before waking its waiter")
                .deadline,
        )
        .await;
    }

    pub(super) fn cut(&self) -> bool {
        let Some(notice) = self.notice() else {
            return false;
        };
        registration::wait_for_resumed_minter(notice.deadline);
        true
    }

    pub(super) fn on_frame(&self, frame: &Frame, ready: bool) -> bool {
        if !self.can_upgrade || frame.header.channel != 0 || frame.header.ty != FrameType::Push {
            return false;
        }
        let Ok(ModuleControlCommand::Upgrading { reconnect_by_ms }) =
            serde_json::from_slice(&frame.body)
        else {
            return false;
        };
        let received = Instant::now();
        let deadline = monotonic_deadline(received, now_epoch_ms().max(0) as u64, reconnect_by_ms);
        // Readiness is recorded in wire order. A push already queued during construction
        // cannot be mistaken for a live upgrade just because the serve loop read it later.
        if self
            .notice
            .set(Notice {
                received,
                deadline,
                ready_at_receipt: ready,
            })
            .is_ok()
        {
            daemon_upgrade::announce(deadline);
            self.notice_arrived.cancel();
            tracing::warn!(target: "lifecycle", reconnect_by_ms, "daemon upgrade announced");
        }
        true
    }
}

fn monotonic_deadline(now: Instant, unix_now_ms: u64, reconnect_by_ms: u64) -> Instant {
    let remaining = Duration::from_millis(reconnect_by_ms.saturating_sub(unix_now_ms));
    now.checked_add(remaining).unwrap_or(now)
}

fn check_resume(
    notice: Notice,
    ended: &Result<ServeEnd, ServeIoError>,
    stopping: bool,
    now: Instant,
) -> Result<(), &'static str> {
    if !notice.ready_at_receipt {
        Err("daemon upgrade interrupted module startup; the supervisor will restart broca")
    } else if ended.is_err() {
        Err("the old connection ended with a protocol error")
    } else if stopping {
        Err("module stop started")
    } else if now >= notice.deadline {
        Err("reconnect deadline passed")
    } else {
        Ok(())
    }
}

/// Serve every module connection with the same process-owned handlers and registry.
#[allow(clippy::too_many_arguments)]
pub async fn serve_supervised<H: ServeHandler + 'static>(
    connection_file: &Path,
    connection: Arc<ModuleConnection>,
    mut registered: GatedRegistration,
    module_id: &str,
    handler: Arc<H>,
    health_registry: Arc<LineageRegistry>,
    vault_gauge: Arc<VaultGauge>,
    catalog_cache: Option<Arc<broca_catalog::CatalogCache>>,
    consumer_routes: crate::consumer_routes::ConsumerRoutes,
    vault_auditor: Option<Arc<dyn crate::vault_audit::VaultAuditor>>,
    mut daemon_capabilities: Option<Vec<String>>,
    scope_describer: Option<Arc<broca_subc::ModuleScopeDescriber>>,
    agent_data: Option<Arc<crate::agent_data::AgentDataService>>,
    system_one: Option<Arc<crate::system_one::SystemOne>>,
    llm_tools: Option<Arc<crate::llm_tools::LlmTools>>,
    gateway_listing: Option<Arc<dyn crate::serves::GatewayListing>>,
) -> Result<ServeEnd, ServeIoError> {
    let services = Services {
        module_id: module_id.to_owned(),
        handler,
        health_registry,
        vault_gauge,
        serves: Arc::new(crate::serves::Source {
            catalog: catalog_cache,
            vault: gateway_listing,
        }),
        consumer_routes,
        vault_auditor,
        scope_describer,
        agent_data,
        system_one,
        llm_tools,
    };
    let drain = accepting_drain::Drain::default();
    let stop = services.health_registry.stopping_signal();
    let mut resumed_from = None;
    let mut token = LatestToken::default();
    let mut wire = connection.hand_over().await;
    if connection.startup_interrupted() {
        return Err(ServeIoError(
            "daemon upgrade interrupted module startup".into(),
        ));
    }
    loop {
        token.refresh(&mut registered.ack);
        let gate = Arc::clone(&registered.gate);
        let can_upgrade = cfg!(any(target_os = "linux", target_os = "macos")) && token.0.is_some();
        daemon_upgrade::expect_upgrade_notices(can_upgrade);
        let generation = Generation::new(resumed_from, can_upgrade);
        let (reader, writer) = wire;
        let result = serve_generation(
            reader,
            writer,
            &services,
            daemon_capabilities.as_deref(),
            Some(registered),
            &drain,
            (can_upgrade || resumed_from.is_some()).then_some(&generation),
        )
        .await;
        let Some(notice) = generation.notice() else {
            if resumed_from.is_some() && generation.started.get().is_none() {
                daemon_upgrade::abandon();
                registration::abandon_resumed_minter();
                tracing::warn!(target: "lifecycle", "daemon upgrade resume failed: replacement connection could not start");
                return Err(ServeIoError(
                    "replacement connection could not start after daemon upgrade".into(),
                ));
            }
            daemon_upgrade::abandon();
            return result;
        };
        let resume = async {
            check_resume(notice, &result, stop.is_cancelled(), Instant::now())?;
            let image_references = gate.ready()
                && services
                    .system_one
                    .as_ref()
                    .is_some_and(|system| system.image_references());
            let mut manifest =
                hello_manifest(module_id, &services.consumer_routes, image_references);
            manifest.ready = Some(gate.ready());
            reconnect(
                connection_file,
                notice.deadline,
                manifest,
                token.0.take().ok_or("no resume token was issued")?,
                &stop,
            )
            .await
        }
        .await;
        let (stream, ack) = match resume {
            Ok(resume) => resume,
            Err(cause) => {
                if Instant::now() >= notice.deadline {
                    // Stop new work before releasing callers waiting on this upgrade. Main
                    // still attempts its normal seal, but SUBC may kill us at this deadline.
                    services.health_registry.begin_stop();
                }
                daemon_upgrade::abandon();
                registration::abandon_resumed_minter();
                tracing::warn!(target: "lifecycle", cause, "daemon upgrade resume failed");
                return Err(ServeIoError(format!(
                    "daemon upgrade resume failed: {cause}"
                )));
            }
        };
        let manages_settings = ack
            .subc_capabilities
            .iter()
            .any(|capability| capability == subc_protocol::config::CONFIG_GATE_CAPABILITY);
        if let Err(error) = gate.check_resume_mode(manages_settings) {
            daemon_upgrade::abandon();
            registration::abandon_resumed_minter();
            return Err(error);
        }
        let gate = gate.resumed(ack.negotiated_ver);
        let connection = Arc::new(ModuleConnection::new(stream, Arc::clone(&gate)));
        wire = connection.hand_over().await;
        registration::install_resumed_minter(module_id, connection);
        registered = GatedRegistration { ack, gate };
        resumed_from = Some(notice);
        daemon_capabilities = None;
    }
}

type Resume = (Box<dyn registration::DaemonStream>, ModuleHelloAckBody);

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn reconnect(
    path: &Path,
    deadline: Instant,
    manifest: subc_protocol::manifest::ModuleManifest,
    token: String,
    stop: &CancellationToken,
) -> Result<Resume, &'static str> {
    let hello = hello::gated_hello(manifest, None, Some(token))
        .map_err(|_| "could not build resume HELLO")?;
    retry_resume(deadline, stop, || resume_attempt(path, &hello)).await
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn retry_resume<T, F, Fut>(
    deadline: Instant,
    stop: &CancellationToken,
    mut attempt: F,
) -> Result<T, &'static str>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, AttemptError>>,
{
    let attempts = async {
        loop {
            let next = Instant::now() + Duration::from_millis(100);
            match attempt().await {
                Ok(resumed) => return Ok(resumed),
                Err(AttemptError::Retry) => tokio::time::sleep_until(next).await,
                Err(AttemptError::Terminal(cause)) => return Err(cause),
            }
        }
    };
    tokio::select! {
        biased;
        _ = stop.cancelled() => Err("module stop started"),
        _ = tokio::time::sleep_until(deadline) => Err("reconnect deadline passed"),
        result = attempts => result,
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
async fn reconnect(
    _: &Path,
    _: Instant,
    _: subc_protocol::manifest::ModuleManifest,
    _: String,
    _: &CancellationToken,
) -> Result<Resume, &'static str> {
    Err("daemon upgrade is not supported on this platform")
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
enum AttemptError {
    Retry,
    Terminal(&'static str),
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn retry_socket_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
    )
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn resume_attempt(path: &Path, hello: &Frame) -> Result<Resume, AttemptError> {
    use subc_transport::connection_file::{self, ConnectionFileError, LocalEndpoint};
    use AttemptError::{Retry, Terminal};
    #[cfg(feature = "test-hooks")]
    if std::env::var_os("BROCA_TEST_HOOKS_RESUME_HELLO_DELAY_V1").as_deref()
        == Some(std::ffi::OsStr::new("1"))
    {
        // Withhold HELLO until the ordinary reconnect deadline ends the attempt. No
        // timeout or shutdown behavior is replaced by the fixture.
        tracing::warn!(target: "lifecycle", "test hook withholding resume HELLO");
        std::future::pending::<()>().await;
    }
    let read =
        connection_file::read_for_client_with_local_endpoint(path).map_err(
            |error| match error {
                ConnectionFileError::Io { source, .. } if retry_socket_error(&source) => Retry,
                _ => Terminal("could not read daemon connection file"),
            },
        )?;
    let Some(LocalEndpoint::Unix { path: socket }) = read.local_endpoint else {
        return Err(Retry);
    };
    let mut stream = tokio::net::UnixStream::connect(socket)
        .await
        .map_err(|error| {
            if retry_socket_error(&error) {
                Retry
            } else {
                Terminal("local socket connect failed")
            }
        })?;
    subc_transport::authenticate_client(&mut stream, &read.info, Duration::from_secs(5))
        .await
        .map_err(|_| Retry)?;
    write_frame(&mut stream, hello)
        .await
        .map_err(|error| match error {
            FrameIoError::Io(_) => Retry,
            _ => Terminal("could not send resume HELLO"),
        })?;
    // Never print the error or the ACK: daemon-controlled text can contain the credential.
    let ack = hello::read_hello_ack(&mut stream)
        .await
        .map_err(hello_attempt_error)?;
    Ok((Box::new(stream), ack))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn hello_attempt_error(error: hello::HelloError) -> AttemptError {
    use AttemptError::{Retry, Terminal};
    match error {
        hello::HelloError::Closed | hello::HelloError::Wire(FrameIoError::Io(_)) => Retry,
        hello::HelloError::Refused(body)
            if subc_protocol::error_codes::is_retryable_resume_hello(&body.code) =>
        {
            Retry
        }
        hello::HelloError::Refused(_) => Terminal("daemon refused resume HELLO"),
        _ => Terminal("invalid resume HELLO acknowledgement"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::pin::Pin;
    use std::sync::atomic::AtomicBool;
    use std::task::{Context, Poll};

    fn services() -> Services<LineageRegistry> {
        let registry = Arc::new(LineageRegistry::new(Arc::new(
            super::super::test_openers::RefusingOpener,
        )));
        Services {
            module_id: "broca".into(),
            handler: registry.clone(),
            health_registry: registry,
            vault_gauge: VaultGauge::not_in_use(),
            serves: Default::default(),
            consumer_routes: Default::default(),
            vault_auditor: None,
            scope_describer: None,
            agent_data: None,
            system_one: None,
            llm_tools: None,
        }
    }

    fn registration() -> GatedRegistration {
        GatedRegistration {
            ack: ModuleHelloAckBody {
                negotiated_ver: PROTOCOL_VERSION,
                subc_ops: vec![],
                subc_capabilities: vec![],
                storage: None,
                machine_id: None,
                resume_token: Some("synthetic".into()),
            },
            gate: crate::settings::gate::Gate::ungated(PROTOCOL_VERSION),
        }
    }

    fn upgrade_push(reconnect_by_ms: u64) -> Frame {
        Frame::build(
            FrameType::Push,
            control_flags(),
            0,
            0,
            0,
            serde_json::to_vec(&ModuleControlCommand::Upgrading { reconnect_by_ms }).unwrap(),
        )
        .unwrap()
    }

    // Make a PING readable in the same poll where the writer gets BrokenPipe. The serve loop
    // reads the PING first, then finds that it cannot enqueue its PONG on the closed writer.
    #[derive(Default)]
    struct BrokenPeer {
        reset: AtomicBool,
        reader: futures::task::AtomicWaker,
    }

    struct ResetWriter(Arc<BrokenPeer>);

    impl AsyncWrite for ResetWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.0.reset.store(true, Ordering::Release);
            self.0.reader.wake();
            Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    struct ResetReader(Arc<BrokenPeer>, io::Cursor<Vec<u8>>);

    impl AsyncRead for ResetReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            self.0.reader.register(cx.waker());
            if self.0.reset.load(Ordering::Acquire) {
                Pin::new(&mut self.1).poll_read(cx, buf)
            } else {
                Poll::Pending
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ordinary_reset_on_an_upgrade_capable_wire_reports_peer_reset() {
        let peer = Arc::new(BrokenPeer::default());
        let mut ping = Vec::new();
        write_frame(
            &mut ping,
            &Frame::build(FrameType::Ping, control_flags(), 0, 0, 42, vec![]).unwrap(),
        )
        .await
        .unwrap();
        let generation = Generation::new(None, true);
        let end = serve_generation(
            ResetReader(peer.clone(), io::Cursor::new(ping)),
            ResetWriter(peer),
            &services(),
            None,
            Some(registration()),
            &accepting_drain::Drain::default(),
            Some(&generation),
        )
        .await;
        assert!(
            matches!(end, Ok(ServeEnd::PeerReset)),
            "ordinary wire reset: {end:?}"
        );
        assert!(generation.notice().is_none());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test(start_paused = true)]
    async fn an_upgrade_queued_before_first_readiness_restarts_cold() {
        let (module, mut daemon) = tokio::io::duplex(4096);
        let registration = registration();
        let conn = Arc::new(ModuleConnection::new(
            Box::new(module),
            registration.gate.clone(),
        ));
        write_frame(&mut daemon, &upgrade_push(now_epoch_ms() as u64 + 1000))
            .await
            .unwrap();
        drop(daemon);
        let Services {
            handler,
            health_registry,
            vault_gauge,
            consumer_routes,
            ..
        } = services();
        let result = serve_supervised(
            Path::new("/no-daemon-discovery-file"),
            conn,
            registration,
            "broca",
            handler,
            health_registry,
            vault_gauge,
            None,
            consumer_routes,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await;
        assert!(
            matches!(result, Err(ref error) if error.0.contains("interrupted module startup")),
            "must not dial for a queued cold-start notice: {result:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_announced_but_half_open_old_wire_ends_by_the_deadline() {
        let (module, mut daemon) = tokio::io::duplex(4096);
        let (reader, writer) = tokio::io::split(module);
        let generation = Generation::new(None, true);
        let services = services();
        let drain = accepting_drain::Drain::default();
        let registered = registration();
        let startup = async {
            let catalog = read_frame(&mut daemon).await.unwrap().unwrap();
            write_frame(
                &mut daemon,
                &Frame::build(
                    FrameType::Response,
                    control_flags(),
                    0,
                    0,
                    catalog.header.corr,
                    vec![],
                )
                .unwrap(),
            )
            .await
            .unwrap();
            write_frame(&mut daemon, &upgrade_push(now_epoch_ms() as u64 + 150))
                .await
                .unwrap();
        };
        let serve = serve_generation(
            reader,
            writer,
            &services,
            None,
            Some(registered),
            &drain,
            Some(&generation),
        );
        let (end, ()) = tokio::join!(tokio::time::timeout(Duration::from_secs(2), serve), startup);
        assert!(
            end.is_ok(),
            "the old wire is still open past its upgrade deadline"
        );
        assert!(generation.notice().is_some());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test(start_paused = true)]
    async fn a_missed_upgrade_deadline_stops_outward_work_before_serve_returns() {
        let (module, mut daemon) = tokio::io::duplex(4096);
        let registered = registration();
        let conn = Arc::new(ModuleConnection::new(
            Box::new(module),
            registered.gate.clone(),
        ));
        let services = services();
        let stopped = services.health_registry.stopping_signal();
        let old_wire = async move {
            let catalog = read_frame(&mut daemon).await.unwrap().unwrap();
            write_frame(
                &mut daemon,
                &Frame::build(
                    FrameType::Response,
                    control_flags(),
                    0,
                    0,
                    catalog.header.corr,
                    vec![],
                )
                .unwrap(),
            )
            .await
            .unwrap();
            write_frame(&mut daemon, &upgrade_push(now_epoch_ms() as u64 + 250))
                .await
                .unwrap();
        };
        let serve = serve_supervised(
            Path::new("/no-daemon-discovery-file"),
            conn,
            registered,
            "broca",
            services.handler,
            services.health_registry,
            services.vault_gauge,
            None,
            services.consumer_routes,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let (result, ()) = tokio::join!(serve, old_wire);
        assert!(
            matches!(result, Err(ref error) if error.0.contains("reconnect deadline passed")),
            "{result:?}"
        );
        assert!(
            stopped.is_cancelled(),
            "the deadline must stop outward work before returning to main for the best-effort seal"
        );
    }

    #[test]
    fn each_ack_replaces_the_previous_resume_token() {
        let mut ack = ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: Vec::new(),
            subc_capabilities: Vec::new(),
            storage: None,
            machine_id: None,
            resume_token: Some("synthetic-first".into()),
        };
        let mut token = LatestToken::default();
        token.refresh(&mut ack);
        assert!(ack.resume_token.is_none());
        ack.resume_token = Some("synthetic-next".into());
        token.refresh(&mut ack);
        assert!(ack.resume_token.is_none());
        assert_eq!(token.0.as_deref(), Some("synthetic-next"));
        token.refresh(&mut ack);
        assert!(
            token.0.is_none(),
            "a token is never retained across an ACK that omitted it"
        );
    }

    #[test]
    fn upgrade_deadline_is_converted_once_and_clamps_past_time() {
        let now = Instant::now();
        assert_eq!(
            monotonic_deadline(now, 1000, 1250),
            now + Duration::from_millis(250)
        );
        assert_eq!(monotonic_deadline(now, 1250, 1000), now);
    }

    #[test]
    fn unannounced_connection_loss_has_no_resume_window() {
        assert!(Generation::new(None, true).notice().is_none());
    }

    #[test]
    fn upgrade_resume_checks_the_deadline_even_after_goodbye() {
        let received = Instant::now();
        let notice = Notice {
            received,
            deadline: received + Duration::from_secs(1),
            ready_at_receipt: true,
        };
        let goodbye = Ok(ServeEnd::Goodbye);
        assert!(check_resume(notice, &goodbye, false, received).is_ok());
        assert_eq!(
            check_resume(notice, &goodbye, false, notice.deadline),
            Err("reconnect deadline passed")
        );
        assert_eq!(
            check_resume(notice, &goodbye, true, received),
            Err("module stop started")
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn resume_hello_refusal_is_terminal_without_exposing_its_body() {
        let refusal = ErrorBody::new(
            subc_protocol::error_codes::RESUME_REFUSED,
            "synthetic-credential",
        );
        assert!(matches!(
            hello_attempt_error(hello::HelloError::Refused(refusal)),
            AttemptError::Terminal("daemon refused resume HELLO")
        ));
        let unavailable = ErrorBody::new(
            subc_protocol::error_codes::LOCAL_ENDPOINT_CLIENTS_ONLY,
            "not listening yet",
        );
        assert!(matches!(
            hello_attempt_error(hello::HelloError::Refused(unavailable)),
            AttemptError::Retry
        ));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test(start_paused = true)]
    async fn resume_refused_ends_attempts_for_seal_before_deadline() {
        let (module, mut daemon) = tokio::io::duplex(4096);
        let body = ErrorBody::new(subc_protocol::error_codes::RESUME_REFUSED, "resume refused");
        let frame = Frame::build(
            FrameType::Error,
            control_flags(),
            0,
            0,
            HELLO_CORR,
            serde_json::to_vec(&body).unwrap(),
        )
        .unwrap();
        write_frame(&mut daemon, &frame).await.unwrap();
        let mut module = Some(module);
        let deadline = Instant::now() + Duration::from_secs(30);
        let result = retry_resume(deadline, &CancellationToken::new(), || {
            let mut stream = module
                .take()
                .expect("a terminal refusal must not open another socket");
            async move {
                hello::read_hello_ack(&mut stream)
                    .await
                    .map_err(hello_attempt_error)
            }
        })
        .await;
        // Both this refusal and a missed deadline end serving with an error. Main tries the
        // final session log syncs before exit, but a deadline kill can interrupt that work.
        assert!(matches!(result, Err("daemon refused resume HELLO")));
        assert!(Instant::now() < deadline);
    }
}
