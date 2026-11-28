//! One registered wire generation and its connection-local work.

use super::*;

pub(super) struct Services<H> {
    pub module_id: String,
    pub handler: Arc<H>,
    pub health_registry: Arc<LineageRegistry>,
    pub vault_gauge: Arc<VaultGauge>,
    pub bus_connector: Option<Arc<NatsConnector>>,
    pub serves: Arc<crate::serves::Source>,
    pub consumer_routes: crate::consumer_routes::ConsumerRoutes,
    pub vault_auditor: Option<Arc<dyn crate::vault_audit::VaultAuditor>>,
    pub scope_describer: Option<Arc<broca_subc::ModuleScopeDescriber>>,
    pub agent_data: Option<Arc<crate::agent_data::AgentDataService>>,
    pub system_one: Option<Arc<crate::system_one::SystemOne>>,
    pub llm_tools: Option<Arc<crate::llm_tools::LlmTools>>,
}

/// Add the sessionless model-tool service without changing existing classifier callers.
#[allow(clippy::too_many_arguments)]
pub async fn serve_connection_with_llm_tools<R, W, H>(
    read_half: R,
    write_half: W,
    module_id: &str,
    handler: Arc<H>,
    health_registry: Arc<LineageRegistry>,
    vault_gauge: Arc<VaultGauge>,
    catalog_cache: Option<Arc<broca_catalog::CatalogCache>>,
    consumer_routes: crate::consumer_routes::ConsumerRoutes,
    vault_auditor: Option<Arc<dyn crate::vault_audit::VaultAuditor>>,
    daemon_capabilities: Option<Vec<String>>,
    scope_describer: Option<Arc<broca_subc::ModuleScopeDescriber>>,
    agent_data: Option<Arc<crate::agent_data::AgentDataService>>,
    system_one: Option<Arc<crate::system_one::SystemOne>>,
    llm_tools: Option<Arc<crate::llm_tools::LlmTools>>,
    gateway_listing: Option<Arc<dyn crate::serves::GatewayListing>>,
    registered: Option<GatedRegistration>,
) -> Result<ServeEnd, ServeIoError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
    H: ServeHandler + 'static,
{
    let services = Services {
        module_id: module_id.to_owned(),
        handler,
        health_registry,
        vault_gauge,
        bus_connector: None,
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
    serve_generation(
        read_half,
        write_half,
        &services,
        daemon_capabilities.as_deref(),
        registered,
        &accepting_drain::Drain::default(),
        None,
    )
    .await
}

pub(super) async fn serve_generation<R, W, H>(
    mut read_half: R,
    write_half: W,
    services: &Services<H>,
    daemon_capabilities: Option<&[String]>,
    registered: Option<GatedRegistration>,
    drain: &accepting_drain::Drain,
    upgrade: Option<&live_upgrade::Generation>,
) -> Result<ServeEnd, ServeIoError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
    H: ServeHandler + 'static,
{
    initialize_session_readers();
    let stall_state = stall::connection_state();
    let (tx, rx) = broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, stall_state.egress.clone());
    let mut writer = tokio::spawn(drain_writer(write_half, rx));
    let reader_lifetime = stall::reader_lifetime(stall_state.clone());

    // A connection-level cancel: when the frame loop ends (EOF / GOODBYE / error), this
    // cancels every live subscribe forward task so they drop their egress sender clones,
    // letting the writer task's channel close and `drain_writer` exit. Without it, a
    // lineage-scoped subscription's parked forward task would keep the egress open forever
    // and hang the connection on teardown.
    let conn_cancel = CancellationToken::new();
    let gate = registered
        .as_ref()
        .map(|registered| registered.gate.clone());
    let frame_loop = stall::READER.scope(
        stall_state,
        run_frame_loop(
            &mut read_half,
            tx.clone(),
            services,
            conn_cancel.clone(),
            daemon_capabilities,
            registered,
            drain,
            upgrade,
        ),
    );
    let mut writer_finished = false;
    tokio::pin!(frame_loop);
    let (frame_result, writer_end) = if let Some(upgrade) = upgrade {
        // Read an announcement already queued on the old connection before observing its
        // failing writer. That announcement distinguishes a switch from a plain disconnect.
        tokio::select! {
            biased;
            _ = upgrade.old_wire_deadline() => (Some(Ok(ServeEnd::PeerReset)), None),
            result = &mut frame_loop => (Some(result), None),
            result = &mut writer => (None, Some(result)),
        }
    } else {
        tokio::select! {
            biased;
            // A reset reported by the write half closes the TCP connection, not one route
            // (RFC 9293 §3.10.7.4). Observe it before a request sees the closed egress queue.
            result = &mut writer => (None, Some(result)),
            result = &mut frame_loop => (Some(result), None),
        }
    };
    let mut loop_result = match writer_end {
        Some(result) => {
            writer_finished = true;
            match result {
                Ok(Ok(())) => Ok(ServeEnd::PeerReset),
                Ok(Err(error)) => Err(error),
                Err(error) => Err(ServeIoError(error.to_string())),
            }
        }
        None => frame_result.expect("one wire half finished"),
    };
    if !writer_finished
        && upgrade.is_some()
        && matches!(&loop_result, Err(error) if error.0 == "egress channel closed")
    {
        // The reader may discover the closed writer queue while replying to the last frame.
        // Check why the writer stopped before deciding whether this was a transport cut.
        writer_finished = true;
        loop_result = match (&mut writer).await {
            Ok(Ok(())) => Ok(ServeEnd::PeerReset),
            Ok(Err(error)) => Err(error),
            Err(error) => Err(ServeIoError(error.to_string())),
        };
    }
    drop(reader_lifetime);
    let switching = loop_result.is_ok() && upgrade.is_some_and(live_upgrade::Generation::cut);
    // Before the egress queue can close, release the describer's and settings gate's senders.
    // The code that obtains daemon claims for consumer sockets still holds the gate itself.
    if let Some(describer) = &services.scope_describer {
        describer.detach();
    }
    if let Some(gate) = &gate {
        if switching {
            gate.detach_for_upgrade();
        } else {
            gate.detach();
        }
    }
    conn_cancel.cancel();
    drop(tx);
    if switching {
        // The old socket cannot carry another reply. A full or blocked egress queue must
        // not consume the daemon's reconnect window.
        if !writer_finished {
            writer.abort();
            let _ = writer.await;
        }
        return loop_result;
    }
    let writer_result = if writer_finished {
        Ok(Ok(()))
    } else {
        writer.await
    };

    match (loop_result, writer_result) {
        (Err(e), _) => Err(e),
        (Ok(end), Ok(Ok(()))) => Ok(end),
        (Ok(_), Ok(Err(e))) => Err(e),
        (Ok(_), Err(e)) => Err(ServeIoError(e.to_string())),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_frame_loop<R, H>(
    read_half: &mut R,
    writer: EgressSender<Frame>,
    services: &Services<H>,
    conn_cancel: CancellationToken,
    daemon_capabilities: Option<&[String]>,
    registered: Option<GatedRegistration>,
    drain: &accepting_drain::Drain,
    upgrade: Option<&live_upgrade::Generation>,
) -> Result<ServeEnd, ServeIoError>
where
    R: AsyncRead + Unpin,
    H: ServeHandler + 'static,
{
    let module_id = &services.module_id;
    let health_registry = &services.health_registry;
    let scope_describer = services.scope_describer.as_deref();
    let image_references = services
        .system_one
        .as_ref()
        .is_some_and(|system| system.image_references());
    let (ack, gate) = match registered {
        None => {
            send_hello(
                &writer,
                module_id,
                &services.consumer_routes,
                image_references,
            )
            .await?;
            let _phase = stall::enter(Phase::AwaitFrame).await;
            (expect_hello_ack(read_half).await?, None)
        }
        Some(registered) => (registered.ack, Some(registered.gate)),
    };
    // Registered: from here the daemon answers this connection's `scope.describe`.
    if let Some(describer) = scope_describer {
        describer.attach_observed(writer.clone());
    }
    let daemon_scopes = daemon_capabilities
        .unwrap_or(&ack.subc_capabilities)
        .iter()
        .any(|capability| capability == subc_protocol::scope::CAP_SCOPES_V1);

    let mut routes = Routes::new(daemon_scopes);
    routes.drain = drain.negotiated(&ack);
    routes.gate = gate;
    routes.agent_data = services.agent_data.clone();
    routes.system_one = services.system_one.clone();
    routes.llm_tools = services.llm_tools.clone();
    let request_slots = Arc::new(Semaphore::new(MAX_INFLIGHT_ROUTE_REQUESTS));
    let mut request_tasks = JoinSet::new();
    if let Some(gate) = routes.gate.clone() {
        // The daemon's answers reach the gate through this loop, so it runs beside it. A refused
        // readiness ends the loop, and broca restarts.
        let provides =
            hello_manifest(module_id, &services.consumer_routes, image_references).provides;
        let writer = writer.clone();
        let resumed = upgrade.and_then(live_upgrade::Generation::on_started);
        let config_gate = ack
            .subc_capabilities
            .iter()
            .any(|cap| cap == subc_protocol::config::CONFIG_GATE_CAPABILITY);
        let registry = Arc::clone(health_registry);
        request_tasks.spawn(async move {
            match resumed {
                Some(started) => {
                    gate.finish_resume(writer, provides, config_gate).await?;
                    started(registry.slot_counts().runs_in_flight);
                    Ok(())
                }
                None => gate.finish_start(writer, provides).await,
            }
        });
    }

    // ONE READ STAYS IN FLIGHT ACROSS SELECT TURNS. `read_frame` is not cancellation-safe: it
    // reads the header and then the body in separate steps, and dropping it part-way loses the
    // bytes it already took. The loop used to start a fresh `read_frame` inside the select on
    // every turn, so whenever a request task finished while a frame was half-read, the partial
    // frame was dropped and the next read began mid-frame. Measured 2026-09-26 00:09:26Z:
    // broca exited 1 with "unsupported envelope version 32", a JSON space read as a header.
    // So the read future is created once, pinned here, polled by reference, and replaced only
    // after it has completed; losing the select race just leaves it pending for the next turn.
    let mut pending_read = std::pin::pin!(read_next_frame(read_half));
    let _ = stall::READER.try_with(|s| s.set_reader(Phase::AwaitFrame, 0, 0));

    let loop_result = loop {
        tokio::select! {
            _ = routes.drain.next_check() => {
                routes.drain.check(health_registry, &writer, routes.llm_tools.as_deref()).await;
            }
            joined = routes.bind_tasks.join_next(), if !routes.bind_tasks.is_empty() => {
                match joined.expect("guarded by !routes.bind_tasks.is_empty()") {
                    Ok(completed) => finish_bind(completed, &writer, &mut routes).await?,
                    // A panic inside the adoption check is caught in `authorize_bind` and
                    // answered as a transient refusal. Reaching here means the task failed
                    // outside it; that is still this bind's failure, not the connection's. Its
                    // pending entry is replaced by a rebind or removed by its GOODBYE.
                    Err(error) => tracing::error!(
                        target: "wire.ingress",
                        "bind authorization task failed; refusing only that bind: {error}"
                    ),
                }
            }
            joined = request_tasks.join_next(), if !request_tasks.is_empty() => {
                match joined.expect("guarded by !request_tasks.is_empty()") {
                    Ok(Ok(())) => {}
                    // An egress failure inside a request task means the connection itself is
                    // unusable, so it still ends the loop.
                    Ok(Err(e)) => break Err(e),
                    Err(e) if e.is_cancelled() => {}
                    // A panic in a request is answered inside its task (`reply_on_panic`).
                    // Reaching here means the task failed outside that guard. It is still
                    // confined to its own request: ending the loop would cut every session's
                    // streams and in-flight sends on this connection for one request's bug.
                    Err(e) => tracing::error!(
                        target: "wire.ingress",
                        "route request task failed; dropping only that request: {e}"
                    ),
                }
            }
            (frame, reader) = &mut pending_read => {
                let frame = match frame {
                    Ok(Some(frame)) => frame,
                    // The peer closed the connection, so an empty read is a normal end-of-stream.
                    Ok(None) => break Ok(ServeEnd::Eof),
                    // THE DAEMON GOING AWAY IS A NORMAL END, whichever way the socket reports it.
                    // A clean close reads as EOF (`Ok(None)` below), but at a daemon cut the
                    // daemon exits and the kernel RESETS the socket instead. Measured 2026-09-24,
                    // 16:25Z cut: "frame I/O error: Connection reset by peer (os error 54)",
                    // made main exit 1 even after it had safely sealed the active session logs,
                    // so the supervisor recorded a clean stop as a crash. Match the I/O error
                    // kind, never the message text. A torn frame (FrameIoError::UnexpectedEof) is
                    // NOT treated this way: that could be a protocol fault, and stays an error.
                    Err(FrameIoError::Io(error)) if peer_went_away(error.kind()) => {
                        break Ok(ServeEnd::PeerReset)
                    }
                    Err(error) => return Err(ServeIoError(error.to_string())),
                };
                stall::frame_context(0, frame.header.channel);
                // Log before route validation: a stale-epoch frame is discarded before its
                // method is decoded, so describe milestones alone cannot show that it arrived.
                tracing::debug!(
                    target: "wire.ingress",
                    channel = frame.header.channel, epoch = frame.header.epoch,
                    corr = frame.header.corr, ty = ?frame.header.ty,
                    "module frame received"
                );
                // A reply to the describer's own request is its alone; every other frame is
                // handled as before.
                let describe_reply = scope_describer.is_some_and(|describer| describer.on_frame(&frame))
                    || routes.gate.as_ref().is_some_and(|gate| gate.on_frame(&frame))
                    || routes.drain.on_reply(&frame, health_registry);
                let upgrading = !describe_reply && upgrade.is_some_and(|upgrade| {
                    upgrade.on_frame(&frame, routes.gate.as_ref().is_some_and(|gate| gate.ready()))
                });
                if upgrading && upgrade.is_some_and(live_upgrade::Generation::interrupted_startup) {
                    break Ok(ServeEnd::PeerReset);
                }
                if !describe_reply
                    && !upgrading
                    && !handle_frame(
                    frame,
                    &writer,
                    &mut routes,
                    &services.handler,
                    health_registry,
                    &services.vault_gauge,
                    services.bus_connector.as_deref(),
                    &services.serves,
                    &conn_cancel,
                    &request_slots,
                    &mut request_tasks,
                    services.vault_auditor.clone(),
                )
                .await?
                {
                    // handle_frame stops the loop only for a channel-0 GOODBYE.
                    break Ok(ServeEnd::Goodbye);
                }
                pending_read.set(read_next_frame(reader));
                let _ = stall::READER.try_with(|s| s.set_reader(Phase::AwaitFrame, 0, 0));
            }
        }
    };

    conn_cancel.cancel();
    request_tasks.abort_all();
    while request_tasks.join_next().await.is_some() {}
    loop_result
}

/// Read one frame and hand the reader back with it, so the frame loop can keep a single read
/// future alive across select turns and start the next read from the reader it returns.
async fn read_next_frame<R>(reader: &mut R) -> (Result<Option<Frame>, FrameIoError>, &mut R)
where
    R: AsyncRead + Unpin,
{
    let frame = read_frame(reader).await;
    (frame, reader)
}
