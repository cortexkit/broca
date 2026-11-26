//! Drive a single provider response, enforcing the live attempt deadlines.

use super::*;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(super) async fn drive_body_stream_traced<S, B, E>(
    stream: S,
    normalizer: Box<dyn StreamNormalizer>,
    tx: &mpsc::Sender<NormalizedEvent>,
    max_stream_bytes: usize,
    trace: &mut AttemptTrace,
) -> Result<Vec<NormalizedEvent>, AttemptFailure>
where
    S: futures::Stream<Item = Result<B, E>>,
    B: AsRef<[u8]>,
    E: std::error::Error,
{
    let deadlines = StepDeadlines::start(
        trace.live_progress.clone(),
        STREAM_IDLE_TIMEOUT,
        normalizer.content_deadline(),
    );
    drive_live_body(stream, normalizer, tx, max_stream_bytes, trace, deadlines).await
}

fn timeout_failure(deadlines: &StepDeadlines, stage: &str) -> AttemptFailure {
    if deadlines.content() < deadlines.idle() {
        AttemptFailure::transport(
            content_timeout_error(deadlines.content_limit()),
            AttemptOutcome::ContentTimeout,
        )
    } else {
        AttemptFailure::transport(stream_timeout_error(stage), AttemptOutcome::IdleTimeout)
    }
}

pub(super) async fn drive_live_body<S, B, E>(
    stream: S,
    mut normalizer: Box<dyn StreamNormalizer>,
    tx: &mpsc::Sender<NormalizedEvent>,
    max_stream_bytes: usize,
    trace: &mut AttemptTrace,
    mut deadlines: StepDeadlines,
) -> Result<Vec<NormalizedEvent>, AttemptFailure>
where
    S: futures::Stream<Item = Result<B, E>>,
    B: AsRef<[u8]>,
    E: std::error::Error,
{
    futures::pin_mut!(stream);
    if trace.attempt_hook.is_some() {
        let evidence = trace.response_evidence.get_or_insert_with(Default::default);
        normalizer.set_response_evidence(evidence.clone());
    }
    let mut stream_bytes = 0usize;
    if trace.transport.as_deref() == Some("websocket") {
        normalizer.websocket_telemetry();
    }
    let mut progress_marker = normalizer.progress_marker();
    let mut liveness_marker = normalizer.liveness_marker();
    let stage = "waiting for a complete event";
    let mut completion = Vec::new();
    let mut rate_window = stream_floor_from_env().map(StreamRateWindow::new);
    // Socket frames are tapped by the receive pump. SSE borrows the normalizer's decoded
    // payloads so ordinary deltas incur no second decoder or per-record allocation.
    if let Some(observer) = trace.quota_observer.as_ref().filter(|observer| {
        observer.wants_frames() && trace.transport.as_deref() != Some("websocket")
    }) {
        let observer = observer.clone();
        normalizer.set_raw_sse_observer(Arc::new(move |frame| {
            crate::quota::observe_frame(observer.as_ref(), frame);
        }));
    }

    loop {
        let Some(next) = before_expiry!(deadlines, stream.next()) else {
            // A body left open after the step's last record has nothing more to give.
            if step_closed(normalizer.as_ref(), &completion) {
                break;
            }
            return Err(timeout_failure(&deadlines, stage));
        };
        let Some(chunk) = next else { break };
        // The step's last record already arrived (a proxy reset the body before its clean end,
        // say): nothing was lost, and asking the model again would bill a second answer.
        if chunk.is_err() && step_closed(normalizer.as_ref(), &completion) {
            break;
        }
        let bytes = chunk.map_err(|e| {
            let failure = AttemptFailure::transport(
                transport_error(&format!("stream chunk: {e}")),
                AttemptOutcome::ChunkError,
            );
            trace.chunk_warning = Some(ChunkWarning {
                last_event_type: crate::sse::safe_event_type_name(
                    normalizer.last_event_type().unwrap_or("unknown"),
                )
                .into(),
                event_type_counts: format_event_type_counts(normalizer.event_type_counts()),
                error_chain: if trace.ephemeral_session {
                    String::new()
                } else {
                    transport_error_chain(&e)
                },
            });
            if !trace.defer_failure_warning {
                warn_attempt_retry(&failure, trace, "not_retryable", "standalone_attempt");
            }
            trace.chunk_error_warned = true;
            failure
        })?;
        let chunk = bytes.as_ref();
        // Digest the raw chunk before any limit or parse decision so the attempt's recorded
        // response digest covers it even when the chunk ends the attempt.
        trace.observe(chunk);
        if let Some(observation) = trace
            .websocket_observation
            .upgrade()
            .filter(|_| trace.transport.as_deref() == Some("websocket"))
        {
            observation.lock().unwrap().observe_text(chunk);
        }
        if chunk.len() > max_stream_bytes.saturating_sub(stream_bytes) {
            return Err(AttemptFailure::decode(
                stream_limit_error(&format!(
                    "provider stream exceeds the {max_stream_bytes}-byte per-attempt limit"
                )),
                AttemptOutcome::ByteLimit,
            ));
        }
        stream_bytes += chunk.len();
        let websocket = trace.transport.as_deref() == Some("websocket");
        if websocket {
            if let Some(error) = responses_websocket::wrapped_error(chunk, trace) {
                deadlines.record(false, None);
                trace.content_gaps.observe(Some("error"));
                return Err(error);
            }
        }
        let framed;
        let normalized = if websocket {
            framed = responses_websocket::frame_record(chunk);
            framed.as_slice()
        } else {
            chunk
        };
        let result = normalizer.push(normalized);
        if normalizer.progress_marker() != progress_marker {
            trace.content_gaps.observe(normalizer.last_event_type());
        }
        let progress = normalizer.content_progress();
        let made_content = progress.content_bytes != trace.content_progress.content_bytes;
        if made_content {
            trace.last_content_elapsed = trace.elapsed_since_send();
        }
        *trace.content_progress = progress;
        let events = result
            .map_err(|error| AttemptFailure::normalize(error, AttemptOutcome::DecodeError))?;
        observe_response_finish(&trace.response_evidence, &events);
        trace.output_seen |= events.iter().any(|event| {
            matches!(event,
            NormalizedEvent::Display(DisplayEvent::TextDelta { delta, .. }
                | DisplayEvent::ReasoningDelta { delta, .. }
                | DisplayEvent::ToolInputDelta { delta, .. }) if !delta.is_empty())
        });
        if trace.input_transformations.is_none() {
            trace.input_transformations = normalizer.input_transformations();
        }
        trace.input_transformations_unmeasured = normalizer.input_transformations_unmeasured();
        let next_marker = normalizer.progress_marker();
        let made_progress = next_marker != progress_marker || !events.is_empty();
        if next_marker != progress_marker {
            trace.content_event_count = next_marker;
        }
        trace.keepalives_after_content = normalizer.keepalives_after_content().unwrap_or_default();
        progress_marker = next_marker;
        // Raw bytes are never liveness: only a marker that counts complete records moves.
        let next_liveness = normalizer.liveness_marker();
        let alive = made_progress || made_content || next_liveness != liveness_marker;
        liveness_marker = next_liveness;
        if alive {
            deadlines.record(made_content, Some(&trace.content_progress));
        }
        if let Some(window) = rate_window.as_mut() {
            window.observe(&events);
            if let Some(rate) = window.breach() {
                // Transient (so the router sees provider health) and non-reissuable (so this loop
                // does not burn the attempt budget on the same degraded endpoint first). The
                // measured rate rides in the message because a router deciding between providers
                // needs the magnitude, not just the verdict.
                return Err(AttemptFailure::transport_without_reissue(
                    stream_too_slow_error(rate, window.floor),
                    AttemptOutcome::RateFloor,
                ));
            }
        }
        let cut = tool_args_cut(&events);
        let sent = before_expiry!(
            deadlines,
            record_attempt_events(events, &mut completion, tx, trace)
        )
        .ok_or_else(|| timeout_failure(&deadlines, stage))??;
        if !sent {
            return Ok(Vec::new());
        }
        if cut {
            return validate_attempt_completion(completion);
        }
        if websocket
            && matches!(
                normalizer.last_event_type(),
                Some("response.completed" | "response.incomplete")
            )
        {
            break;
        }
    }

    // `finish` runs only at a clean end of body. A grammar that cannot close the step from
    // what arrived emits nothing here, and the completion check below reports the cut as a
    // transport fault, the same `eof_before_completion` for every wire family; `finish`
    // fails only for what was received but does not decode.
    let result = normalizer.finish();
    let made_progress = normalizer.progress_marker() != progress_marker;
    if made_progress {
        trace.content_gaps.observe(normalizer.last_event_type());
    }
    let progress = normalizer.content_progress();
    let made_content = progress.content_bytes != trace.content_progress.content_bytes;
    if made_content {
        trace.last_content_elapsed = trace.elapsed_since_send();
    }
    *trace.content_progress = progress;
    let final_events = result
        .map_err(|error| AttemptFailure::normalize(error, AttemptOutcome::EofBeforeCompletion))?;
    if made_progress || made_content || normalizer.liveness_marker() != liveness_marker {
        deadlines.record(made_content, Some(&trace.content_progress));
    }
    // EOF has already stopped the network attempt, even if delivery of the decoded answer
    // to the run loop is backpressured.
    drop(deadlines);
    observe_response_finish(&trace.response_evidence, &final_events);
    if !record_attempt_events(final_events, &mut completion, tx, trace).await? {
        return Ok(Vec::new());
    }
    validate_attempt_completion(completion)
}
