use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use async_nats::{ClientError, ConnectErrorKind, Event};
use cortexkit_bus_nats::NatsConnectionEvent;
use tokio::time::Instant;

const OUTAGE_ERROR_AFTER: Duration = Duration::from_secs(5 * 60);

/// Logs one outage across automatic retries and explicit redials by the shared publisher.
#[derive(Default)]
pub(super) struct ReconnectLog(Mutex<State>);

#[derive(Default)]
struct State {
    dial: u64,
    outage: Option<Outage>,
}

struct Outage {
    started: Instant,
    failed_attempts: u64,
    alarm: tokio::task::AbortHandle,
}

impl Drop for Outage {
    fn drop(&mut self) {
        self.alarm.abort();
    }
}

impl ReconnectLog {
    pub(super) fn observe_dial(
        self: &Arc<Self>,
    ) -> impl Fn(NatsConnectionEvent<'_>) + Send + Sync + 'static {
        let dial = {
            let mut state = self.0.lock().unwrap();
            state.dial += 1;
            state.dial
        };
        let log = Arc::downgrade(self);
        move |event| {
            if let Some(log) = log.upgrade() {
                log.observe(dial, event);
            }
        }
    }

    fn observe(self: &Arc<Self>, dial: u64, event: NatsConnectionEvent<'_>) {
        let mut state = self.0.lock().unwrap();
        match event {
            NatsConnectionEvent::ConnectFailed(error) => {
                let kind = error.kind();
                if is_transport_failure(kind) {
                    if dial == state.dial {
                        self.failed_attempt(&mut state, &kind.to_string());
                    }
                } else {
                    tracing::error!(target: "bus", %kind, "ck-bus connection attempt refused or failed");
                }
            }
            NatsConnectionEvent::Event(Event::ClientError(ClientError::Other(reason))) => {
                // async-nats sends ConnectErrorKind's Display text, not the source error,
                // for failures inside its automatic reconnect loop.
                let kind = [
                    ConnectErrorKind::Io,
                    ConnectErrorKind::TimedOut,
                    ConnectErrorKind::Dns,
                ]
                .into_iter()
                .find(|kind| kind.to_string() == *reason);
                if kind.is_some() {
                    if dial == state.dial {
                        self.failed_attempt(&mut state, reason);
                    }
                } else {
                    if dial == state.dial {
                        if let Some(outage) = &mut state.outage {
                            outage.failed_attempts += 1;
                        }
                    }
                    tracing::error!(target: "bus", %reason, "ck-bus reconnect attempt refused or failed");
                }
            }
            NatsConnectionEvent::Event(Event::Disconnected) if dial == state.dial => {
                if state.outage.is_none() {
                    tracing::warn!(target: "bus", "ck-bus connection lost; reconnecting");
                    state.outage = Some(self.start_outage(0));
                }
            }
            NatsConnectionEvent::Event(Event::Connected) if dial == state.dial => {
                if let Some(outage) = state.outage.take() {
                    tracing::info!(target: "bus", downtime_ms = outage.started.elapsed().as_millis() as u64,
                        attempts = outage.failed_attempts + 1, "ck-bus connection restored");
                }
            }
            // Preserve other client diagnostics hidden by the async-nats log ceiling.
            NatsConnectionEvent::Event(
                event @ (Event::ServerError(_)
                | Event::ClientError(_)
                | Event::SlowConsumer(_)
                | Event::LameDuckMode),
            ) => {
                tracing::info!(target: "bus", %event, "ck-bus client event");
            }
            _ => {}
        }
    }

    fn failed_attempt(self: &Arc<Self>, state: &mut State, reason: &str) {
        if let Some(outage) = &mut state.outage {
            outage.failed_attempts += 1;
            tracing::debug!(target: "bus", reason, attempts = outage.failed_attempts,
                "ck-bus connection retry failed");
        } else {
            tracing::warn!(target: "bus", reason, "ck-bus connection attempt failed; reconnecting");
            state.outage = Some(self.start_outage(1));
        }
    }

    fn start_outage(self: &Arc<Self>, failed_attempts: u64) -> Outage {
        let started = Instant::now();
        let log = Arc::downgrade(self);
        // A quiet publisher may have no more callers to check the outage deadline.
        let alarm = tokio::spawn(async move {
            tokio::time::sleep_until(started + OUTAGE_ERROR_AFTER).await;
            if let Some(log) = log.upgrade() {
                let state = log.0.lock().unwrap();
                if let Some(outage) = state.outage.as_ref().filter(|outage| outage.started == started) {
                    tracing::error!(target: "bus", downtime_ms = started.elapsed().as_millis() as u64,
                        attempts = outage.failed_attempts, "ck-bus connection is still down");
                }
            }
        }).abort_handle();
        Outage {
            started,
            failed_attempts,
            alarm,
        }
    }
}

fn is_transport_failure(kind: ConnectErrorKind) -> bool {
    matches!(
        kind,
        ConnectErrorKind::Io | ConnectErrorKind::TimedOut | ConnectErrorKind::Dns
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tracing::{
        field::{Field, Visit},
        Level, Subscriber,
    };
    use tracing_subscriber::{layer::Context, prelude::*, Layer};

    #[derive(Default)]
    struct Fields(BTreeMap<String, String>);

    impl Visit for Fields {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().into(), format!("{value:?}"));
        }
    }

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<(Level, Fields)>>>);

    impl<S: Subscriber> Layer<S> for Capture {
        fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
            if event.metadata().target() == "bus" {
                let mut fields = Fields::default();
                event.record(&mut fields);
                self.0
                    .lock()
                    .unwrap()
                    .push((*event.metadata().level(), fields));
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn broker_restart_logs_one_warning_and_one_recovery() {
        let capture = Capture::default();
        let _subscriber =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
        let log = Arc::new(ReconnectLog::default());
        let observe = log.observe_dial();
        observe(NatsConnectionEvent::Event(&Event::Connected));
        observe(NatsConnectionEvent::Event(&Event::Disconnected));
        for kind in [ConnectErrorKind::Io, ConnectErrorKind::TimedOut] {
            tokio::time::advance(Duration::from_secs(10)).await;
            observe(NatsConnectionEvent::Event(&Event::ClientError(
                ClientError::Other(kind.to_string()),
            )));
        }
        observe(NatsConnectionEvent::Event(&Event::Connected));
        tokio::time::advance(OUTAGE_ERROR_AFTER).await;
        let captured = capture.0.lock().unwrap();
        let levels: Vec<_> = captured.iter().map(|(level, _)| *level).collect();
        assert_eq!(
            levels,
            [Level::WARN, Level::DEBUG, Level::DEBUG, Level::INFO]
        );
        let (_, fields) = captured.last().unwrap();
        assert_eq!(fields.0["message"], "ck-bus connection restored");
        assert_eq!(fields.0["attempts"], "3");
        assert_eq!(fields.0["downtime_ms"], "20000");
    }

    #[tokio::test(start_paused = true)]
    async fn broker_dial_failure_keeps_the_outage_across_redials_and_reports_auth() {
        let capture = Capture::default();
        let _subscriber =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
        let log = Arc::new(ReconnectLog::default());
        let initial = log.observe_dial();
        let io = async_nats::ConnectError::new(ConnectErrorKind::Io);
        initial(NatsConnectionEvent::ConnectFailed(&io));
        let replacement = log.observe_dial();
        replacement(NatsConnectionEvent::ConnectFailed(&io));
        // A superseded link may still report Connected while it shuts down; only the current
        // dial's events can end the publisher's outage.
        initial(NatsConnectionEvent::Event(&Event::Connected));
        let refused = Event::ClientError(ClientError::Other(
            ConnectErrorKind::AuthorizationViolation.to_string(),
        ));
        replacement(NatsConnectionEvent::Event(&refused));
        tokio::time::advance(OUTAGE_ERROR_AFTER).await;
        tokio::task::yield_now().await;
        {
            let captured = capture.0.lock().unwrap();
            let levels: Vec<_> = captured.iter().map(|(level, _)| *level).collect();
            assert_eq!(
                levels,
                [Level::WARN, Level::DEBUG, Level::ERROR, Level::ERROR]
            );
            assert_eq!(captured[2].1 .0["reason"], "authorization violation");
            assert_eq!(
                captured[3].1 .0["message"],
                "ck-bus connection is still down"
            );
        }
        log.observe_dial()(NatsConnectionEvent::Event(&Event::Connected));
        let captured = capture.0.lock().unwrap();
        let (level, fields) = captured.last().unwrap();
        assert_eq!(*level, Level::INFO);
        assert_eq!(fields.0["attempts"], "4");
    }
}
