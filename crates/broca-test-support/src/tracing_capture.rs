//! Scoped log assertions using one permanent process-wide subscriber.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex, Once, OnceLock, Weak};
use tracing::{Instrument, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{Layer, Registry};

/// An event's target and unquoted string/debug field values.
#[derive(Clone, Debug)]
pub struct LogEvent {
    pub target: &'static str,
    pub level: tracing::Level,
    pub fields: BTreeMap<String, String>,
}

impl LogEvent {
    pub fn message(&self) -> &str {
        self.fields.get("message").map_or("", String::as_str)
    }
}

impl tracing::field::Visit for LogEvent {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields
            .insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields.insert(
            field.name().to_owned(),
            format!("{value:?}").trim_matches('"').to_owned(),
        );
    }
}

#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<LogEvent>>>);

struct CaptureLayer;

struct Observer {
    filter: Box<dyn Fn(&LogEvent) -> bool + Send + Sync>,
    logs: CapturedLogs,
}

fn observers() -> &'static Mutex<Vec<Weak<Observer>>> {
    static OBSERVERS: OnceLock<Mutex<Vec<Weak<Observer>>>> = OnceLock::new();
    OBSERVERS.get_or_init(Mutex::default)
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for CaptureLayer {
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        // Capture scopes come and go, but interest must never depend on their lifetime.
        tracing::subscriber::Interest::always()
    }

    fn on_event(&self, event: &tracing::Event<'_>, context: Context<'_, S>) {
        let mut record = LogEvent {
            target: event.metadata().target(),
            level: *event.metadata().level(),
            fields: BTreeMap::new(),
        };
        event.record(&mut record);
        if let Some(scope) = context.event_scope(event) {
            for span in scope {
                if let Some(logs) = span.extensions().get::<CapturedLogs>() {
                    logs.0.lock().unwrap().push(record.clone());
                }
            }
        }
        let active: Vec<_> = {
            let mut observers = observers().lock().unwrap();
            observers.retain(|observer| observer.strong_count() > 0);
            observers.iter().filter_map(Weak::upgrade).collect()
        };
        for observer in active {
            if (observer.filter)(&record) {
                observer.logs.0.lock().unwrap().push(record.clone());
            }
        }
    }
}

/// A capture scope, isolated from concurrent tests by its root span.
///
/// Instrument every future that should contribute events with [`Self::scope_future`].
/// Spawned tasks do not inherit tracing spans: instrument them explicitly, and use
/// [`Self::in_scope`] in spawned threads or blocking closures. The subscriber is
/// global, so moving an instrumented future between threads does not lose its logs.
/// A pre-existing global subscriber is an error, not a silently empty capture.
#[derive(Clone)]
pub struct LogCapture {
    span: tracing::Span,
    logs: CapturedLogs,
    _observer: Option<Arc<Observer>>,
}

impl Default for LogCapture {
    fn default() -> Self {
        Self::new()
    }
}

impl LogCapture {
    pub fn new() -> Self {
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| {
            tracing::subscriber::set_global_default(Registry::default().with(CaptureLayer))
                .expect("log assertions need the shared global capture subscriber");
            // tracing-core's single-dispatcher fast path asks the registering
            // thread's default instead of the global registry. Keep two registered
            // dispatchers alive so a temporary local default cannot poison a new
            // callsite's interest for other threads. The inert anchor records nothing.
            static CACHE_ANCHOR: OnceLock<tracing::Dispatch> = OnceLock::new();
            CACHE_ANCHOR.get_or_init(|| {
                tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default())
            });
            tracing::callsite::rebuild_interest_cache();
        });
        let logs = CapturedLogs::default();
        let span = tracing::trace_span!(parent: None, "broca_test_log_capture");
        span.with_subscriber(|(id, dispatch)| {
            let registry = dispatch
                .downcast_ref::<Registry>()
                .expect("log capture must not be created under a thread-local subscriber");
            registry
                .span(id)
                .unwrap()
                .extensions_mut()
                .insert(logs.clone());
        })
        .expect("the global subscriber must enable the capture span");
        Self {
            span,
            logs,
            _observer: None,
        }
    }

    /// Observe events even from uninstrumented background tasks.
    ///
    /// The filter must identify this test's operation (for example by a unique
    /// run ID), because unrelated concurrent tests also use the global subscriber.
    /// Use scoped capture instead whenever the test owns the emitting future.
    pub fn observe(filter: impl Fn(&LogEvent) -> bool + Send + Sync + 'static) -> Self {
        let mut capture = Self::new();
        let observer = Arc::new(Observer {
            filter: Box::new(filter),
            logs: CapturedLogs::default(),
        });
        observers().lock().unwrap().push(Arc::downgrade(&observer));
        capture.logs = observer.logs.clone();
        capture._observer = Some(observer);
        capture
    }

    pub fn scope_future<F: Future>(&self, future: F) -> tracing::instrument::Instrumented<F> {
        future.instrument(self.span.clone())
    }

    pub fn in_scope<T>(&self, operation: impl FnOnce() -> T) -> T {
        self.span.in_scope(operation)
    }

    pub fn events(&self) -> Vec<LogEvent> {
        self.logs.0.lock().unwrap().clone()
    }
}

/// Run a future in an isolated capture scope and return all its events.
pub async fn capture_logs<F: Future>(future: F) -> (F::Output, Vec<LogEvent>) {
    let capture = LogCapture::new();
    let output = capture.scope_future(future).await;
    (output, capture.events())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capture_follows_spawned_tasks_blocking_work_and_threads() {
        let capture = LogCapture::new();
        tokio::spawn(capture.scope_future(async {
            tokio::task::yield_now().await;
            tracing::warn!(target: "capture_test", "task");
        }))
        .await
        .unwrap();
        let blocking = capture.clone();
        tokio::task::spawn_blocking(move || {
            blocking.in_scope(|| {
                tracing::warn!(target: "capture_test", "blocking");
            })
        })
        .await
        .unwrap();
        let thread = capture.clone();
        std::thread::spawn(move || {
            thread.in_scope(|| {
                tracing::warn!(target: "capture_test", "thread");
            })
        })
        .join()
        .unwrap();
        let messages: Vec<_> = capture
            .events()
            .iter()
            .map(|event| event.message().to_owned())
            .collect();
        assert_eq!(messages, ["task", "blocking", "thread"]);
    }

    #[tokio::test]
    async fn simultaneous_captures_do_not_collect_each_others_events() {
        let first = LogCapture::new();
        let second = LogCapture::new();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let other_barrier = barrier.clone();
        tokio::join!(
            first.scope_future(async {
                barrier.wait().await;
                tracing::warn!("first");
            }),
            second.scope_future(async {
                other_barrier.wait().await;
                tracing::warn!("second");
            }),
        );
        tracing::warn!("outside either capture");
        assert_eq!(first.events().len(), 1);
        assert_eq!(first.events()[0].message(), "first");
        assert_eq!(second.events().len(), 1);
        assert_eq!(second.events()[0].message(), "second");
    }

    #[test]
    fn first_callsite_use_under_a_local_noop_does_not_disable_global_capture() {
        fn emit() {
            tracing::warn!(target: "first_use_probe", "enabled after local noop");
        }
        let capture = LogCapture::new();
        std::thread::spawn(|| {
            tracing::dispatcher::with_default(&tracing::Dispatch::none(), emit);
        })
        .join()
        .unwrap();
        capture.in_scope(emit);
        assert_eq!(capture.events().len(), 1);
        assert_eq!(capture.events()[0].message(), "enabled after local noop");
    }

    #[test]
    fn observer_collects_only_its_operation_from_uninstrumented_threads() {
        let capture = LogCapture::observe(|event| {
            event.target == "observer_probe"
                && event
                    .fields
                    .get("run")
                    .is_some_and(|run| run == "owned-run")
        });
        std::thread::spawn(|| {
            tracing::warn!(target: "observer_probe", run = "other-run", "unrelated");
            tracing::warn!(target: "observer_probe", run = "owned-run", "owned");
        })
        .join()
        .unwrap();
        assert_eq!(capture.events().len(), 1);
        assert_eq!(capture.events()[0].message(), "owned");
    }
}
