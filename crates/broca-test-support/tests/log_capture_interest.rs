//! A separate test executable keeps dispatcher registration history deterministic.
//! Set `BROCA_REPLAY_THREAD_LOCAL_CAPTURE=1` to reproduce the missing-event failure
//! with the per-future thread-local subscriber instead of the shared helper.
//! Also set `BROCA_REPLAY_FIRST_USE=1` to replay a callsite's first use on an
//! uncaptured thread, without registering a competing subscriber at all.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc, Mutex,
};
use tracing::subscriber::Interest;

struct Uninterested {
    armed: Arc<AtomicBool>,
    reached: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl tracing::Subscriber for Uninterested {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        false
    }
    fn register_callsite(&self, metadata: &'static tracing::Metadata<'static>) -> Interest {
        if metadata.target() == "credentials" && self.armed.swap(false, Ordering::SeqCst) {
            self.reached.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
        }
        Interest::never()
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

fn credential_probe() {
    tracing::warn!(target: "credentials", "credential capture cache probe");
}

// This per-future subscriber holds a thread-local default across an await, as the
// credential tests used to do. Keep it only to reproduce the missing-event failure;
// the replay uses the same one-event assertion as the shared helper.
async fn legacy_capture<F: std::future::Future>(future: F) -> Vec<String> {
    struct Capture(Arc<Mutex<Vec<String>>>);
    impl tracing::Subscriber for Capture {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            if event.metadata().target() == "credentials" {
                struct Message(String);
                impl tracing::field::Visit for Message {
                    fn record_debug(
                        &mut self,
                        field: &tracing::field::Field,
                        value: &dyn std::fmt::Debug,
                    ) {
                        if field.name() == "message" {
                            self.0 = format!("{value:?}").trim_matches('"').to_owned();
                        }
                    }
                }
                let mut message = Message(String::new());
                event.record(&mut message);
                self.0.lock().unwrap().push(message.0);
            }
        }
    }
    let logs = Arc::new(Mutex::new(Vec::new()));
    let dispatch = tracing::Dispatch::new(Capture(logs.clone()));
    let _guard = tracing::dispatcher::set_default(&dispatch);
    future.await;
    let logs = logs.lock().unwrap().clone();
    logs
}

async fn capture_probe<F: std::future::Future>(future: F, legacy: bool) -> Vec<String> {
    if legacy {
        legacy_capture(future).await
    } else {
        broca_test_support::capture_logs(future)
            .await
            .1
            .into_iter()
            .filter(|event| event.target == "credentials")
            .map(|event| event.message().to_owned())
            .collect()
    }
}

#[tokio::test]
async fn credential_capture_survives_an_uninterested_cache_rebuild() {
    let legacy = std::env::var_os("BROCA_REPLAY_THREAD_LOCAL_CAPTURE").is_some();
    // Install the permanent subscriber before the competing dispatcher exists.
    // The legacy fixture deliberately has no process-lifetime dispatcher.
    let _installed = (!legacy).then(broca_test_support::LogCapture::new);
    if std::env::var_os("BROCA_REPLAY_FIRST_USE").is_some() {
        let logs = capture_probe(
            async {
                // A concurrent test can refresh without capturing logs. On the
                // first use of this warn! invocation, tracing's single-dispatcher
                // fast path asks the uncaptured thread's absent subscriber, not
                // the live capture on the other thread. It globally caches `never`,
                // so the capturing thread's later warning is skipped too.
                std::thread::spawn(credential_probe).join().unwrap();
                credential_probe();
            },
            legacy,
        )
        .await;
        assert_eq!(
            logs.len(),
            1,
            "only the capturing thread's event belongs here"
        );
        assert_eq!(logs[0], "credential capture cache probe");
        return;
    }
    let armed = Arc::new(AtomicBool::new(false));
    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let uninterested = tracing::Dispatch::new(Uninterested {
        armed: armed.clone(),
        reached: reached_tx,
        release: Mutex::new(release_rx),
    });
    tracing::dispatcher::with_default(&uninterested, credential_probe);
    armed.store(true, Ordering::SeqCst);
    let worker = std::thread::spawn(move || {
        tracing::dispatcher::with_default(&uninterested, tracing::callsite::rebuild_interest_cache);
    });
    reached_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .unwrap();
    // With only the per-future subscriber, tracing-core's single-dispatcher
    // fast path (JustOne) rebuilds without a registry lock. The worker's deferred
    // `never` interest overwrites the newly installed capture's interest, causing
    // the macro to skip the probe even though it runs on the capturing thread.
    let exercise = async {
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        credential_probe();
    };
    let logs = capture_probe(exercise, legacy).await;
    assert_eq!(
        logs.len(),
        1,
        "the event is emitted on the capturing thread"
    );
    assert_eq!(logs[0], "credential capture cache probe");
}
