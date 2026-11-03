//! Controls scoped to the async driver future by `capture`. No hook reaches a shipping build.

use super::*;
use std::{cell::RefCell, future::Future};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Event {
    Send {
        ordinal: u32,
        index: u32,
        websocket: bool,
    },
    Wait(Duration),
    EscapeClient,
    SwitchToHttps,
    Attempt {
        ordinal: u32,
        outcome: AttemptOutcome,
    },
    Refresh {
        rejected: bool,
        credential: bool,
    },
    Decision {
        action: String,
        reason: String,
    },
    End {
        retries_used: u32,
        quiet: bool,
        failed: bool,
    },
}

struct Hooks {
    now: Instant,
    first_attempt: u32,
    current_attempt: usize,
    jitter: Vec<u8>,
    delay_delta: Duration,
    events: Vec<Event>,
}

tokio::task_local! {
    static HOOKS: RefCell<Hooks>;
}

pub(super) fn is_active() -> bool {
    HOOKS.try_with(|_| ()).is_ok()
}

pub(super) fn record(event: Event) {
    let _ = HOOKS.try_with(|hooks| hooks.borrow_mut().events.push(event));
}

pub(super) fn send(ordinal: u32, index: u32, websocket: bool) {
    let _ = HOOKS.try_with(|hooks| {
        let mut hooks = hooks.borrow_mut();
        hooks.current_attempt = (ordinal - hooks.first_attempt) as usize;
        hooks.events.push(Event::Send {
            ordinal,
            index,
            websocket,
        });
    });
}

pub(super) fn jitter(codex: bool) -> Option<f64> {
    HOOKS
        .try_with(|hooks| {
            let hooks = hooks.borrow();
            let raw = f64::from(hooks.jitter[hooks.current_attempt % hooks.jitter.len()]);
            if codex {
                0.9 + raw / 1000.0
            } else {
                0.8 + raw / 500.0
            }
        })
        .ok()
}

pub(super) fn deadline_now() -> Instant {
    virtual_now().unwrap_or_else(Instant::now)
}

pub(super) fn virtual_now() -> Option<Instant> {
    HOOKS.try_with(|hooks| hooks.borrow().now).ok()
}

pub(super) fn advance(elapsed: Duration) {
    let _ = HOOKS.try_with(|hooks| hooks.borrow_mut().now += elapsed);
}

pub(super) fn delay(requested: Duration) -> Duration {
    HOOKS
        .try_with(|hooks| requested + hooks.borrow().delay_delta)
        .unwrap_or(requested)
}

pub(super) async fn capture(
    now: Instant,
    first_attempt: u32,
    jitter: Vec<u8>,
    delay_delta: Duration,
    future: impl Future<Output = Result<(Vec<NormalizedEvent>, u32), (ProviderError, u32)>>,
) -> Vec<Event> {
    HOOKS
        .scope(
            RefCell::new(Hooks {
                now,
                first_attempt,
                current_attempt: 0,
                jitter,
                delay_delta,
                events: Vec::new(),
            }),
            async {
                let end = match future.await {
                    Ok((completion, retries_used)) => Event::End {
                        retries_used,
                        quiet: completion.is_empty(),
                        failed: false,
                    },
                    Err((_, retries_used)) => Event::End {
                        retries_used,
                        quiet: false,
                        failed: true,
                    },
                };
                record(end);
                HOOKS.with(|hooks| std::mem::take(&mut hooks.borrow_mut().events))
            },
        )
        .await
}
