//! Task-local barriers for failed-attempt effects; absent from shipping builds.

use std::future::Future;

#[derive(Clone, Copy)]
pub(super) enum Boundary {
    FailedAttempt,
    Switch,
    Jitter,
}

tokio::task_local! {
    static EFFECT: Box<dyn Fn(Boundary) + Send + Sync>;
}

pub(super) fn at(boundary: Boundary) {
    let _ = EFFECT.try_with(|effect| effect(boundary));
}

pub(super) async fn scope<T>(
    effect: impl Fn(Boundary) + Send + Sync + 'static,
    future: impl Future<Output = T>,
) -> T {
    EFFECT.scope(Box::new(effect), future).await
}
