use std::collections::BTreeMap;
use std::future::{pending, Future};

use futures::stream::{self, StreamExt};
use tokio::time::Instant;

use crate::account_routing::{ListingCache, LISTING_MAX_AGE};

use super::*;

impl KeyLoader {
    /// Shares the scoped listing with account routing and also refreshes it on idle machines,
    /// where no admissions would trigger discovery of a new grant.
    pub async fn run_following<F, Fut>(
        self,
        registry: broca_module::LineageRegistry,
        listing: Arc<ListingCache>,
        on_keys_loaded: F,
    ) where
        F: Fn() -> Fut,
        Fut: Future<Output = ()>,
    {
        let Some(source) = self.source.clone() else {
            self.run(registry).await;
            return;
        };
        let stopping = registry.stopping_signal();
        let refresh = async {
            let mut delay = self.backoff.first;
            loop {
                let mut next = delay;
                match listing.get() {
                    Some((_, false)) => {
                        delay = self.backoff.first;
                        next = LISTING_MAX_AGE;
                    }
                    _ if listing.begin_refresh() => {
                        let listed = tokio::select! {
                            biased;
                            _ = stopping.cancelled() => {
                                listing.end_refresh();
                                return;
                            }
                            listed = source.list() => listed,
                        };
                        if let Ok(listed) = listed {
                            listing.store(listed);
                            delay = self.backoff.first;
                            next = LISTING_MAX_AGE;
                        } else {
                            // A malformed reply can contain credential data. Never echo it.
                            delay = (delay * 2).min(self.backoff.max);
                        }
                        listing.end_refresh();
                    }
                    _ => {}
                }
                tokio::select! {
                    biased;
                    _ = stopping.cancelled() => return,
                    _ = tokio::time::sleep(next) => {}
                }
            }
        };
        tokio::join!(
            refresh,
            self.follow_keys(registry, &listing, on_keys_loaded)
        );
    }

    async fn follow_keys<F, Fut>(
        &self,
        registry: broca_module::LineageRegistry,
        listing: &ListingCache,
        on_keys_loaded: F,
    ) where
        F: Fn() -> Fut,
        Fut: Future<Output = ()>,
    {
        let source = self.source.as_deref().expect("following requires a vault");
        let stopping = registry.stopping_signal();
        let ready = self.slot.open_without_key(&self.setup).await.is_ok();
        // Key reads and the first recovery sweep can run together. Later sweeps wait for the
        // preceding pass to finish so no two passes recover the same session at once.
        let mut first = Some(tokio::spawn({
            let registry = registry.clone();
            async move { recover_sessions(&registry, None).await }
        }));
        let mut left = Left::Everything;
        let mut changed = listing.watch();
        let mut retry = BTreeMap::<u16, (Instant, Duration)>::new();
        'load: while !stopping.is_cancelled() {
            let current = listing.get();
            let granted = current
                .as_ref()
                .map_or_else(BTreeSet::new, |(rows, _)| granted_keys(rows));
            let runtime = self.slot.runtime();
            if let Some(runtime) = &runtime {
                runtime.deployment().set_granted(granted.clone());
            }
            // Keep the direct scoped read for the configured key so it can load even when
            // listing is unavailable. The vault still enforces its exact read grant.
            let mut wanted = granted;
            wanted.insert(self.slot.key_id());
            wanted.retain(|id| {
                !runtime
                    .as_ref()
                    .is_some_and(|r| r.deployment().has_key(*id))
            });
            retry.retain(|id, _| wanted.contains(id));
            let now = Instant::now();
            let due: Vec<_> = wanted
                .into_iter()
                .filter(|id| retry.get(id).is_none_or(|(at, _)| *at <= now))
                .collect();
            let mut reads = std::pin::pin!(stream::iter(due)
                .map(|id| async move { (id, read_key(source, id).await) })
                .buffer_unordered(4));
            let mut loaded = false;
            while let Some((id, result)) = tokio::select! {
                biased;
                _ = stopping.cancelled() => break 'load,
                read = reads.next() => read,
            } {
                let result = match result {
                    Ok(key) => self
                        .slot
                        .load_read_key(&self.setup, key)
                        .await
                        .map_err(|_| KeyUnavailable::Open),
                    Err(error) => Err(error),
                };
                match result {
                    Ok(()) => {
                        loaded = true;
                        retry.remove(&id);
                        tracing::info!(target: "session_storage", key_id = id,
                            "the session store's deployment key {id} has loaded");
                    }
                    Err(reason) => {
                        let wait = match retry.get(&id) {
                            Some((_, wait)) => (*wait * 2).min(self.backoff.max),
                            None => {
                                warn_missing(id, reason);
                                self.backoff.first
                            }
                        };
                        retry.insert(id, (Instant::now() + wait, wait));
                    }
                }
            }
            if loaded {
                if let Some(pass) = first.take() {
                    let result = pass.await.unwrap_or(Left::Everything);
                    if ready {
                        left = result;
                    }
                }
                if stopping.is_cancelled() {
                    break;
                }
                left = match left {
                    Left::Nothing => Left::Nothing,
                    Left::Sessions(left) => recover_sessions(&registry, Some(&left)).await,
                    Left::Everything => recover_sessions(&registry, None).await,
                };
                self.reopen_refused(&registry).await;
                on_keys_loaded().await;
            }
            let next = retry.values().map(|(at, _)| *at).min();
            tokio::select! {
                biased;
                _ = stopping.cancelled() => break,
                _ = changed.changed() => {},
                _ = async { if let Some(next) = next { tokio::time::sleep_until(next).await; } else { pending::<()>().await; } } => {},
            }
        }
        if let Some(pass) = first {
            let _ = pass.await;
        }
    }

    async fn reopen_refused(&self, registry: &broca_module::LineageRegistry) {
        let refused = self.slot.take_refused();
        if refused.is_empty() {
            return;
        }
        let (opened, failed) = registry.open_registered_sessions(refused, 4).await;
        tracing::info!(target: "session_storage", opened, failed = failed.len(),
            "retried sessions refused before their deployment key loaded");
        for (session, error) in failed {
            if broca_module::store_open::is_key_unavailable(&error) {
                self.slot.remember_refused(&session);
            } else {
                tracing::warn!(target: "session_storage", session = %session.session,
                    "a session refused before its deployment key loaded could not be opened: {error}");
            }
        }
    }
}
