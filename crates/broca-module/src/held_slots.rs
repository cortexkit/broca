//! Where a provider-held call waits for its late result, and how a pulled result reaches it.
//!
//! A held call's tool already ran (or is running inside its provider). Its answer arrives later
//! as a `late_results` entry, and that entry IS the call's result: broca never sends the call to
//! the provider again. The run's own batch stays the only writer of the call's terminal:
//!
//! 1. The puller maps the entry to the call's tool result and deposits it here, under the
//!    session and the call it answers.
//! 2. It asks the session's actor to let the run take it. A run that is still live finds the
//!    deposit in its waiting slot; a paused run is resumed into its original batch, whose held
//!    call waits here again and finds the deposit at once.
//! 3. The batch writes the result as the call's `ToolResult`, at the position a synchronous
//!    reply would have taken, and syncs it. Only then is the deposit answered as delivered, so
//!    only then may the provider's entry be acknowledged.
//!
//! A deposit nobody takes in time is answered as not delivered and stays in place. The entry
//! is not acknowledged, so the next pull hands it over again, and a later resume still finds
//! this deposit.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use broca_core::tool_plane::{
    BatchCtx, DispatchResult, LateResultRegistration, ProviderHeldLifecycle, ProviderHeldReply,
};
use broca_protocol::{BindIdentity, ToolCall, ToolCallId, ToolResult};
use cortexkit_role_tool_provider::late_results::LateEntry;
use tokio::sync::{oneshot, Notify};

use crate::late_results_puller::HeldDelivery;

/// One held call's waiting slot.
#[derive(Default)]
struct Slot {
    /// The key the held record names. A deposit answers only the call that carried this key.
    call_key: Option<String>,
    /// The late result, once deposited. Kept until the batch has synced it, so a batch that
    /// paused before taking it finds it again when it resumes.
    result: Option<ToolResult>,
    /// Wakes a waiting batch when a result is deposited.
    arrived: Arc<Notify>,
    /// Answered once the batch has synced the terminal.
    delivered: Vec<oneshot::Sender<()>>,
}

/// Every held call's waiting slot in this process, keyed by session and call.
///
/// Within a session only one batch is open at a time, and a batch refuses a repeated
/// `tool_call_id`, so the pair names one held call.
#[derive(Default)]
pub struct HeldSlots {
    slots: Mutex<HashMap<(BindIdentity, ToolCallId), Slot>>,
    /// Told the provider module each time a batch starts waiting on a held call, so that
    /// module's puller runs while the call waits.
    on_hold: std::sync::OnceLock<OnHold>,
}

type OnHold = Box<dyn Fn(&str) + Send + Sync>;

impl HeldSlots {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Install what to do when a batch starts waiting on a call held by a module: start that
    /// module's puller. Installed once; a later call is ignored.
    pub fn on_hold(&self, hook: impl Fn(&str) + Send + Sync + 'static) {
        let _ = self.on_hold.set(Box::new(hook));
    }

    /// Hand `result` to the held call `tool_call_id` of `session`, which carried `call_key`.
    /// The receiver resolves once the run's batch has synced it as the call's terminal.
    pub fn deposit(
        &self,
        session: &BindIdentity,
        tool_call_id: &ToolCallId,
        call_key: &str,
        result: ToolResult,
    ) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        let mut slots = self.slots.lock().expect("held slots");
        let slot = slots
            .entry((session.clone(), tool_call_id.clone()))
            .or_default();
        slot.call_key = Some(call_key.to_owned());
        slot.result = Some(result);
        slot.delivered.push(tx);
        slot.arrived.notify_waiters();
        rx
    }

    /// How many slots this process holds, for tests that check nothing is left behind.
    pub fn len(&self) -> usize {
        self.slots.lock().expect("held slots").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The deposited result for this call, if it answers the key the held record names.
    fn take_ready(&self, key: &(BindIdentity, ToolCallId), call_key: &str) -> Option<ToolResult> {
        let slots = self.slots.lock().expect("held slots");
        let slot = slots.get(key)?;
        if slot.call_key.as_deref() != Some(call_key) {
            return None;
        }
        slot.result.clone()
    }

    fn arrived(&self, key: &(BindIdentity, ToolCallId)) -> Arc<Notify> {
        let mut slots = self.slots.lock().expect("held slots");
        Arc::clone(&slots.entry(key.clone()).or_default().arrived)
    }

    /// Forget a slot that has neither a deposit nor anyone waiting on one: the batch stopped
    /// waiting (it paused, or the run ended) before anything arrived.
    fn release_if_idle(&self, key: &(BindIdentity, ToolCallId)) {
        let mut slots = self.slots.lock().expect("held slots");
        if slots
            .get(key)
            .is_some_and(|slot| slot.result.is_none() && slot.delivered.is_empty())
        {
            slots.remove(key);
        }
    }
}

/// Removes an empty slot when a batch stops waiting on it.
struct WaitGuard<'a> {
    slots: &'a HeldSlots,
    key: (BindIdentity, ToolCallId),
}

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        self.slots.release_if_idle(&self.key);
    }
}

#[async_trait]
impl ProviderHeldLifecycle for HeldSlots {
    async fn wait(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        reply: &ProviderHeldReply,
    ) -> DispatchResult {
        if let Some(hook) = self.on_hold.get() {
            hook(&reply.module);
        }
        let key = (ctx.bind.clone(), call.tool_call_id.clone());
        let _guard = WaitGuard {
            slots: self,
            key: key.clone(),
        };
        loop {
            let arrived = self.arrived(&key);
            let notified = arrived.notified();
            tokio::pin!(notified);
            // Register for the wake-up before looking, so a deposit between the look and the
            // wait is not missed.
            notified.as_mut().enable();
            if let Some(result) = self.take_ready(&key, &reply.call_key) {
                return DispatchResult::Completed(result);
            }
            notified.await;
        }
    }

    async fn terminal_synced(&self, ctx: &BatchCtx, tool_call_id: &ToolCallId) {
        let slot = self
            .slots
            .lock()
            .expect("held slots")
            .remove(&(ctx.bind.clone(), tool_call_id.clone()));
        if let Some(slot) = slot {
            for delivered in slot.delivered {
                let _ = delivered.send(());
            }
        }
    }
}

/// Maps a late entry to the result it settles its held call with; `None` for an entry that is
/// not a settlement. Supplied by the daemon, which reads result bodies with the tool plane's
/// own `content/v1` mapper.
pub type EntryMapper = dyn Fn(&ToolCallId, &LateEntry) -> Option<ToolResult> + Send + Sync;

/// Asks a session to let its run take a deposited result: resume it if it is paused on a held
/// call, and do nothing if a run is live (its batch is waiting in the slot already).
#[async_trait]
pub trait HeldRunResumer: Send + Sync {
    async fn resume_for_late_result(&self, session: &BindIdentity) -> Result<(), String>;
}

#[async_trait]
impl HeldRunResumer for crate::registry::LineageRegistry {
    async fn resume_for_late_result(&self, session: &BindIdentity) -> Result<(), String> {
        self.resume_held_run(session).await
    }
}

/// The daemon's delivery: deposit the mapped result, have the session's actor let the run take
/// it, and answer only once the run's batch has synced the terminal.
pub struct SlotDelivery {
    slots: Arc<HeldSlots>,
    resumer: Arc<dyn HeldRunResumer>,
    mapper: Arc<EntryMapper>,
    timeout: Duration,
}

impl SlotDelivery {
    /// How long a delivery waits for the batch's sync before it is retried on a later pull.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

    pub fn new(
        slots: Arc<HeldSlots>,
        resumer: Arc<dyn HeldRunResumer>,
        mapper: Arc<EntryMapper>,
    ) -> Self {
        Self {
            slots,
            resumer,
            mapper,
            timeout: Self::DEFAULT_TIMEOUT,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[async_trait]
impl HeldDelivery for SlotDelivery {
    async fn deliver(
        &self,
        registration: &LateResultRegistration,
        tool_call_id: &ToolCallId,
        entry: &LateEntry,
    ) -> Result<(), String> {
        let Some(result) = (self.mapper)(tool_call_id, entry) else {
            return Err(format!("a {} entry settles no call", entry.kind));
        };
        let delivered = self.slots.deposit(
            &registration.bind,
            tool_call_id,
            &registration.call_key,
            result,
        );
        self.resumer
            .resume_for_late_result(&registration.bind)
            .await?;
        match tokio::time::timeout(self.timeout, delivered).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err("the held call's slot closed before its terminal was synced".into()),
            Err(_) => Err("the run did not sync the held call's terminal in time".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::{BatchId, RunId, ToolOutput};

    fn ctx() -> BatchCtx {
        BatchCtx {
            batch_id: BatchId("r-b1".into()),
            bind: BindIdentity::new("/p", "h", "s"),
            run_id: RunId("r".into()),
            scope: None,
        }
    }

    fn call() -> ToolCall {
        ToolCall {
            tool_call_id: ToolCallId("tc_1".into()),
            tool_name: "ask".into(),
            input: serde_json::json!({}),
            thought_signature: None,
        }
    }

    fn reply(call_key: &str) -> ProviderHeldReply {
        ProviderHeldReply {
            tool_call_id: ToolCallId("tc_1".into()),
            call_key: call_key.into(),
            module: "core".into(),
            reason: "parent_answer".into(),
            detail_digest: None,
        }
    }

    fn answer(text: &str) -> ToolResult {
        ToolResult {
            tool_call_id: ToolCallId("tc_1".into()),
            output: ToolOutput::Text { text: text.into() },
            is_error: false,
        }
    }

    /// A deposit made while the batch waits wakes it with exactly that result, and the deposit
    /// counts as delivered only once the batch reports the terminal synced.
    #[tokio::test]
    async fn a_waiting_batch_takes_the_deposit_and_delivery_waits_for_its_sync() {
        let slots = HeldSlots::new();
        let waiting = {
            let slots = Arc::clone(&slots);
            tokio::spawn(async move { slots.wait(&ctx(), &call(), &reply("lin:4")).await })
        };
        tokio::task::yield_now().await;
        let mut delivered =
            slots.deposit(&ctx().bind, &call().tool_call_id, "lin:4", answer("late"));
        let DispatchResult::Completed(result) = waiting.await.unwrap() else {
            panic!("the deposit settles the call")
        };
        assert_eq!(result, answer("late"));
        assert!(
            delivered.try_recv().is_err(),
            "a deposit is not delivered before the batch syncs its terminal"
        );
        slots.terminal_synced(&ctx(), &call().tool_call_id).await;
        delivered.await.expect("delivered once synced");
        assert!(slots.is_empty());
    }

    /// A batch that paused before taking the deposit finds it when the run resumes, and a slot
    /// whose waiter left with nothing deposited is not kept.
    #[tokio::test]
    async fn a_deposit_outlives_a_paused_batch_and_an_empty_slot_does_not() {
        let slots = HeldSlots::new();
        let paused = tokio::time::timeout(
            Duration::from_millis(20),
            slots.wait(&ctx(), &call(), &reply("lin:4")),
        )
        .await;
        assert!(
            paused.is_err(),
            "nothing deposited, so the batch keeps waiting"
        );
        assert!(slots.is_empty(), "an abandoned empty slot is released");

        let _delivered = slots.deposit(&ctx().bind, &call().tool_call_id, "lin:4", answer("late"));
        let DispatchResult::Completed(result) = slots.wait(&ctx(), &call(), &reply("lin:4")).await
        else {
            panic!("the resumed batch finds the deposit")
        };
        assert_eq!(result, answer("late"));
    }

    /// A deposit made under another key never answers this held call.
    #[tokio::test]
    async fn a_deposit_for_another_key_is_not_taken() {
        let slots = HeldSlots::new();
        let _delivered = slots.deposit(&ctx().bind, &call().tool_call_id, "lin:9", answer("other"));
        let waited = tokio::time::timeout(
            Duration::from_millis(20),
            slots.wait(&ctx(), &call(), &reply("lin:4")),
        )
        .await;
        assert!(waited.is_err());
    }
}
