use super::*;
use broca_test_support::{wait_for, wait_until};
use std::sync::Mutex;
use tokio::sync::Notify;

struct MemoryWal(Arc<Mutex<Vec<WalRecord>>>);

impl broca_wal::Wal for MemoryWal {
    fn append(&mut self, _: u64, record: &WalRecord, _: bool) -> Result<u64, broca_wal::WalError> {
        let mut records = self.0.lock().unwrap();
        records.push(record.clone());
        Ok(records.len() as u64)
    }

    fn sync(&self) -> Result<(), broca_wal::WalError> {
        Ok(())
    }

    fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        Ok(Vec::new())
    }

    fn tail_after_seq(&self, _: u64) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        Ok(Vec::new())
    }

    fn last_seq(&self) -> u64 {
        self.0.lock().unwrap().len() as u64
    }
}

struct GatedPlane {
    fast_done: Notify,
    slow_entered: Notify,
    release_slow: Notify,
    unknown_fast: bool,
}

#[async_trait::async_trait]
impl ToolPlane for GatedPlane {
    async fn batch_started(
        &self,
        _: &BatchCtx,
        _: &[ToolCall],
        _: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        Ok(())
    }

    async fn batch_finished(&self, _: &BatchCtx) -> Result<(), ToolPlaneError> {
        Ok(())
    }

    async fn dispatch(&self, _: &BatchCtx, call: &ToolCall) -> DispatchResult {
        if call.tool_call_id.0 == "slow" {
            self.slow_entered.notify_one();
            self.release_slow.notified().await;
        } else {
            self.fast_done.notify_one();
            if self.unknown_fast {
                return DispatchResult::SentButOutcomeUnknown {
                    reason: "reply lost".into(),
                };
            }
        }
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::Text {
                text: call.tool_call_id.0.clone(),
            },
            is_error: false,
        })
    }
}

async fn gated_batch(mode: ExecutionMode, unknown_fast: bool) -> (Vec<WalRecord>, u64) {
    let records = Arc::new(Mutex::new(Vec::new()));
    let writer = WalHandle::spawn(Box::new(MemoryWal(records.clone())), 1).unwrap();
    let plane = GatedPlane {
        fast_done: Notify::new(),
        slow_entered: Notify::new(),
        release_slow: Notify::new(),
        unknown_fast,
    };
    let ids = if mode == ExecutionMode::Sequential {
        ["fast", "slow"]
    } else {
        // Complete out of source order so a timestamp cannot accidentally follow its index.
        ["slow", "fast"]
    };
    let calls = ids
        .iter()
        .map(|id| ToolCall {
            tool_call_id: ToolCallId((*id).into()),
            tool_name: "work".into(),
            input: serde_json::json!({}),
            thought_signature: None,
        })
        .collect();
    let manifest = ToolManifest::from_tools(vec![Tool {
        name: "work".into(),
        description: "Work".into(),
        input_schema: serde_json::json!({"type":"object"}),
        module: None,
        execution_mode: mode,
        reply: None,
    }]);
    let bind = BindIdentity::new("/answer-times", "runner", "gated");
    let run_id = RunId("answer-times".into());
    let (out, _rx) = mpsc::channel(64);
    let batch = dispatch_batch(
        &run_id,
        StepId(1),
        &bind,
        None,
        calls,
        &manifest,
        &plane,
        &crate::AllowAll,
        &writer,
        None,
        &[],
        false,
        None,
        None,
        None,
        &out,
        None,
    );
    let release = async {
        plane.fast_done.notified().await;
        plane.slow_entered.notified().await;
        let fast_resolved_by = epoch_millis().unwrap();
        // Wait for a distinct clock tick, not for either tool to make progress. Only the
        // explicit release below lets the slow call answer.
        let released_at = wait_until("a clock tick after the fast answer", || async {
            let now = epoch_millis().unwrap();
            if now > fast_resolved_by {
                Ok(now)
            } else {
                Err(format!("clock still at {now}"))
            }
        })
        .await;
        plane.release_slow.notify_one();
        released_at
    };
    let (batch, released_at) = wait_for(
        "both gated tool calls",
        async { tokio::join!(batch, release) },
        || broca_test_support::record_kinds(records.lock().unwrap().iter()),
    )
    .await;
    let batch = batch.unwrap();
    assert_eq!(
        batch.interruption,
        unknown_fast.then_some(BatchInterruption::Ambiguous)
    );
    assert_eq!(
        batch
            .results
            .iter()
            .map(|result| result.tool_call_id.0.as_str())
            .collect::<Vec<_>>(),
        ids
    );
    writer.shutdown().await.unwrap();
    let records = records.lock().unwrap().clone();
    (records, released_at)
}

fn finished(records: &[WalRecord], id: &str) -> u64 {
    records
        .iter()
        .find_map(|record| match record {
            WalRecord::ToolResult {
                result,
                finished_at_ms,
                ..
            } if result.tool_call_id.0 == id => *finished_at_ms,
            _ => None,
        })
        .expect("a durable result has a finish time")
}

#[tokio::test]
async fn parallel_answers_keep_their_own_resolution_times() {
    let (records, slow_released_at) = gated_batch(ExecutionMode::Parallel, false).await;
    assert!(
        finished(&records, "fast") < slow_released_at,
        "the fast answer must be timed before the slow call is released"
    );
    assert!(finished(&records, "slow") >= slow_released_at);
}

#[tokio::test]
async fn sequential_answers_keep_the_enqueue_time() {
    let (records, slow_released_at) = gated_batch(ExecutionMode::Sequential, false).await;
    assert!(finished(&records, "fast") >= slow_released_at);
    assert!(finished(&records, "slow") >= slow_released_at);
}

#[tokio::test]
async fn unknown_close_out_keeps_the_enqueue_time() {
    let (records, slow_released_at) = gated_batch(ExecutionMode::Parallel, true).await;
    assert!(finished(&records, "fast") >= slow_released_at);
    assert!(records.iter().any(|record| matches!(record,
        WalRecord::ToolResult { result, outcome: DispatchOutcome::Unknown, .. }
            if result.tool_call_id.0 == "fast")));
}

#[tokio::test]
async fn interrupted_and_cancelled_close_outs_keep_the_enqueue_time() {
    let id = ToolCallId("unsent".into());
    let close_outs = [interrupted_resolution(&id), stopped_resolution(&id)];
    let resolved_by = epoch_millis().unwrap();
    let enqueue_after = wait_until("a clock tick after the synthetic close-outs", || async {
        let now = epoch_millis().unwrap();
        (now > resolved_by)
            .then_some(now)
            .ok_or_else(|| format!("clock still at {now}"))
    })
    .await;
    let records = Arc::new(Mutex::new(Vec::new()));
    let writer = WalHandle::spawn(Box::new(MemoryWal(records.clone())), 1).unwrap();
    for close_out in close_outs {
        assert_eq!(close_out.finished_at_ms, None);
        assert!(!close_out.plane_subject);
        assert_eq!(close_out.outcome, Some(DispatchOutcome::Interrupted));
        writer.record(WalRecord::ToolResult {
            finished_at_ms: close_out.finished_at_ms,
            batch_id: BatchId("close-out".into()),
            result: close_out.result,
            outcome: close_out.outcome.unwrap(),
            tags: None,
            reason: None,
            hooks: None,
            withdraw_elicitation: None,
            provider_held: None,
        });
    }
    writer.shutdown().await.unwrap();
    let records = records.lock().unwrap();
    assert_eq!(records.len(), 2);
    assert!(records.iter().all(|record| matches!(record,
        WalRecord::ToolResult { finished_at_ms: Some(at), .. } if *at >= enqueue_after)));
}
