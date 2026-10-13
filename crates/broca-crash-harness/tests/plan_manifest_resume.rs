//! A session admitted with a fetch plan, cut at every WAL record boundary on the shared harness.
//!
//! A cut before the first `RunStarted` leaves nothing of the plan on disk: admission starts
//! again from the caller's send, fetching afresh. A cut after it leaves the frozen manifest, and
//! a resume renders from that record: it is driven without any manifest to fetch or render
//! from, keeps the frozen one, writes no second `RunStarted`, and every request it sends is
//! byte-identical to the uninterrupted run's.

mod scenarios;

use broca_core::{FileWalStore, RunControl};
use broca_crash_harness::{
    drive, HarnessPlane, KillPoint, KillTrigger, RunnerHandle, ScriptedProvider,
};
use broca_wal::WalRecord;

broca_crash_harness::crash_harness_child!(scenarios::by_name);

fn manifests(records: &[WalRecord]) -> Vec<Option<broca_wal::PlanManifest>> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::RunStarted { manifest, .. } => Some(manifest.as_deref().cloned()),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_cut_before_run_started_leaves_nothing_and_a_resume_after_it_renders_the_frozen_manifest()
{
    let handle = RunnerHandle::new(scenarios::PLANNED_EDITS).await;
    let reference = handle.reference.log.records();
    assert_eq!(
        manifests(&reference),
        [Some(scenarios::planned_manifest())],
        "the uninterrupted run froze the manifest on its RunStarted"
    );

    // Every whole or torn record-boundary cut, resumed and checked against the harness's resume
    // invariants (no side effect repeated, every request byte-identical to the reference).
    handle.every_record_boundary(true).await;

    // Before the RunStarted, including half of it: nothing is durable.
    for torn in [false, true] {
        let kill = handle
            .kill_at(
                KillPoint::RecordBoundary { records: 0, torn },
                KillTrigger::Truncate,
            )
            .await;
        assert_eq!(kill.durable.record_count(), 0, "{}", kill.label());
        assert!(manifests(&kill.durable.records()).is_empty());
    }

    // After it: a resume handed no manifest sends the uninterrupted run's requests, byte for
    // byte, and keeps the frozen manifest as the episode's only RunStarted.
    let total = handle.reference.log.record_count();
    let mut resumed_with_requests = 0;
    for records in 1..=total {
        let kill = handle
            .kill_at(
                KillPoint::RecordBoundary {
                    records,
                    torn: false,
                },
                KillTrigger::Truncate,
            )
            .await;
        // What the session's actor hands a resume (the episode's message and the manifest's
        // tools), but no manifest: the loop has nothing to fetch from, and the manifest on the
        // episode's durable `RunStarted` is the only one.
        let request = scenarios::planned_request();
        let provider = ScriptedProvider::for_scenario(&handle.scenario);
        let plane = HarnessPlane::for_scenario(&handle.scenario);
        let store = FileWalStore::new(kill.state_root.join("wal"));
        drive(
            request,
            RunControl::default(),
            &kill.state_root,
            &store,
            &provider,
            &plane,
        )
        .await;
        for (step, bytes) in provider.sent() {
            assert_eq!(
                Some(&bytes),
                handle.reference.sent.get(&step),
                "{}: step {step} renders from the frozen record",
                kill.label()
            );
            resumed_with_requests += 1;
        }
        let after = broca_crash_harness::LogImage::read(&store.wal_path(&scenarios::session()));
        assert_eq!(
            manifests(&after.records()),
            [Some(scenarios::planned_manifest())],
            "{}: the resume wrote no second RunStarted and kept the manifest",
            kill.label()
        );
    }
    assert!(
        resumed_with_requests > 0,
        "some cut left a model call for the resume to make"
    );
}

struct PresetPlane {
    inner: HarnessPlane,
    seen: std::sync::Mutex<Vec<Option<String>>>,
}

#[async_trait::async_trait]
impl broca_core::ToolPlane for PresetPlane {
    async fn batch_started(
        &self,
        ctx: &broca_core::BatchCtx,
        calls: &[broca_protocol::ToolCall],
        manifest: &broca_core::ToolManifest,
    ) -> Result<(), broca_core::ToolPlaneError> {
        self.seen
            .lock()
            .unwrap()
            .push(manifest.provider_preset("aft").map(str::to_owned));
        self.inner.batch_started(ctx, calls, manifest).await
    }
    async fn dispatch(
        &self,
        ctx: &broca_core::BatchCtx,
        call: &broca_protocol::ToolCall,
    ) -> broca_core::DispatchResult {
        self.inner.dispatch(ctx, call).await
    }
    async fn batch_finished(
        &self,
        ctx: &broca_core::BatchCtx,
    ) -> Result<(), broca_core::ToolPlaneError> {
        self.inner.batch_finished(ctx).await
    }
}

#[tokio::test]
async fn resumed_dispatch_uses_the_wal_frozen_provider_preset_not_new_control() {
    let handle = RunnerHandle::new(scenarios::PLANNED_EDITS).await;
    let kill = handle
        .kill_at(
            KillPoint::RecordBoundary {
                records: 1,
                torn: false,
            },
            KillTrigger::Truncate,
        )
        .await;
    let store = FileWalStore::new(kill.state_root.join("wal"));
    let provider = ScriptedProvider::for_scenario(&handle.scenario);
    let plane = PresetPlane {
        inner: HarnessPlane::for_scenario(&handle.scenario),
        seen: Default::default(),
    };
    let mut later = scenarios::planned_manifest();
    later.plan = broca_wal::OpaqueJson::from_json_text(
        r#"{"tool_items":[{"provider":"aft","preset":"reader"}]}"#,
    )
    .unwrap();
    let control = RunControl {
        manifest: Some(std::sync::Arc::new(later)),
        ..Default::default()
    };
    let lease = broca_core::FileLeaseStore::new(kill.state_root.join("lease"));
    let (tx, mut rx) = tokio::sync::mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    broca_core::run_loop_with_control(
        scenarios::planned_request(),
        &provider,
        &plane,
        &lease,
        &broca_core::AllowAll,
        &store,
        None,
        control,
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    collector.await.unwrap();
    let seen = plane.seen.lock().unwrap();
    assert!(!seen.is_empty(), "resume dispatched a tool batch");
    assert!(
        seen.iter()
            .all(|preset| preset.as_deref() == Some("worker")),
        "{seen:?}"
    );
}
