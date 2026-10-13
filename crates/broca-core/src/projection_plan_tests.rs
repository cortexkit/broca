use crate::projection_plan::{
    plan_projection, ProjectionPlan, ProjectionPlanError, ProjectionSnapshot,
};
use crate::{Cursor, ProjectionSeed};
use broca_protocol::StepId;
use broca_wal::{FramedRecord, WalLineageId, WalRangeRead, WalRecord};

fn stored_snapshot() -> ProjectionSnapshot {
    ProjectionSnapshot {
        attribution_complete: true,
        seed: Some(ProjectionSeed::empty()),
        watermark: 19,
        lineage_id: Some("old-lineage".into()),
        next_ord: 7,
        previous_control_head: Some(Cursor {
            wal_seq: 18,
            sub_index: 2,
        }),
    }
}

#[test]
fn changed_lineage_refuses_a_nonzero_replay_start() {
    let records = vec![FramedRecord {
        seq: 2,
        fence: 1,
        record: WalRecord::StepStarted { step_id: StepId(1) },
    }];
    let mut range = WalRangeRead::without_anchors(records);
    range.lineage_id = Some(incoming_lineage());
    assert!(matches!(
        plan_projection(
            stored_snapshot(),
            match range.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            },
            Some(&range)
        ),
        Err(ProjectionPlanError::InconsistentSnapshot(
            "WAL lineage changed and projection requires a sequence-zero replay"
        ))
    ));
}

#[test]
fn changed_lineage_without_boundary_resets_literal_expected_state() {
    let records = vec![FramedRecord {
        seq: 1,
        fence: 1,
        record: WalRecord::StepStarted { step_id: StepId(1) },
    }];
    let mut range = WalRangeRead::without_anchors(records);
    range.lineage_id = Some(incoming_lineage());
    assert_eq!(
        plan_projection(
            stored_snapshot(),
            match range.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            },
            Some(&range)
        )
        .unwrap(),
        ProjectionPlan::Reset {
            expected_watermark: 19,
            expected_lineage_id: Some("old-lineage".into()),
        }
    );
}

pub(crate) fn incoming_lineage() -> WalLineageId {
    use broca_wal::{FileWal, Wal};
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = broca_test_support::scratch_root().join(format!(
        "broca-planner-lineage-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let mut wal = FileWal::open(&path).unwrap();
    wal.append(1, &WalRecord::StepStarted { step_id: StepId(1) }, true)
        .unwrap();
    let lineage = broca_wal::read_records_range(&path, 4096, 0, None, None)
        .unwrap()
        .lineage_id
        .unwrap();
    drop(wal);
    std::fs::remove_file(path).unwrap();
    lineage
}
