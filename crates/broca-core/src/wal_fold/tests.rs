use super::*;
use crate::{FileWalStore, WalStore};
use broca_protocol::{RunId, StepId};
use broca_wal::{RunConfig, RunOrigin};
use std::sync::atomic::{AtomicU64, Ordering};

const START: u64 = COLD_MS * 3;

#[test]
fn fold_separates_durable_containers_and_preserves_placement_on_restore() {
    let f = Fixture::new();
    let (ordinary, ordinary_bytes) = f.session("ordinary-tree", Some(1));
    let (durable, _) = f.session("durable-tree", Some(1));
    let path = f.store.wal_path(&durable);
    // Existing directory placement survives changes to the backup_classes setting.
    fs::create_dir_all(f.store.durable_root()).unwrap();
    fs::rename(&path, f.store.durable_wal_path(&durable)).unwrap();
    let durable_bytes = fs::read(f.store.durable_wal_path(&durable)).unwrap();
    let report = f.fold();
    assert_eq!(report.removed, 2);
    for (tree, wanted, forbidden) in [
        ("wal-archive", &ordinary, &durable),
        ("wal-archive-durable", &durable, &ordinary),
    ] {
        let containers: Vec<_> = fs::read_dir(f.root.join(tree))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "ark"))
            .collect();
        assert_eq!(containers.len(), 1);
        assert!(broca_wal::archive::read_newest_member(
            &containers,
            &crate::session_addr(wanted),
            MAX_BYTES
        )
        .unwrap()
        .is_some());
        assert!(broca_wal::archive::read_newest_member(
            &containers,
            &crate::session_addr(forbidden),
            MAX_BYTES
        )
        .unwrap()
        .is_none());
    }
    let reader =
        std::sync::Arc::new(FileWalStore::new(f.root.join("wal")).with_backup_classes(vec![]));
    assert_eq!(
        reader.bytes_for_move(&durable).unwrap().unwrap(),
        durable_bytes
    );
    assert_eq!(
        reader.read_records(&durable).unwrap().len(),
        reader
            .read_address_records(&crate::session_addr(&durable))
            .unwrap()
            .len()
    );
    assert!(reader
        .addresses()
        .unwrap()
        .contains(&crate::session_addr(&durable)));
    reader.start_move_inventory(tokio_util::sync::CancellationToken::new());
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_secs(20),
                reader.wait_move_inventory(),
            )
            .await
            .unwrap();
        });
    let reference =
        cortexkit_role_llm_runner::session_ref("runner", "/fold-test", &durable.session);
    let inventory = reader.move_session_refs(std::iter::once(reference.as_str()));
    assert!(
        inventory.unreadable.is_empty(),
        "{:?}",
        inventory.unreadable
    );
    assert!(inventory.sessions.contains_key(&reference));
    drop(
        reader
            .open_with_class(&durable, None, Some("head"))
            .unwrap(),
    );
    assert_eq!(
        fs::read(reader.durable_wal_path(&durable)).unwrap(),
        durable_bytes
    );
    assert!(!reader.wal_path(&durable).exists());
    drop(
        reader
            .open_with_class(&ordinary, None, Some("head"))
            .unwrap(),
    );
    assert_eq!(
        fs::read(reader.wal_path(&ordinary)).unwrap(),
        ordinary_bytes
    );
    assert!(!reader.durable_wal_path(&ordinary).exists());
    let folded = f.fold_at(START + 1);
    let journal = folded.journal.unwrap();
    assert!(journal.starts_with("wal-archive-durable/"));
    let restored = run(
        &f.root,
        &FoldOptions {
            write: true,
            resume: Some(journal),
            unfold: true,
            ..Default::default()
        },
        START + 2,
        &mut |_| Ok(()),
    )
    .unwrap();
    assert_eq!(restored.restored, 1);
    assert_eq!(
        fs::read(reader.durable_wal_path(&durable)).unwrap(),
        durable_bytes
    );
    assert!(!reader.wal_path(&durable).exists());
}
struct Fixture {
    root: PathBuf,
    store: FileWalStore,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "broca-fold-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(root.join("wal")).unwrap();
        Self {
            store: FileWalStore::new(root.join("wal")),
            root,
        }
    }
    fn session(&self, name: &str, ts: Option<u64>) -> (BindIdentity, Vec<u8>) {
        self.session_with_scope(name, ts, None)
    }
    fn session_with_scope(
        &self,
        name: &str,
        ts: Option<u64>,
        scope: Option<broca_protocol::SessionScope>,
    ) -> (BindIdentity, Vec<u8>) {
        let session = BindIdentity::new("/fold-test", "runner", name);
        let mut wal = self.store.open(&session).unwrap();
        wal.append(
            1,
            &WalRecord::RunStarted {
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                run_id: RunId("run".into()),
                session: session.clone(),
                ts_ms: ts,
                config: Box::new(RunConfig {
                    model: broca_protocol::ModelHandle::new("stub", "m"),
                    tools: vec![].into(),
                    tool_choice: broca_protocol::ToolChoice::Auto,
                    generation: Default::default(),
                    response_format: Default::default(),
                    provider_options: serde_json::Value::Null,
                    cache_tiers: serde_json::Value::Null,
                    context_epoch: 0,
                    renderer_version: 1,
                    frozen_render_config: serde_json::Value::Null,
                    auth_selection: None,
                    tools_were_omitted: false,
                    budget: None,
                    context_limit: None,
                    work_class: None,
                    variant: None,
                    service_tier: None,
                    stop_when: vec![],
                    context_management: None,
                }),
                input: Default::default(),
                origin: RunOrigin::Fresh,
                submission_id: None,
            },
            true,
        )
        .unwrap();
        wal.append(1, &WalRecord::StepStarted { step_id: StepId(1) }, true)
            .unwrap();
        let bytes = fs::read(self.store.wal_path(&session)).unwrap();
        (session, bytes)
    }
    /// The same pass at an explicit wall-clock stamp, so successive passes are distinct.
    fn fold_at(&self, at: u64) -> FoldReport {
        run(
            &self.root,
            &FoldOptions {
                write: true,
                ..Default::default()
            },
            at,
            &mut |_| Ok(()),
        )
        .unwrap()
    }
    fn fold(&self) -> FoldReport {
        run(
            &self.root,
            &FoldOptions {
                write: true,
                ..Default::default()
            },
            START,
            &mut |_| Ok(()),
        )
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn fold_never_enumerates_ephemeral_wals_even_when_cold() {
    let f = Fixture::new();
    let (portable, _) = f.session("portable-control", Some(1));
    let ephemeral = BindIdentity::new("/fold-test", "runner", "retained");
    let path = f.store.ephemeral_wal_path(&ephemeral);
    let mut records = crate::retention::tests::frames(Some(2592000));
    if let WalRecord::RunStarted { ts_ms, session, .. } = &mut records[0].record {
        *ts_ms = Some(1);
        *session = ephemeral.clone();
    }
    crate::wal_writer::retention_tests::write_decoded_fixture(&path, &records);
    let bytes = fs::read(&path).unwrap();
    assert_eq!(
        wal_paths(&f.root, Tree::Ordinary).unwrap(),
        vec![f.store.wal_path(&portable)]
    );
    let report = f.fold();
    assert_eq!(
        report.candidates,
        vec![session_addr(&portable)],
        "portable positive control"
    );
    assert_eq!(fs::read(&path).unwrap(), bytes);
    assert_eq!(f.store.read_records(&ephemeral).unwrap(), records);
    assert!(!f.store.wal_path(&portable).exists());
    assert_eq!(f.store.addresses().unwrap().len(), 2);
}

// The feature remains intentionally disabled pending reader-release placement. These tests feed
// decoded payloads through the fold boundary; the existing newer-writer test covers feature refusal.
fn complete_records(range: &broca_wal::WalRangeRead) -> &[broca_wal::FramedRecord] {
    match range.records() {
        broca_wal::WalRangeRecords::Complete(records) => records,
        _ => panic!("expected complete fixture"),
    }
}

fn append_decoded_approval_records(bytes: &mut Vec<u8>, records: Vec<WalRecord>) {
    let range = broca_wal::file_wal::read_verified_member_range(bytes, 1, 0, None).unwrap();
    let count = complete_records(&range).len();
    for (index, record) in records.iter().enumerate() {
        bytes.extend(
            broca_wal::framing::encode_payload(
                (count + index + 1) as u64,
                1,
                &serde_json::to_vec(record).unwrap(),
            )
            .unwrap(),
        );
    }
}

fn approval_fixture() -> (
    Fixture,
    BindIdentity,
    Vec<u8>,
    broca_wal::WalLineageId,
    String,
) {
    let fixture = Fixture::new();
    let (session, mut bytes) = fixture.session("approvals", Some(1));
    let lineage = broca_wal::file_wal::read_verified_member_range(&bytes, 1, 0, None)
        .unwrap()
        .lineage_id
        .unwrap();
    append_decoded_approval_records(
        &mut bytes,
        vec![crate::replay::deferred_tests::prepared("call", "b")],
    );
    let key = crate::context::call_key(lineage, 3);
    (fixture, session, bytes, lineage, key)
}

#[test]
fn fold_skips_expired_until_a_late_record() {
    use crate::replay::deferred_tests::approval;
    use broca_wal::{DeferredDecision, LateDecision};
    let (f, session, mut bytes, _, key) = approval_fixture();
    append_decoded_approval_records(
        &mut bytes,
        vec![approval("call", &key, DeferredDecision::Expired)],
    );
    assert_eq!(eligible(&bytes, START, COLD_MS), Err("deferred_call_open"));
    fs::write(f.store.wal_path(&session), &bytes).unwrap();
    let report = f.fold();
    assert_eq!(
        report.skipped["deferred_call_open"],
        [session_addr(&session)]
    );
    assert_eq!(fs::read(f.store.wal_path(&session)).unwrap(), bytes);
    for decision in [
        LateDecision::Declined,
        LateDecision::None,
        LateDecision::Approved,
    ] {
        let mut resolved = bytes.clone();
        append_decoded_approval_records(
            &mut resolved,
            vec![WalRecord::LateApprovalRecorded {
                call_key: key.clone(),
                elicitation_id: "question".into(),
                decision,
                approval_digest: None,
                late_execution: Some(broca_wal::LateExecution::NotifyOnly),
            }],
        );
        assert_eq!(
            eligible(&resolved, START, COLD_MS),
            Ok((Some(session.clone()), false))
        );
    }
}

#[test]
fn fold_skips_late_execute_until_every_terminal_outcome() {
    use crate::replay::deferred_tests::{approval, result};
    use broca_wal::{DeferredDecision, LateDecision, LateExecution, LateOutcome};
    let (_, session, mut bytes, _, key) = approval_fixture();
    append_decoded_approval_records(
        &mut bytes,
        vec![
            approval("call", &key, DeferredDecision::Expired),
            WalRecord::LateApprovalRecorded {
                call_key: key.clone(),
                elicitation_id: "question".into(),
                decision: LateDecision::Approved,
                approval_digest: Some("digest".into()),
                late_execution: Some(LateExecution::Execute),
            },
        ],
    );
    assert_eq!(eligible(&bytes, START, COLD_MS), Err("deferred_call_open"));
    append_decoded_approval_records(
        &mut bytes,
        vec![WalRecord::LateDispatchIntent {
            call_key: key.clone(),
            approval_digest: "digest".into(),
            batch_id: broca_protocol::BatchId(format!("late:{key}")),
        }],
    );
    assert_eq!(eligible(&bytes, START, COLD_MS), Err("deferred_call_open"));
    for outcome in [
        LateOutcome::Completed,
        LateOutcome::NotSent,
        LateOutcome::Unknown,
    ] {
        let mut resolved = bytes.clone();
        append_decoded_approval_records(
            &mut resolved,
            vec![WalRecord::LateResult {
                call_key: key.clone(),
                outcome,
                result: result("call", "late terminal"),
            }],
        );
        assert_eq!(
            eligible(&resolved, START, COLD_MS),
            Ok((Some(session.clone()), false))
        );
    }
}

#[test]
fn fold_skips_reader_and_writer_withdrawals_until_acknowledged() {
    use crate::replay::deferred_tests::terminal;
    use broca_wal::{WithdrawElicitation, WithdrawalAnswer};
    let (_, session, prepared, lineage, key) = approval_fixture();
    for withdrawal in [
        None,
        Some(WithdrawElicitation {
            call_key: key.clone(),
            elicitation_id: None,
        }),
        Some(WithdrawElicitation {
            call_key: key.clone(),
            elicitation_id: Some("question".into()),
        }),
    ] {
        let mut bytes = prepared.clone();
        append_decoded_approval_records(&mut bytes, vec![terminal("call", withdrawal.clone())]);
        let range = broca_wal::file_wal::read_verified_member_range(&bytes, 1, 0, None).unwrap();
        let debt = crate::replay::deferred_call_obligations(lineage, complete_records(&range));
        assert_eq!(debt.withdrawals.len(), 1);
        assert_eq!(debt.withdrawals[0].withdrawal.call_key, key);
        assert_eq!(
            debt.withdrawals[0].withdrawal.elicitation_id,
            withdrawal.and_then(|w| w.elicitation_id)
        );
        assert_eq!(eligible(&bytes, START, COLD_MS), Err("deferred_call_open"));
        for answer in [WithdrawalAnswer::Acknowledged, WithdrawalAnswer::NotSent] {
            let mut acknowledged = bytes.clone();
            append_decoded_approval_records(
                &mut acknowledged,
                vec![WalRecord::QuestionWithdrawalAcknowledged {
                    call_key: key.clone(),
                    elicitation_id: None,
                    answer,
                    reason: None,
                }],
            );
            assert_eq!(
                eligible(&acknowledged, START, COLD_MS),
                Ok((Some(session.clone()), false))
            );
        }
    }
}

#[test]
fn withdrawal_debt_excludes_every_recorded_approval_decision() {
    use crate::replay::deferred_tests::{approval, terminal};
    use broca_wal::{DeferredDecision, WithdrawElicitation};
    let (_, _, prepared, lineage, key) = approval_fixture();
    for decision in [
        DeferredDecision::Approved,
        DeferredDecision::Declined,
        DeferredDecision::Expired,
        DeferredDecision::Unavailable,
    ] {
        for withdrawal in [
            None,
            Some(WithdrawElicitation {
                call_key: key.clone(),
                elicitation_id: None,
            }),
        ] {
            let mut bytes = prepared.clone();
            append_decoded_approval_records(
                &mut bytes,
                vec![
                    approval("call", &key, decision),
                    terminal("call", withdrawal),
                ],
            );
            let range =
                broca_wal::file_wal::read_verified_member_range(&bytes, 1, 0, None).unwrap();
            assert!(crate::replay::deferred_call_obligations(lineage, complete_records(&range)).withdrawals.is_empty(),
                "an approval of {decision:?} answers the question, even when a later terminal closes the call");
        }
    }
}

#[test]
fn withdrawal_debt_requires_prepared_and_the_exact_reader_terminal() {
    use crate::replay::deferred_tests::{result, terminal};
    use broca_wal::{DispatchOutcome, WithdrawElicitation};
    let (_, _, prepared, lineage, key) = approval_fixture();
    let base = broca_wal::file_wal::read_verified_member_range(&prepared, 1, 0, None).unwrap();
    let prepared_frames = complete_records(&base);
    assert!(
        crate::replay::deferred_call_obligations(lineage, prepared_frames)
            .withdrawals
            .is_empty()
    );
    let mut frames = prepared_frames.to_vec();
    let mut no_dispatch = terminal("call", None);
    if let WalRecord::ToolResult {
        result: terminal_result,
        ..
    } = &mut no_dispatch
    {
        *terminal_result = result("call", "different interruption");
    }
    frames.push(broca_wal::FramedRecord {
        seq: 4,
        fence: 1,
        record: no_dispatch,
    });
    assert!(crate::replay::deferred_call_obligations(lineage, &frames)
        .withdrawals
        .is_empty());
    frames[3].record = terminal("call", None);
    if let WalRecord::ToolResult { outcome, .. } = &mut frames[3].record {
        *outcome = DispatchOutcome::Unknown;
    }
    assert!(crate::replay::deferred_call_obligations(lineage, &frames)
        .withdrawals
        .is_empty());
    frames[3].record = terminal(
        "call",
        Some(WithdrawElicitation {
            call_key: key,
            elicitation_id: None,
        }),
    );
    frames.remove(2);
    assert!(crate::replay::deferred_call_obligations(lineage, &frames)
        .withdrawals
        .is_empty());
}

#[test]
fn reused_call_ids_have_distinct_prepared_anchors() {
    use crate::replay::deferred_tests::{prepared, terminal};
    let (_, _, mut bytes, lineage, first_key) = approval_fixture();
    append_decoded_approval_records(
        &mut bytes,
        vec![terminal("call", None), prepared("call", "b2")],
    );
    let second_key = crate::context::call_key(lineage, 5);
    let mut second_terminal = terminal("call", None);
    if let WalRecord::ToolResult { batch_id, .. } = &mut second_terminal {
        *batch_id = broca_protocol::BatchId("b2".into());
    }
    append_decoded_approval_records(
        &mut bytes,
        vec![
            second_terminal,
            WalRecord::QuestionWithdrawalAcknowledged {
                call_key: first_key.clone(),
                elicitation_id: None,
                answer: broca_wal::WithdrawalAnswer::Acknowledged,
                reason: None,
            },
        ],
    );
    let range = broca_wal::file_wal::read_verified_member_range(&bytes, 1, 0, None).unwrap();
    let debt = crate::replay::deferred_call_obligations(lineage, complete_records(&range));
    assert_ne!(first_key, second_key);
    assert_eq!(debt.withdrawals.len(), 1);
    assert_eq!(debt.withdrawals[0].withdrawal.call_key, second_key);
}

#[test]
fn tree_shrinks_and_members_equal_prefold_digests() {
    let f = Fixture::new();
    let cold = [
        f.session("cold-a", Some(1)),
        f.session("cold-b", Some(START - COLD_MS)),
    ];
    let warm = [
        f.session("warm-a", Some(START)),
        f.session("warm-b", Some(START - COLD_MS + 1)),
    ];
    let report = f.fold();
    assert_eq!(report.removed, 2);
    assert_eq!(
        wal_paths(&f.root, Tree::Ordinary).unwrap().len(),
        warm.len()
    );
    let journal = Journal::load(f.root.join("wal-archive").join(report.journal.unwrap())).unwrap();
    assert_eq!(
        report.removed,
        journal
            .latest()
            .values()
            .filter(|e| matches!(
                e,
                Event::Session {
                    state: State::Done,
                    ..
                }
            ))
            .count()
    );
    for (session, bytes) in &cold {
        assert!(!f.store.wal_path(session).exists());
        let member = ArchiveReader::open(journal.path.with_extension("ark"), MAX_BYTES)
            .unwrap()
            .read_member(&session_addr(session))
            .unwrap()
            .unwrap();
        assert_eq!(digest(&member.payload), digest(bytes));
        let records = f.store.read_records(session).unwrap();
        assert!(!records.is_empty());
        assert_eq!(
            broca_wal::WalRangeRecords::Complete(records),
            broca_wal::file_wal::read_verified_member_range(bytes, 1, 0, None)
                .unwrap()
                .into_records()
        );
    }
    for (session, bytes) in warm {
        assert_eq!(fs::read(f.store.wal_path(&session)).unwrap(), bytes);
    }
}

#[test]
fn excluded_classes_have_exact_membership_and_report_only_writes_nothing() {
    let f = Fixture::new();
    let (cold, _) = f.session("cold", Some(1));
    let (moved, _) = f.session("moved", Some(1));
    let zero = "0000000000000000";
    fs::write(f.root.join("wal").join(format!("{zero}.wal")), []).unwrap();
    let lineage = BindIdentity::new("/fold-test", "runner", "lineage");
    drop(f.store.open(&lineage).unwrap());
    assert_eq!(fs::metadata(f.store.wal_path(&lineage)).unwrap().len(), 69);
    let moved_addr = "1111111111111111";
    fs::rename(
        f.store.wal_path(&moved),
        f.root.join("wal").join(format!("{moved_addr}.wal")),
    )
    .unwrap();
    let paths = wal_paths(&f.root, Tree::Ordinary).unwrap();
    let before: Vec<_> = paths
        .iter()
        .map(|p| {
            (
                fs::metadata(p).unwrap().modified().unwrap(),
                fs::read(p).unwrap(),
            )
        })
        .collect();
    let root_mtime = fs::metadata(&f.root).unwrap().modified().unwrap();
    let preview = run(&f.root, &FoldOptions::default(), START, &mut |_| {
        panic!("report wrote journal")
    })
    .unwrap();
    assert_eq!(preview.candidates, [session_addr(&cold)]);
    assert_eq!(preview.skipped["zero_byte"], [zero]);
    assert_eq!(preview.skipped["lineage_only"], [session_addr(&lineage)]);
    assert_eq!(preview.skipped["no_authoritative_identity"], [moved_addr]);
    assert!(!f.root.join("wal-archive").exists());
    assert_eq!(
        fs::metadata(&f.root).unwrap().modified().unwrap(),
        root_mtime
    );
    for (path, (mtime, bytes)) in paths.iter().zip(before) {
        assert_eq!(fs::metadata(path).unwrap().modified().unwrap(), mtime);
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
    let report = f.fold();
    assert_eq!(report.candidates, [session_addr(&cold)]);
    assert_eq!(report.removed, 1);
    assert_eq!(wal_paths(&f.root, Tree::Ordinary).unwrap().len(), 3);
}

#[test]
fn append_between_phases_is_superseded_and_lease_contention_continues() {
    let f = Fixture::new();
    let (changed, _) = f.session("changed", Some(1));
    let (held, _) = f.session("held", Some(1));
    let (cold, _) = f.session("cold", Some(1));
    let leases = FileLeaseStore::new(f.root.join("leases"));
    let _held = leases.acquire(&lease_key_for(&held)).unwrap();
    let root = f.root.clone();
    let changed_copy = changed.clone();
    AFTER_SEAL.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            let leases = FileLeaseStore::new(root.join("leases"));
            let lease = leases.acquire(&lease_key_for(&changed_copy)).unwrap();
            let mut wal = FileWalStore::new(root.join("wal"))
                .open(&changed_copy)
                .unwrap();
            wal.append(
                lease.epoch(),
                &WalRecord::StepStarted {
                    step_id: StepId(99),
                },
                true,
            )
            .unwrap();
        }))
    });
    let report = f.fold();
    assert_eq!(report.superseded, 1);
    assert_eq!(report.skipped["lease_held_phase1"], [session_addr(&held)]);
    assert!(f.store.wal_path(&held).exists());
    assert!(f.store.wal_path(&changed).exists());
    assert!(!f.store.wal_path(&cold).exists());
}

#[test]
fn crash_at_each_artifact_journal_boundary_never_strands_sessions() {
    let baseline = Fixture::new();
    baseline.session("a", Some(1));
    baseline.session("b", Some(1));
    let report = baseline.fold();
    let artifact = fs::read(
        baseline
            .root
            .join("wal-archive")
            .join(report.journal.unwrap()),
    )
    .unwrap();
    let cuts: Vec<_> = artifact
        .iter()
        .enumerate()
        .filter_map(|(i, b)| (*b == b'\n').then_some(i + 1))
        .collect();
    assert!(cuts.len() >= 7);
    for (cut_index, _) in cuts.iter().enumerate() {
        let f = Fixture::new();
        let sessions = [f.session("a", Some(1)), f.session("b", Some(1))];
        let mut boundaries = 0;
        let mut journal_path = None;
        let result = run(
            &f.root,
            &FoldOptions {
                write: true,
                ..Default::default()
            },
            START,
            &mut |path| {
                boundaries += 1;
                journal_path = Some(path.to_owned());
                if boundaries == cut_index + 1 {
                    Err(io::Error::other("injected journal boundary crash"))
                } else {
                    Ok(())
                }
            },
        );
        assert!(result.is_err());
        assert_eq!(boundaries, cut_index + 1);
        for (session, bytes) in &sessions {
            let live = live_bytes(&f.store.wal_path(session)).unwrap();
            let paths: Vec<_> = fs::read_dir(f.root.join("wal-archive"))
                .unwrap()
                .map(|e| e.unwrap().path())
                .filter(|p| p.extension().is_some_and(|e| e == "ark"))
                .collect();
            let member =
                broca_wal::archive::read_newest_member(&paths, &session_addr(session), MAX_BYTES)
                    .unwrap();
            let actual = live
                .or_else(|| member.map(|m| m.payload))
                .expect("session neither archived nor live");
            assert_eq!(digest(&actual), digest(bytes));
        }
        let path = journal_path.unwrap();
        let before = Journal::load(path.clone()).unwrap();
        let done_before = before
            .events
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    Event::Session {
                        state: State::Done,
                        ..
                    }
                )
            })
            .count();
        let resumed = run(
            &f.root,
            &FoldOptions {
                write: true,
                resume: Some(path.file_name().unwrap().to_string_lossy().into_owned()),
                abandon: false,
                unfold: false,
            },
            START + 1,
            &mut |_| Ok(()),
        )
        .unwrap();
        let after = Journal::load(path.clone()).unwrap();
        if !matches!(after.events.last(), Some(Event::Restarted)) {
            let done_after = after
                .events
                .iter()
                .filter(|e| {
                    matches!(
                        e,
                        Event::Session {
                            state: State::Done,
                            ..
                        }
                    )
                })
                .count();
            assert_eq!(done_after, 2);
            assert_eq!(resumed.removed, 2 - done_before);
        }
        assert!(after.terminal());
        assert!(wal_paths(&f.root, Tree::Ordinary).unwrap().is_empty());
        for (session, _) in &sessions {
            assert!(!f.store.read_records(session).unwrap().is_empty());
        }
    }
}

/// No admission means no archive, but the journal still records a terminal pass.
#[test]
fn no_candidates_leave_only_a_terminal_journal_and_allow_the_next_pass() {
    let f = Fixture::new();
    for _ in 0..3 {
        // Reusing the start also proves journal-only stamps remain monotonic.
        let report = f.fold_at(START);
        assert!(report.candidates.is_empty());
        let path = f.root.join("wal-archive").join(report.journal.unwrap());
        let journal = Journal::load(path.clone()).unwrap();
        assert!(journal.latest().is_empty());
        assert!(matches!(journal.events.last(), Some(Event::Complete)));
        assert!(journal.terminal());
        assert!(!path.with_extension("ark").exists());
        assert_eq!(unfinished_pass(&f.root).unwrap(), None);
    }
    assert_eq!(fs::read_dir(f.root.join("wal-archive")).unwrap().count(), 3);
}

#[test]
fn pre_admission_crash_does_not_block_and_can_resume_or_abandon() {
    for abandon in [false, true] {
        let f = Fixture::new();
        let dir = f.root.join("wal-archive");
        fs::create_dir_all(&dir).unwrap();
        let name = format!("fold-{START}.journal");
        let path = dir.join(&name);
        fs::write(&path, b"").unwrap();
        assert_eq!(unfinished_pass(&f.root).unwrap(), None);
        f.fold_at(START);
        for write in [false, true, true] {
            run(
                &f.root,
                &FoldOptions {
                    write,
                    resume: Some(name.clone()),
                    abandon,
                    ..Default::default()
                },
                START,
                &mut |_| Ok(()),
            )
            .unwrap();
            assert_eq!(Journal::load(path.clone()).unwrap().terminal(), write);
            assert!(!path.with_extension("ark").exists());
        }
        assert!(matches!(
            Journal::load(path).unwrap().events.last(),
            Some(Event::Complete | Event::Abandoned { .. })
        ));
        f.fold_at(START);
    }
}

#[test]
fn legacy_empty_container_and_journal_are_left_intact() {
    let f = Fixture::new();
    let dir = f.root.join("wal-archive");
    fs::create_dir_all(&dir).unwrap();
    let ark = dir.join("fold-1789459623420.ark");
    ArchiveWriter::create(&ark)
        .unwrap()
        .without_feature_floor()
        .seal()
        .unwrap();
    let journal = ark.with_extension("journal");
    fs::write(&journal, b"{\"event\":\"complete\"}\n").unwrap();
    let before = fs::read(&ark).unwrap();
    f.fold_at(START);
    assert_eq!(fs::read(&ark).unwrap(), before);
    assert_eq!(fs::read(&journal).unwrap(), b"{\"event\":\"complete\"}\n");
}

/// A LATER PASS MUST NOT DISTURB AN EXISTING CONTAINER, and its members stay readable.
///
/// This was a reclaim test. The reclaim is gone -- no pass deletes a container now -- so
/// its `reclaimed_empty == 0` assertion became a tautology and was removed rather than
/// left reading as a guard. What survives is the property that outlived the mechanism:
/// after a fold archives a session, a subsequent pass leaves that container alone and the
/// transcript still reads through the archive.
#[test]
fn a_later_pass_leaves_an_existing_container_and_its_transcript_intact() {
    let f = Fixture::new();
    let (session, _) = f.session("kept", Some(1));
    let real = f.fold();
    assert_eq!(real.removed, 1, "the session was archived");
    assert!(!f.store.wal_path(&session).exists());

    // A later no-op pass must not take the container holding that transcript.
    let later = f.fold_at(START + 1_000);
    assert_eq!(later.removed, 0);
    assert!(
        !f.store.read_records(&session).unwrap().is_empty(),
        "the archived transcript must still read after a later pass"
    );
}

/// The blocking journal is discoverable FROM DISK, which is what a restart leaves.
///
/// `phase2_contention_resumes_and_unfold_preserves_bytes` recovers using `report.journal`
/// -- an in-memory handle from the pass that just ran. A scheduled caller does not have
/// that after a restart, and a fresh fold is refused while the journal exists, so without
/// a disk-side query one transient lease collision stops automatic archiving permanently
/// under this root.
#[test]
fn an_unfinished_pass_is_discoverable_from_disk_and_resumable() {
    let f = Fixture::new();
    let (session, bytes) = f.session("wedge", Some(1));
    assert_eq!(
        unfinished_pass(&f.root).unwrap(),
        None,
        "nothing blocks a fold before one has run"
    );

    let holder = std::rc::Rc::new(std::cell::RefCell::new(None));
    let slot = holder.clone();
    let root = f.root.clone();
    let identity = session.clone();
    AFTER_SEAL.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            *slot.borrow_mut() = Some(
                FileLeaseStore::new(root.join("leases"))
                    .acquire(&lease_key_for(&identity))
                    .unwrap(),
            );
        }))
    });
    let contended = f.fold();
    assert_eq!(
        contended.skipped["lease_held_phase2"],
        [session_addr(&session)]
    );
    assert_eq!(fs::read(f.store.wal_path(&session)).unwrap(), bytes);

    // The state a restarted daemon wakes to: a journal on disk, no report in hand.
    let blocking = unfinished_pass(&f.root).unwrap();
    assert_eq!(
        blocking, contended.journal,
        "the disk query must name the same pass the report did"
    );

    // A FRESH fold stays refused while it exists -- the wedge, if nothing resumes.
    let fresh = run(
        &f.root,
        &FoldOptions {
            write: true,
            ..Default::default()
        },
        START + 1,
        &mut |_| Ok(()),
    );
    assert!(
        fresh.is_err_and(|e| e.to_string().contains("unfinished archive pass")),
        "a fresh fold must stay refused; that gate is what makes the query necessary"
    );

    holder.borrow_mut().take();
    let resumed = run(
        &f.root,
        &FoldOptions {
            write: true,
            resume: blocking,
            ..Default::default()
        },
        START + 2,
        &mut |_| Ok(()),
    )
    .expect("the disk-discovered journal must resume");
    assert_eq!(resumed.removed, 1);
    assert!(!f.store.wal_path(&session).exists());
    assert!(!f.store.read_records(&session).unwrap().is_empty());
    assert_eq!(
        unfinished_pass(&f.root).unwrap(),
        None,
        "a completed pass must stop blocking the next one"
    );
}

/// Between the seal and `finish`, a crash would leave a sealed container whose pass never
/// completed. Lookups in that window must already serve its members, and the membership
/// built for it there must come from its verified v2 index: the sealed container is
/// complete and its index digest matches, whether or not `finish` ever runs.
#[cfg(unix)]
#[test]
fn a_sealed_unfinished_pass_serves_members_through_the_verified_index() {
    use broca_wal::archive::{cached_membership, read_newest_member, CachedMembership};
    let f = Fixture::new();
    let sessions = [
        f.session("sealed-a", Some(1)),
        f.session("sealed-b", Some(1)),
    ];
    let expected = sessions
        .iter()
        .map(|(session, bytes)| (session_addr(session), bytes.clone()))
        .collect::<Vec<_>>();
    let archive = f.root.join("wal-archive");
    let checked = std::rc::Rc::new(std::cell::Cell::new(false));
    let done = checked.clone();
    AFTER_SEAL.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            let paths: Vec<_> = fs::read_dir(&archive)
                .unwrap()
                .map(|e| e.unwrap().path())
                .filter(|p| p.extension().is_some_and(|e| e == "ark"))
                .collect();
            assert_eq!(paths.len(), 1);
            assert!(read_newest_member(&paths, "ffffffffffffffff", MAX_BYTES)
                .unwrap()
                .is_none());
            assert_eq!(
                cached_membership(&paths[0]),
                Some(CachedMembership::Verified)
            );
            for (addr, bytes) in &expected {
                let member = read_newest_member(&paths, addr, MAX_BYTES)
                    .unwrap()
                    .expect("a sealed member must be served before the pass finishes");
                assert_eq!(digest(&member.payload), digest(bytes));
            }
            done.set(true);
        }))
    });
    let report = f.fold();
    assert!(checked.get(), "the post-seal hook must have run");
    assert_eq!(report.removed, 2);
}

#[test]
fn phase2_contention_resumes_and_unfold_preserves_bytes() {
    let f = Fixture::new();
    let (session, bytes) = f.session("phase2", Some(1));
    let holder = std::rc::Rc::new(std::cell::RefCell::new(None));
    let slot = holder.clone();
    let root = f.root.clone();
    let identity = session.clone();
    AFTER_SEAL.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            *slot.borrow_mut() = Some(
                FileLeaseStore::new(root.join("leases"))
                    .acquire(&lease_key_for(&identity))
                    .unwrap(),
            );
        }))
    });
    let report = f.fold();
    assert_eq!(
        report.skipped["lease_held_phase2"],
        [session_addr(&session)]
    );
    assert_eq!(report.removed, 0);
    assert_eq!(fs::read(f.store.wal_path(&session)).unwrap(), bytes);
    holder.borrow_mut().take();
    let options = FoldOptions {
        write: true,
        resume: report.journal,
        ..Default::default()
    };
    let report = run(&f.root, &options, START + 1, &mut |_| Ok(())).unwrap();
    assert_eq!(report.removed, 1);
    let report = run(
        &f.root,
        &FoldOptions {
            unfold: true,
            ..options
        },
        START + 2,
        &mut |_| Ok(()),
    )
    .unwrap();
    assert_eq!(report.restored, 1);
    assert_eq!(fs::read(f.store.wal_path(&session)).unwrap(), bytes);
}

#[test]
fn abandoned_missing_container_records_missing_live_even_without_done() {
    let f = Fixture::new();
    let (session, _) = f.session("lost", Some(1));
    let report = f.fold();
    let path = f.root.join("wal-archive").join(report.journal.unwrap());
    let journal = Journal::load(path.clone()).unwrap();
    // Reconstruct the legitimate crash window: unlink ran, but the done record
    // did not become durable. The journal alone cannot prove live presence.
    let archived_end: usize = journal
        .events
        .iter()
        .take_while(|e| {
            !matches!(
                e,
                Event::Session {
                    state: State::Done,
                    ..
                }
            )
        })
        .map(|e| serde_json::to_vec(e).unwrap().len() + 1)
        .sum();
    OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(archived_end as u64)
        .unwrap();
    fs::remove_file(path.with_extension("ark")).unwrap();
    let options = FoldOptions {
        write: true,
        resume: Some(path.file_name().unwrap().to_string_lossy().into_owned()),
        ..Default::default()
    };
    let error = run(&f.root, &options, START + 1, &mut |_| Ok(()))
        .unwrap_err()
        .to_string();
    assert!(error.contains("LOST DATA") && error.contains(&session_addr(&session)));
    let before = fs::read(&path).unwrap();
    run(
        &f.root,
        &FoldOptions {
            abandon: true,
            ..options
        },
        START + 1,
        &mut |_| Ok(()),
    )
    .unwrap();
    assert!(fs::read(&path).unwrap().starts_with(&before));
    let journal = Journal::load(path).unwrap();
    assert!(
        matches!(journal.events.last(), Some(Event::Abandoned { live_present }) if !live_present[&session_addr(&session)])
    );
}

#[test]
fn concurrent_operator_refused_but_report_remains_read_only() {
    let f = Fixture::new();
    let (session, _) = f.session("cold", Some(1));
    let _pass = acquire_archive_pass(&f.root).unwrap();
    let report = run(&f.root, &FoldOptions::default(), START, &mut |_| {
        panic!("report wrote")
    })
    .unwrap();
    assert_eq!(report.candidates, [session_addr(&session)]);
    let error = run(
        &f.root,
        &FoldOptions {
            write: true,
            ..Default::default()
        },
        START,
        &mut |_| Ok(()),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("holds the fold lock"), "{error}");
    let lock_path = crate::lease::archive_pass_lock_path(&f.root);
    assert!(
        lock_path.is_file(),
        "diagnostic path must name the actual lease"
    );
    assert!(error.contains(&lock_path.display().to_string()), "{error}");
    assert!(error.contains("lsof -- '"), "{error}");
    assert!(!f.root.join("wal-archive").exists());
}

#[test]
fn crash_after_retiring_unsealed_pass_still_resumes_live_candidates() {
    let f = Fixture::new();
    let (session, _) = f.session("restart", Some(1));
    let mut name = None;
    assert!(run(
        &f.root,
        &FoldOptions {
            write: true,
            ..Default::default()
        },
        START,
        &mut |path| {
            name = Some(path.file_name().unwrap().to_string_lossy().into_owned());
            Err(io::Error::other("crash before archive append"))
        }
    )
    .is_err());
    let options = FoldOptions {
        write: true,
        resume: name,
        ..Default::default()
    };
    assert!(run(&f.root, &options, START + 1, &mut |path| {
        assert!(matches!(
            Journal::load(path.to_owned())?.events.last(),
            Some(Event::Restarted)
        ));
        Err(io::Error::other("crash after retirement"))
    })
    .is_err());
    let report = run(&f.root, &options, START + 2, &mut |_| Ok(())).unwrap();
    assert_eq!(report.removed, 1);
    assert!(!f.store.wal_path(&session).exists());
    assert!(!f.store.read_records(&session).unwrap().is_empty());

    // AND THE RETIRED CONTAINER MUST BE GONE. `Restarted` is appended and fsynced
    // BEFORE its unlink, so the crash above lands precisely between them -- and the
    // resume arm used to recurse straight past the unlink, while `Journal::terminal`
    // treats `Restarted` as terminal so no later pass recovered it either. The
    // orphan stayed forever, and every instrument built on `logical_wals` then
    // refuses the whole population on it, by design.
    //
    // This assertion is the round-172 gap: the scenario above already reached the
    // window, and nothing looked at what it left behind. Without it, disabling the
    // retirement entirely leaves all 22 tests in this file green.
    let orphans: Vec<_> = fs::read_dir(f.root.join("wal-archive"))
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "ark"))
        .collect();
    assert_eq!(
        orphans.len(),
        1,
        "exactly the sealed replacement should remain, found {orphans:?}"
    );
    assert!(
        ArchiveReader::open(&orphans[0], u64::MAX)
            .and_then(|mut r| r.state())
            .is_ok_and(|s| s == ContainerState::Sealed),
        "the surviving container must be the sealed one, not the retired attempt"
    );
}

#[test]
#[ignore = "operator diagnostic verification requires the lsof executable"]
fn lsof_instruction_identifies_current_archive_pass_holder() {
    let f = Fixture::new();
    let pass = acquire_archive_pass(&f.root).unwrap();
    let path = crate::lease::archive_pass_lock_path(&f.root);
    let output = std::process::Command::new("lsof")
        .arg("--")
        .arg(&path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout
            .split_whitespace()
            .any(|field| field == std::process::id().to_string()),
        "{stdout}"
    );
    println!("verified lsof -- '{}':\n{stdout}", path.display());
    drop(pass);
}

#[tokio::test]
async fn fold_send_age_refold_reads_post_append_member() {
    use crate::{AllowAll, RunRequest, StubProvider, StubToolPlane};
    use broca_protocol::{
        content::{AssistantMessage, ContentBlock},
        provider::{NormalizedEvent, ProviderControl},
        FinishReason, MessageId, ModelHandle, Usage,
    };
    let f = Fixture::new();
    let (session, _) = f.session("refold-send", Some(1));
    {
        let mut wal = f.store.open(&session).unwrap();
        wal.append(
            1,
            &WalRecord::RunFinished {
                ts_ms: Some(1),
                reason: broca_protocol::caller::RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
            true,
        )
        .unwrap();
    }
    let original = fs::read(f.store.wal_path(&session)).unwrap();
    assert_eq!(f.fold().removed, 1);
    let provider = StubProvider::new(vec![vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("answer".into()),
                content: vec![ContentBlock::Text {
                    text: "post-append answer".into(),
                }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]]);
    let req = RunRequest {
        session_class: None,
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId("appended".into()),
        session: session.clone(),
        model: ModelHandle::new("stub", "m"),
        input: vec![],
        tools: vec![],
        policy_allowed: Default::default(),
        tool_choice: broca_protocol::ToolChoice::Auto,
        generation: Default::default(),
        cache: None,
        stop_when: vec![],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
    };
    let (out, _rx) = tokio::sync::mpsc::channel(100);
    crate::run_loop_append_episode(
        req,
        &provider,
        &StubToolPlane::new(),
        &FileLeaseStore::new(f.root.join("leases")),
        &AllowAll,
        &f.store,
        None,
        &out,
    )
    .await
    .unwrap();
    let path = f.store.wal_path(&session);
    let appended = fs::read(&path).unwrap();
    assert!(appended.starts_with(&original));
    assert!(appended.len() > original.len());
    let expected = f.store.read_records(&session).unwrap();
    assert!(matches!(
        &expected.last().unwrap().record,
        WalRecord::RunFinished {
            reason: broca_protocol::caller::RunFinishReason::Completed,
            ..
        }
    ));
    assert!(expected.iter().any(|r| matches!(&r.record,
        WalRecord::ModelStepFinished { assistant_message, .. }
        if assistant_message.content.iter().any(|block| matches!(block,
            ContentBlock::Text { text } if text == "post-append answer"))
    )));
    // Advance the fold clock, not the bytes: record timestamps are the age authority.
    let future = now_ms().unwrap() + COLD_MS + 1;
    let second = run(
        &f.root,
        &FoldOptions {
            write: true,
            ..Default::default()
        },
        future,
        &mut |_| Ok(()),
    )
    .unwrap();
    assert_eq!(second.removed, 1);
    assert!(!path.exists());
    assert_eq!(f.store.read_records(&session).unwrap(), expected);
    let member = f.store.read_records_range(&session, 0, None, None).unwrap();
    assert!(matches!(
        member.records(),
        broca_wal::WalRangeRecords::Complete(_)
    ));
    assert!(!path.exists(), "read never extracts");
    let newest = ArchiveReader::open(
        f.root
            .join("wal-archive")
            .join(second.journal.unwrap())
            .with_extension("ark"),
        MAX_BYTES,
    )
    .unwrap()
    .read_member(&session_addr(&session))
    .unwrap()
    .unwrap();
    assert_eq!(newest.payload, appended);
    let first = ArchiveReader::open(
        f.root.join("wal-archive").join(format!("fold-{START}.ark")),
        MAX_BYTES,
    )
    .unwrap()
    .read_member(&session_addr(&session))
    .unwrap()
    .unwrap();
    assert_eq!(first.payload, original);
}

#[test]
fn lost_unsealed_members_are_rebuilt_from_live_originals() {
    let f = Fixture::new();
    let sessions = [f.session("a", Some(1)), f.session("b", Some(1))];
    let mut journal_path = None;
    let result = run(
        &f.root,
        &FoldOptions {
            write: true,
            ..Default::default()
        },
        START,
        &mut |path| {
            let journal = Journal::load(path.to_owned())?;
            let archived = journal
                .latest()
                .values()
                .filter(|event| {
                    matches!(
                        event,
                        Event::Session {
                            state: State::Archived,
                            ..
                        }
                    )
                })
                .count();
            if archived == 2 {
                journal_path = Some(path.to_owned());
                Err(io::Error::other("stop after both archived records"))
            } else {
                Ok(())
            }
        },
    );
    assert!(result.is_err());
    let path = journal_path.expect("both archived records reached");
    let ark = path.with_extension("ark");
    assert_eq!(
        ArchiveReader::open(&ark, MAX_BYTES)
            .unwrap()
            .state()
            .unwrap(),
        ContainerState::Unsealed
    );
    // Model loss of every unsynced member byte while keeping the durable journal.
    let file = OpenOptions::new().write(true).open(&ark).unwrap();
    file.set_len(0).unwrap();
    file.sync_all().unwrap();
    drop(file);
    for (session, bytes) in &sessions {
        assert_eq!(fs::read(f.store.wal_path(session)).unwrap(), *bytes);
    }
    let resumed = run(
        &f.root,
        &FoldOptions {
            write: true,
            resume: Some(path.file_name().unwrap().to_string_lossy().into_owned()),
            ..Default::default()
        },
        START + 1,
        &mut |_| Ok(()),
    )
    .unwrap();
    assert_eq!(resumed.removed, 2);
    assert!(matches!(
        Journal::load(path.clone()).unwrap().events.last(),
        Some(Event::Restarted)
    ));
    let replacement = f
        .root
        .join("wal-archive")
        .join(resumed.journal.unwrap())
        .with_extension("ark");
    assert!(
        broca_wal::archive::fold_timestamp(&replacement).unwrap()
            > broca_wal::archive::fold_timestamp(&ark).unwrap()
    );
    let mut reader = ArchiveReader::open(replacement, MAX_BYTES).unwrap();
    assert_eq!(reader.state().unwrap(), ContainerState::Sealed);
    for (session, bytes) in sessions {
        assert_eq!(
            reader
                .read_member(&session_addr(&session))
                .unwrap()
                .unwrap()
                .payload,
            bytes
        );
    }
}

#[test]
fn intended_and_archived_each_call_the_journal_barrier() {
    let f = Fixture::new();
    f.session("a", Some(1));
    f.session("b", Some(1));
    JOURNAL_SYNCS.with(|count| count.set(0));
    let mut boundaries = 0;
    let mut intended = 0;
    let mut archived = 0;
    // This observes successful journal sync calls, not power-loss durability.
    run(
        &f.root,
        &FoldOptions {
            write: true,
            ..Default::default()
        },
        START,
        &mut |path| {
            boundaries += 1;
            let journal = Journal::load(path.to_owned())?;
            match journal.events.last().unwrap() {
                Event::Session {
                    state: State::Intended,
                    ..
                } => intended += 1,
                Event::Session {
                    state: State::Archived,
                    ..
                } => archived += 1,
                _ => {}
            }
            JOURNAL_SYNCS.with(|count| {
                assert_eq!(
                    count.get(),
                    boundaries,
                    "each journal record must call its own barrier before the boundary"
                )
            });
            Ok(())
        },
    )
    .unwrap();
    assert_eq!((intended, archived), (2, 2));
}

#[test]
fn rekeyed_session_archives_under_store_triple_lease_without_rewriting_provenance() {
    let f = Fixture::new();
    let (embedded, bytes) = f.session("rekeyed", Some(1));
    let resolved = BindIdentity::new("/fold-test", "broca", "rekeyed");
    let addr = session_addr(&resolved);
    fs::rename(f.store.wal_path(&embedded), f.store.wal_path(&resolved)).unwrap();
    let identities = BTreeMap::from([(addr.clone(), resolved.clone())]);
    let leases = FileLeaseStore::new(f.root.join("leases"));
    let expected_key =
        cortexkit_lease::LeaseKey::new("broca", "wal", "/fold-test\u{1f}broca\u{1f}rekeyed");
    let mut observed = 0;
    let report = run_with_identities(
        &f.root,
        &FoldOptions {
            write: true,
            ..Default::default()
        },
        START,
        &mut |path| {
            let journal = Journal::load(path.to_owned()).unwrap();
            if let Some(Event::Session { session, .. }) = journal.events.last() {
                assert_eq!(lease_key_for(session), expected_key);
                assert!(matches!(
                    leases.acquire(&expected_key),
                    Err(LeaseError::Held { .. })
                ));
                assert!(leases.acquire(&lease_key_for(&embedded)).is_ok());
                observed += 1;
            }
            Ok(())
        },
        &identities,
    )
    .unwrap();
    assert_eq!(
        observed, 3,
        "Intended, Archived and Done hold the store lease"
    );
    assert_eq!(report.removed, 1);
    assert_eq!(report.resolved_from_store, std::slice::from_ref(&addr));
    let mut archive = ArchiveReader::open(
        f.root
            .join("wal-archive")
            .join(report.journal.unwrap())
            .with_extension("ark"),
        MAX_BYTES,
    )
    .unwrap();
    assert_eq!(archive.read_member(&addr).unwrap().unwrap().payload, bytes);
    assert!(!f.store.wal_path(&resolved).exists());
}

#[test]
fn missing_store_identity_skips_permanently_and_embedded_identity_still_archives() {
    let f = Fixture::new();
    let (embedded, bytes) = f.session("orphan", Some(1));
    let moved = f.root.join("wal/1111111111111111.wal");
    fs::rename(f.store.wal_path(&embedded), &moved).unwrap();
    let (normal, _) = f.session("never-rekeyed", Some(1));
    let report = f.fold();
    assert_eq!(report.removed, 1);
    assert_eq!(report.candidates, [session_addr(&normal)]);
    assert_eq!(
        report.skipped["no_authoritative_identity"],
        ["1111111111111111"]
    );
    assert!(report
        .permanent_skip_notice
        .unwrap()
        .contains("will stay in the backup manifest"));
    assert!(report.resolved_from_store.is_empty());
    assert_eq!(fs::read(moved).unwrap(), bytes);
}

#[test]
fn unverified_store_triple_cannot_select_a_wrong_lease() {
    let f = Fixture::new();
    let (embedded, bytes) = f.session("bad-store", Some(1));
    let addr = "1111111111111111";
    let moved = f.root.join("wal").join(format!("{addr}.wal"));
    fs::rename(f.store.wal_path(&embedded), &moved).unwrap();
    let wrong_store = BindIdentity::new("/fold-test", "broca", "bad-store");
    assert_ne!(session_addr(&wrong_store), addr);
    assert_ne!(session_addr(&embedded), addr);
    // Inject an inconsistent address lookup at the public fold boundary. Neither
    // source may authorize a lease for this filename merely because it exists.
    let identities = BTreeMap::from([(addr.to_owned(), wrong_store)]);
    let report = run_with_identities(
        &f.root,
        &FoldOptions {
            write: true,
            ..Default::default()
        },
        START,
        &mut |_| Ok(()),
        &identities,
    )
    .unwrap();
    assert_eq!(report.skipped["no_authoritative_identity"], [addr]);
    assert_eq!(report.removed, 0);
    assert!(report.candidates.is_empty());
    assert_eq!(fs::read(moved).unwrap(), bytes);
}

#[test]
fn pre_schema_is_admitted_and_reported_in_preview_and_write() {
    let f = Fixture::new();
    let (session, bytes) = f.session("pre-schema", None);
    let addr = session_addr(&session);
    let preview = run(&f.root, &FoldOptions::default(), START, &mut |_| Ok(())).unwrap();
    assert_eq!(preview.cold_pre_schema, std::slice::from_ref(&addr));
    assert_eq!(preview.candidates, std::slice::from_ref(&addr));
    assert!(preview.skipped.is_empty());
    assert_eq!(fs::read(f.store.wal_path(&session)).unwrap(), bytes);
    let report = f.fold();
    assert_eq!(report.cold_pre_schema, std::slice::from_ref(&addr));
    assert_eq!(report.candidates, std::slice::from_ref(&addr));
    assert_eq!(report.removed, 1);
    let mut archive = ArchiveReader::open(
        f.root
            .join("wal-archive")
            .join(report.journal.unwrap())
            .with_extension("ark"),
        MAX_BYTES,
    )
    .unwrap();
    assert_eq!(archive.read_member(&addr).unwrap().unwrap().payload, bytes);
    assert!(!f.store.wal_path(&session).exists());
}

#[test]
fn pre_schema_corruption_is_undecodable() {
    let f = Fixture::new();
    let (session, mut bytes) = f.session("corrupt-pre-schema", None);
    *bytes.last_mut().unwrap() ^= 1;
    fs::write(f.store.wal_path(&session), &bytes).unwrap();
    let report = run_with_identities(
        &f.root,
        &FoldOptions {
            write: true,
            ..Default::default()
        },
        START,
        &mut |_| Ok(()),
        &BTreeMap::from([(session_addr(&session), session.clone())]),
    )
    .unwrap();
    assert_eq!(report.skipped["undecodable"], [session_addr(&session)]);
    assert!(report.cold_pre_schema.is_empty());
    assert!(report.candidates.is_empty());
    assert_eq!(report.removed, 0);
    assert_eq!(fs::read(f.store.wal_path(&session)).unwrap(), bytes);
}

/// A cold WAL that a newer build appended to is skipped under its own reason, not as
/// undecodable, and is left exactly as it is: its bytes are intact, and this build cannot
/// interpret them to prove age or identity.
#[test]
fn a_newer_writer_wal_is_skipped_as_newer_writer_not_undecodable() {
    let f = Fixture::new();
    let (session, _) = f.session("newer-writer", Some(1));
    let frame = broca_wal::framing::encode_with_requires(
        3,
        1,
        &["from-the-future/v1"],
        &WalRecord::RunFinished {
            ts_ms: Some(1),
            reason: broca_protocol::RunFinishReason::Completed,
            usage: Default::default(),
            error: None,
            retries_used: 0,
        },
    )
    .unwrap();
    let path = f.store.wal_path(&session);
    let mut bytes = fs::read(&path).unwrap();
    bytes.extend(frame);
    fs::write(&path, &bytes).unwrap();

    let report = run_with_identities(
        &f.root,
        &FoldOptions {
            write: true,
            ..Default::default()
        },
        START,
        &mut |_| Ok(()),
        &BTreeMap::from([(session_addr(&session), session.clone())]),
    )
    .unwrap();
    assert_eq!(report.skipped["newer_writer"], [session_addr(&session)]);
    assert!(!report.skipped.contains_key("undecodable"));
    assert!(report.candidates.is_empty());
    assert_eq!(report.removed, 0);
    assert_eq!(fs::read(&path).unwrap(), bytes);
}

/// Fold, unfold and extraction on open move a WAL holding a readable gated frame without
/// rewriting it, so the frame keeps its version byte and its `requires` list.
///
/// The fold decodes every record to prove age and identity. Archiving those decoded records
/// instead of the bytes it read would re-encode the gated frame as an ordinary version-1 frame,
/// silently removing the gate, and the member would still decode to the same records. So the
/// assertions are on bytes.
#[test]
fn fold_unfold_and_extraction_keep_a_gated_frame_byte_for_byte() {
    let f = Fixture::new();
    let (session, _) = f.session("gated-known", Some(1));
    let gated = broca_wal::framing::encode_with_requires(
        3,
        1,
        &[broca_wal::framing::TEST_KNOWN_FEATURE],
        &WalRecord::StepStarted { step_id: StepId(2) },
    )
    .unwrap();
    let path = f.store.wal_path(&session);
    let mut bytes = fs::read(&path).unwrap();
    let gated_at = bytes.len();
    bytes.extend(&gated);
    fs::write(&path, &bytes).unwrap();
    let carries_gate = |b: &[u8]| b.get(gated_at..gated_at + gated.len()) == Some(&gated[..]);

    let report = f.fold();
    assert_eq!(
        report.removed, 1,
        "a readable gated frame must not block the fold"
    );
    let journal = report.journal.unwrap();
    let member = ArchiveReader::open(
        f.root
            .join("wal-archive")
            .join(&journal)
            .with_extension("ark"),
        MAX_BYTES,
    )
    .unwrap()
    .read_member(&session_addr(&session))
    .unwrap()
    .unwrap();
    assert!(
        carries_gate(&member.payload),
        "the archived member lost its gate"
    );
    assert_eq!(member.payload, bytes);

    let report = run(
        &f.root,
        &FoldOptions {
            write: true,
            unfold: true,
            resume: Some(journal),
            ..Default::default()
        },
        START + 1,
        &mut |_| Ok(()),
    )
    .unwrap();
    assert_eq!(report.restored, 1);
    let restored = fs::read(&path).unwrap();
    assert!(carries_gate(&restored), "unfold lost the gate");
    assert_eq!(restored, bytes);

    let report = f.fold_at(START + 2);
    assert_eq!(report.removed, 1);
    assert!(!path.exists());
    // Opening a folded session extracts its archived bytes back to the live path.
    drop(f.store.open(&session).unwrap());
    let extracted = fs::read(&path).unwrap();
    assert!(carries_gate(&extracted), "extraction lost the gate");
    assert_eq!(extracted, bytes);
    assert_eq!(f.store.read_records(&session).unwrap().len(), 3);
}

/// A cold session whose `RunStarted` froze a scope folds like any other: the fold decodes its
/// gated frame to prove age and identity instead of skipping it, archives the bytes unchanged
/// (the frame keeps its version byte and `requires`), and opening the folded session reads
/// the scope back.
#[test]
fn a_scoped_session_folds_and_extracts_with_its_gate_and_scope() {
    let f = Fixture::new();
    let scope = broca_protocol::SessionScope {
        owner: broca_protocol::BindPrincipal::Reserved {
            module_id: "prefrontal-core".into(),
        },
        scope_ref: "head-1".into(),
        scope_epoch: 3,
        flow_id: None,
        sender: None,
        agent_id: None,
        run_id: None,
    };
    let (session, bytes) = f.session_with_scope("scoped", Some(1), Some(scope.clone()));
    // The WAL opens with a lineage frame, then the RunStarted's own frame.
    let run_started_at = broca_wal::framing::HEADER_LEN + 16;
    assert_eq!(
        bytes[run_started_at + 4],
        broca_wal::framing::GATED_FRAMING_VERSION
    );

    let report = f.fold();
    assert_eq!(report.removed, 1, "a scoped session must fold: {report:?}");
    assert!(!report.skipped.contains_key("newer_writer"));
    let journal = report.journal.unwrap();
    let member = ArchiveReader::open(
        f.root
            .join("wal-archive")
            .join(&journal)
            .with_extension("ark"),
        MAX_BYTES,
    )
    .unwrap()
    .read_member(&session_addr(&session))
    .unwrap()
    .unwrap();
    assert_eq!(
        member.payload, bytes,
        "the archived member must keep the gate"
    );

    drop(f.store.open(&session).unwrap());
    let records = f.store.read_records(&session).unwrap();
    match &records[0].record {
        WalRecord::RunStarted { scope: read, .. } => assert_eq!(read.as_ref(), Some(&scope)),
        other => panic!("expected RunStarted, got {other:?}"),
    }
}

/// A cold session whose `RunStarted` froze a plan manifest folds like any other, and fold,
/// unfold and extraction each give back the WAL byte for byte: the gated frame keeps its
/// version byte, its `requires` and every byte of the manifest, and reading the extracted
/// session gives the manifest back. The fold decodes every record to prove age and identity,
/// so archiving re-encoded records instead of the bytes it read would be enough to lose or
/// alter the manifest; the assertions are therefore on bytes, then on the decoded manifest.
#[test]
fn a_manifest_session_folds_unfolds_and_extracts_byte_for_byte() {
    let f = Fixture::new();
    let session = BindIdentity::new("/fold-test", "runner", "manifest");
    let mut record: WalRecord = serde_json::from_str(
        &include_str!("../../../broca-wal/tests/fixtures/plan_manifest_run_started.json")
            .trim_end()
            .replace("{RUN_ID}", "run"),
    )
    .unwrap();
    let WalRecord::RunStarted {
        session: recorded,
        ts_ms,
        manifest,
        ..
    } = &mut record
    else {
        unreachable!()
    };
    *recorded = session.clone();
    *ts_ms = Some(1);
    let manifest = manifest.clone().expect("the fixture carries a manifest");
    {
        let mut wal = f.store.open(&session).unwrap();
        wal.append(1, &record, true).unwrap();
        wal.append(1, &WalRecord::StepStarted { step_id: StepId(1) }, true)
            .unwrap();
    }
    let path = f.store.wal_path(&session);
    let bytes = fs::read(&path).unwrap();
    let gate = br#"{"requires":["plan-manifest/v1"],"record":{"type":"run_started""#;
    assert!(bytes.windows(gate.len()).any(|window| window == gate));

    let report = f.fold();
    assert_eq!(
        report.removed, 1,
        "a manifest session must fold: {report:?}"
    );
    assert!(!report.skipped.contains_key("newer_writer"));
    let journal = report.journal.unwrap();
    let member = ArchiveReader::open(
        f.root
            .join("wal-archive")
            .join(&journal)
            .with_extension("ark"),
        MAX_BYTES,
    )
    .unwrap()
    .read_member(&session_addr(&session))
    .unwrap()
    .unwrap();
    assert_eq!(member.payload, bytes, "the archived member changed the WAL");

    let report = run(
        &f.root,
        &FoldOptions {
            write: true,
            unfold: true,
            resume: Some(journal),
            ..Default::default()
        },
        START + 1,
        &mut |_| Ok(()),
    )
    .unwrap();
    assert_eq!(report.restored, 1);
    assert_eq!(fs::read(&path).unwrap(), bytes, "unfold changed the WAL");

    let report = f.fold_at(START + 2);
    assert_eq!(report.removed, 1);
    assert!(!path.exists());
    drop(f.store.open(&session).unwrap());
    assert_eq!(
        fs::read(&path).unwrap(),
        bytes,
        "extraction changed the WAL"
    );
    let records = f.store.read_records(&session).unwrap();
    match &records[0].record {
        WalRecord::RunStarted {
            manifest: Some(read),
            ..
        } => assert_eq!(read, &manifest),
        other => panic!("expected RunStarted with its manifest, got {other:?}"),
    }
}

/// A cold session holding a steered message folds like any other: the fold decodes the gated
/// `steer-queue/v1` frame rather than stopping at it, archives the bytes unchanged, and opening
/// the folded session reads the steer back with its message intact.
#[test]
fn a_steered_session_folds_and_extracts_with_its_gate() {
    let f = Fixture::new();
    let (session, _) = f.session("steered", Some(1));
    let message = broca_protocol::content::Message::new(
        broca_protocol::content::Role::User,
        vec![broca_protocol::content::ContentBlock::Text {
            text: "write up now".into(),
        }],
    );
    {
        let mut wal = f.store.open(&session).unwrap();
        wal.append(
            1,
            &WalRecord::Steered {
                retention: None,
                ts_ms: None,
                title: None,
                send_id: Some("steer-1".into()),
                request_identity_digest: None,
                delivery: Some(broca_protocol::Delivery::Steer),
                input: vec![message.clone()].into(),
                hooks: None,
            },
            true,
        )
        .unwrap();
    }
    let bytes = fs::read(f.store.wal_path(&session)).unwrap();
    let gate = br#"{"requires":["steer-queue/v1"],"record":{"type":"steered""#;
    assert!(bytes.windows(gate.len()).any(|window| window == gate));

    let report = f.fold();
    assert_eq!(report.removed, 1, "a steered session must fold: {report:?}");
    assert!(!report.skipped.contains_key("newer_writer"));
    let journal = report.journal.unwrap();
    let member = ArchiveReader::open(
        f.root
            .join("wal-archive")
            .join(&journal)
            .with_extension("ark"),
        MAX_BYTES,
    )
    .unwrap()
    .read_member(&session_addr(&session))
    .unwrap()
    .unwrap();
    assert_eq!(member.payload, bytes, "the archived member keeps the gate");

    drop(f.store.open(&session).unwrap());
    let records = f.store.read_records(&session).unwrap();
    match &records[2].record {
        WalRecord::Steered { input, .. } => assert_eq!(input.as_ref(), &vec![message]),
        other => panic!("expected Steered, got {other:?}"),
    }
}

/// A cold session holding a tool dispatch intent that names its module folds like any other:
/// the fold decodes the gated `dispatch-module/v1` frame rather than stopping at it, archives
/// the bytes unchanged, and opening the folded session reads the module and pin back intact.
#[test]
fn a_dispatch_module_session_folds_and_extracts_with_its_gate() {
    let f = Fixture::new();
    let (session, _) = f.session("dispatch-module", Some(1));
    let intent: WalRecord = serde_json::from_str(
        r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t1","tool_name":"edit","args":{"f":"a"},"module_id":"aft","schema_pin":{"schema_digest":"sha256:5f2c","semantics":2}}"#,
    )
    .unwrap();
    {
        let mut wal = f.store.open(&session).unwrap();
        wal.append(1, &intent, true).unwrap();
    }
    let bytes = fs::read(f.store.wal_path(&session)).unwrap();
    let gate = br#"{"requires":["dispatch-module/v1"],"record":{"type":"tool_dispatch_intent""#;
    assert!(bytes.windows(gate.len()).any(|window| window == gate));

    let report = f.fold();
    assert_eq!(
        report.removed, 1,
        "a dispatch-module session must fold: {report:?}"
    );
    assert!(!report.skipped.contains_key("newer_writer"));
    let journal = report.journal.unwrap();
    let member = ArchiveReader::open(
        f.root
            .join("wal-archive")
            .join(&journal)
            .with_extension("ark"),
        MAX_BYTES,
    )
    .unwrap()
    .read_member(&session_addr(&session))
    .unwrap()
    .unwrap();
    assert_eq!(member.payload, bytes, "the archived member keeps the gate");

    drop(f.store.open(&session).unwrap());
    assert_eq!(
        fs::read(f.store.wal_path(&session)).unwrap(),
        bytes,
        "extraction changed the WAL"
    );
    let records = f.store.read_records(&session).unwrap();
    assert_eq!(records.last().map(|framed| &framed.record), Some(&intent));
}

#[test]
fn pre_schema_without_authoritative_identity_is_refused() {
    let f = Fixture::new();
    let (session, bytes) = f.session("pre-schema-orphan", None);
    let addr = "1111111111111111";
    let path = f.root.join("wal").join(format!("{addr}.wal"));
    fs::rename(f.store.wal_path(&session), &path).unwrap();
    let report = f.fold();
    assert_eq!(report.skipped["no_authoritative_identity"], [addr]);
    assert!(report.cold_pre_schema.is_empty());
    assert_eq!(report.removed, 0);
    assert_eq!(fs::read(path).unwrap(), bytes);
}

#[test]
fn pre_schema_store_identity_still_resolves_rekeyed_provenance() {
    let f = Fixture::new();
    let (embedded, _) = f.session("legacy-rekeyed", None);
    let resolved = BindIdentity::new("/fold-test", "broca", "legacy-rekeyed");
    let addr = session_addr(&resolved);
    fs::rename(f.store.wal_path(&embedded), f.store.wal_path(&resolved)).unwrap();
    let report = run_with_identities(
        &f.root,
        &FoldOptions {
            write: true,
            ..Default::default()
        },
        START,
        &mut |_| Ok(()),
        &BTreeMap::from([(addr.clone(), resolved)]),
    )
    .unwrap();
    assert_eq!(report.resolved_from_store, std::slice::from_ref(&addr));
    assert_eq!(report.cold_pre_schema, [addr]);
    assert_eq!(report.removed, 1);
}

fn append_fold_test_finish(f: &Fixture, session: &BindIdentity, ts_ms: u64, torn: bool) {
    let mut frame = broca_wal::framing::encode(
        3,
        1,
        &WalRecord::RunFinished {
            ts_ms: Some(ts_ms),
            reason: broca_protocol::RunFinishReason::Completed,
            usage: Default::default(),
            error: None,
            retries_used: 0,
        },
    )
    .unwrap();
    if torn {
        // Tear the lifecycle record itself, not FileWal's trailing lineage copy.
        frame.pop().unwrap();
        assert!(matches!(
            broca_wal::framing::decode_next(&frame),
            broca_wal::framing::DecodeStep::TornTail
        ));
    }
    let path = f.store.wal_path(session);
    let mut bytes = fs::read(&path).unwrap();
    bytes.extend(frame);
    fs::write(path, bytes).unwrap();
}

#[test]
fn pre_schema_head_with_later_warm_timestamp_is_warm() {
    let f = Fixture::new();
    let (session, _) = f.session("resumed-pre-schema", None);
    append_fold_test_finish(&f, &session, START, false);
    let report = f.fold();
    assert_eq!(report.skipped["warm"], [session_addr(&session)]);
    assert!(report.cold_pre_schema.is_empty());
    assert_eq!(report.removed, 0);
}

#[test]
fn pre_schema_torn_newer_timestamp_leaves_age_unproven() {
    let f = Fixture::new();
    let (session, _) = f.session("torn-resumed-pre-schema", None);
    append_fold_test_finish(&f, &session, START, true);
    let bytes = fs::read(f.store.wal_path(&session)).unwrap();
    let report = f.fold();
    assert_eq!(
        report.skipped["age_unproven_torn_tail"],
        [session_addr(&session)]
    );
    assert!(report.cold_pre_schema.is_empty());
    assert_eq!(report.removed, 0);
    assert_eq!(fs::read(f.store.wal_path(&session)).unwrap(), bytes);
}

#[test]
fn timestamped_heads_with_torn_tails_keep_ordinary_age_rules() {
    let f = Fixture::new();
    let (warm, _) = f.session("torn-warm", Some(START));
    let (cold, _) = f.session("torn-cold", Some(1));
    append_fold_test_finish(&f, &warm, START, true);
    append_fold_test_finish(&f, &cold, START, true);
    let report = f.fold();
    assert_eq!(report.skipped["warm"], [session_addr(&warm)]);
    assert_eq!(report.candidates, [session_addr(&cold)]);
    assert!(report.cold_pre_schema.is_empty());
    assert_eq!(report.removed, 1);
}

#[test]
fn store_identity_without_opening_run_cannot_prove_pre_schema_age() {
    let f = Fixture::new();
    let session = BindIdentity::new("/fold-test", "runner", "no-opening-run");
    {
        let mut wal = f.store.open(&session).unwrap();
        wal.append(1, &WalRecord::StepStarted { step_id: StepId(1) }, true)
            .unwrap();
    }
    let addr = session_addr(&session);
    let report = run_with_identities(
        &f.root,
        &FoldOptions {
            write: true,
            ..Default::default()
        },
        START,
        &mut |_| Ok(()),
        &BTreeMap::from([(addr.clone(), session)]),
    )
    .unwrap();
    assert_eq!(report.skipped["age_unproven_without_run_start"], [addr]);
    assert!(report.cold_pre_schema.is_empty());
    assert_eq!(report.removed, 0);
}

/// The fold raises the WAL feature floor with `archive-index/v2` before its seal writes a
/// v2 index, durably, so placement refuses any build that cannot read the container. When
/// the floor cannot be raised the seal never happens: the pass fails with the container
/// unsealed and every live WAL untouched.
#[test]
fn fold_raises_the_archive_index_floor_before_its_first_v2_seal() {
    let f = Fixture::new();
    let (cold, _) = f.session("cold", Some(1));
    let floor = f.root.join(broca_wal::FEATURE_FLOOR_FILENAME);
    assert!(!broca_wal::read_floor(&floor)
        .unwrap()
        .contains(broca_wal::ARCHIVE_INDEX_V2_FEATURE));
    let report = f.fold();
    assert_eq!(report.removed, 1);
    assert!(broca_wal::read_floor(&floor)
        .unwrap()
        .contains(broca_wal::ARCHIVE_INDEX_V2_FEATURE));
    let ark = f
        .root
        .join("wal-archive")
        .join(report.journal.unwrap())
        .with_extension("ark");
    let mut reader = ArchiveReader::open(&ark, MAX_BYTES).unwrap();
    let indexed = reader
        .verified_members()
        .unwrap()
        .expect("a v2 index that verifies");
    assert_eq!(indexed.len(), 1);
    assert_eq!(indexed[0].session_addr, session_addr(&cold));

    let g = Fixture::new();
    let (session, bytes) = g.session("cold", Some(1));
    // A directory at the floor file's path makes raising the floor fail.
    fs::create_dir_all(g.root.join(broca_wal::FEATURE_FLOOR_FILENAME)).unwrap();
    let error = run(
        &g.root,
        &FoldOptions {
            write: true,
            ..Default::default()
        },
        START,
        &mut |_| Ok(()),
    )
    .unwrap_err();
    assert!(!error.to_string().is_empty());
    let containers = fs::read_dir(g.root.join("wal-archive"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|e| e == "ark"))
        .collect::<Vec<_>>();
    assert_eq!(containers.len(), 1);
    assert_eq!(
        ArchiveReader::open(&containers[0], MAX_BYTES)
            .unwrap()
            .state()
            .unwrap(),
        ContainerState::Unsealed
    );
    assert_eq!(fs::read(g.store.wal_path(&session)).unwrap(), bytes);
}

/// Fold three sessions, then damage the container under its intact, verified index.
fn folded_with_damaged_second_header(
    damage: impl FnOnce(&mut [u8], usize),
) -> (Fixture, String, Vec<PathBuf>) {
    let f = Fixture::new();
    let sessions = ["a", "b", "c"].map(|name| f.session(name, Some(1)));
    let report = f.fold();
    assert_eq!(report.removed, 3);
    let journal = report.journal.unwrap();
    let ark = f
        .root
        .join("wal-archive")
        .join(&journal)
        .with_extension("ark");
    let indexed = ArchiveReader::open(&ark, MAX_BYTES)
        .unwrap()
        .verified_members()
        .unwrap()
        .unwrap();
    let mut bytes = fs::read(&ark).unwrap();
    damage(&mut bytes, indexed[1].offset as usize);
    fs::write(&ark, bytes).unwrap();
    let live = sessions
        .iter()
        .map(|(session, _)| f.store.wal_path(session))
        .collect();
    (f, journal, live)
}

fn unfold_pass(f: &Fixture, journal: &str) -> io::Result<FoldReport> {
    run(
        &f.root,
        &FoldOptions {
            write: true,
            unfold: true,
            resume: Some(journal.to_owned()),
            ..Default::default()
        },
        START + 1,
        &mut |_| Ok(()),
    )
}

/// The header walk stops at the first damaged member magic and used to let unfold skip
/// every member after it without a word. Enumerated from the verified index, the damaged
/// member is read and refused instead.
#[test]
fn unfold_of_a_verified_container_does_not_skip_members_past_damaged_magic() {
    let (f, journal, live) = folded_with_damaged_second_header(|bytes, header| bytes[header] ^= 1);
    let error = unfold_pass(&f, &journal).unwrap_err();
    assert_eq!(error.to_string(), "invalid archive member header");
    assert!(!live[1].exists(), "a damaged member must not be restored");
}

/// A header the walk reads that names a different member than the verified index at the
/// same offset is a contradiction, and unfold refuses the container outright.
#[test]
fn unfold_refuses_a_verified_index_that_disagrees_with_a_header() {
    let (f, journal, live) = folded_with_damaged_second_header(|bytes, header| {
        bytes[header + 8..header + 24].copy_from_slice(b"ffffffffffffffff")
    });
    let error = unfold_pass(&f, &journal).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("its index disagrees with its member headers"),
        "{error}"
    );
    assert!(live.iter().all(|path| !path.exists()));
}

#[cfg(unix)]
#[test]
fn state_root_artifacts_are_private_and_mode_walker_detects_public_files() {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

    let f = Fixture::new();
    fs::set_permissions(f.root.join("wal"), fs::Permissions::from_mode(0o700)).unwrap();
    let leases = f.root.join("leases");
    fs::DirBuilder::new().mode(0o700).create(&leases).unwrap();
    let (session, _) = f.session("private-mode", Some(1));
    let report = f.fold();
    assert_eq!(report.removed, 1, "real WAL fold must create an archive");
    let journal = report.journal.unwrap();
    let _ = run(
        &f.root,
        &FoldOptions {
            write: true,
            unfold: true,
            resume: Some(journal),
            ..Default::default()
        },
        START + 1,
        &mut |_| Ok(()),
    )
    .unwrap();
    let _ = f.fold_at(START + 2);
    drop(f.store.open(&session).unwrap());
    assert!(
        broca_test_support::public_tree_entries(&f.root)
            .unwrap()
            .is_empty(),
        "state-root entries with group/world permissions: {:?}",
        broca_test_support::public_tree_entries(&f.root).unwrap()
    );

    let planted = f.root.join("planted-0644");
    fs::write(&planted, b"positive control").unwrap();
    fs::set_permissions(&planted, fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(
        broca_test_support::public_tree_entries(&f.root).unwrap(),
        vec!["planted-0644"],
        "walker must name a deliberately public file"
    );
}

#[test]
fn move_sidecars_survive_fold_and_never_enter_archives() {
    for extension in ["moving", "moved"] {
        let f = Fixture::new();
        let (session, bytes) = f.session("protected", Some(1));
        let path = f.store.wal_path(&session);
        let sidecar = path.with_extension(extension);
        if extension == "moving" {
            broca_wal::migration::freeze(
                &f.root,
                &path,
                &broca_wal::migration::Freeze {
                    cut_id: "cut".into(),
                    end_offset: bytes.len() as u64,
                    lineage_id: "lineage".into(),
                },
            )
            .unwrap()
            .unwrap();
        } else {
            broca_wal::migration::atomic_json(
                &f.root,
                &sidecar,
                &broca_wal::migration::Moved {
                    cut_id: "cut".into(),
                    holder: "remote".into(),
                    lineage_id: "lineage".into(),
                },
            )
            .unwrap();
        }
        let sidecar_before = fs::read(&sidecar).unwrap();
        assert_eq!(f.fold().removed, 0);
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(fs::read(sidecar).unwrap(), sidecar_before);
    }
}

#[test]
fn fold_fences_only_refs_named_by_pending_activation() {
    let f = Fixture::new();
    let (protected, bytes) = f.session("protected", Some(1));
    let (other, _) = f.session("other", Some(1));
    let reference = cortexkit_role_llm_runner::session_ref(
        &protected.harness,
        &protected.project_root.to_string_lossy(),
        &protected.session,
    );
    let authority = f.root.join("agent-data/cuts/cut.json");
    broca_wal::migration::atomic_json(&f.root, &authority, &serde_json::json!({"refs":[{"ref_kind":"broca-session","ref":reference}],"intent":{"op":"activate","request":{}}})).unwrap();
    let before = fs::read(&authority).unwrap();
    assert_eq!(f.fold().removed, 1);
    assert_eq!(fs::read(f.store.wal_path(&protected)).unwrap(), bytes);
    assert!(!f.store.wal_path(&other).exists());
    assert_eq!(fs::read(authority).unwrap(), before);
}

#[test]
fn moved_sidecar_hides_archived_session_without_restoring_it() {
    let f = Fixture::new();
    let (session, bytes) = f.session("hidden-archive", Some(1));
    assert_eq!(f.fold().removed, 1);
    let path = f.store.wal_path(&session);
    broca_wal::migration::atomic_json(
        &f.root,
        &path.with_extension("moved"),
        &broca_wal::migration::Moved {
            cut_id: "cut".into(),
            holder: "remote".into(),
            lineage_id: "lineage".into(),
        },
    )
    .unwrap();
    assert!(matches!(
        f.store.read_records(&session),
        Err(broca_wal::WalError::SessionMoved { .. })
    ));
    assert!(matches!(
        f.store.open(&session),
        Err(broca_wal::WalError::SessionMoved { .. })
    ));
    assert_eq!(f.store.bytes_for_move(&session).unwrap(), Some(bytes));
    assert!(!path.exists());
}
