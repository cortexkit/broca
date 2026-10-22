//! Read-only replay scan of a broca state root.
//!
//! For every session address under `wal/`, `wal-durable/`, `wal-ephemeral/` and the sealed
//! archive containers (`wal-archive/`, `wal-archive-durable/`), this reads the log a resume
//! would read (the live file when one exists, otherwise the newest archived member) and runs
//! the full replay a resume falls back to. It prints how many lineages replay, how many replay
//! refuses because a tool batch would replace an uncommitted batch holding call evidence
//! (with each address and the sequence number of the refusing record), how many it refuses for
//! any other reason (grouped by reason), and how many could not be read.
//!
//! Every file is opened read-only through [`broca_core::ReadOnlyWalReader`]; nothing is
//! repaired, truncated, locked or created.
//!
//! Usage: `cargo run -p broca-core --example replay-corpus-scan -- <state-root>`

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use broca_core::replay::OVERLAPPING_BATCH_REASON;
use broca_core::{replay, ReadOnlyWalReader, ReplayError};
use broca_wal::{WalError, WalRecord};

/// What one scan of a state root found.
#[derive(Debug, Default, PartialEq)]
struct Report {
    scanned: u64,
    replay_ok: u64,
    /// Address and the sequence number of the record replay refused at.
    overlapping_batch: Vec<(String, u64)>,
    /// Refusal reason to the addresses refused with it.
    other_refusals: BTreeMap<String, Vec<String>>,
    /// Address and the read error.
    unreadable: Vec<(String, String)>,
}

fn scan(state_root: &Path) -> Result<Report, WalError> {
    let reader = ReadOnlyWalReader::new(state_root);
    let mut report = Report::default();
    for address in reader.addresses()? {
        report.scanned += 1;
        let frames = match reader.records(&address) {
            Ok((frames, _)) => frames,
            Err(error) => {
                report.unreadable.push((address, error.to_string()));
                continue;
            }
        };
        let records: Vec<WalRecord> = frames.iter().map(|frame| frame.record.clone()).collect();
        match replay(&records) {
            Ok(_) => report.replay_ok += 1,
            Err(ReplayError::CorruptLineage { reason, .. })
                if reason == OVERLAPPING_BATCH_REASON =>
            {
                let at = refusing_record(&records);
                report.overlapping_batch.push((address, frames[at].seq));
            }
            Err(ReplayError::CorruptLineage { reason, .. }) => {
                report
                    .other_refusals
                    .entry(reason)
                    .or_default()
                    .push(address);
            }
            // Tag numbering that would overflow is also a refusal, grouped under its own reason.
            Err(error @ ReplayError::TagNumbering(_)) => {
                report
                    .other_refusals
                    .entry(error.to_string())
                    .or_default()
                    .push(address);
            }
        }
    }
    Ok(report)
}

/// The index of the record at which replay first refuses with the overlapping-batch reason.
/// Replay stops at the first record it refuses, so every prefix that includes that record is
/// refused the same way and every shorter one is not: a binary search over prefix lengths.
fn refusing_record(records: &[WalRecord]) -> usize {
    let refused = |len: usize| {
        matches!(replay(&records[..len]),
            Err(ReplayError::CorruptLineage { reason, .. }) if reason == OVERLAPPING_BATCH_REASON)
    };
    let (mut shortest_refused, mut longest_accepted) = (records.len(), 0);
    while shortest_refused - longest_accepted > 1 {
        let mid = longest_accepted + (shortest_refused - longest_accepted) / 2;
        if refused(mid) {
            shortest_refused = mid;
        } else {
            longest_accepted = mid;
        }
    }
    shortest_refused - 1
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "lineages_scanned={}", self.scanned)?;
        writeln!(f, "replay_ok={}", self.replay_ok)?;
        writeln!(
            f,
            "refused_overlapping_batch={}",
            self.overlapping_batch.len()
        )?;
        for (address, seq) in &self.overlapping_batch {
            writeln!(f, "  address={address} seq={seq}")?;
        }
        let other: usize = self.other_refusals.values().map(Vec::len).sum();
        writeln!(f, "refused_other={other}")?;
        for (reason, addresses) in &self.other_refusals {
            writeln!(f, "  count={} reason={reason:?}", addresses.len())?;
            for address in addresses {
                writeln!(f, "    address={address}")?;
            }
        }
        writeln!(f, "unreadable={}", self.unreadable.len())?;
        for (address, error) in &self.unreadable {
            writeln!(f, "  address={address} error={error:?}")?;
        }
        Ok(())
    }
}

fn main() {
    let Some(root) = std::env::args_os().nth(1) else {
        eprintln!("usage: replay-corpus-scan <state-root>");
        std::process::exit(2);
    };
    let root = Path::new(&root);
    match scan(root) {
        Ok(report) => {
            println!("state_root={}", root.display());
            print!("{report}");
        }
        Err(error) => {
            eprintln!("replay-corpus-scan: could not enumerate WAL addresses: {error}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_core::{session_addr, FileWalStore, WalStore};
    use broca_protocol::{
        content::{AssistantMessage, ContentBlock, Message, Role, ToolCall},
        request::ModelHandle,
        BatchId, BindIdentity, MessageId, RunId, StepId, ToolCallId, Usage,
    };
    use broca_wal::{RunConfig, RunOrigin};
    use std::path::PathBuf;
    use std::sync::Arc;

    fn started(run: &str, session: &BindIdentity) -> WalRecord {
        WalRecord::RunStarted {
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            ts_ms: None,
            run_id: RunId(run.into()),
            session: session.clone(),
            config: Box::new(RunConfig {
                turn_directives_v1: None,
                render_tags: false,
                computed_tags: None,
                model: ModelHandle::new("stub", "m"),
                tools: vec![].into(),
                tool_choice: Default::default(),
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
                stop_when: Vec::new(),
                context_management: None,
            }),
            input: vec![Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "task".into(),
                }],
            )]
            .into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        }
    }

    fn step(content: ContentBlock) -> WalRecord {
        WalRecord::ModelStepFinished {
            tags: None,
            hooks: None,
            step_id: StepId(1),
            assistant_message: Arc::new(AssistantMessage {
                message_id: MessageId("m".into()),
                content: vec![content],
            }),
            usage: Usage::default(),
            finish_reason: None,
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        }
    }

    fn batch_started() -> WalRecord {
        WalRecord::ToolBatchStarted {
            batch_id: BatchId("r-b1".into()),
            step_id: StepId(1),
            planned_calls: vec![ToolCallId("t1".into())],
        }
    }

    /// RunStarted, a tool step, its batch and an unresolved intent, then a second batch start:
    /// replay refuses at the sixth record.
    fn overlapping(session: &BindIdentity) -> Vec<WalRecord> {
        vec![
            started("r", session),
            WalRecord::StepStarted { step_id: StepId(1) },
            step(ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId("t1".into()),
                tool_name: "work".into(),
                input: serde_json::json!({}),
                thought_signature: None,
            })),
            batch_started(),
            WalRecord::ToolDispatchIntent {
                hooks: None,
                batch_id: BatchId("r-b1".into()),
                tool_call_id: ToolCallId("t1".into()),
                tool_name: Some("work".into()),
                args: Arc::new(serde_json::json!({})),
                module_id: None,
                schema_pin: None,
                approval_digest: None,
                call_key: None,
            },
            batch_started(),
        ]
    }

    fn write(store: &FileWalStore, session: &BindIdentity, records: &[WalRecord]) {
        let mut wal = store.open(session).unwrap();
        for record in records {
            wal.append(1, record, true).unwrap();
        }
    }

    type Snapshot = BTreeMap<PathBuf, (Vec<u8>, std::time::SystemTime, bool)>;

    fn snapshot(path: &Path, into: &mut Snapshot) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            let metadata = std::fs::metadata(&path).unwrap();
            if metadata.is_dir() {
                into.insert(
                    path.clone(),
                    (Vec::new(), metadata.modified().unwrap(), true),
                );
                snapshot(&path, into);
            } else {
                let bytes = std::fs::read(&path).unwrap();
                into.insert(path, (bytes, metadata.modified().unwrap(), false));
            }
        }
    }

    #[cfg(unix)]
    fn set_read_only(path: &Path, read_only: bool) {
        use std::os::unix::fs::PermissionsExt;
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                set_read_only(&path, read_only);
            } else {
                let mode = if read_only { 0o400 } else { 0o600 };
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            }
        }
        let mode = if read_only { 0o500 } else { 0o700 };
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    /// A scratch state root with one lineage per outcome: a healthy live WAL, an archived WAL
    /// whose replay hits the overlapping-batch refusal, a live WAL refused for another reason,
    /// and a live WAL whose first record is corrupt.
    #[test]
    fn scan_counts_each_outcome_and_changes_no_file() {
        let root = broca_test_support::scratch_root()
            .join(format!("replay-corpus-scan-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = FileWalStore::new(root.join("wal"));
        let healthy = BindIdentity::new("/scan", "test", "healthy");
        let archived = BindIdentity::new("/scan", "test", "overlap");
        let other = BindIdentity::new("/scan", "test", "unfinished-episode");
        let unreadable = BindIdentity::new("/scan", "test", "corrupt");

        write(
            &store,
            &healthy,
            &[
                started("h", &healthy),
                WalRecord::StepStarted { step_id: StepId(1) },
                step(ContentBlock::Text {
                    text: "done".into(),
                }),
            ],
        );
        write(&store, &archived, &overlapping(&archived));
        let bytes = store.bytes_for_move(&archived).unwrap().unwrap();
        std::fs::create_dir_all(root.join("wal-archive")).unwrap();
        let mut container =
            broca_wal::archive::ArchiveWriter::create(root.join("wal-archive/fold-1.ark"))
                .unwrap()
                .without_feature_floor();
        container.append(&session_addr(&archived), &bytes).unwrap();
        container.seal().unwrap();
        std::fs::remove_file(store.wal_path(&archived)).unwrap();
        // A new episode over one with no terminal: refused, but not for an overlapping batch.
        write(
            &store,
            &other,
            &[started("o1", &other), started("o2", &other)],
        );
        write(
            &store,
            &unreadable,
            &[
                started("u", &unreadable),
                WalRecord::StepStarted { step_id: StepId(1) },
            ],
        );
        // Corrupt a byte inside the first record's payload; the intact record after it makes
        // this a corrupt log rather than a torn tail.
        let path = store.wal_path(&unreadable);
        let mut corrupt = std::fs::read(&path).unwrap();
        let lineage_frame = broca_wal::framing::HEADER_LEN + 16;
        corrupt[lineage_frame + broca_wal::framing::HEADER_LEN + 10] ^= 0xff;
        std::fs::write(&path, corrupt).unwrap();

        let mut before = Snapshot::new();
        snapshot(&root, &mut before);
        #[cfg(unix)]
        set_read_only(&root, true);
        let report = scan(&root);
        #[cfg(unix)]
        set_read_only(&root, false);
        let report = report.unwrap();
        let mut after = Snapshot::new();
        snapshot(&root, &mut after);
        assert_eq!(before, after, "the scan changed the state root");

        assert_eq!(report.scanned, 4, "{report}");
        assert_eq!(report.replay_ok, 1, "{report}");
        assert_eq!(
            report.overlapping_batch,
            vec![(session_addr(&archived), 6)],
            "{report}"
        );
        assert_eq!(
            report.other_refusals.values().map(Vec::len).sum::<usize>(),
            1,
            "{report}"
        );
        assert_eq!(
            report.other_refusals.values().next(),
            Some(&vec![session_addr(&other)])
        );
        assert_eq!(report.unreadable.len(), 1, "{report}");
        assert_eq!(report.unreadable[0].0, session_addr(&unreadable));
        let _ = std::fs::remove_dir_all(root);
    }
}
