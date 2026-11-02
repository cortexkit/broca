// Portable libtest matrix: one real transport capture and one immutable set of
// recorded histories per process. Linux preflight enables this feature; native
// release lanes do not. All shards must share the captured histories so the
// coverage check can verify that their assigned image sets are disjoint and complete.

use broca_crash_harness::power_loss::{Family, RecordingFs, State};
use broca_wal::{FileWal, PreparedV4Frame, Wal, WalRecord};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

const SHARDS: usize = 1024;
const CUTS_PER_ENUMERATION_GROUP: usize = 8;
const INTERVALS: [(&str, usize, usize); 5] = [
    ("before_first_send", 0, 1),
    ("send_to_outcome", 1, 2),
    ("refusal_to_resend", 1, 3),
    ("resend_to_outcome", 3, 4),
    ("outcome_to_terminal", 3, 5),
];

struct History {
    phase: &'static str,
    recording: Arc<RecordingFs>,
    cut_groups: Vec<Range<usize>>,
}

struct Input {
    phase: &'static str,
    cut: usize,
    family: Family,
    state: State,
    reserved: usize,
}

struct Fixture {
    histories: Vec<History>,
    inputs: Vec<Input>,
    first_cuts: usize,
}

fn partition_cuts(count: usize) -> Vec<Range<usize>> {
    (0..count)
        .step_by(CUTS_PER_ENUMERATION_GROUP)
        .map(|start| start..(start + CUTS_PER_ENUMERATION_GROUP).min(count))
        .collect()
}

fn assert_cut_partition(count: usize, groups: &[Range<usize>]) {
    assert!(count > 0 && !groups.is_empty());
    let mut coverage = vec![0usize; count];
    for group in groups {
        assert!(group.start < group.end && group.end <= count);
        for cut in group.clone() {
            coverage[cut] += 1;
        }
    }
    assert!(
        coverage.iter().all(|count| *count == 1),
        "operation cuts must be complete and disjoint: {coverage:?}"
    );
}

fn assert_registered_shards(input_count: usize) {
    assert!(
        input_count >= SHARDS,
        "every shard must own at least one history; inputs={input_count}, shards={SHARDS}"
    );
    let mut registered: Vec<_> = SHARD_BASES
        .iter()
        .flat_map(|base| SHARD_OFFSETS.iter().map(move |offset| base + offset))
        .collect();
    registered.sort_unstable();
    assert_eq!(
        registered,
        (0..SHARDS).collect::<Vec<_>>(),
        "forgotten or overlapping registered shard"
    );
    let mut coverage = vec![0usize; input_count];
    for shard in registered {
        let mut count = 0;
        for input in (shard..input_count).step_by(SHARDS) {
            coverage[input] += 1;
            count += 1;
        }
        assert!(count > 0);
    }
    assert!(
        coverage.iter().all(|count| *count == 1),
        "input histories must be complete and disjoint"
    );
}

fn fixture() -> &'static Arc<Fixture> {
    static FIXTURE: OnceLock<Result<Arc<Fixture>, String>> = OnceLock::new();
    let fixture = FIXTURE.get_or_init(|| {
        assert!(std::env::var_os("NEXTEST_RUN_ID").is_none(),
            "portable power-loss shards require shared-process libtest; run the Linux preflight feature, not isolated nextest unit processes");
        std::thread::spawn(|| tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(capture_fixture())).join().map_err(|payload| {
                payload.downcast_ref::<String>().cloned().or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                    .unwrap_or_else(|| "shared capture failed".into())
            })
    });
    fixture
        .as_ref()
        .unwrap_or_else(|error| panic!("shared portable fixture: {error}"))
}

async fn capture_fixture() -> Arc<Fixture> {
    let server = ScriptedServer::start(vec![
        ScriptResponse::Http {
            status: 415,
            headers: vec![],
            body: b"",
        },
        ScriptResponse::Http {
            status: 400,
            headers: vec![],
            body: b"invalid",
        },
    ])
    .await;
    let address: std::net::SocketAddr =
        server.url.strip_prefix("http://").unwrap().parse().unwrap();
    let client = ProviderHttpClient::resolving_anthropic_api_to(address, true);
    let mut frozen = crate::families::anthropic_frozen_config("m");
    frozen.base_url = Some(format!("http://api.anthropic.com:{}", address.port()));
    let provider = LiveProvider::with_client_and_clock(
        client,
        LargeAttemptFamily,
        frozen,
        AuthHandle::None,
        Clock::default(),
    );
    let root = attempt_scratch("portable-attempt-capture");
    let capture = root.join("capture");
    std::fs::create_dir_all(&capture).unwrap();
    let recording = Arc::new(RecordingFs::new(&capture).unwrap());
    let frames = run_recorded_attempt(&capture, &provider, Default::default(), recording).await;
    assert_eq!(assert_attempts_settled_or_superseded(&frames), 2);
    let records: Vec<_> = frames
        .into_iter()
        .map(|frame| frame.record)
        .filter(|record| {
            matches!(
                record,
                WalRecord::AttemptReserved { .. }
                    | WalRecord::ModelAttemptFinished {
                        attempt_id: Some(_),
                        ..
                    }
                    | WalRecord::RunFinished { .. }
            )
        })
        .collect();
    assert_eq!(records.len(), 5);
    let log = log_path();
    let mut fixture = Fixture {
        histories: Vec::new(),
        inputs: Vec::new(),
        first_cuts: 0,
    };
    let mut phases = BTreeSet::new();
    for (phase, prefix, end) in INTERVALS {
        let phase_root = root.join(phase);
        std::fs::create_dir_all(phase_root.join("wal")).unwrap();
        broca_wal::floor::raise_floor(
            &phase_root.join(broca_wal::FEATURE_FLOOR_FILENAME),
            broca_wal::framing::KNOWN_FEATURES,
        )
        .unwrap();
        let previous_writer_id = {
            let mut wal = FileWal::open_with_recovery(phase_root.join(&log), broca_wal::WalConfig::OVERHAUL, phase_root.join(broca_wal::FEATURE_FLOOR_FILENAME))
                .unwrap()
                .with_feature_floor(phase_root.join(broca_wal::FEATURE_FLOOR_FILENAME))
                .with_wal_config(broca_wal::WalConfig::OVERHAUL)
                .unwrap();
            let id = wal.writer_id().unwrap();
            let record = WalRecord::WriterOpened {
                writer_id: id,
                previous_writer_id: None,
                recovered_through_seq: 0,
                cause: "test".into(),
            };
            wal.append_v4(
                1,
                &record,
                true,
                PreparedV4Frame::prepare(&record).unwrap(),
                0,
            )
            .unwrap();
            id
        };
        let recording = Arc::new(RecordingFs::new(&phase_root).unwrap());
        broca_wal::fs_ops::with_fs_ops(recording.clone(), || {
            let mut wal = FileWal::open_with_recovery(phase_root.join(&log), broca_wal::WalConfig::OVERHAUL, phase_root.join(broca_wal::FEATURE_FLOOR_FILENAME))
                .unwrap()
                .with_feature_floor(phase_root.join(broca_wal::FEATURE_FLOOR_FILENAME))
                .with_wal_config(broca_wal::WalConfig::OVERHAUL)
                .unwrap();
            let opened = WalRecord::WriterOpened {
                writer_id: wal.writer_id().unwrap(),
                previous_writer_id: Some(previous_writer_id),
                recovered_through_seq: wal.last_seq(),
                cause: "test".into(),
            };
            wal.append_v4(
                1,
                &opened,
                true,
                PreparedV4Frame::prepare(&opened).unwrap(),
                1,
            )
            .unwrap();
            for (index, record) in records[..end].iter().enumerate() {
                let sync = index < prefix
                    || index + 1 == end
                        && matches!(
                            record,
                            WalRecord::AttemptReserved { .. } | WalRecord::RunFinished { .. }
                        );
                wal.append_v4(
                    1,
                    record,
                    sync,
                    PreparedV4Frame::prepare(record).unwrap(),
                    2 + index as u64,
                )
                .unwrap();
            }
        });
        let count = recording.operation_kinds().len() + 1;
        let groups = partition_cuts(count);
        assert_cut_partition(count, &groups);
        let mut seen = BTreeSet::new();
        for group in &groups {
            for cut in group.clone() {
                let states = recording.enumerate_cut(cut).unwrap_or_else(|error| {
                    panic!("first-cut phase={phase} operation={cut}: {error}")
                });
                assert!(!states.is_empty());
                fixture.first_cuts += states.len();
                for state in states {
                    if !seen.insert(state.state.clone()) {
                        continue;
                    }
                    let bytes = state
                        .state
                        .files
                        .get(&log)
                        .map(Vec::as_slice)
                        .unwrap_or_default();
                    let raw = raw_attempt_frames(bytes);
                    let reserved = raw
                        .iter()
                        .filter(|f| matches!(f.record, WalRecord::AttemptReserved { .. }))
                        .count();
                    let finished = raw
                        .iter()
                        .filter(|f| {
                            matches!(
                                f.record,
                                WalRecord::ModelAttemptFinished {
                                    attempt_id: Some(_),
                                    ..
                                }
                            )
                        })
                        .count();
                    let terminal = raw
                        .iter()
                        .any(|f| matches!(f.record, WalRecord::RunFinished { .. }));
                    phases.insert((reserved, finished, terminal));
                    fixture.inputs.push(Input {
                        phase,
                        cut,
                        family: state.family,
                        state: state.state,
                        reserved,
                    });
                }
            }
        }
        println!("portable source phase={phase} recorded_operations={} enumerated_cuts={count} distinct_inputs={}", count - 1, seen.len());
        fixture.histories.push(History {
            phase,
            recording,
            cut_groups: groups,
        });
    }
    assert!(
        phases.contains(&(1, 0, false)),
        "cut after reservation, before outcome"
    );
    assert!(
        phases.contains(&(1, 1, false)),
        "cut between refusal outcome and resend reservation"
    );
    assert!(
        phases.contains(&(2, 1, false)),
        "cut before resend and before its outcome"
    );
    assert!(
        phases.contains(&(2, 2, false)),
        "cut between outcomes and terminal"
    );
    assert!(fixture.first_cuts > 0 && !fixture.inputs.is_empty());
    println!(
        "portable shared capture: histories={} first_cut_states={} distinct_inputs={}",
        fixture.histories.len(),
        fixture.first_cuts,
        fixture.inputs.len()
    );
    assert_registered_shards(fixture.inputs.len());
    // Recorded histories own their initial images and operation bytes; no query
    // reads these real source files again after capture completes.
    std::fs::remove_dir_all(root).unwrap();
    Arc::new(fixture)
}

fn log_path() -> PathBuf {
    PathBuf::from("wal").join(format!(
        "{}.wal",
        broca_core::wal_writer::session_addr(&attempt_session())
    ))
}

fn recovery_phase(
    state: &State,
    log: &std::path::Path,
    original: &[u8],
    retained: usize,
) -> &'static str {
    let bytes = state.files.get(log).map(Vec::as_slice).unwrap_or_default();
    if raw_attempt_frames(bytes)
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::RecoveryCut { .. }))
    {
        return "marker_present";
    }
    if bytes != original {
        return if bytes.len() == retained {
            "truncate"
        } else {
            "marker_write"
        };
    }
    let paths: Vec<_> = state
        .files
        .keys()
        .map(|path| path.to_string_lossy())
        .collect();
    if paths
        .iter()
        .any(|path| path.contains(".cut-") && path.ends_with(".json.tmp"))
    {
        return "manifest_write";
    }
    if paths
        .iter()
        .any(|path| path.contains(".cut-") && path.ends_with(".json"))
    {
        return "manifest_published";
    }
    if paths
        .iter()
        .any(|path| path.contains(".cut-") && path.ends_with(".tmp"))
    {
        return "sidecar_write";
    }
    if paths.iter().any(|path| path.contains(".cut-")) {
        return "sidecar_published";
    }
    "scan_or_floor"
}

async fn run_shard(shard: usize) {
    let wall = Instant::now();
    let fixture = fixture();
    let initialization_ms = wall.elapsed().as_millis();
    let root = attempt_scratch(&format!("portable-power-loss-shard-{shard:04}"));
    let log = log_path();
    let mut histories = 0;
    let mut second_cuts = 0;
    let mut unique_second = 0;
    let mut phases = BTreeMap::<&str, (usize, Duration)>::new();
    for index in (shard..fixture.inputs.len()).step_by(SHARDS) {
        histories += 1;
        let input = &fixture.inputs[index];
        let original = input
            .state
            .files
            .get(&log)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let first_root = root.join(format!("first-{index}"));
        input.state.install(&first_root).unwrap();
        let recording = Arc::new(RecordingFs::new(&first_root).unwrap());
        let retained = broca_wal::fs_ops::with_fs_ops(recording.clone(), || {
            let wal = FileWal::open_with_recovery(first_root.join(&log), broca_wal::WalConfig::OVERHAUL, first_root.join(broca_wal::FEATURE_FLOOR_FILENAME)).unwrap_or_else(|error| {
                panic!(
                    "phase={} input={index} operation={} family={:?}: {error:?}",
                    input.phase, input.cut, input.family
                )
            });
            let records = wal.replay().unwrap();
            records
                .iter()
                .find_map(|frame| match &frame.record {
                    WalRecord::RecoveryCut { recovery } => Some(recovery.cut_offset as usize),
                    _ => None,
                })
                .unwrap_or(original.len())
        });
        let count = recording.operation_kinds().len() + 1;
        let groups = partition_cuts(count);
        assert_cut_partition(count, &groups);
        let mut states = BTreeSet::new();
        let mut covered = Vec::new();
        for group in groups {
            for cut in group {
                let images = recording.enumerate_cut(cut).unwrap_or_else(|error| {
                    panic!(
                        "second-cut phase={} input={index} operation={cut}: {error}",
                        input.phase
                    )
                });
                assert!(!images.is_empty());
                second_cuts += images.len();
                covered.push(images[0].clone());
                states.extend(images.into_iter().map(|image| image.state));
            }
        }
        recording.assert_all_cuts_covered(&covered);
        unique_second += states.len();
        let recovered =
            reclaim_attempt_image(&first_root, Arc::new(broca_wal::fs_ops::RealFs)).await;
        assert_eq!(
            assert_attempts_settled_or_superseded(&recovered),
            input.reserved
        );
        for state in states {
            let phase = recovery_phase(&state, &log, original, retained);
            let started = Instant::now();
            let second_root = root.join("second");
            state.install(&second_root).unwrap();
            let recovered =
                reclaim_attempt_image(&second_root, Arc::new(broca_wal::fs_ops::RealFs)).await;
            assert_eq!(
                assert_attempts_settled_or_superseded(&recovered),
                input.reserved,
                "phase={} input={index} repair_phase={phase}",
                input.phase
            );
            std::fs::remove_dir_all(second_root).unwrap();
            let entry = phases.entry(phase).or_insert((0, Duration::ZERO));
            entry.0 += 1;
            entry.1 += started.elapsed();
        }
        std::fs::remove_dir_all(first_root).unwrap();
    }
    assert!(
        histories > 0 && second_cuts >= histories && unique_second > 0,
        "every shard must cover positive histories and states"
    );
    let dominant = phases
        .iter()
        .max_by_key(|(_, (_, elapsed))| *elapsed)
        .map(|(phase, _)| *phase)
        .unwrap();
    println!("portable power-loss shard={shard:04} wall_ms={} initialization_ms={initialization_ms} histories={histories} second_cuts={second_cuts} distinct_second={unique_second} dominant_operation={dominant} phases={phases:?}", wall.elapsed().as_millis());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn shard_plan_is_complete_and_disjoint_for_the_captured_histories() {
    let fixture = fixture();
    assert!(!fixture.histories.is_empty() && fixture.first_cuts > 0);
    for history in &fixture.histories {
        let count = history.recording.operation_kinds().len() + 1;
        assert_cut_partition(count, &history.cut_groups);
        assert!(!history.phase.is_empty());
    }
    assert_registered_shards(fixture.inputs.len());
}

macro_rules! define_shards {
    (bases: [$($module:ident => $base:expr),* $(,)?], leaves: [$($leaf:ident => $offset:expr),* $(,)?] $(,)?) => {
        const SHARD_BASES: &[usize] = &[$($base),*];
        const SHARD_OFFSETS: &[usize] = &[$($offset),*];
        macro_rules! emit_block {
            ($name:ident, $start:expr) => {
                mod $name {
                    use super::*;
                    $(#[tokio::test] async fn $leaf() { run_shard($start + $offset).await; })*
                }
            };
        }
        $(emit_block!($module, $base);)*
    };
}

define_shards! {
    bases: [
        s000 => 0, s001 => 16, s002 => 32, s003 => 48,
        s004 => 64, s005 => 80, s006 => 96, s007 => 112,
        s008 => 128, s009 => 144, s010 => 160, s011 => 176,
        s012 => 192, s013 => 208, s014 => 224, s015 => 240,
        s016 => 256, s017 => 272, s018 => 288, s019 => 304,
        s020 => 320, s021 => 336, s022 => 352, s023 => 368,
        s024 => 384, s025 => 400, s026 => 416, s027 => 432,
        s028 => 448, s029 => 464, s030 => 480, s031 => 496,
        s032 => 512, s033 => 528, s034 => 544, s035 => 560,
        s036 => 576, s037 => 592, s038 => 608, s039 => 624,
        s040 => 640, s041 => 656, s042 => 672, s043 => 688,
        s044 => 704, s045 => 720, s046 => 736, s047 => 752,
        s048 => 768, s049 => 784, s050 => 800, s051 => 816,
        s052 => 832, s053 => 848, s054 => 864, s055 => 880,
        s056 => 896, s057 => 912, s058 => 928, s059 => 944,
        s060 => 960, s061 => 976, s062 => 992, s063 => 1008,
    ],
    leaves: [
        c00 => 0, c01 => 1, c02 => 2, c03 => 3,
        c04 => 4, c05 => 5, c06 => 6, c07 => 7,
        c08 => 8, c09 => 9, c10 => 10, c11 => 11,
        c12 => 12, c13 => 13, c14 => 14, c15 => 15,
    ],
}
