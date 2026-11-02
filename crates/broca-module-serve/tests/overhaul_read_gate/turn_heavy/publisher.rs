use super::*;
use broca_store::db::{LogLocation, OverhaulDb};
use broca_wal::Wal;

#[derive(Clone, Copy)]
struct Writes {
    logical: u64,
    physical: u64,
}

fn writes() -> Writes {
    let io = std::fs::read_to_string("/proc/self/io").expect("Linux process I/O counters");
    let value = |key: &str| {
        io.lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                (name == key).then(|| value.trim().parse::<u64>().unwrap())
            })
            .unwrap()
    };
    Writes {
        logical: value("wchar"),
        physical: value("write_bytes"),
    }
}

async fn publisher_case(steps: usize) {
    let template = partial_turn().await;
    let root = root(&format!("publisher-{steps}"));
    let bind = BindIdentity::new("/installed", "runner", format!("publisher-{steps}"));
    let db = OverhaulDb::open(&root).unwrap();
    let mut source = records(&template, steps, false);
    if let WalRecord::RunStarted {
        session,
        run_id,
        submission_id,
        ..
    } = &mut source[0]
    {
        *session = bind.clone();
        *run_id = RunId("turn-run".into());
        *submission_id = Some(broca_protocol::SubmissionId("sub-turn-run".into()));
    }
    let mut wal = broca_wal::FileWal::open_with_recovery(
        root.join("publisher.wal"),
        broca_wal::WalConfig::OVERHAUL,
        root.join("wal-feature-floor"),
    )
    .unwrap()
    .with_feature_floor(root.join("wal-feature-floor"));
    let initial_rows = db.publication_row_changes().unwrap();
    let mut durations = Vec::new();
    let mut logical = 0;
    let mut physical = 0;
    for record in source {
        let boundary = matches!(record, WalRecord::TurnFinished { .. });
        wal.append(1, &record, false).unwrap();
        if boundary {
            wal.sync().unwrap();
            let publication = wal.durable_publication().unwrap();
            let before = writes();
            let started = std::time::Instant::now();
            db.publish(
                &bind,
                &broca_core::agent_session_ref(&bind),
                LogLocation::Live,
                &publication,
            )
            .unwrap();
            durations.push(started.elapsed().as_secs_f64() * 1_000.0);
            let after = writes();
            logical += after.logical - before.logical;
            physical += after.physical - before.physical;
        }
    }
    assert_eq!(
        durations.len(),
        steps,
        "one real publication per complete tool turn"
    );
    let row_changes = db.publication_row_changes().unwrap() - initial_rows;
    let (seed_rows, seed_bytes): (u64, u64) = db
        .read_snapshot(|tx| {
            Ok((
                tx.query_row("SELECT count(*) FROM episode_seed", [], |r| r.get(0))?,
                tx.query_row(
                    "SELECT COALESCE(sum(length(CAST(continuation AS BLOB))),0) FROM episode_seed",
                    [],
                    |r| r.get(0),
                )?,
            ))
        })
        .unwrap();
    assert!(
        row_changes > 0,
        "publication writer must report its row changes"
    );
    assert_eq!(seed_rows, steps as u64);
    let total: f64 = durations.iter().sum();
    let window = (steps / 10).max(1);
    let first: f64 = durations[..window].iter().sum::<f64>() / window as f64;
    let last: f64 = durations[steps - window..].iter().sum::<f64>() / window as f64;
    let db_bytes = std::fs::metadata(root.join("broca.db")).unwrap().len();
    println!("PUBLISHER_GROWTH steps={steps} total_ms={total:.3} per_step_ms={:.3} first_window_ms={first:.3} last_window_ms={last:.3} logical_write_bytes={logical} physical_write_bytes={physical} row_changes={row_changes} seed_rows={seed_rows} seed_bytes={seed_bytes} database_bytes={db_bytes}", total / steps as f64);
    drop(wal);
    drop(db);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_publisher_growth_50_steps() {
    publisher_case(50).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_publisher_growth_200_steps() {
    publisher_case(200).await;
}

// A measurement, not a gate: on today's publisher it takes about 500 s, past the test
// runner's 300 s limit. Run it explicitly with `--run-ignored all` when measuring publication.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "measurement probe; about 500 s on today's publisher"]
async fn real_publisher_growth_1000_steps() {
    publisher_case(1_000).await;
}
