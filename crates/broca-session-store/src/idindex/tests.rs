use std::collections::{BTreeMap, BTreeSet};

use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::request::ModelHandle;
use futures::executor::block_on;
use serde_json::json;

use super::{record_ids, Entry, IdKind, IndexPages, IndexRoot, FANIN};
use crate::backend::{MemoryBackend, SessionAddr, SessionBackend};
use crate::codec::Id128;
use crate::frame::FrameKey;
use crate::head::SessionTriple;
use crate::keys::DeploymentKey;
use crate::record::{
    Event, Input, Placement, QueuedSend, Record, RunEnd, RunStart, SessionRefresh, Steer,
};
use crate::session::{Session, GRACE_MS};

/// A deterministic generator, so a failure replays.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 11
    }
}

/// A uniformly spread hash for id number `n`, as `id_hash` makes.
fn hash(n: u64) -> Id128 {
    let digest = crate::prune::keyed_digest(&[3; 32], &n.to_le_bytes());
    Id128(digest.0[..16].try_into().unwrap())
}

fn model() -> ModelHandle {
    serde_json::from_value(json!({"provider_module_id": "p", "model_id": "m"})).unwrap()
}

fn start(run: &str, send: Option<&str>) -> Record {
    Record::RunStart(Box::new(RunStart {
        run_id: run.into(),
        model: model(),
        setup: Vec::new(),
        renderer_version: 1,
        submission_id: Some(format!("sub-{run}")),
        send_id: send.map(str::to_owned),
        request_identity_digest: None,
        ts_ms: None,
        written_at_ms: None,
        session_class: None,
        retention_secs: None,
        title: None,
        context_limit: None,
        service_tier: None,
        route_decision_id: None,
        compaction_plan_version: None,
        render_config: None,
        auth_selection: None,
        run_choices: None,
        origin: None,
        principal: None,
        scope: None,
        hooks: None,
        keep_warm: None,
        on_restart: None,
        seeded: None,
        refusal_schedule: None,
    }))
}

fn end(run: &str) -> Record {
    Record::RunEnd(RunEnd {
        run_id: run.into(),
        reason: RunFinishReason::Completed,
        usage: None,
        ts_ms: None,
        retries_used: 0,
        interrupt_reason: None,
        error: None,
    })
}

/// Each record kind names the ids its fields hold, under the right kind, and no others.
#[test]
fn records_name_their_ids() {
    let ids = |record: &Record| -> BTreeSet<(IdKind, String)> {
        record_ids(record).into_iter().collect()
    };
    assert_eq!(
        ids(&start("r1", Some("send-1"))),
        BTreeSet::from([
            (IdKind::RunId, "r1".into()),
            (IdKind::SubmissionId, "sub-r1".into()),
            (IdKind::SendId, "send-1".into()),
        ])
    );
    assert_eq!(
        ids(&end("r1")),
        BTreeSet::from([(IdKind::RunId, "r1".into())])
    );
    let steer = Record::Steer(Steer {
        input: Vec::new(),
        written_at_ms: None,
        tags_v1: None,
        receipt: Some(json!({"send_id": "steer-1", "delivery": "steer"})),
    });
    assert_eq!(
        ids(&steer),
        BTreeSet::from([(IdKind::SendId, "steer-1".into())])
    );
    let event = Record::Event(Event {
        name: "late_result".into(),
        run_id: Some("r1".into()),
        body: Some(json!({"call_key": "lin:7", "send_id": "flush-1", "other": "x"})),
    });
    assert_eq!(
        ids(&event),
        BTreeSet::from([
            (IdKind::CallKey, "lin:7".into()),
            (IdKind::SendId, "flush-1".into()),
        ])
    );
    let queued = Record::QueuedSend(QueuedSend {
        submission_id: "sub-9".into(),
        send_id: Some("send-9".into()),
        input: Vec::new(),
        setup: Vec::new(),
        written_at_ms: None,
        ts_ms: None,
        request: None,
        receipt: None,
        delivery: None,
    });
    assert_eq!(
        ids(&queued),
        BTreeSet::from([
            (IdKind::SubmissionId, "sub-9".into()),
            (IdKind::SendId, "send-9".into()),
        ])
    );
    let refresh = Record::SessionRefresh(SessionRefresh {
        generation: 7,
        policy: "immediately".into(),
        run_in_progress: false,
        applies_from_next_run: false,
        setup: Vec::new(),
        manifest: None,
    });
    assert_eq!(
        ids(&refresh),
        BTreeSet::from([(IdKind::RefreshGeneration, "7".into())])
    );
    // A transcript record names nothing the index files, even when it carries an id.
    let input = Record::Input(Input {
        run_id: "r1".into(),
        placement: Placement::RunInput,
        message: Message::new(Role::User, vec![ContentBlock::Text { text: "hi".into() }]),
        written_at_ms: None,
        submission_id: Some("sub-r1".into()),
        notice: None,
        tags_v1: None,
        receipt_seq: None,
        pruned: Vec::new(),
    });
    assert!(record_ids(&input).is_empty());
}

/// Many changes, each adding a run of a random size, with a share so small that merges take
/// many changes and some levels fill up and force theirs. After every change no level holds more
/// than `2 * FANIN` runs; every tenth change, every id ever filed answers with exactly its seqs
/// and ids never filed answer with none. Retired runs, once deleted, leave the backend holding
/// exactly the objects the root names.
#[test]
fn merges_in_steps_keep_every_id_findable() {
    block_on(merges_in_steps())
}

async fn merges_in_steps() {
    let backend = MemoryBackend::default();
    let addr = SessionAddr("a".into());
    backend.write_head(&addr, b"h", None, 1).await.unwrap();
    let key = FrameKey::new(1, &[7; 32]);
    let pages = IndexPages {
        backend: &backend,
        addr: &addr,
        session: Id128([1; 16]),
        key: &key,
    };
    let mut rng = Lcg(1);
    let mut model: BTreeMap<Id128, Vec<u64>> = BTreeMap::new();
    let mut root = IndexRoot::default();
    let mut retired = Vec::new();
    let (mut next_id, mut next_seq) = (0u64, 1u64);
    let (mut resumed, mut forced) = (false, 0u32);
    for change in 0..160u32 {
        let mut entries = Vec::new();
        for _ in 0..1 + rng.next() % 300 {
            // One in five records names an id an earlier record named.
            let id = if next_id > 0 && rng.next().is_multiple_of(5) {
                rng.next() % next_id
            } else {
                next_id += 1;
                next_id - 1
            };
            entries.push(Entry {
                hash: hash(id),
                seq: next_seq,
            });
            model.entry(hash(id)).or_default().push(next_seq);
            next_seq += 1;
        }
        let updated = pages
            .update_with_share(root, entries, &format!("c{change}"), 1, 200)
            .await
            .unwrap();
        root = updated.root;
        retired.extend(updated.retired);
        forced += updated.forced;
        resumed |= updated.merged_entries > 0 && root.levels.iter().any(|l| l.merge.is_some());
        for level in &root.levels {
            assert!(
                level.runs.len() <= 2 * FANIN,
                "a level holds {} runs",
                level.runs.len()
            );
        }
        if change % 10 == 9 {
            for (hash, seqs) in &model {
                assert_eq!(&pages.lookup(&root, hash).await.unwrap(), seqs);
            }
            for absent in next_id..next_id + 20 {
                assert!(pages
                    .lookup(&root, &self::hash(absent))
                    .await
                    .unwrap()
                    .is_empty());
            }
        }
    }
    // Every entry is in exactly one finished run: a merge neither drops nor repeats one.
    let held: u64 = root
        .levels
        .iter()
        .flat_map(|level| level.runs.iter())
        .map(|run| run.entries)
        .sum();
    assert_eq!(held, next_seq - 1);
    assert!(
        root.levels.len() >= 3,
        "merges reached {} levels",
        root.levels.len()
    );
    assert!(resumed, "no merge continued across changes");
    assert!(
        forced > 0,
        "no level filled up, so the forced path never ran"
    );

    pages.delete_retired(&retired, 1).await.unwrap();
    for (hash, seqs) in &model {
        assert_eq!(&pages.lookup(&root, hash).await.unwrap(), seqs);
    }
    let named: BTreeSet<String> = pages.objects(&root).await.unwrap().into_iter().collect();
    let stored: BTreeSet<String> = backend.object_names(&addr).into_iter().collect();
    assert_eq!(
        stored, named,
        "objects the index no longer names were left behind"
    );
}

/// With the production share, seals of steady size never fill a level, so no merge is forced
/// to finish within one change.
#[test]
fn the_production_share_keeps_up_without_forcing() {
    block_on(production_share())
}

async fn production_share() {
    let backend = MemoryBackend::default();
    let addr = SessionAddr("a".into());
    backend.write_head(&addr, b"h", None, 1).await.unwrap();
    let key = FrameKey::new(1, &[7; 32]);
    let pages = IndexPages {
        backend: &backend,
        addr: &addr,
        session: Id128([1; 16]),
        key: &key,
    };
    let mut root = IndexRoot::default();
    let mut seq = 1u64;
    let mut forced = 0;
    for change in 0..600u32 {
        let entries = (0..64)
            .map(|_| {
                seq += 1;
                Entry {
                    hash: hash(seq),
                    seq,
                }
            })
            .collect();
        let updated = pages
            .update(root, entries, &format!("c{change}"), 1)
            .await
            .unwrap();
        root = updated.root;
        forced += updated.forced;
    }
    assert!(root.levels.len() >= 4, "only {} levels", root.levels.len());
    assert_eq!(forced, 0);
}

fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/project".into(),
        harness: "broca".into(),
        session: "s".into(),
    }
}

/// Through a real session: runs whose start and end name a run id, and whose start names a send
/// id, sealed every few runs, so the index merges. Every id is found, whether its records are in
/// the open log or sealed and merged, after deleting what merges retired, and after a reload.
#[test]
fn a_session_finds_ids_open_sealed_and_merged() {
    block_on(session_finds_ids())
}

async fn session_finds_ids() {
    let backend = MemoryBackend::default();
    let addr = SessionAddr("0123456789abcdef".into());
    let deployment = DeploymentKey::new(1, &[5; 32]);
    let mut session = Session::create(&backend, addr.clone(), triple(), &deployment, false)
        .await
        .unwrap();
    let runs = 100;
    let mut merged = 0;
    let mut now = 1_000;
    for n in 0..runs {
        let run = format!("run-{n}");
        session
            .append(&[start(&run, Some(&format!("send-{n}")))], true)
            .await
            .unwrap();
        session.append(&[end(&run)], true).await.unwrap();
        // The last runs stay in the open log.
        if n % 3 == 2 && n < runs - 4 {
            now += 1;
            merged += session.seal(None, now).await.unwrap().merged_entries;
        }
    }
    assert!(merged > 0, "no merge ran");

    async fn check<B: SessionBackend>(session: &Session<'_, B>, runs: usize) {
        for n in 0..runs {
            let run = format!("run-{n}");
            let found = session.find_id(IdKind::RunId, &run).await.unwrap();
            let names: Vec<&str> = found.iter().map(|(_, record)| record.name()).collect();
            assert_eq!(names, ["RunStart", "RunEnd"], "{run}");
            let found = session
                .find_id(IdKind::SendId, &format!("send-{n}"))
                .await
                .unwrap();
            assert_eq!(found.len(), 1);
            assert!(matches!(&found[0].1, Record::RunStart(start) if start.run_id == run));
        }
        assert!(session
            .find_id(IdKind::RunId, "send-1")
            .await
            .unwrap()
            .is_empty());
        assert!(session
            .find_id(IdKind::SendId, "missing")
            .await
            .unwrap()
            .is_empty());
    }
    check(&session, runs).await;
    assert!(session.collect(now + GRACE_MS).await.unwrap() > 0);
    check(&session, runs).await;
    drop(session);
    let session = Session::load(&backend, addr, &triple(), &deployment)
        .await
        .unwrap();
    check(&session, runs).await;
}
