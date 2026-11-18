//! A session that moved here tells its first run here so, until a run's start is durable.

use std::sync::{Arc, Mutex};

use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::session::Session;
use tokio::sync::mpsc;

use super::tests;
use crate::actor::SendKey;
use crate::store_open::moved_here;

/// Runs every job on the loop executor and keeps each run's `moved_here`. The jobs listed in
/// `refuse` end refused before their start, as a routing refusal would.
struct Recording {
    inner: tests::LoopExecutor,
    moved_here: Mutex<Vec<bool>>,
    refuse: Vec<usize>,
}

#[async_trait::async_trait]
impl crate::executor::RunExecutor for Recording {
    async fn execute_on_store(
        &self,
        job: crate::executor::StoreRunJob,
        out: mpsc::Sender<broca_core::OutputEvent>,
    ) -> crate::executor::StoreRunEnd {
        let n = {
            let mut seen = self.moved_here.lock().unwrap();
            seen.push(job.control.moved_here);
            seen.len() - 1
        };
        if self.refuse.contains(&n) {
            return crate::executor::StoreRunEnd {
                parts: job.parts,
                result: Err(crate::executor::StoreRunError::Admission(
                    broca_core::RunError::Config("refused before its start".into()),
                )),
            };
        }
        self.inner.execute_on_store(job, out).await
    }
}

/// The `moved_here` each of `sends` runs saw, and whether the session is still marked after,
/// for a session marked as moved here when `marked`.
async fn moved_here_runs(marked: bool, sends: usize, refuse: Vec<usize>) -> (Vec<bool>, bool) {
    let root = broca_test_support::scratch_root().join(format!(
        "store-moved-here-{}-{marked}-{sends}-{refuse:?}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let addr = SessionAddr(broca_core::session_addr(&tests::bind()));
    // A run registers its session as a recovery candidate in the same root before it starts.
    crate::store_open::recovery::register_candidate(&root, &addr).unwrap();
    if marked {
        moved_here::mark(&root, &addr).unwrap();
    }
    let steps = (0..sends)
        .map(|n| tests::text_step(&format!("m{n}"), "done"))
        .collect();
    let executor = Arc::new(Recording {
        inner: tests::executor(steps, None),
        moved_here: Mutex::default(),
        refuse,
    });
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let session = Session::create(
        backend,
        SessionAddr(tests::ADDR.into()),
        tests::triple(),
        &tests::deployment(),
        false,
    )
    .await
    .unwrap();
    let actor = super::StoreActor::new(
        tests::bind(),
        session,
        executor.clone(),
        Box::new(broca_session_loop::parts::MemoryParts::default()),
    )
    .with_readers(Arc::new(move || backend))
    .with_recovery_index(root.clone());
    let (commands, receive) = mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receive));
    let commands = tests::Commands::over(commands, backend);
    for n in 0..sends {
        let run = format!("r{n}");
        tests::send_with(&commands, &run, tests::request("hello"), SendKey::Minted).await;
        tests::idle(&commands).await;
    }
    drop(commands);
    served.abort();
    let seen = executor.moved_here.lock().unwrap().clone();
    let still_marked = moved_here::is_marked(&root, &addr);
    let _ = std::fs::remove_dir_all(&root);
    (seen, still_marked)
}

/// The first run here is told, and once its start is durable the mark is gone: the next run
/// isn't told again, since routing now keeps the conversation's account itself.
#[tokio::test]
async fn a_moved_sessions_first_run_here_is_told_and_only_it() {
    assert_eq!(
        moved_here_runs(true, 2, vec![]).await,
        (vec![true, false], false)
    );
}

/// A run refused before its start (routing refusing it, say) leaves the mark, so the next run
/// is told instead.
#[tokio::test]
async fn a_run_refused_before_its_start_leaves_the_mark_for_the_next() {
    assert_eq!(
        moved_here_runs(true, 2, vec![0]).await,
        (vec![true, true], false)
    );
}

/// A session that didn't move here tells no run.
#[tokio::test]
async fn a_session_that_did_not_move_tells_no_run() {
    assert_eq!(
        moved_here_runs(false, 2, vec![]).await,
        (vec![false, false], false)
    );
}
