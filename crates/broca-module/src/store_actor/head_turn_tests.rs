//! A run is a head's turn when a send reached its session over a head-stamped route.

use std::sync::{Arc, Mutex};

use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::session::Session;
use tokio::sync::mpsc;

use super::tests;
use crate::actor::SendKey;

/// Runs every job on the loop executor and keeps each run's `head_turn`.
struct Recording {
    inner: tests::LoopExecutor,
    head_turns: Mutex<Vec<bool>>,
}

#[async_trait::async_trait]
impl crate::executor::RunExecutor for Recording {
    async fn execute(
        &self,
        _: crate::executor::RunJob,
        _: mpsc::Sender<broca_core::OutputEvent>,
    ) -> Result<broca_core::RunSummary, broca_core::RunError> {
        unreachable!("the fixture uses a real store actor")
    }

    async fn execute_on_store(
        &self,
        job: crate::executor::StoreRunJob,
        out: mpsc::Sender<broca_core::OutputEvent>,
    ) -> crate::executor::StoreRunEnd {
        self.head_turns.lock().unwrap().push(job.req.head_turn);
        self.inner.execute_on_store(job, out).await
    }
}

fn stamp(kind: broca_protocol::ScopeKind) -> broca_protocol::SendScope {
    broca_protocol::SendScope::Stamped(broca_protocol::BindScope {
        scope: broca_protocol::SessionScope {
            owner: broca_protocol::BindPrincipal::Reserved {
                module_id: "prefrontal-core".into(),
            },
            agent_id_frozen: false,
            scope_ref: "d6f57d11448da25d6bba5edb0d2fd941".into(),
            scope_epoch: 1,
            flow_id: None,
            sender: None,
            agent_id: None,
            run_id: None,
        },
        owner_authorized: true,
        kind,
        parent: None,
        parent_state: None,
    })
}

/// The `head_turn` of each run a session runs for `scopes`, one send each, in order.
async fn head_turns(scopes: &[broca_protocol::SendScope]) -> Vec<bool> {
    let steps = (0..scopes.len())
        .map(|n| tests::text_step(&format!("m{n}"), "done"))
        .collect();
    let executor = Arc::new(Recording {
        inner: tests::executor(steps, None),
        head_turns: Mutex::default(),
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
    .with_readers(Arc::new(move || backend));
    let (commands, receive) = mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receive));
    let commands = tests::Commands::over(commands, backend);
    for (n, scope) in scopes.iter().enumerate() {
        let mut request = tests::request("hello");
        request.principal = Some(broca_protocol::BindPrincipal::Reserved {
            module_id: "prefrontal-core".into(),
        });
        request.scope = scope.clone();
        // A scoped send declares tools only through a plan; this one needs none.
        request.tools = None;
        let run = format!("r{n}");
        let outcome = tests::send_with(&commands, &run, request, SendKey::Minted).await;
        assert!(
            matches!(
                outcome,
                crate::actor::SendOutcome::Active { .. }
                    | crate::actor::SendOutcome::Finished { .. }
            ),
            "{outcome:?}"
        );
        tests::idle(&commands).await;
    }
    drop(commands);
    served.abort();
    let turns = executor.head_turns.lock().unwrap().clone();
    turns
}

#[tokio::test]
async fn a_head_stamped_send_makes_its_run_a_heads_turn() {
    assert_eq!(
        head_turns(&[stamp(broca_protocol::ScopeKind::Head)]).await,
        [true]
    );
}

#[tokio::test]
async fn a_worker_or_unstamped_send_is_not_a_heads_turn() {
    assert_eq!(
        head_turns(&[stamp(broca_protocol::ScopeKind::Worker)]).await,
        [false]
    );
    assert_eq!(
        head_turns(&[broca_protocol::SendScope::Internal]).await,
        [false]
    );
}

/// The session stays a head's once a head-stamped send reached it: a later run started by a
/// send that carries no stamp (as the actor drains one it queued) is a head's turn too.
#[tokio::test]
async fn a_later_unstamped_run_on_a_head_session_is_a_heads_turn() {
    assert_eq!(
        head_turns(&[
            stamp(broca_protocol::ScopeKind::Head),
            broca_protocol::SendScope::Internal,
        ])
        .await,
        [true, true]
    );
}
