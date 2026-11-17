//! A run holds the window's messages several times over: the session's conversation, the run's
//! own conversation folded again from the same records, and the stored prompt built from it.
//! Message text is one shared allocation, so each copy costs its structure only. This test
//! measures that on a production-shaped window. When text was copied by value, each of these
//! holders held about 0.9 to 1.0 times the window's bytes again, far above the bound below.

use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::{Cipher, DeploymentKey};
use broca_session_store::record::Stream;
use broca_session_store::session::Session;
use broca_session_store::synthetic::{window_bytes, write, Shape};
use broca_test_support::alloc::measure;

/// The share of the window's bytes one copy of it may hold: its messages, blocks and vectors,
/// but none of the text.
const SHARED_COPY_LIMIT: f64 = 0.2;

fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/synthetic".into(),
        harness: "budget".into(),
        session: "s1".into(),
    }
}

#[test]
fn a_runs_copies_of_the_window_share_its_text() {
    assert!(broca_test_support::alloc::installed());
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let addr = SessionAddr("5e55105e55105e56".into());
    let deployment = DeploymentKey::new(1, &[7; 32]).creating(Cipher::XChaCha20Poly1305);
    let (records, base, base_operations) = futures::executor::block_on(async {
        let mut session = Session::create(backend, addr.clone(), triple(), &deployment, false)
            .await
            .unwrap();
        let base = (session.conversation.clone(), session.operations.clone());
        write(&mut session, &Shape::production(2_000))
            .await
            .unwrap();
        (
            session.records_between(0, u64::MAX).await.unwrap(),
            base.0,
            base.1,
        )
    });
    // The fold a session and a run each run over the same records.
    let fold = || {
        let mut conversation: Conversation = base.clone();
        let mut operations: Operations = base_operations.clone();
        for (seq, record) in &records {
            if record.stream() == Stream::Transcript {
                broca_session_store::session::apply(
                    &mut conversation,
                    &mut operations,
                    *seq,
                    record,
                )
                .unwrap();
            }
        }
        conversation
    };
    // The records hold the text. The session's fold and the run's fold each share it.
    let (session_copy, session_held) = measure(fold);
    let window = window_bytes(&session_copy) as f64;
    let (run_copy, run_held) = measure(fold);
    let (prompt, prompt_held) = measure(|| {
        crate::compaction::prompt::contract_prompt(&run_copy, &[], None, false, "s1").unwrap()
    });
    let session_share = session_held.held as f64 / window;
    let run_share = run_held.held as f64 / window;
    let prompt_share = prompt_held.held as f64 / window;
    eprintln!(
        "window {window} bytes, {} messages: session's conversation {session_share:.3} W, run's \
         conversation {run_share:.3} W, stored prompt {prompt_share:.3} W",
        session_copy.messages.len()
    );
    assert!(
        session_share < SHARED_COPY_LIMIT,
        "the session's conversation holds {session_share:.3} of the window's bytes"
    );
    assert!(
        run_share < SHARED_COPY_LIMIT,
        "the run's conversation holds {run_share:.3} of the window's bytes"
    );
    assert!(
        prompt_share < SHARED_COPY_LIMIT,
        "the stored prompt holds {prompt_share:.3} of the window's bytes"
    );
    drop((session_copy, run_copy, prompt, records));
}
