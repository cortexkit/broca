use futures::executor::block_on;

use super::{window_bytes, write, Shape};
use crate::backend::{MemoryBackend, SessionAddr};
use crate::head::SessionTriple;
use crate::keys::{Cipher, DeploymentKey};
use crate::session::Session;

fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/synthetic".into(),
        harness: "broca".into(),
        session: "synthetic".into(),
    }
}

/// The production shape at 1,000 messages has the measured proportions, and loads back.
#[test]
fn the_production_shape_has_the_measured_proportions() {
    let backend = MemoryBackend::default();
    let deployment = DeploymentKey::new(1, &[5; 32]).creating(Cipher::XChaCha20Poly1305);
    let addr = SessionAddr("5e55105e55105e55".into());
    let shape = Shape::production(1_000);
    let (written, w) = block_on(async {
        let mut session = Session::create(&backend, addr.clone(), triple(), &deployment, false)
            .await
            .unwrap();
        let written = write(&mut session, &shape).await.unwrap();
        (written, window_bytes(&session.conversation))
    });
    let per_1000 = |n: usize| n * 1_000 / written.messages;
    assert!((1_000..1_010).contains(&written.messages), "{written:?}");
    assert!((500..560).contains(&per_1000(written.calls)), "{written:?}");
    assert!(
        (5_900..6_400).contains(&per_1000(written.records)),
        "{written:?}"
    );
    let per_message = w / written.messages;
    assert!(
        (4_000..5_000).contains(&per_message),
        "{per_message} bytes per message"
    );
    assert!(
        written.latest_run_records * 4 < written.records,
        "the latest run is a fraction of the log: {written:?}"
    );
    let session = block_on(Session::load(&backend, addr, &triple(), &deployment)).unwrap();
    assert_eq!(
        session.conversation.held_messages().unwrap().len(),
        written.messages
    );
    assert_eq!(window_bytes(&session.conversation), w);
    // The latest run's records, and the `WriterOpened` this load appended.
    assert_eq!(session.kept_records(), written.latest_run_records + 1);
}
