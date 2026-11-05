use super::{
    open, seal, Head, HeadError, OpenLog, Residence, SessionTriple, WrappedKey, HEAD_FORMAT,
};
use crate::codec::ByteBuf;
use crate::keys::{random_id, wrap, DataKey, DeploymentKey};

fn head(deployment: &DeploymentKey, key: &DataKey) -> Head {
    let session_id = random_id();
    Head {
        format: HEAD_FORMAT,
        session_id,
        triple: SessionTriple {
            project_root: "/project".into(),
            harness: "broca".into(),
            session: "s".into(),
        },
        incarnation: random_id(),
        history_version: 1,
        residence: Residence::Local(()),
        features: Vec::new(),
        keys: vec![WrappedKey {
            id: key.id,
            deployment: deployment.id(),
            wrapped: ByteBuf(wrap(deployment, &session_id, key)),
        }],
        generation: random_id(),
        next_seq: 7,
        open_log: Some(OpenLog {
            change: random_id(),
            synced_len: 120,
        }),
        last_change: None,
        transcript_dir: None,
        ledger_dir: None,
        pending: Vec::new(),
        id_index: None,
        cold_since_ms: None,
    }
}

#[test]
fn a_sealed_head_opens_with_its_deployment_key() {
    let deployment = DeploymentKey::new(1, &[9; 32]);
    let key = DataKey::generate(1);
    let head = head(&deployment, &key);
    let (opened, _) = open(&seal(&head, &key), &deployment).unwrap();
    assert_eq!(opened, head);
}

#[test]
fn a_changed_head_or_another_deployment_key_refuses() {
    let deployment = DeploymentKey::new(1, &[9; 32]);
    let key = DataKey::generate(1);
    let head = head(&deployment, &key);
    let mut sealed = seal(&head, &key);
    // Changing the next seq in the body, keeping the tag.
    let mut changed = head.clone();
    changed.next_seq = 1;
    let tag_len = sealed.len() - crate::codec::Message::encode(&head).len();
    let mut forged = sealed[..tag_len].to_vec();
    forged.extend_from_slice(&crate::codec::Message::encode(&changed));
    assert!(matches!(
        open(&forged, &deployment),
        Err(HeadError::Authentication)
    ));
    // A different deployment key with the same id can't unwrap the data key.
    assert!(matches!(
        open(&sealed, &DeploymentKey::new(1, &[8; 32])),
        Err(HeadError::Unwrap(_))
    ));
    // A deployment key the head has no entry for.
    assert!(matches!(
        open(&sealed, &DeploymentKey::new(2, &[9; 32])),
        Err(HeadError::NoKey)
    ));
    sealed.truncate(10);
    assert!(matches!(
        open(&sealed, &deployment),
        Err(HeadError::Malformed)
    ));
}
