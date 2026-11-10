use super::{
    open, seal, stored_cipher, Head, HeadError, MoveResidence, OpenLog, Residence, SessionTriple,
    WrappedKey, HEAD_FORMAT,
};
use crate::codec::{ByteBuf, Message as _};
use crate::keys::{random_id, wrap, Cipher, DataKey, DeploymentKey};

cipher_tests!(
    a_sealed_head_opens_with_its_deployment_key,
    a_flipped_byte_anywhere_refuses_the_head,
);

/// A head in mode `cipher` and the key it is sealed with: an encrypted head wraps a new data key
/// under `deployment`, and a plain head wraps none.
fn head(deployment: &DeploymentKey, cipher: Cipher) -> (Head, DataKey) {
    let session_id = random_id();
    let (key, keys, field) = match cipher {
        Cipher::XChaCha20Poly1305 => {
            let key = DataKey::generate(1);
            let keys = vec![WrappedKey {
                id: key.id,
                deployment: deployment.id(),
                wrapped: ByteBuf(wrap(deployment, &session_id, &key).unwrap()),
            }];
            (key, keys, None)
        }
        Cipher::None => (DataKey::plain(), Vec::new(), Some(Cipher::None)),
    };
    let head = Head {
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
        keys,
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
        appends_in_conversation: None,
        cipher: field,
    };
    (head, key)
}

#[test]
fn move_residences_roundtrip_and_older_readers_refuse_by_name() {
    crate::oneof! {
        #[derive(Debug)]
        enum OlderResidence {
            1 Local(()),
            2 Creating(crate::codec::Id128),
            3 Recovering(crate::codec::Id128),
        }
    }
    let deployment = DeploymentKey::new(1, &[9; 32]);
    for cipher in [Cipher::None, Cipher::XChaCha20Poly1305] {
        for (tag, residence) in [
            (4, Residence::Moving("cut".into())),
            (
                5,
                Residence::Moved(MoveResidence {
                    holder: "receiver".into(),
                    cut_id: "cut".into(),
                }),
            ),
        ] {
            let (mut head, key) = head(&deployment, cipher);
            let error = OlderResidence::decode(&residence.encode()).unwrap_err();
            assert_eq!(error.at, "OlderResidence");
            assert_eq!(
                error.kind,
                crate::codec::DecodeErrorKind::UnknownVariant(tag)
            );
            head.residence = residence;
            assert_eq!(open(&seal(&head, &key), &deployment).unwrap().0, head);
        }
    }
}

/// A head opens in its own mode, which its stored bytes name. A plain head opens without the
/// deployment key; an encrypted one refuses until the key has loaded.
fn a_sealed_head_opens_with_its_deployment_key(cipher: Cipher) {
    let deployment = DeploymentKey::new(1, &[9; 32]);
    let (head, key) = head(&deployment, cipher);
    let sealed = seal(&head, &key);
    assert_eq!(stored_cipher(&sealed).unwrap(), cipher);
    let (opened, opened_key) = open(&sealed, &deployment).unwrap();
    assert_eq!(opened, head);
    assert_eq!(opened_key.cipher(), cipher);
    let unloaded = open(&sealed, &DeploymentKey::unloaded(1)).map(|(head, _)| head);
    match cipher {
        Cipher::XChaCha20Poly1305 => {
            assert!(
                matches!(unloaded, Err(HeadError::NotLoaded)),
                "{unloaded:?}"
            )
        }
        Cipher::None => assert_eq!(unloaded.unwrap(), head),
    }
}

/// One flipped byte anywhere in a stored head refuses it: a head never opens changed, in either
/// mode.
fn a_flipped_byte_anywhere_refuses_the_head(cipher: Cipher) {
    let deployment = DeploymentKey::new(1, &[9; 32]);
    let (head, key) = head(&deployment, cipher);
    let sealed = seal(&head, &key);
    for at in 0..sealed.len() {
        let mut flipped = sealed.clone();
        flipped[at] ^= 0x10;
        assert!(open(&flipped, &deployment).is_err(), "flip at {at}");
    }
}

#[test]
fn a_changed_head_or_another_deployment_key_refuses() {
    let deployment = DeploymentKey::new(1, &[9; 32]);
    let (head, key) = head(&deployment, Cipher::XChaCha20Poly1305);
    let mut sealed = seal(&head, &key);
    // Changing the next seq in the body, keeping the tag.
    let mut changed = head.clone();
    changed.next_seq = 1;
    let tag_len = sealed.len() - head.encode().len();
    let mut forged = sealed[..tag_len].to_vec();
    forged.extend_from_slice(&changed.encode());
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

/// A plain head changed in its body, keeping its check, fails the check. Its check holds no key,
/// so someone who recomputes it can change a plain head undetected: plain mode gives up
/// protection against deliberate edits by design.
#[test]
fn a_changed_plain_head_fails_its_check() {
    let deployment = DeploymentKey::new(1, &[9; 32]);
    let (head, key) = head(&deployment, Cipher::None);
    let sealed = seal(&head, &key);
    let mut changed = head.clone();
    changed.next_seq = 1;
    let prefix = sealed.len() - head.encode().len();
    let mut forged = sealed[..prefix].to_vec();
    forged.extend_from_slice(&changed.encode());
    assert!(matches!(
        open(&forged, &deployment),
        Err(HeadError::Authentication)
    ));
    assert_eq!(open(&seal(&changed, &key), &deployment).unwrap().0, changed);
}

/// A head's mode can't be flipped. An encrypted head that names plain mode, keeping its bytes,
/// fails the plain check (and still wraps a key, which no plain head does); a plain head that
/// no longer names its mode is read as encrypted and wraps no key, which no encrypted head
/// lacks. Each refuses as `Authentication`, damage, under the loaded key and without it, and is
/// never read in its first mode.
#[test]
fn a_flipped_head_mode_refuses_as_damage_both_ways() {
    let deployment = DeploymentKey::new(1, &[9; 32]);
    let unloaded = DeploymentKey::unloaded(1);
    for (from, to) in [
        (Cipher::XChaCha20Poly1305, Some(Cipher::None)),
        (Cipher::None, None),
    ] {
        let (head, key) = head(&deployment, from);
        let sealed = seal(&head, &key);
        let mut flipped = head.clone();
        flipped.cipher = to;
        let prefix = sealed.len() - head.encode().len();
        let mut forged = sealed[..prefix].to_vec();
        forged.extend_from_slice(&flipped.encode());
        assert_ne!(stored_cipher(&forged).unwrap(), from);
        for opener in [&deployment, &unloaded] {
            let opened = open(&forged, opener).map(|(head, _)| head);
            assert!(
                matches!(opened, Err(HeadError::Authentication)),
                "a {from:?} head flipped to {to:?}: {opened:?}"
            );
        }
    }
    // A plain head given a key ring copied from an encrypted head still fails: its prefix is a
    // plain check, not the tag the ring's key would check.
    let (encrypted, _) = head(&deployment, Cipher::XChaCha20Poly1305);
    let (plain, key) = head(&deployment, Cipher::None);
    let sealed = seal(&plain, &key);
    let mut flipped = plain.clone();
    flipped.cipher = None;
    flipped.keys = encrypted.keys.clone();
    let prefix = sealed.len() - plain.encode().len();
    let mut forged = sealed[..prefix].to_vec();
    forged.extend_from_slice(&flipped.encode());
    assert!(open(&forged, &deployment).is_err());
}
