use proptest::prelude::*;

use super::*;

cipher_tests!(
    a_frame_round_trips_with_its_header_and_size,
    equal_frames_differ_only_when_encrypted,
    a_short_slice_is_incomplete_and_never_damage,
    a_damaged_header_or_length_is_damage_not_an_unfinished_write,
    a_damaged_body_tag_or_nonce_fails_authentication,
    a_frame_read_under_another_log_fails_authentication,
    an_unknown_key_refuses,
    header_links_must_point_backwards,
    a_frame_is_read_only_in_its_own_mode,
    a_unit_opens_only_in_its_own_place_and_whole,
);

/// The test key in mode `cipher`.
fn key(cipher: Cipher) -> FrameKey {
    FrameKey::for_tests(cipher, 1, &[9; 32])
}

fn header(seq: u64) -> FrameHeader {
    FrameHeader {
        flags: 0,
        stream: FrameStream::Ledger,
        kind: 72,
        kind_version: 1,
        key_id: 1,
        seq,
        batch_first: seq.saturating_sub(1),
        batch_count: 2,
        frame_index: 5,
        synced_through: 4096,
        prev_in_stream: Some(seq.saturating_sub(3)).filter(|p| *p < seq),
    }
}

const BIND: &[u8] = b"session-id/log-name";

fn open_with(cipher: Cipher, bytes: &[u8], bind: &[u8]) -> Result<FrameRead, FrameError> {
    let key = key(cipher);
    open(bytes, |id| (id == 1).then_some(&key), bind)
}

fn a_frame_round_trips_with_its_header_and_size(cipher: Cipher) {
    let frame = seal(&header(10), b"body", &key(cipher), BIND).unwrap();
    let overhead = match cipher {
        Cipher::XChaCha20Poly1305 => FRAME_OVERHEAD,
        Cipher::None => PLAIN_FRAME_OVERHEAD,
    };
    assert_eq!(key(cipher).frame_overhead(), overhead);
    assert_eq!(frame.len(), overhead + 4);
    let read = open_with(cipher, &frame, BIND).unwrap();
    assert_eq!(
        read,
        FrameRead::Frame {
            header: header(10),
            body: b"body".to_vec(),
            len: frame.len(),
        }
    );
}

/// Encrypted frames take a random nonce, so equal frames differ; a plain frame has none, so it
/// is the same bytes each time it is sealed.
fn equal_frames_differ_only_when_encrypted(cipher: Cipher) {
    let a = seal(&header(10), b"x", &key(cipher), BIND).unwrap();
    let b = seal(&header(10), b"x", &key(cipher), BIND).unwrap();
    assert_eq!(a != b, cipher == Cipher::XChaCha20Poly1305);
}

fn a_short_slice_is_incomplete_and_never_damage(cipher: Cipher) {
    let frame = seal(&header(10), b"body", &key(cipher), BIND).unwrap();
    for cut in 0..frame.len() {
        assert_eq!(
            open_with(cipher, &frame[..cut], BIND),
            Ok(FrameRead::Incomplete),
            "cut {cut}"
        );
    }
}

fn a_damaged_header_or_length_is_damage_not_an_unfinished_write(cipher: Cipher) {
    let frame = seal(&header(10), b"body", &key(cipher), BIND).unwrap();
    // Every byte of the length, header and checksum.
    for at in 0..LENGTH_LEN + HEADER_LEN + CRC_LEN {
        let mut damaged = frame.clone();
        damaged[at] ^= 0x40;
        assert_eq!(
            open_with(cipher, &damaged, BIND),
            Err(FrameError::HeaderChecksum),
            "byte {at}"
        );
    }
}

/// Any changed byte after the header: the body, and the nonce and tag of an encrypted frame or
/// the check of a plain one.
fn a_damaged_body_tag_or_nonce_fails_authentication(cipher: Cipher) {
    let frame = seal(&header(10), b"body", &key(cipher), BIND).unwrap();
    for at in LENGTH_LEN + HEADER_LEN + CRC_LEN..frame.len() {
        let mut damaged = frame.clone();
        damaged[at] ^= 1;
        assert_eq!(
            open_with(cipher, &damaged, BIND),
            Err(FrameError::Authentication),
            "byte {at}"
        );
    }
}

fn a_frame_read_under_another_log_fails_authentication(cipher: Cipher) {
    let frame = seal(&header(10), b"body", &key(cipher), BIND).unwrap();
    assert_eq!(
        open_with(cipher, &frame, b"session-id/other-log"),
        Err(FrameError::Authentication)
    );
}

fn an_unknown_key_refuses(cipher: Cipher) {
    let other = FrameKey::for_tests(cipher, 2, &[9; 32]);
    // A frame naming a key id the reader doesn't hold has its length judged as an encrypted
    // frame's, the largest overhead, so the body is long enough to pass that in either mode.
    let frame = seal(
        &FrameHeader {
            key_id: 2,
            ..header(10)
        },
        b"a body long enough for either mode",
        &other,
        BIND,
    )
    .unwrap();
    assert_eq!(
        open_with(cipher, &frame, BIND),
        Err(FrameError::UnknownKey(2))
    );
}

fn header_links_must_point_backwards(cipher: Cipher) {
    let bad_batch = FrameHeader {
        batch_first: 11,
        ..header(10)
    };
    assert!(matches!(
        seal(&bad_batch, b"", &key(cipher), BIND),
        Err(FrameError::BadHeader(_))
    ));
    let bad_link = FrameHeader {
        prev_in_stream: Some(10),
        ..header(10)
    };
    assert!(matches!(
        seal(&bad_link, b"", &key(cipher), BIND),
        Err(FrameError::BadHeader(_))
    ));
}

/// A frame read under a key of the other mode with the same id is damage: a plain frame never
/// authenticates as an encrypted one, nor the reverse, and nothing falls back to the other mode.
fn a_frame_is_read_only_in_its_own_mode(cipher: Cipher) {
    let other = match cipher {
        Cipher::XChaCha20Poly1305 => Cipher::None,
        Cipher::None => Cipher::XChaCha20Poly1305,
    };
    for body in [&b""[..], b"body", &[0x5a; 200]] {
        let frame = seal(&header(10), body, &key(cipher), BIND).unwrap();
        let read = open_with(other, &frame, BIND);
        assert!(
            matches!(
                read,
                Err(FrameError::Authentication | FrameError::BadHeader(_))
            ),
            "a {cipher:?} frame of {} bytes read as {other:?}: {read:?}",
            body.len()
        );
    }
}

/// A sealed unit (a block, page, part or medium) opens only under the authenticated data it was
/// sealed with, so a unit moved to another object, position or session fails, and only whole: any
/// changed byte or a cut fails too. In the plain mode the check does what the tag does.
fn a_unit_opens_only_in_its_own_place_and_whole(cipher: Cipher) {
    let key = key(cipher);
    let sealed = key.seal_unit(b"session-1/object-a#0", b"plaintext");
    assert_eq!(sealed.len(), key.unit_overhead() + b"plaintext".len());
    if cipher == Cipher::None {
        assert_eq!(
            &sealed[..9],
            b"plaintext",
            "a plain unit stores its plaintext"
        );
    }
    assert_eq!(
        key.open_unit(b"session-1/object-a#0", &sealed).as_deref(),
        Some(&b"plaintext"[..])
    );
    for elsewhere in [
        &b"session-1/object-b#0"[..],
        b"session-1/object-a#1",
        b"session-2/object-a#0",
        b"",
    ] {
        assert_eq!(key.open_unit(elsewhere, &sealed), None);
    }
    for at in 0..sealed.len() {
        let mut damaged = sealed.clone();
        damaged[at] ^= 0x01;
        assert_eq!(
            key.open_unit(b"session-1/object-a#0", &damaged),
            None,
            "byte {at}"
        );
    }
    for cut in 0..sealed.len() {
        assert_eq!(
            key.open_unit(b"session-1/object-a#0", &sealed[..cut]),
            None,
            "cut {cut}"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2_000, ..ProptestConfig::default() })]

    #[test]
    fn arbitrary_bytes_never_panic_and_never_authenticate(
        bytes in proptest::collection::vec(any::<u8>(), 0..256),
        plain in any::<bool>(),
    ) {
        let cipher = if plain { Cipher::None } else { Cipher::XChaCha20Poly1305 };
        let read = open_with(cipher, &bytes, BIND);
        prop_assert!(
            !matches!(read, Ok(FrameRead::Frame { .. })),
            "random bytes authenticated"
        );
    }
}
