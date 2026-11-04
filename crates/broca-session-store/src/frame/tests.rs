use proptest::prelude::*;

use super::*;

fn key() -> FrameKey {
    FrameKey::new(1, &[9; 32])
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

fn open_with(bytes: &[u8], bind: &[u8]) -> Result<FrameRead, FrameError> {
    let key = key();
    open(bytes, |id| (id == 1).then_some(&key), bind)
}

#[test]
fn a_frame_round_trips_with_its_header_and_size() {
    let frame = seal(&header(10), b"body", &key(), BIND).unwrap();
    assert_eq!(frame.len(), FRAME_OVERHEAD + 4);
    let read = open_with(&frame, BIND).unwrap();
    assert_eq!(
        read,
        FrameRead::Frame {
            header: header(10),
            body: b"body".to_vec(),
            len: frame.len(),
        }
    );
}

#[test]
fn equal_frames_get_different_nonces() {
    let a = seal(&header(10), b"x", &key(), BIND).unwrap();
    let b = seal(&header(10), b"x", &key(), BIND).unwrap();
    assert_ne!(a, b);
}

#[test]
fn a_short_slice_is_incomplete_and_never_damage() {
    let frame = seal(&header(10), b"body", &key(), BIND).unwrap();
    for cut in 0..frame.len() {
        assert_eq!(
            open_with(&frame[..cut], BIND),
            Ok(FrameRead::Incomplete),
            "cut {cut}"
        );
    }
}

#[test]
fn a_damaged_header_or_length_is_damage_not_an_unfinished_write() {
    let frame = seal(&header(10), b"body", &key(), BIND).unwrap();
    // Every byte of the length, header and checksum.
    for at in 0..LENGTH_LEN + HEADER_LEN + CRC_LEN {
        let mut damaged = frame.clone();
        damaged[at] ^= 0x40;
        assert_eq!(
            open_with(&damaged, BIND),
            Err(FrameError::HeaderChecksum),
            "byte {at}"
        );
    }
}

#[test]
fn a_damaged_body_tag_or_nonce_fails_authentication() {
    let frame = seal(&header(10), b"body", &key(), BIND).unwrap();
    for at in LENGTH_LEN + HEADER_LEN + CRC_LEN..frame.len() {
        let mut damaged = frame.clone();
        damaged[at] ^= 1;
        assert_eq!(
            open_with(&damaged, BIND),
            Err(FrameError::Authentication),
            "byte {at}"
        );
    }
}

#[test]
fn a_frame_read_under_another_log_fails_authentication() {
    let frame = seal(&header(10), b"body", &key(), BIND).unwrap();
    assert_eq!(
        open_with(&frame, b"session-id/other-log"),
        Err(FrameError::Authentication)
    );
}

#[test]
fn an_unknown_key_refuses() {
    let other = FrameKey::new(2, &[9; 32]);
    let frame = seal(
        &FrameHeader {
            key_id: 2,
            ..header(10)
        },
        b"body",
        &other,
        BIND,
    )
    .unwrap();
    assert_eq!(open_with(&frame, BIND), Err(FrameError::UnknownKey(2)));
}

#[test]
fn header_links_must_point_backwards() {
    let bad_batch = FrameHeader {
        batch_first: 11,
        ..header(10)
    };
    assert!(matches!(
        seal(&bad_batch, b"", &key(), BIND),
        Err(FrameError::BadHeader(_))
    ));
    let bad_link = FrameHeader {
        prev_in_stream: Some(10),
        ..header(10)
    };
    assert!(matches!(
        seal(&bad_link, b"", &key(), BIND),
        Err(FrameError::BadHeader(_))
    ));
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2_000, ..ProptestConfig::default() })]

    #[test]
    fn arbitrary_bytes_never_panic_and_never_authenticate(
        bytes in proptest::collection::vec(any::<u8>(), 0..256)
    ) {
        let read = open_with(&bytes, BIND);
        prop_assert!(
            !matches!(read, Ok(FrameRead::Frame { .. })),
            "random bytes authenticated"
        );
    }
}
