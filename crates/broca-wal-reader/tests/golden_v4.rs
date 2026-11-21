use broca_wal_reader::features::is_feature_name;
use broca_wal_reader::frame::{self, DecodeStep, Metadata};
use broca_wal_reader::{Error, HeaderCorruption};

// Fixed vectors produced independently with Python struct/hashlib and a
// normal-polynomial CRC32C implementation (check value 0xe3069283).
const EMPTY: &str = concat!(
    "0f00000004010000000000000002000000000000000300000000000000",
    "000102030405060708090a0b0c0d0e0f02005b5d",
    "ec3ac9679505be3bb8233c4ef0b39c8ee206d2c37fc8610edc19f41fbfb9661e",
    "6325ce797b2274797065223a2274657374227d"
);
const REQUIRED: &str = concat!(
    "0f00000004010000000000000002000000000000000300000000000000",
    "000102030405060708090a0b0c0d0e0f2000",
    "5b2262696c6c2d6578616d706c652f7631222c22636f6e74656e742f7632225d",
    "ec3ac9679505be3bb8233c4ef0b39c8ee206d2c37fc8610edc19f41fbfb9661e",
    "2ae8c4fa7b2274797065223a2274657374227d"
);
const PAYLOAD: &[u8] = br#"{"type":"test"}"#;

fn unhex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn metadata() -> Metadata {
    Metadata {
        seq: 1,
        fence: 2,
        appended_ms: 3,
        writer_id: std::array::from_fn(|i| i as u8),
    }
}

#[test]
fn header_crc_failure_stops_but_payload_failure_can_resume() {
    let first = unhex(EMPTY);
    let second = frame::encode(
        Metadata {
            seq: 2,
            ..metadata()
        },
        &[],
        PAYLOAD,
    )
    .unwrap();
    let mut bad_header = first.clone();
    bad_header[21] ^= 1;
    bad_header.extend_from_slice(&second);
    assert_eq!(
        frame::decode_next(&bad_header, &[]),
        Err(Error::HeaderCorrupt {
            reason: HeaderCorruption::CrcMismatch
        })
    );
    let mut bad_payload = first.clone();
    *bad_payload.last_mut().unwrap() ^= 1;
    bad_payload.extend_from_slice(&second);
    let DecodeStep::PayloadCorrupt {
        header,
        raw_frame,
        consumed,
    } = frame::decode_next(&bad_payload, &[]).unwrap()
    else {
        panic!("expected recoverable payload corruption")
    };
    assert_eq!(header.metadata.seq, 1);
    assert_eq!(raw_frame, &bad_payload[..first.len()]);
    assert_eq!(consumed, first.len());
    let DecodeStep::Frame(next) = frame::decode_next(&bad_payload[consumed..], &[]).unwrap() else {
        panic!("expected next intact frame")
    };
    assert_eq!(next.header.metadata.seq, 2);
}

#[test]
fn unknown_billing_refuses_even_with_unknown_content_or_bad_payload() {
    let mut bytes = unhex(REQUIRED);
    *bytes.last_mut().unwrap() ^= 1;
    let expected = Err(Error::NewerWriter {
        features: vec!["bill-example/v1".into()],
    });
    assert_eq!(frame::decode_next(&bytes, &[]), expected);
    // Refusal is known from the header, before waiting for or parsing any record.
    assert_eq!(frame::decode_next(&bytes[..115], &[]), expected);
    let duplicate_names = frame::encode(
        metadata(),
        &["bill-z/v1", "bill-a/v2", "bill-z/v1"],
        b"not JSON",
    )
    .unwrap();
    assert_eq!(
        frame::decode_next(&duplicate_names, &[]),
        Err(Error::NewerWriter {
            features: vec!["bill-a/v2".into(), "bill-z/v1".into()]
        })
    );
}

#[test]
fn feature_grammar_on_write_and_read() {
    for name in ["a/v0", "a-9/v123", "-/v01"] {
        assert!(is_feature_name(name), "{name}");
        assert!(frame::encode(metadata(), &[name], PAYLOAD).is_ok());
    }
    for name in [
        "", "/v1", "a/v", "a/V1", "A/v1", "a_b/v1", "a/v1/x", "a/v-1", "a/v1\n", "é/v1",
    ] {
        assert!(!is_feature_name(name), "{name}");
        assert_eq!(
            frame::encode(metadata(), &[name], PAYLOAD),
            Err(Error::InvalidFeatureName { name: name.into() })
        );
    }
    let mut bad_name = unhex(REQUIRED);
    bad_name[49] = b'B';
    reseal_header(&mut bad_name);
    assert_eq!(
        frame::decode_next(&bad_name, &[]),
        Err(Error::HeaderCorrupt {
            reason: HeaderCorruption::InvalidFeatureName {
                name: "Bill-example/v1".into()
            }
        })
    );
    let mut bad_json = unhex(EMPTY);
    bad_json[47..49].copy_from_slice(b"{}");
    reseal_header(&mut bad_json);
    assert_eq!(
        frame::decode_next(&bad_json, &[]),
        Err(Error::HeaderCorrupt {
            reason: HeaderCorruption::InvalidRequires
        })
    );
}

#[test]
fn payload_and_requires_lengths_are_bounded() {
    assert_eq!(
        frame::encode_header(
            metadata(),
            &[],
            frame::MAX_PAYLOAD_LEN as usize + 1,
            [0; 32]
        ),
        Err(Error::PayloadTooLarge {
            len: frame::MAX_PAYLOAD_LEN as usize + 1
        })
    );
    let long_name = format!("{}/v1", "a".repeat(u16::MAX as usize));
    assert!(matches!(
        frame::encode(metadata(), &[&long_name], PAYLOAD),
        Err(Error::RequiresTooLarge { .. })
    ));
    let mut impossible_len = unhex(EMPTY);
    impossible_len[..4].copy_from_slice(&(frame::MAX_PAYLOAD_LEN + 1).to_le_bytes());
    reseal_header(&mut impossible_len);
    assert_eq!(
        frame::decode_next(&impossible_len, &[]),
        Err(Error::HeaderCorrupt {
            reason: HeaderCorruption::PayloadTooLarge {
                len: frame::MAX_PAYLOAD_LEN + 1
            }
        })
    );
}

// Reseal deliberately malformed headers so negative tests reach validation
// beyond the CRC check. This is not the oracle for the literal golden vectors.
fn reseal_header(bytes: &mut [u8]) {
    let header_len = 83 + u16::from_le_bytes(bytes[45..47].try_into().unwrap()) as usize;
    let mut crc = !0u32;
    for byte in &bytes[..header_len - 4] {
        crc ^= u32::from(byte.reverse_bits()) << 24;
        for _ in 0..8 {
            crc = if crc & 0x8000_0000 != 0 {
                (crc << 1) ^ 0x1edc_6f41
            } else {
                crc << 1
            };
        }
    }
    bytes[header_len - 4..header_len].copy_from_slice(&(!crc.reverse_bits()).to_le_bytes());
}
