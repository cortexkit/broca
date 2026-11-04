use std::collections::BTreeMap;

use proptest::prelude::*;

use super::*;
use crate::{message, oneof, values};

values! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Colour {
        1 Red,
        2 Green,
    }
}

message! {
    #[derive(Debug, Clone, PartialEq)]
    pub struct Inner {
        1 req label: String,
    }
}

oneof! {
    #[derive(Debug, Clone, PartialEq)]
    pub enum Shape {
        1 Circle(u64),
        2 Named(Inner),
        3 Nothing(()),
    }
}

message! {
    #[derive(Debug, Clone, PartialEq)]
    pub struct Sample {
        1 req count: u64,
        2 opt measured: u64,
        3 req name: String,
        4 rep tags: String,
        5 req colour: Colour,
        6 opt inner: Inner,
        7 map totals: BTreeMap<String, u64>,
        8 req shape: Shape,
        9 req delta: i64,
        10 req ratio: f64,
        11 req flag: bool,
        12 opt blob: ByteBuf,
        13 opt json: serde_json::Value,
    }
}

// `Sample` as a later build might write it: two fields added, one of them must-understand.
message! {
    #[derive(Debug, Clone, PartialEq)]
    pub struct SampleNext {
        1 req count: u64,
        3 req name: String,
        5 req colour: Colour,
        8 req shape: Shape,
        20 opt added: u64,
        21 opt [must] changes_rendering: String,
    }
}

fn sample() -> Sample {
    Sample {
        count: 3,
        measured: Some(0),
        name: "a".into(),
        tags: vec!["x".into(), "".into(), "y".into()],
        colour: Colour::Green,
        inner: Some(Inner {
            label: String::new(),
        }),
        totals: BTreeMap::from([("b".into(), 2), ("a".into(), 0)]),
        shape: Shape::Named(Inner { label: "n".into() }),
        delta: -5,
        ratio: -0.0,
        flag: true,
        blob: Some(ByteBuf(vec![0, 255])),
        json: Some(serde_json::json!({"k": [1, 2.5, null]})),
    }
}

fn err_kind<T: std::fmt::Debug>(result: Result<T, DecodeError>) -> DecodeErrorKind {
    result.expect_err("decode must refuse").kind
}

#[test]
fn round_trips_every_cardinality_and_type() {
    let value = sample();
    let bytes = value.encode();
    let back = Sample::decode(&bytes).unwrap();
    assert_eq!(
        back,
        Sample {
            ratio: back.ratio,
            ..value.clone()
        }
    );
    assert_eq!(
        back.ratio.to_bits(),
        (-0.0f64).to_bits(),
        "-0.0 keeps its sign"
    );
    assert_eq!(back.encode(), bytes, "re-encoding is byte-identical");
}

#[test]
fn optional_zero_and_absent_stay_distinct() {
    let mut value = sample();
    value.measured = Some(0);
    assert_eq!(Sample::decode(&value.encode()).unwrap().measured, Some(0));
    value.measured = None;
    assert_eq!(Sample::decode(&value.encode()).unwrap().measured, None);
}

#[test]
fn required_defaults_are_omitted_and_read_back() {
    let value = Inner {
        label: String::new(),
    };
    assert!(value.encode().is_empty());
    assert_eq!(Inner::decode(&[]).unwrap(), value);
}

#[test]
fn required_field_without_a_default_must_be_present() {
    // Inner has no fields set, so this is a Sample with only count: shape and colour missing.
    let mut out = Vec::new();
    put_req(&mut out, 1, false, &7u64);
    let kind = err_kind(Sample::decode(&out));
    assert_eq!(kind, DecodeErrorKind::Missing);
}

#[test]
fn equal_values_encode_to_equal_bytes_whatever_the_map_insertion_order() {
    let mut a = sample();
    let mut b = sample();
    a.totals = BTreeMap::from([("x".into(), 1), ("y".into(), 2)]);
    b.totals = BTreeMap::new();
    b.totals.insert("y".into(), 2);
    b.totals.insert("x".into(), 1);
    assert_eq!(a.encode(), b.encode());
}

#[test]
fn an_older_reader_skips_an_unknown_field_but_refuses_a_must_understand_one() {
    let base = SampleNext {
        count: 1,
        name: "n".into(),
        colour: Colour::Red,
        shape: Shape::Circle(4),
        added: Some(9),
        changes_rendering: None,
    };
    let read = Sample::decode(&base.encode()).unwrap();
    assert_eq!((read.count, read.name.as_str()), (1, "n"));

    let newer = SampleNext {
        changes_rendering: Some("x".into()),
        ..base
    };
    assert_eq!(
        err_kind(Sample::decode(&newer.encode())),
        DecodeErrorKind::MustUnderstand { field: 21 }
    );
}

#[test]
fn malformed_fields_refuse() {
    assert_eq!(
        err_kind(Inner::decode(&{
            let mut out = Vec::new();
            put_one(&mut out, 1, false, &"a".to_string());
            put_one(&mut out, 1, false, &"b".to_string());
            out
        })),
        DecodeErrorKind::Duplicate
    );

    let mut wrong_wire = Vec::new();
    put_one(&mut wrong_wire, 1, false, &5u64);
    assert!(matches!(
        err_kind(Inner::decode(&wrong_wire)),
        DecodeErrorKind::WrongWire {
            expected: Wire::Len,
            found: Wire::Varint
        }
    ));

    let mut bad_bool = Vec::new();
    put_one(&mut bad_bool, 11, false, &2u64);
    assert!(matches!(
        Sample::decode(&bad_bool).unwrap_err().kind,
        DecodeErrorKind::BadValue(_)
    ));

    // Field 1 (Len) claims 10 bytes and carries 1.
    assert_eq!(
        err_kind(Inner::decode(&[0x12, 10, b'a'])),
        DecodeErrorKind::Truncated
    );
    // A key whose varint never ends.
    assert_eq!(
        err_kind(Inner::decode(&[0x80; 11])),
        DecodeErrorKind::VarintOverflow
    );
    // Field 1 as a varint with a redundant trailing zero byte.
    assert!(matches!(
        err_kind(Inner::decode(&[0x10, 0x81, 0x00])),
        DecodeErrorKind::BadValue("non-minimal varint")
    ));
    // Field number 0 and wire type 3.
    assert_eq!(
        err_kind(Inner::decode(&[0x00, 0x00])),
        DecodeErrorKind::BadKey
    );
    assert_eq!(err_kind(Inner::decode(&[0x13])), DecodeErrorKind::BadKey);
}

#[test]
fn oneof_is_closed_and_holds_exactly_one_variant() {
    for shape in [
        Shape::Circle(0),
        Shape::Named(Inner { label: "z".into() }),
        Shape::Nothing(()),
    ] {
        assert_eq!(Shape::decode(&shape.encode()).unwrap(), shape);
    }
    assert_eq!(err_kind(Shape::decode(&[])), DecodeErrorKind::OneofEmpty);

    let mut unknown = Vec::new();
    put_one(&mut unknown, 9, false, &1u64);
    assert_eq!(
        err_kind(Shape::decode(&unknown)),
        DecodeErrorKind::UnknownVariant(9)
    );

    let mut two = Shape::Circle(1).encode();
    two.extend(Shape::Circle(2).encode());
    assert_eq!(
        err_kind(Shape::decode(&two)),
        DecodeErrorKind::OneofMultiple
    );
}

#[test]
fn values_refuse_an_unknown_number() {
    let mut out = Vec::new();
    put_one(&mut out, 5, false, &3u64);
    assert_eq!(
        Sample::decode(&out).unwrap_err().kind,
        DecodeErrorKind::UnknownVariant(3)
    );
}

#[test]
fn map_keys_out_of_order_refuse() {
    let mut out = Vec::new();
    for key in ["b", "a"] {
        let mut entry = Vec::new();
        put_one(&mut entry, 1, false, &key.to_string());
        put_one(&mut entry, 2, false, &1u64);
        put_key(&mut out, 7, false, Wire::Len);
        put_varint(&mut out, entry.len() as u64);
        out.extend(entry);
    }
    assert!(matches!(
        Sample::decode(&out).unwrap_err().kind,
        DecodeErrorKind::BadValue("map keys out of order")
    ));
}

#[test]
fn f32_round_trips_exactly_and_refuses_a_wider_value() {
    for value in [0.1f32, -0.0, f32::MAX, f32::MIN_POSITIVE] {
        let mut out = Vec::new();
        value.write(&mut out);
        let raw = Raw::Fixed64(u64::from_le_bytes(out.try_into().unwrap()));
        assert_eq!(f32::read(raw).unwrap().to_bits(), value.to_bits());
    }
    assert!(f32::read(Raw::Fixed64(0.1f64.to_bits())).is_err());
}

fn arb_inner() -> impl Strategy<Value = Inner> {
    ".{0,8}".prop_map(|label| Inner { label })
}

fn arb_shape() -> impl Strategy<Value = Shape> {
    prop_oneof![
        any::<u64>().prop_map(Shape::Circle),
        arb_inner().prop_map(Shape::Named),
        Just(Shape::Nothing(())),
    ]
}

fn arb_sample() -> impl Strategy<Value = Sample> {
    (
        (
            any::<u64>(),
            any::<Option<u64>>(),
            ".{0,12}",
            proptest::collection::vec(".{0,4}", 0..4),
            prop_oneof![Just(Colour::Red), Just(Colour::Green)],
            proptest::option::of(arb_inner()),
        ),
        (
            proptest::collection::btree_map(".{0,4}", any::<u64>(), 0..4),
            arb_shape(),
            any::<i64>(),
            any::<f64>().prop_filter("NaN never equals itself", |f| !f.is_nan()),
            any::<bool>(),
            proptest::option::of(proptest::collection::vec(any::<u8>(), 0..16)),
        ),
    )
        .prop_map(
            |(
                (count, measured, name, tags, colour, inner),
                (totals, shape, delta, ratio, flag, blob),
            )| {
                Sample {
                    count,
                    measured,
                    name,
                    tags,
                    colour,
                    inner,
                    totals,
                    shape,
                    delta,
                    ratio,
                    flag,
                    blob: blob.map(ByteBuf),
                    json: None,
                }
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2_000, ..ProptestConfig::default() })]

    #[test]
    fn any_value_round_trips_to_identical_bytes(value in arb_sample()) {
        let bytes = value.encode();
        let back = Sample::decode(&bytes).unwrap();
        prop_assert_eq!(back.encode(), bytes);
        prop_assert_eq!(back.ratio.to_bits(), value.ratio.to_bits());
        prop_assert_eq!(Sample { ratio: 0.0, ..back }, Sample { ratio: 0.0, ..value });
    }

    #[test]
    fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
        let _ = Sample::decode(&bytes);
        let _ = Shape::decode(&bytes);
        let _ = SampleNext::decode(&bytes);
    }

    #[test]
    fn whatever_decodes_re_encodes_to_a_value_that_decodes_the_same(
        bytes in proptest::collection::vec(any::<u8>(), 0..256)
    ) {
        // Bytes with skipped unknown fields re-encode without them, so the bytes may differ;
        // the value may not.
        if let Ok(value) = Sample::decode(&bytes) {
            let again = Sample::decode(&value.encode()).unwrap();
            prop_assert_eq!(again.encode(), value.encode());
        }
        if let Ok(value) = Shape::decode(&bytes) {
            prop_assert_eq!(Shape::decode(&value.encode()).unwrap(), value);
        }
    }
}
