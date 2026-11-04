//! The record encoding: a compact, tagged binary format of our own.
//!
//! A message is a sequence of fields. Each field starts with a varint key,
//! `(number << 4) | (must_understand << 3) | wire`, followed by its payload:
//!
//! | wire | payload |
//! |---|---|
//! | 0 `Varint` | one LEB128 varint (unsigned ints, zigzag signed ints, bools, enum values) |
//! | 1 `Fixed64` | eight little-endian bytes (floats) |
//! | 2 `Len` | a varint length, then that many bytes (strings, bytes, JSON text, nested messages) |
//!
//! Evolution works per field, not per record version:
//! - A reader skips a field number it doesn't know, unless the writer set the field's
//!   must-understand bit. Then the reader refuses, because ignoring the field would misread the
//!   record (for example, a new field that changes what a request renders). A field added later
//!   decides which it is when it is added. Every reader knows every field that exists today, so
//!   the bit only matters for fields added in the future.
//! - Enums and one-of messages are closed: a variant the reader doesn't know refuses the record.
//!
//! Absence and zero stay distinct. A field declared optional is written whenever it is `Some`,
//! including `Some(0)`, so "not measured" and "measured zero" never collapse. A required field
//! is omitted when it holds its type's default (0, false, empty) and reads back as that default;
//! a required field whose type has no default (enums, nested messages) is always written, and a
//! record missing one is refused.
//!
//! Encoding is deterministic: fields are written in ascending number order (checked at compile
//! time by the macros), defaults are omitted, and maps are written in key order. So equal values
//! always encode to equal bytes, which content-addressed setup parts rely on.
//!
//! Decoding is strict, because stored bytes are also an attack surface for a codec of our own:
//! a field repeated where one is expected, a known field with the wrong wire type, an invalid
//! bool or enum value, a truncated payload, an overlong varint and trailing bytes all refuse.

use std::collections::BTreeMap;
use std::fmt;

mod macros;
#[cfg(test)]
mod tests;

/// The three payload shapes a field can have.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wire {
    Varint = 0,
    Fixed64 = 1,
    Len = 2,
}

/// A field's payload as read off the wire, before its type decodes it.
#[derive(Debug, Clone, Copy)]
pub enum Raw<'a> {
    Varint(u64),
    Fixed64(u64),
    Len(&'a [u8]),
}

impl Raw<'_> {
    fn wire(&self) -> Wire {
        match self {
            Raw::Varint(_) => Wire::Varint,
            Raw::Fixed64(_) => Wire::Fixed64,
            Raw::Len(_) => Wire::Len,
        }
    }
}

/// Why bytes failed to decode. `at` names the message type and field where it happened, so a
/// refused record says which part of it was wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError {
    pub at: &'static str,
    pub kind: DecodeErrorKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeErrorKind {
    Truncated,
    VarintOverflow,
    BadKey,
    WrongWire {
        expected: Wire,
        found: Wire,
    },
    Duplicate,
    Missing,
    /// A field number this reader doesn't know, written with its must-understand bit.
    MustUnderstand {
        field: u32,
    },
    BadValue(&'static str),
    UnknownVariant(u64),
    OneofEmpty,
    OneofMultiple,
    BadUtf8,
    BadJson(String),
    TrailingBytes,
}

impl DecodeError {
    pub fn new(at: &'static str, kind: DecodeErrorKind) -> Self {
        Self { at, kind }
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: ", self.at)?;
        match &self.kind {
            DecodeErrorKind::Truncated => write!(f, "truncated"),
            DecodeErrorKind::VarintOverflow => write!(f, "varint longer than 64 bits"),
            DecodeErrorKind::BadKey => write!(f, "invalid field key"),
            DecodeErrorKind::WrongWire { expected, found } => {
                write!(f, "wire type {found:?} where {expected:?} is expected")
            }
            DecodeErrorKind::Duplicate => write!(f, "field written more than once"),
            DecodeErrorKind::Missing => write!(f, "required field missing"),
            DecodeErrorKind::MustUnderstand { field } => write!(
                f,
                "unknown field {field} marked must-understand: written by a newer build"
            ),
            DecodeErrorKind::BadValue(what) => write!(f, "invalid value: {what}"),
            DecodeErrorKind::UnknownVariant(n) => write!(f, "unknown variant {n}"),
            DecodeErrorKind::OneofEmpty => write!(f, "one-of message with no variant"),
            DecodeErrorKind::OneofMultiple => write!(f, "one-of message with several variants"),
            DecodeErrorKind::BadUtf8 => write!(f, "string is not UTF-8"),
            DecodeErrorKind::BadJson(e) => write!(f, "invalid JSON text: {e}"),
            DecodeErrorKind::TrailingBytes => write!(f, "bytes after the last field"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// A type that can be one field's payload.
///
/// `Len` types write their payload without the length prefix; the writer adds it. A nested
/// message is a `Len` value whose payload is its own fields.
pub trait Value: Sized {
    const WIRE: Wire;
    /// The type name used in decode errors.
    const NAME: &'static str;

    fn write(&self, out: &mut Vec<u8>);
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError>;

    /// Whether a required field holding this value is omitted. Types without a default
    /// (enums, messages) are always written.
    fn is_default(&self) -> bool {
        false
    }

    /// The value a required field takes when absent, or `None` when absence is an error.
    fn missing() -> Option<Self> {
        None
    }
}

/// A message: the top-level unit a record body or a nested field is made of.
pub trait Message: Value {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.write(&mut out);
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::read(Raw::Len(bytes))
    }
}

pub fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn take_varint(data: &mut &[u8], at: &'static str) -> Result<u64, DecodeError> {
    let mut n: u64 = 0;
    for shift in (0..64).step_by(7) {
        let Some((&byte, rest)) = data.split_first() else {
            return Err(DecodeError::new(at, DecodeErrorKind::Truncated));
        };
        *data = rest;
        // The tenth byte may only carry the 64th bit.
        if shift == 63 && byte > 1 {
            return Err(DecodeError::new(at, DecodeErrorKind::VarintOverflow));
        }
        n |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            // A non-minimal encoding (a trailing 0x00 continuation) would let two byte strings
            // decode to one value, which content addressing can't allow.
            if byte == 0 && shift > 0 {
                return Err(DecodeError::new(
                    at,
                    DecodeErrorKind::BadValue("non-minimal varint"),
                ));
            }
            return Ok(n);
        }
    }
    Err(DecodeError::new(at, DecodeErrorKind::VarintOverflow))
}

/// The largest field number. Keys are varints holding `number << 4`, so this keeps keys in 64
/// bits with room to spare, and numbers that large are never declared.
pub const MAX_FIELD_NUMBER: u32 = (1 << 28) - 1;

/// One decoded field: its number, must-understand bit and payload.
#[derive(Debug, Clone, Copy)]
pub struct FieldRef<'a> {
    pub number: u32,
    pub must: bool,
    pub raw: Raw<'a>,
}

/// Iterates over a message's fields. Stops at the first error, which it yields once.
pub struct Fields<'a> {
    data: &'a [u8],
    at: &'static str,
    done: bool,
}

impl<'a> Fields<'a> {
    pub fn new(data: &'a [u8], at: &'static str) -> Self {
        Self {
            data,
            at,
            done: false,
        }
    }

    fn next_field(&mut self) -> Result<FieldRef<'a>, DecodeError> {
        let at = self.at;
        let key = take_varint(&mut self.data, at)?;
        let wire = key & 0x7;
        let must = key & 0x8 != 0;
        let number = key >> 4;
        if number == 0 || number > u64::from(MAX_FIELD_NUMBER) {
            return Err(DecodeError::new(at, DecodeErrorKind::BadKey));
        }
        let raw = match wire {
            0 => Raw::Varint(take_varint(&mut self.data, at)?),
            1 => {
                let Some((bytes, rest)) = self.data.split_first_chunk::<8>() else {
                    return Err(DecodeError::new(at, DecodeErrorKind::Truncated));
                };
                self.data = rest;
                Raw::Fixed64(u64::from_le_bytes(*bytes))
            }
            2 => {
                let len = take_varint(&mut self.data, at)?;
                let len = usize::try_from(len)
                    .map_err(|_| DecodeError::new(at, DecodeErrorKind::Truncated))?;
                if len > self.data.len() {
                    return Err(DecodeError::new(at, DecodeErrorKind::Truncated));
                }
                let (bytes, rest) = self.data.split_at(len);
                self.data = rest;
                Raw::Len(bytes)
            }
            _ => return Err(DecodeError::new(at, DecodeErrorKind::BadKey)),
        };
        Ok(FieldRef {
            number: number as u32,
            must,
            raw,
        })
    }
}

impl<'a> Iterator for Fields<'a> {
    type Item = Result<FieldRef<'a>, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done || self.data.is_empty() {
            return None;
        }
        let item = self.next_field();
        self.done = item.is_err();
        Some(item)
    }
}

/// The payload of a `Len` value, or a wire error naming `at`.
pub fn len_payload<'a>(raw: Raw<'a>, at: &'static str) -> Result<&'a [u8], DecodeError> {
    match raw {
        Raw::Len(bytes) => Ok(bytes),
        other => Err(DecodeError::new(
            at,
            DecodeErrorKind::WrongWire {
                expected: Wire::Len,
                found: other.wire(),
            },
        )),
    }
}

fn check_wire(raw: Raw<'_>, expected: Wire, at: &'static str) -> Result<(), DecodeError> {
    if raw.wire() == expected {
        Ok(())
    } else {
        Err(DecodeError::new(
            at,
            DecodeErrorKind::WrongWire {
                expected,
                found: raw.wire(),
            },
        ))
    }
}

// Writing fields. The macros and hand-written impls call these in ascending field order.

fn put_key(out: &mut Vec<u8>, number: u32, must: bool, wire: Wire) {
    debug_assert!((1..=MAX_FIELD_NUMBER).contains(&number));
    put_varint(
        out,
        (u64::from(number) << 4) | (u64::from(must) << 3) | wire as u64,
    );
}

/// Writes a field unconditionally. One-of variants use it, since a variant is always present.
pub fn put_one<V: Value>(out: &mut Vec<u8>, number: u32, must: bool, value: &V) {
    put_value(out, number, must, value);
}

fn put_value<V: Value>(out: &mut Vec<u8>, number: u32, must: bool, value: &V) {
    put_key(out, number, must, V::WIRE);
    match V::WIRE {
        Wire::Varint | Wire::Fixed64 => value.write(out),
        Wire::Len => {
            let mut payload = Vec::new();
            value.write(&mut payload);
            put_varint(out, payload.len() as u64);
            out.extend_from_slice(&payload);
        }
    }
}

/// Writes a nested message field whose payload `body` writes in place. Hand-written impls for
/// types defined elsewhere use it to encode borrowed fields without first copying them into a
/// macro-declared struct.
pub fn put_nested(out: &mut Vec<u8>, number: u32, must: bool, body: impl FnOnce(&mut Vec<u8>)) {
    let mut payload = Vec::new();
    body(&mut payload);
    put_key(out, number, must, Wire::Len);
    put_varint(out, payload.len() as u64);
    out.extend_from_slice(&payload);
}

/// Runs `each` over a `Len` value's fields: the decoding counterpart of `put_nested`.
pub fn read_fields<'a>(
    raw: Raw<'a>,
    at: &'static str,
    mut each: impl FnMut(FieldRef<'a>) -> Result<(), DecodeError>,
) -> Result<(), DecodeError> {
    for field in Fields::new(len_payload(raw, at)?, at) {
        each(field?)?;
    }
    Ok(())
}

/// A required field: omitted when it holds its type's default.
pub fn put_req<V: Value>(out: &mut Vec<u8>, number: u32, must: bool, value: &V) {
    if !value.is_default() {
        put_value(out, number, must, value);
    }
}

/// An optional field: written whenever it is `Some`, even when the value is a default.
pub fn put_opt<V: Value>(out: &mut Vec<u8>, number: u32, must: bool, value: &Option<V>) {
    if let Some(value) = value {
        put_value(out, number, must, value);
    }
}

/// A repeated field: one occurrence per element, in order.
pub fn put_rep<V: Value>(out: &mut Vec<u8>, number: u32, must: bool, values: &[V]) {
    for value in values {
        put_value(out, number, must, value);
    }
}

/// A map field: one `{1: key, 2: value}` entry per pair, in key order.
pub fn put_map<K: Value + Ord, V: Value>(
    out: &mut Vec<u8>,
    number: u32,
    must: bool,
    map: &BTreeMap<K, V>,
) {
    for (key, value) in map {
        put_key(out, number, must, Wire::Len);
        let mut entry = Vec::new();
        put_value(&mut entry, 1, false, key);
        put_value(&mut entry, 2, false, value);
        put_varint(out, entry.len() as u64);
        out.extend_from_slice(&entry);
    }
}

// Reading fields. Each decoder keeps one slot per declared field and fills it from `Fields`.

/// Takes a single-valued field into its slot, refusing a second occurrence.
pub fn take_one<V: Value>(
    slot: &mut Option<V>,
    field: FieldRef<'_>,
    at: &'static str,
) -> Result<(), DecodeError> {
    check_wire(field.raw, V::WIRE, at)?;
    if slot.is_some() {
        return Err(DecodeError::new(at, DecodeErrorKind::Duplicate));
    }
    *slot = Some(V::read(field.raw)?);
    Ok(())
}

/// Appends one occurrence of a repeated field.
pub fn take_rep<V: Value>(
    slot: &mut Vec<V>,
    field: FieldRef<'_>,
    at: &'static str,
) -> Result<(), DecodeError> {
    check_wire(field.raw, V::WIRE, at)?;
    slot.push(V::read(field.raw)?);
    Ok(())
}

/// Adds one map entry, refusing a repeated key or keys out of order.
pub fn take_map<K: Value + Ord, V: Value>(
    slot: &mut BTreeMap<K, V>,
    field: FieldRef<'_>,
    at: &'static str,
) -> Result<(), DecodeError> {
    let bytes = len_payload(field.raw, at)?;
    let mut key = None;
    let mut value = None;
    for entry in Fields::new(bytes, at) {
        let entry = entry?;
        match entry.number {
            1 => take_one(&mut key, entry, at)?,
            2 => take_one(&mut value, entry, at)?,
            _ => skip_unknown(entry, at)?,
        }
    }
    let key = finish_req(key, at)?;
    let value = finish_req(value, at)?;
    // Writers emit keys in order, so anything else was not written by a writer of this format.
    if slot.keys().next_back().is_some_and(|last| *last >= key) {
        return Err(DecodeError::new(
            at,
            DecodeErrorKind::BadValue("map keys out of order"),
        ));
    }
    slot.insert(key, value);
    Ok(())
}

/// Handles a field number the decoder doesn't know: skipped, unless marked must-understand.
pub fn skip_unknown(field: FieldRef<'_>, at: &'static str) -> Result<(), DecodeError> {
    if field.must {
        Err(DecodeError::new(
            at,
            DecodeErrorKind::MustUnderstand {
                field: field.number,
            },
        ))
    } else {
        Ok(())
    }
}

/// A required field's final value: what was read, else the type's default, else an error.
pub fn finish_req<V: Value>(slot: Option<V>, at: &'static str) -> Result<V, DecodeError> {
    match slot {
        Some(value) => Ok(value),
        None => V::missing().ok_or_else(|| DecodeError::new(at, DecodeErrorKind::Missing)),
    }
}

/// True when field numbers are strictly ascending: the encoders write in declaration order, so
/// this is what makes encoding deterministic. The macros check it at compile time.
pub const fn ascending(numbers: &[u32]) -> bool {
    let mut i = 0;
    while i < numbers.len() {
        if numbers[i] == 0 || numbers[i] > MAX_FIELD_NUMBER {
            return false;
        }
        if i > 0 && numbers[i] <= numbers[i - 1] {
            return false;
        }
        i += 1;
    }
    true
}

// Scalar types.

macro_rules! unsigned {
    ($($ty:ty),*) => {$(
        impl Value for $ty {
            const WIRE: Wire = Wire::Varint;
            const NAME: &'static str = stringify!($ty);
            fn write(&self, out: &mut Vec<u8>) {
                put_varint(out, u64::from(*self));
            }
            fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
                match raw {
                    Raw::Varint(n) => <$ty>::try_from(n).map_err(|_| {
                        DecodeError::new(Self::NAME, DecodeErrorKind::BadValue("out of range"))
                    }),
                    other => Err(DecodeError::new(Self::NAME, DecodeErrorKind::WrongWire {
                        expected: Wire::Varint,
                        found: other.wire(),
                    })),
                }
            }
            fn is_default(&self) -> bool {
                *self == 0
            }
            fn missing() -> Option<Self> {
                Some(0)
            }
        }
    )*};
}
unsigned!(u8, u16, u32, u64);

macro_rules! signed {
    ($($ty:ty),*) => {$(
        impl Value for $ty {
            const WIRE: Wire = Wire::Varint;
            const NAME: &'static str = stringify!($ty);
            fn write(&self, out: &mut Vec<u8>) {
                let n = i64::from(*self);
                put_varint(out, ((n << 1) ^ (n >> 63)) as u64);
            }
            fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
                match raw {
                    Raw::Varint(n) => {
                        let value = ((n >> 1) as i64) ^ -((n & 1) as i64);
                        <$ty>::try_from(value).map_err(|_| {
                            DecodeError::new(Self::NAME, DecodeErrorKind::BadValue("out of range"))
                        })
                    }
                    other => Err(DecodeError::new(Self::NAME, DecodeErrorKind::WrongWire {
                        expected: Wire::Varint,
                        found: other.wire(),
                    })),
                }
            }
            fn is_default(&self) -> bool {
                *self == 0
            }
            fn missing() -> Option<Self> {
                Some(0)
            }
        }
    )*};
}
signed!(i32, i64);

impl Value for usize {
    const WIRE: Wire = Wire::Varint;
    const NAME: &'static str = "usize";
    fn write(&self, out: &mut Vec<u8>) {
        put_varint(out, *self as u64);
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        let n = u64::read(raw)?;
        usize::try_from(n)
            .map_err(|_| DecodeError::new(Self::NAME, DecodeErrorKind::BadValue("out of range")))
    }
    fn is_default(&self) -> bool {
        *self == 0
    }
    fn missing() -> Option<Self> {
        Some(0)
    }
}

impl Value for bool {
    const WIRE: Wire = Wire::Varint;
    const NAME: &'static str = "bool";
    fn write(&self, out: &mut Vec<u8>) {
        put_varint(out, u64::from(*self));
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        match u64::read(raw).map_err(|e| DecodeError::new(Self::NAME, e.kind))? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(DecodeError::new(
                Self::NAME,
                DecodeErrorKind::BadValue("bool other than 0 or 1"),
            )),
        }
    }
    fn is_default(&self) -> bool {
        !*self
    }
    fn missing() -> Option<Self> {
        Some(false)
    }
}

impl Value for f64 {
    const WIRE: Wire = Wire::Fixed64;
    const NAME: &'static str = "f64";
    fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.to_bits().to_le_bytes());
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        match raw {
            Raw::Fixed64(bits) => Ok(f64::from_bits(bits)),
            other => Err(DecodeError::new(
                Self::NAME,
                DecodeErrorKind::WrongWire {
                    expected: Wire::Fixed64,
                    found: other.wire(),
                },
            )),
        }
    }
    // Only positive zero is the default, so -0.0 keeps its sign.
    fn is_default(&self) -> bool {
        self.to_bits() == 0
    }
    fn missing() -> Option<Self> {
        Some(0.0)
    }
}

impl Value for f32 {
    const WIRE: Wire = Wire::Fixed64;
    const NAME: &'static str = "f32";
    fn write(&self, out: &mut Vec<u8>) {
        f64::from(*self).write(out);
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        let wide = f64::read(raw)?;
        let narrow = wide as f32;
        // f32 -> f64 is exact, so a value written from an f32 always comes back exactly; any
        // other f64 was not written by this impl.
        if f64::from(narrow).to_bits() != wide.to_bits() && !wide.is_nan() {
            return Err(DecodeError::new(
                Self::NAME,
                DecodeErrorKind::BadValue("not an f32 value"),
            ));
        }
        Ok(narrow)
    }
    fn is_default(&self) -> bool {
        self.to_bits() == 0
    }
    fn missing() -> Option<Self> {
        Some(0.0)
    }
}

impl Value for String {
    const WIRE: Wire = Wire::Len;
    const NAME: &'static str = "string";
    fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.as_bytes());
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        let bytes = len_payload(raw, Self::NAME)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| DecodeError::new(Self::NAME, DecodeErrorKind::BadUtf8))
    }
    fn is_default(&self) -> bool {
        self.is_empty()
    }
    fn missing() -> Option<Self> {
        Some(String::new())
    }
}

/// Raw bytes. A newtype, because `Vec<u8>` would read as a repeated `u8` field.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ByteBuf(pub Vec<u8>);

impl Value for ByteBuf {
    const WIRE: Wire = Wire::Len;
    const NAME: &'static str = "bytes";
    fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.0);
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        Ok(ByteBuf(len_payload(raw, Self::NAME)?.to_vec()))
    }
    fn is_default(&self) -> bool {
        self.0.is_empty()
    }
    fn missing() -> Option<Self> {
        Some(ByteBuf::default())
    }
}

impl serde::Serialize for ByteBuf {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex(&self.0))
    }
}

/// A 32-byte digest (SHA-256, keyed hashes, ids). Exactly 32 bytes on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Digest(pub [u8; 32]);

impl Value for Digest {
    const WIRE: Wire = Wire::Len;
    const NAME: &'static str = "digest";
    fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.0);
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        let bytes = len_payload(raw, Self::NAME)?;
        let array: [u8; 32] = bytes.try_into().map_err(|_| {
            DecodeError::new(
                Self::NAME,
                DecodeErrorKind::BadValue("digest is not 32 bytes"),
            )
        })?;
        Ok(Digest(array))
    }
}

impl serde::Serialize for Digest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex(&self.0))
    }
}

/// A 16-byte random id (writer generations, incarnations, session ids).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Id128(pub [u8; 16]);

impl Value for Id128 {
    const WIRE: Wire = Wire::Len;
    const NAME: &'static str = "id128";
    fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.0);
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        let bytes = len_payload(raw, Self::NAME)?;
        let array: [u8; 16] = bytes.try_into().map_err(|_| {
            DecodeError::new(Self::NAME, DecodeErrorKind::BadValue("id is not 16 bytes"))
        })?;
        Ok(Id128(array))
    }
}

impl serde::Serialize for Id128 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex(&self.0))
    }
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[usize::from(byte >> 4)] as char);
        out.push(DIGITS[usize::from(byte & 0xf)] as char);
    }
    out
}

/// A JSON value, stored as its serialized text. Decoding parses it back, so the value (and so
/// everything rendered from it) is identical; serde_json's map is ordered, so the text is too.
impl Value for serde_json::Value {
    const WIRE: Wire = Wire::Len;
    const NAME: &'static str = "json";
    fn write(&self, out: &mut Vec<u8>) {
        serde_json::to_writer(&mut *out, self).expect("a JSON value always serializes");
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        let bytes = len_payload(raw, Self::NAME)?;
        serde_json::from_slice(bytes)
            .map_err(|e| DecodeError::new(Self::NAME, DecodeErrorKind::BadJson(e.to_string())))
    }
}

/// The empty payload of a one-of variant that carries nothing. Fields a later build adds to it
/// are skipped unless marked must-understand, like any message's.
impl Value for () {
    const WIRE: Wire = Wire::Len;
    const NAME: &'static str = "empty";
    fn write(&self, _out: &mut Vec<u8>) {}
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        for field in Fields::new(len_payload(raw, Self::NAME)?, Self::NAME) {
            skip_unknown(field?, Self::NAME)?;
        }
        Ok(())
    }
}

impl<T: Value> Value for Box<T> {
    const WIRE: Wire = T::WIRE;
    const NAME: &'static str = T::NAME;
    fn write(&self, out: &mut Vec<u8>) {
        (**self).write(out);
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        T::read(raw).map(Box::new)
    }
    fn is_default(&self) -> bool {
        (**self).is_default()
    }
    fn missing() -> Option<Self> {
        T::missing().map(Box::new)
    }
}
