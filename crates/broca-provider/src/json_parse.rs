//! JSON tree parsing shared by the streaming readers.
//!
//! Test builds count parses on the current thread so a stream fixture can prove that telemetry,
//! content observation, and normalization reuse one decoded value rather than each building it.

use serde_json::Value;

#[cfg(test)]
thread_local! {
    static VALUE_PARSES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[inline]
pub(crate) fn value_from_str(raw: &str) -> serde_json::Result<Value> {
    #[cfg(test)]
    VALUE_PARSES.with(|count| count.set(count.get() + 1));
    serde_json::from_str(raw)
}

#[cfg(feature = "live")]
#[inline]
pub(crate) fn value_from_slice(raw: &[u8]) -> serde_json::Result<Value> {
    #[cfg(test)]
    VALUE_PARSES.with(|count| count.set(count.get() + 1));
    serde_json::from_slice(raw)
}

#[cfg(test)]
pub(crate) fn reset_value_parses() {
    VALUE_PARSES.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn value_parses() -> usize {
    VALUE_PARSES.with(std::cell::Cell::get)
}
