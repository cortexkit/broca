//! Each estimate against what the counting allocator measured for the same value.

use broca_test_support::alloc::{installed, measure};

use super::{held_bytes, serde_heap_bytes, HeldBytes};

/// The estimate of the value `make` builds is within `percent` of what building it allocated
/// and still holds.
fn close<T: HeldBytes>(what: &str, percent: usize, make: impl FnOnce() -> T) {
    close_by(what, percent, make, |value| held_bytes(value));
}

/// [`close`], estimating with `estimate`.
fn close_by<T>(
    what: &str,
    percent: usize,
    make: impl FnOnce() -> T,
    estimate: impl Fn(&T) -> usize,
) {
    assert!(
        installed(),
        "the test binary counts with the counting allocator"
    );
    let (value, counts) = measure(|| Box::new(make()));
    let measured = usize::try_from(counts.held).unwrap_or(0);
    let estimated = estimate(&*value);
    let off = estimated.abs_diff(measured);
    assert!(
        off * 100 <= measured * percent,
        "{what}: estimated {estimated} bytes, measured {measured}, off by more than {percent}%"
    );
}

#[test]
fn json_values_are_counted_at_their_parsed_size() {
    let text = serde_json::json!({
        "command": "cargo test -p something --lib",
        "description": "run the tests",
        "timeout": 120_000,
        "nested": {"a": [1, 2, 3], "b": "text", "c": {"d": true}},
    })
    .to_string();
    close("a parsed tool input", 10, || {
        serde_json::from_str::<serde_json::Value>(&text).unwrap()
    });
    let many = (0..40)
        .map(|n| format!("\"key{n:02}\":\"value {n}\""))
        .collect::<Vec<_>>()
        .join(",");
    let many = format!("{{{many}}}");
    close("an object of 40 keys", 15, || {
        serde_json::from_str::<serde_json::Value>(&many).unwrap()
    });
}

#[test]
fn maps_are_counted_near_their_allocation() {
    // Built one insert at a time in key order, as the session's indexes are.
    close("a b-tree of 5,000 entries", 15, || {
        let mut map = std::collections::BTreeMap::new();
        for n in 0..5_000u64 {
            map.insert(n, format!("record {n}"));
        }
        map
    });
    close("a hash map of 5,000 entries", 15, || {
        let mut map = std::collections::HashMap::new();
        for n in 0..5_000u64 {
            map.insert(format!("id-{n}"), n);
        }
        map
    });
}

/// The walk for other crates' types estimates a parsed JSON tree about as its own estimator
/// does, both near what the allocator measured.
#[test]
fn a_serialize_walk_estimates_a_json_tree_near_its_allocation() {
    let text = serde_json::json!({
        "path": "src/lib.rs",
        "edits": [
            {"old": "fn a() {}", "new": "fn a() -> u8 { 1 }"},
            {"old": "fn b() {}", "new": "fn b() -> u8 { 2 }"},
        ],
        "dry_run": false,
    })
    .to_string();
    close_by(
        "a parsed edit input",
        15,
        || serde_json::from_str::<serde_json::Value>(&text).unwrap(),
        |value| std::mem::size_of::<serde_json::Value>() + serde_heap_bytes(value),
    );
}
