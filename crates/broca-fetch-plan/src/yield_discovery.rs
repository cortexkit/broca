use std::collections::BTreeMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

static WARNINGS: LazyLock<Mutex<BTreeMap<(String, &'static str), Instant>>> =
    LazyLock::new(Mutex::default);

pub(crate) fn observe(provider: &str, cause: &'static str) {
    let now = Instant::now();
    let mut warnings = WARNINGS.lock().unwrap();
    let key = (provider.to_owned(), cause);
    if warnings
        .get(&key)
        .is_some_and(|last| now.duration_since(*last) < Duration::from_secs(60))
    {
        return;
    }
    warnings.insert(key, now);
    // Provider errors may contain private data; the discovery status is enough to explain
    // why admission could not establish yield support.
    tracing::warn!(target: "tool_yield", provider, cause,
        "tool yield discovery unavailable; admitting without a yield promise");
}
