use std::path::{Path, PathBuf};

// Scan test source for short timeouts around joined futures and sleep/yield
// polling loops. Standalone non-joined timeouts are not covered yet. This checks
// written constants, not whether a wait reaches disk at runtime. Each exception
// names its file, function and reason so a new helper cannot inherit a whole crate's
// exemption. Bounds are measured in milliseconds.
const EXCEPTIONS: &[(&str, &str, &str)] = &[
    ("crates/broca-core/src/compaction/tests.rs", "concurrent_clock_refresh_cannot_move_wait_cap_into_real_time", "Memory WAL and injected compaction clock; no disk barrier"),
    ("crates/broca-core/src/compaction/tests.rs", "clock_reaching_the_cap_mid_poll_takes_the_cap_exit_not_the_bound", "Memory WAL and injected compaction clock; no disk barrier"),
    ("crates/broca-core/src/run/consent_runtime_tests.rs", "await_route_drops_within_one_second_on_cancel_stop_and_restart_pause", "Memory WAL rig (Rig::new); closing the await route within one second is the asserted property"),
    ("crates/broca-module-serve/tests/it/approval_gate.rs", "setup_failure_without_wal_preserves_panic_and_reaps_daemon", "Cleanup grace only: survival is recorded before the wait, and expiry escalates SIGTERM to SIGKILL instead of failing"),
    ("crates/broca-core/tests/it/cancel_watchdog.rs", "wait_finished", "Provider-task cleanup after cancellation, not a WAL transition"),
    ("crates/broca-core/tests/it/run_loop.rs", "check_held_attempt_durability", "CountingWalStore is in memory"),
    ("crates/broca-core/tests/it/run_loop.rs", "wait_sends", "CountingWalStore is in memory"),
    ("crates/broca-core/tests/it/run_loop.rs", "wait_stops", "CountingWalStore is in memory"),
    ("crates/broca-core/tests/it/run_loop.rs", "held_run_with_usage", "CountingWalStore is in memory; cadence/disarm observation windows are intentional"),
    ("crates/broca-module-serve/src/account_routing_tests.rs", "settle", "Fake routing transport, no file WAL or SQLite"),
    ("crates/broca-module-serve/src/stall/tests.rs", "snapshot", "Stall watchdog JSON snapshot, not WAL or SQLite"),
    ("crates/broca-module-serve/tests/cache_warm_admission.rs", "idle", "AdmissionStore suppresses fsync and has no SQLite store"),
    ("crates/broca-module-serve/tests/it/account_selection_real_daemon.rs", "start", "Daemon connection-file publication before a session exists"),
    ("crates/broca-module-serve/tests/it/captured_replay_e2e.rs", "start", "Daemon connection-file publication before a session exists"),
    ("crates/broca-module-serve/tests/it/captured_replay_e2e.rs", "wait_for_module_live", "Module registration before a session exists"),
    ("crates/broca-module-serve/tests/it/graceful_stop_real_daemon.rs", "start", "Daemon connection-file publication before a session exists"),
    ("crates/broca-module-serve/tests/it/graceful_stop_real_daemon.rs", "wait_for_module_live", "Module registration before a session exists"),
    ("crates/broca-module-serve/tests/it/launch_nonce_e2e.rs", "start", "Daemon connection-file publication before a session exists"),
    ("crates/broca-module-serve/tests/it/launch_nonce_e2e.rs", "broca_is_live", "HELLO/registration probe, not a run transition"),
    ("crates/broca-module-serve/tests/it/plan_fetch_real_daemon.rs", "a_plan_is_fetched_through_the_real_daemon_on_v1_routes", "Daemon connection-file publication before the fetch test starts"),
    ("crates/broca-module-serve/tests/it/plan_admission.rs", "compaction_daemon", "Daemon connection-file publication, not a run transition"),
    ("crates/broca-module-serve/tests/it/plan_admission.rs", "step_transforms_admit_through_the_role_route_and_freeze_the_declaration", "Daemon connection-file publication, not a run transition"),
    ("crates/broca-module-serve/src/role_plane_tests.rs", "session_close_releases_hook_and_compaction_routes", "Daemon connection-file publication, not a run transition"),
    ("crates/broca-subc/tests/it/tool_fault_spine.rs", "module_restart_between_dispatches_is_one_transparent_retry", "Fake tool-module respawn registration, no WAL/store transition"),
    ("crates/broca-module-serve/tests/it/seal_line_is_filterable_e2e.rs", "start", "Daemon connection-file publication before a session exists"),
    ("crates/broca-module-serve/tests/it/seal_line_is_filterable_e2e.rs", "wait_for_module_live", "Module registration before a session exists"),
    ("crates/broca-module-serve/tests/it/single_turn_module_e2e.rs", "start_with_env_and_aft_modules", "Daemon connection-file publication before a session exists"),
    ("crates/broca-module-serve/tests/it/single_turn_module_e2e.rs", "start", "HTTP mock port publication, no session persistence"),
    ("crates/broca-module-serve/tests/llm_runner_conformance.rs", "start", "Daemon connection-file publication before a session exists"),
    ("crates/broca-module-serve/tests/llm_runner_conformance.rs", "connection", "Stand-in HELLO nonce publication before a session exists"),
    ("crates/broca-module-serve/tests/llm_runner_conformance.rs", "describe_diagnostics_follow_each_call_across_two_routes_on_one_worker", "role.describe dispatch logging does not open a session"),
    ("crates/broca-module-serve/tests/scope_real_daemon_e2e.rs", "start_configured", "Daemon connection-file publication before a session exists"),
    ("crates/broca-module-serve/tests/scope_real_daemon_e2e.rs", "sync_scopes", "Daemon scope-sync protocol fixture, not a WAL/store wait"),
    ("crates/broca-module-serve/tests/scope_real_daemon_e2e.rs", "drop", "Cleanup of a mock HTTP task, not a WAL/store wait"),
    ("crates/broca-module-serve/tests/step_transform_rig/mod.rs", "start", "Daemon connection-file publication before a session exists"),
    ("crates/broca-module/tests/it/coordinator_races.rs", "send_racing_idle_close_always_reopens_cleanly", "Forty independent race scenarios; 2 ms sleep deliberately enters the close window, not a completion poll"),
    ("crates/broca-provider/tests/it/family_e2e/mod.rs", "start", "HTTP mock port publication, no session persistence"),
    ("crates/broca-subc/tests/common/mod.rs", "connect", "Daemon connection setup, no broca WAL or SQLite"),
    ("crates/broca-subc/tests/common/mod.rs", "wait_for_module_tool", "Tool-module registration, no broca WAL or SQLite"),
    ("crates/broca-subc/tests/it/multi_module_spine.rs", "wait_until", "Fake tool event log, no broca WAL or SQLite"),
    ("crates/broca-subc/tests/it/resume_spine.rs", "settle_event_counts", "Negative duplicate-event observation window, not a positive completion wait"),
    ("crates/broca-subc/tests/it/scope_outbound_real_daemon.rs", "carrier_plane", "Tool-module registration, no broca WAL or SQLite"),
    ("crates/broca-subc/tests/it/scope_outbound_real_daemon.rs", "wait_until", "Daemon scope/tool event log, no broca WAL or SQLite"),
    ("crates/broca-subc/tests/it/tool_call_cancel_real_daemon.rs", "wait_for_event", "Tool cancellation event log, no broca WAL or SQLite"),
    ("crates/ck-import/tests/it/real_module_e2e.rs", "start", "Mock HTTP/daemon registration before an imported session exists"),
];

#[derive(Clone, Copy)]
struct Token<'a> {
    text: &'a str,
    offset: usize,
}

fn tokens(source: &str) -> Vec<Token<'_>> {
    let bytes = source.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() {
            i += 1;
        } else if bytes[i..].starts_with(b"//") {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
        } else if bytes[i..].starts_with(b"/*") {
            i += 2;
            let mut depth = 1;
            while i < bytes.len() && depth > 0 {
                if bytes[i..].starts_with(b"/*") {
                    depth += 1;
                    i += 2;
                } else if bytes[i..].starts_with(b"*/") {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
        } else if bytes[i] == b'\'' && char_end(source, i).is_some() {
            i = char_end(source, i).unwrap();
        } else if bytes[i] == b'"' || raw_string_end(bytes, i).is_some() {
            if let Some(end) = raw_string_end(bytes, i) {
                i = end;
            } else {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\\' {
                        i = (i + 2).min(bytes.len());
                    } else if bytes[i] == b'"' {
                        i += 1;
                        break;
                    } else {
                        i += 1;
                    }
                }
            }
        } else {
            let start = i;
            i += source[start..].chars().next().unwrap().len_utf8();
            if bytes[start].is_ascii_alphanumeric() || bytes[start] == b'_' {
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
            }
            out.push(Token {
                text: &source[start..i],
                offset: start,
            });
        }
    }
    out
}

fn char_end(source: &str, start: usize) -> Option<usize> {
    let rest = &source[start + 1..];
    let length = if rest.starts_with("\\u{") {
        rest.find('}')? + 1
    } else if rest.starts_with("\\x") {
        4
    } else if rest.starts_with('\\') {
        2
    } else {
        rest.chars().next()?.len_utf8()
    };
    (rest.as_bytes().get(length) == Some(&b'\'')).then_some(start + length + 2)
}

fn raw_string_end(bytes: &[u8], start: usize) -> Option<usize> {
    let prefix = if bytes.get(start) == Some(&b'r') {
        1
    } else if matches!(bytes.get(start), Some(b'b' | b'c')) && bytes.get(start + 1) == Some(&b'r') {
        2
    } else {
        return None;
    };
    let mut quote = start + prefix;
    while bytes.get(quote) == Some(&b'#') {
        quote += 1;
    }
    if bytes.get(quote) != Some(&b'"') {
        return None;
    }
    let hashes = quote - start - prefix;
    let mut i = quote + 1;
    while i < bytes.len() {
        if bytes[i] == b'"'
            && bytes
                .get(i + 1..i + 1 + hashes)
                .is_some_and(|s| s.iter().all(|b| *b == b'#'))
        {
            return Some(i + 1 + hashes);
        }
        i += 1;
    }
    Some(bytes.len())
}

fn number(token: &Token<'_>) -> Option<u64> {
    token.text.replace('_', "").parse().ok()
}

fn duration(tokens: &[Token<'_>]) -> Option<u64> {
    tokens.windows(4).find_map(|w| {
        if w[1].text != "(" || w[3].text != ")" {
            return None;
        }
        let scale = match w[0].text {
            "from_millis" => 1,
            "from_secs" => 1000,
            _ => return None,
        };
        number(&w[2])?.checked_mul(scale)
    })
}

fn closing_brace(tokens: &[Token<'_>], open: usize) -> Option<usize> {
    let mut depth = 0;
    for (i, token) in tokens.iter().enumerate().skip(open) {
        match token.text {
            "{" => depth += 1,
            "}" => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

fn short_polls(source: &str) -> Vec<(String, usize, u64)> {
    let ts = tokens(source);
    let mut found = Vec::new();
    for f in 0..ts.len().saturating_sub(2) {
        if ts[f].text != "fn" {
            continue;
        }
        let name = ts[f + 1].text;
        let Some(open) = (f + 2..ts.len()).find(|i| ts[*i].text == "{") else {
            continue;
        };
        let Some(end) = closing_brace(&ts, open) else {
            continue;
        };
        for l in open + 1..end {
            if matches!(ts[l].text, "timeout" | "timeout_at")
                && ts.get(l + 1).is_some_and(|t| t.text == "(")
            {
                // Inspect only the deadline argument, not durations in the future
                // being awaited. Joined event waits need no polling loop.
                let mut depth = 0;
                let argument_end = (l + 2..end).find(|i| match ts[*i].text {
                    "(" | "[" | "{" => {
                        depth += 1;
                        false
                    }
                    ")" | "]" | "}" if depth > 0 => {
                        depth -= 1;
                        false
                    }
                    "," | ")" => depth == 0,
                    _ => false,
                });
                if let Some(stop) = argument_end {
                    let mut depth = 1;
                    let future_end = (stop + 1..end).find(|i| match ts[*i].text {
                        "(" => {
                            depth += 1;
                            false
                        }
                        ")" => {
                            depth -= 1;
                            depth == 0
                        }
                        _ => false,
                    });
                    let joined = future_end.is_some_and(|close| {
                        ts[stop + 1..close]
                            .windows(2)
                            .any(|w| matches!(w[0].text, "join" | "try_join") && w[1].text == "!")
                    });
                    if let Some(bound) =
                        duration(&ts[l + 2..stop]).filter(|ms| joined && *ms < 30_000)
                    {
                        found.push((
                            name.to_owned(),
                            source[..ts[l].offset].lines().count(),
                            bound,
                        ));
                    }
                }
            }
            if !matches!(ts[l].text, "for" | "while" | "loop") {
                continue;
            }
            let Some(body) = (l + 1..end).find(|i| ts[*i].text == "{") else {
                continue;
            };
            let Some(close) = closing_brace(&ts, body) else {
                continue;
            };
            // `tick` covers `tokio::time::interval` polling, which waits without calling sleep.
            let Some(sleep) =
                (body + 1..close).find(|i| matches!(ts[*i].text, "sleep" | "yield_now" | "tick"))
            else {
                continue;
            };
            let cadence = if ts[sleep].text == "sleep" {
                duration(&ts[sleep..(sleep + 16).min(close)])
            } else {
                None
            };
            let count = ts[l..body].windows(5).find_map(|w| {
                if w[0].text == "in" && w[2].text == "." && w[3].text == "." {
                    number(&w[4])?.checked_sub(number(&w[1])?)
                } else {
                    None
                }
            });
            let counted = count.and_then(|n| n.checked_mul(cadence?));
            let deadline = (open + 1..l).rev().find_map(|i| {
                if (ts[i].text == "deadline"
                    && i > 0
                    && ts[i - 1].text == "let"
                    && ts[l..close].iter().any(|t| t.text == "deadline"))
                    // 48 tokens leaves room for a short setup line, such as an interval, between
                    // the timeout and its loop.
                    || (matches!(ts[i].text, "timeout" | "timeout_at") && l - i < 48)
                {
                    let stop = (i..l).find(|j| ts[*j].text == ";").unwrap_or(l);
                    duration(&ts[i..stop.min((i + 32).min(l))])
                } else {
                    None
                }
            });
            if let Some(bound) = counted.or(deadline).filter(|ms| *ms < 30_000) {
                found.push((
                    name.to_owned(),
                    source[..ts[l].offset].lines().count(),
                    bound,
                ));
            }
        }
    }
    found.sort();
    found.dedup();
    found
}

fn rust_tests(directory: &Path, integration: bool, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_tests(
                &path,
                integration || path.file_name().unwrap() == "tests",
                out,
            );
        } else if path.extension().is_some_and(|e| e == "rs")
            && (integration
                || path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .contains("tests"))
        {
            out.push(path);
        }
    }
}

#[test]
fn short_real_time_polls_are_classified() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = Vec::new();
    rust_tests(&root.join("crates"), false, &mut files);
    let mut violations = Vec::new();
    for path in files {
        let relative = path
            .strip_prefix(&root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let source = std::fs::read_to_string(&path).unwrap();
        for (name, line, bound) in short_polls(&source) {
            if !EXCEPTIONS
                .iter()
                .any(|(p, n, reason)| *p == relative && *n == name && !reason.is_empty())
            {
                violations.push(format!("{relative}:{line} {name}: {bound} ms wait; use broca_test_support::event_timeout/wait_until or document a timing/memory-only exception"));
            }
        }
    }
    violations.sort();
    assert!(
        violations.is_empty(),
        "short real-time test waits need classification:\n{}",
        violations.join("\n")
    );
}

#[test]
fn scanner_recognises_literal_counts_and_deadlines() {
    assert_eq!(short_polls("async fn poll() { for _ in 0..100 { probe(); sleep(Duration::from_millis(20)).await; } }")[0].2, 2000);
    assert_eq!(short_polls("async fn poll() { let deadline = Instant::now() + Duration::from_secs(4); loop { assert!(Instant::now() < deadline); probe(); sleep(Duration::from_millis(20)).await; } }")[0].2, 4000);
    assert_eq!(short_polls("async fn poll() { timeout(Duration::from_secs(3), async { loop { probe(); sleep(Duration::from_millis(5)).await; } }).await; }")[0].2, 3000);
    // Interval polling waits through `tick`, not sleep (the shape of a consent crash test that
    // escaped this guard and then failed on a busy runner).
    assert_eq!(short_polls("async fn poll() { timeout(Duration::from_secs(2), async { let mut tick = interval(Duration::from_millis(5)); while !done() { tick.tick().await; } }).await; }")[0].2, 2000);
    assert!(short_polls(
        "async fn poll() { for _ in 0..3000 { sleep(Duration::from_millis(20)).await; } }"
    )
    .is_empty());
}

#[test]
fn scanner_recognises_timeouts_around_joined_futures() {
    let join = "async fn ordinary() { let result = tokio::time::timeout(std::time::Duration::from_secs(3), async { tokio::join!(batch, interrupt) }).await; }";
    assert_eq!(short_polls(join), vec![("ordinary".into(), 1, 3000)]);
    let event = "async fn wait() { timeout(Duration::from_millis(2500), async { futures::join!(left, right); }).await; }";
    assert_eq!(short_polls(event), vec![("wait".into(), 1, 2500)]);
    let deadline = "async fn wait() { timeout_at(Instant::now() + Duration::from_secs(4), async { tokio::try_join!(left, right) }).await; }";
    assert_eq!(short_polls(deadline), vec![("wait".into(), 1, 4000)]);
    assert!(short_polls("async fn wait() { timeout(HANG_GUARD, async { tokio::join!(event, sleep(Duration::from_millis(5))); }).await; }").is_empty());
    assert!(short_polls("async fn wait() { timeout(Duration::from_secs(60), async { tokio::join!(left, right) }).await; }").is_empty());
    assert!(short_polls("async fn wait() { timeout(Duration::from_secs(3), event).await; tokio::join!(left, right); }").is_empty());
}

#[test]
fn scanner_ignores_comments_and_strings() {
    assert!(short_polls(r###"fn example() { let s = r#"for _ in 0..100 { sleep(Duration::from_millis(20)); }"#; /* nested /* loop */ */ }"###).is_empty());
    let prefix = r###"fn before() { let quote = '\"'; let bytes = br#"\"fake { }"#; let s = "escape\\\""; }"###;
    let polling =
        "pub async fn after<'a>() { for _ in 0..100 { sleep(Duration::from_millis(20)).await; } }";
    assert_eq!(short_polls(&format!("{prefix}\n{polling}"))[0].0, "after");
}
