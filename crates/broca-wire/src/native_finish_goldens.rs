mod native_finish_goldens {
    use crate::{RunResultResponse, RunStatusResponse};
    use serde::Serialize;

    fn finish_only_delta(old: &[u8], actual: &[u8], finish: &str) -> Result<(), String> {
        let old_value: serde_json::Value = serde_json::from_slice(old).unwrap();
        let actual_value: serde_json::Value = serde_json::from_slice(actual).unwrap();
        if actual_value["finish"] != finish {
            return Err("the serializer did not emit the expected finish".into());
        }
        if old_value.get("finish").is_some() {
            return if old == actual {
                Ok(())
            } else {
                Err("an already refreshed golden changed bytes".into())
            };
        }
        let actual = std::str::from_utf8(actual).unwrap();
        let member = format!("\"finish\":{}", serde_json::to_string(finish).unwrap());
        if actual.matches(&member).count() != 1 {
            return Err("finish must occur exactly once".into());
        }
        let at = actual.find(&member).unwrap();
        let end = at + member.len();
        let remove = if at > 0 && actual.as_bytes()[at - 1] == b',' {
            at - 1..end
        } else if actual.as_bytes().get(end) == Some(&b',') {
            at..end + 1
        } else {
            return Err("finish is not one additive object member".into());
        };
        let mut kept = actual.to_owned();
        kept.replace_range(remove, "");
        if kept.as_bytes() != old {
            return Err("a byte other than the added finish member changed".into());
        }
        Ok(())
    }

    fn wire_bytes(value: &impl Serialize, sorted_reference: bool) -> Vec<u8> {
        if sorted_reference {
            // Reference replies were captured through Value; direct status replies retain
            // their struct field order. Preserve each capture's existing ordering.
            serde_json::to_vec(&serde_json::to_value(value).unwrap()).unwrap()
        } else {
            serde_json::to_vec(value).unwrap()
        }
    }

    #[test]
    fn refresh_guard_refuses_changed_state_or_indeterminate_calls() {
        let old = br#"{"state":"interrupted","indeterminate_tool_calls":[]}"#;
        let added = br#"{"state":"interrupted","finish":"tool_calls","indeterminate_tool_calls":[]}"#;
        assert!(finish_only_delta(old, added, "tool_calls").is_ok());
        let changed = br#"{"state":"interrupted","finish":"tool_calls","indeterminate_tool_calls":["call_slow"]}"#;
        assert!(finish_only_delta(old, changed, "tool_calls").is_err());
        let changed = br#"{"state":"completed","finish":"tool_calls","indeterminate_tool_calls":[]}"#;
        assert!(finish_only_delta(old, changed, "tool_calls").is_err());
    }

    #[test]
    fn native_reference_goldens_add_only_finish() {
        let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
        for (file, expected, sorted_reference) in [
            ("run_status.grammar_too_large.json", "stop", false),
            ("run_status.crash_orphan.json", "tool_calls", false),
            ("run_status.runner_shutdown.json", "tool_calls", true),
            ("run_status.runner_crash.json", "tool_calls", true),
            ("run_result.runner_shutdown.json", "tool_calls", true),
            ("run_result.runner_crash.json", "tool_calls", true),
        ] {
            let path = directory.join(file);
            let old = std::fs::read(&path).unwrap();
            let baseline: serde_json::Value = serde_json::from_slice(&old).unwrap();
            if file.starts_with("run_status.") && !file.contains("grammar_too_large") {
                let calls = if file.contains("runner_shutdown") {
                    serde_json::json!([])
                } else {
                    serde_json::json!(["call_slow"])
                };
                assert_eq!(baseline["indeterminate_tool_calls"], calls, "{file}");
            }
            let finish: cortexkit_role_llm_runner::finish::Finish =
                serde_json::from_value(serde_json::json!(expected)).unwrap();
            let serialized = if file.starts_with("run_result.") {
                let mut response: RunResultResponse = serde_json::from_slice(&old).unwrap();
                response.finish = Some(finish);
                wire_bytes(&response, sorted_reference)
            } else {
                let mut response: RunStatusResponse = serde_json::from_slice(&old).unwrap();
                match &mut response {
                    RunStatusResponse::Completed { finish: slot, .. }
                    | RunStatusResponse::Interrupted { finish: slot, .. } => *slot = Some(finish),
                    _ => panic!("{file} is not a captured terminal status"),
                }
                wire_bytes(&response, sorted_reference)
            };
            // Some captures predate step/model usage counters. Use the same usage-only
            // adapter as native comparisons so adding finish cannot refresh those counters.
            let actual = crate::usage_compat::legacy_bytes(&serialized, &old);
            finish_only_delta(&old, &actual, expected)
                .unwrap_or_else(|error| panic!("refusing to refresh {file}: {error}"));
            if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
                std::fs::write(&path, &actual).unwrap();
            } else {
                assert_eq!(actual, old, "refresh {file} with BROCA_UPDATE_GOLDEN=1");
            }
        }
    }
}
