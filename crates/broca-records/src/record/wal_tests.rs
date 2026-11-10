use std::collections::BTreeMap;
use std::sync::Arc;

use broca_protocol::{
    caller::{PauseReason, RunFinishReason},
    content::ToolResult,
    request::{
        BudgetConfig, BudgetKind, ContextManagement, GenerationConfig, ModelHandle, ResponseFormat,
        StopCondition, SubmissionRequest, Tool, ToolChoice,
    },
    AttemptOutcome, BatchId, BindIdentity, RunId, StepId, SubmissionId, ToolCallId, Usage,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use broca_records::record::*;

#[cfg(test)]
mod schema_tests {
    //! Schema v2 shape tests: every new/changed record serializes AND deserializes
    //! back to itself, and an absent optional field deserializes to the intended
    //! default — so new writes are checked before a projection reads them, and a forward/back-compat
    //! read of an older-shaped record is sound.

    use super::*;
    use broca_protocol::{
        content::{AssistantMessage, ContentBlock, FinishReason, Message, Role, ToolCall},
        error::{ErrorClass, ProviderError},
        MessageId,
    };
    use serde_json::json;

    fn round_trips(rec: &WalRecord) {
        let bytes = serde_json::to_vec(rec).expect("serialize");
        let back: WalRecord = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(rec, &back, "record did not round-trip: {rec:?}");
    }

    #[test]
    fn model_step_finished_arc_keeps_existing_wal_bytes() {
        let record = WalRecord::ModelStepFinished {
            raw_finish_reason: None,
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(1),
            assistant_message: Arc::new(AssistantMessage {
                message_id: MessageId("m1".into()),
                content: vec![ContentBlock::Text {
                    text: "done".into(),
                }],
            }),
            // Pin the old all-zero record, not the new unreported default.
            usage: Usage {
                input_tokens: Some(0),
                cached_input_tokens: Some(0),
                cache_write_tokens: Some(0),
                cache_write_5m_tokens: None,
                cache_write_1h_tokens: None,
                output_tokens: Some(0),
                reasoning_tokens: Some(0),
            },
            finish_reason: Some(FinishReason::Stop),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        };

        let bytes = serde_json::to_vec(&record).expect("serialize shared assistant message");
        assert_eq!(
            bytes,
            br#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"text","text":"done"}]},"usage":{"input_tokens":0,"cached_input_tokens":0,"cache_write_tokens":0,"output_tokens":0,"reasoning_tokens":0},"finish_reason":"stop","retries_used":0}"#,
            "Arc must serialize its pointee without changing existing WAL bytes"
        );
    }

    #[test]
    fn tool_dispatch_intent_arc_keeps_existing_wal_bytes() {
        let record = WalRecord::ToolDispatchIntent {
            dispatched_at_ms: None,
            hooks: None,
            batch_id: BatchId("b1".into()),
            tool_call_id: ToolCallId("t1".into()),
            tool_name: Some("edit".into()),
            args: Arc::new(json!({"path": "file"})),
            module_id: None,
            schema_pin: None,
            approval_digest: None,
            call_key: None,
        };

        let bytes = serde_json::to_vec(&record).expect("serialize shared tool arguments");
        assert_eq!(
            bytes,
            br#"{"type":"tool_dispatch_intent","batch_id":"b1","tool_call_id":"t1","tool_name":"edit","args":{"path":"file"}}"#,
            "Arc must serialize its pointee without changing existing WAL bytes"
        );
    }

    #[test]
    fn run_config_tools_arc_keeps_existing_wal_bytes() {
        let config = RunConfig {
            global_system: None,
            account_route: None,
            route_decision_id: None,
            account_limit_pause: false,
            turn_directives_v1: None,
            render_tags: false,
            computed_tags: None,
            model: ModelHandle::new("p", "m"),
            tools: Arc::new(vec![Tool {
                name: "edit".into(),
                description: "edit a file".into(),
                input_schema: json!({"type": "object"}),
                module: Some("aft".into()),
                execution_mode: broca_protocol::request::ExecutionMode::Parallel,
                reply: None,
            }]),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            response_format: ResponseFormat::default(),
            provider_options: Value::Null,
            cache_tiers: Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: Value::Null,
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: Vec::new(),
            context_management: None,
        };

        let bytes = serde_json::to_vec(&config).expect("serialize config with shared tools");
        assert_eq!(
            bytes,
            br#"{"model":{"provider_module_id":"p","model_id":"m"},"tools":[{"name":"edit","description":"edit a file","input_schema":{"type":"object"},"module":"aft","execution_mode":"parallel"}],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1}"#,
            "Arc must serialize its pointee without changing existing RunConfig WAL bytes"
        );
    }

    #[test]
    fn tool_result_arc_keeps_existing_wal_bytes() {
        let record = WalRecord::ToolResult {
            finished_at_ms: None,
            tags: None,
            hooks: None,
            batch_id: BatchId("b1".into()),
            result: Arc::new(ToolResult {
                tool_call_id: ToolCallId("t1".into()),
                output: broca_protocol::content::ToolOutput::Text {
                    text: "done".into(),
                },
                is_error: false,
            }),
            outcome: DispatchOutcome::Completed,
            reason: None,
            withdraw_elicitation: None,
            provider_held: None,
        };

        let bytes = serde_json::to_vec(&record).expect("serialize shared tool result");
        assert_eq!(
            bytes,
            br#"{"type":"tool_result","batch_id":"b1","result":{"tool_call_id":"t1","output":{"kind":"text","text":"done"},"is_error":false},"outcome":"completed"}"#,
            "Arc must serialize its pointee without changing existing WAL bytes"
        );
    }

    /// Today a call refused before send is written as a `tool_result` with outcome `completed`
    /// and `is_error`, and a call sent with no result as one with outcome `unknown`. A result that names no
    /// typed reason must keep exactly these bytes, so a session written before any reason field
    /// existed replays and re-serializes unchanged, and an older build reads a newer build's
    /// results the same way.
    #[test]
    fn pre_reason_tool_result_bytes_are_pinned() {
        const REFUSED: &str = r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"error","message":"tool not dispatched: route gone"},"is_error":true},"outcome":"completed"}"#;
        const UNKNOWN: &str = r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"error","message":"tool outcome unknown (interrupted for review): route closed"},"is_error":true},"outcome":"unknown"}"#;
        for (pinned, outcome) in [
            (REFUSED, DispatchOutcome::Completed),
            (UNKNOWN, DispatchOutcome::Unknown),
        ] {
            let decoded: WalRecord = serde_json::from_str(pinned).unwrap();
            let WalRecord::ToolResult {
                outcome: decoded_outcome,
                ref result,
                ..
            } = decoded
            else {
                panic!("expected a tool_result, got {decoded:?}");
            };
            assert_eq!(decoded_outcome, outcome);
            assert!(result.is_error);
            assert!(
                matches!(decoded, WalRecord::ToolResult { reason: None, .. }),
                "a result written before the reason field reads as naming no reason"
            );
            assert_eq!(serde_json::to_vec(&decoded).unwrap(), pinned.as_bytes());
        }
    }

    fn user_msg(text: &str) -> Message {
        Message {
            turn_system: false,
            cache_prefix_blocks: None,
            role: Role::User,
            content: vec![ContentBlock::Text { text: text.into() }],
            origin: None,
            ordinal: None,
            tags: Default::default(),
        }
    }

    fn min_config() -> Box<RunConfig> {
        Box::new(RunConfig {
            global_system: None,
            account_route: None,
            route_decision_id: None,
            account_limit_pause: false,
            turn_directives_v1: None,
            render_tags: false,
            computed_tags: None,
            model: ModelHandle::new("p", "m"),
            tools: vec![].into(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            response_format: ResponseFormat::default(),
            provider_options: Value::Null,
            cache_tiers: Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: Value::Null,
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: Vec::new(),
            context_management: None,
        })
    }

    #[test]
    fn account_limit_pause_consent_is_optional_and_requires_its_reader() {
        let mut config = min_config();
        let legacy = serde_json::to_string(&config).unwrap();
        assert!(!legacy.contains("account_limit_pause"));
        assert!(
            !serde_json::from_str::<RunConfig>(&legacy)
                .unwrap()
                .account_limit_pause
        );
        config.account_limit_pause = true;
        assert_eq!(
            serde_json::to_string(&config).unwrap(),
            format!("{{\"account_limit_pause\":true,{}", &legacy[1..])
        );
        let mut record: WalRecord = serde_json::from_str(
            include_str!("../../../broca-wal/tests/fixtures/pre_compaction_records.jsonl")
                .lines()
                .find(|line| line.contains("\"type\":\"run_started\""))
                .unwrap(),
        )
        .unwrap();
        if let WalRecord::RunStarted { config: stored, .. } = &mut record {
            *stored = config;
        } else {
            panic!("fixture has a run start");
        }
        assert!(record
            .required_features()
            .contains(&crate::framing::ACCOUNT_LIMIT_PAUSE_FEATURE));
    }

    #[test]
    fn pre_resolved_credential_run_config_bytes_are_unchanged() {
        let mut config = min_config();
        config.auth_selection = Some(AuthSelection {
            billing_vault_facts: None,
            auth_method: None,
            credential_id: "apikey:kimi-code-plan-global".into(),
            account_ref: None,
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        });
        // Captured from the serializer before resolved_credential_id was added.
        const LEGACY: &str = r#"{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"auth_selection":{"credential_id":"apikey:kimi-code-plan-global"}}"#;
        assert_eq!(serde_json::to_vec(&config).unwrap(), LEGACY.as_bytes());
        let decoded: RunConfig = serde_json::from_str(LEGACY).unwrap();
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), LEGACY.as_bytes());
    }

    #[test]
    fn pre_vault_request_run_started_bytes_are_unchanged() {
        let mut config = min_config();
        config.auth_selection = Some(AuthSelection {
            billing_vault_facts: None,
            auth_method: None,
            credential_id: "oauth:anthropic:work".into(),
            account_ref: Some("acct-1".into()),
            resolved_credential_id: Some("oauth:anthropic:shared".into()),
            vault_request: None,
            account_decision: None,
        });
        let record = WalRecord::RunStarted {
            message_marks: None,
            compaction_plan_version: None,
            sent_by: None,
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("legacy-run".into()),
            session: BindIdentity::new("/tmp/project", "runner", "session"),
            ts_ms: None,
            config,
            input: vec![user_msg("hello")].into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        };
        // Captured from the serializer before AuthSelection recorded the vault request
        // parameters. A run that made no vault request must keep exactly these bytes.
        const LEGACY: &str = r#"{"type":"run_started","run_id":"legacy-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"auth_selection":{"credential_id":"oauth:anthropic:work","account_ref":"acct-1","resolved_credential_id":"oauth:anthropic:shared"}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&record).unwrap()).unwrap(),
            LEGACY
        );
        let decoded: WalRecord = serde_json::from_str(LEGACY).unwrap();
        assert_eq!(decoded, record);
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), LEGACY.as_bytes());
    }

    /// A RunStarted carrying every auth-selection field this build writes: credential id,
    /// account, resolved credential and a vault request with its operation name. Captured from
    /// the serializer before the selection gained `account_decision`; a selection without one
    /// must keep exactly these bytes, on write and on read-back, so records written before it
    /// replay unchanged.
    #[test]
    fn pre_account_decision_run_started_bytes_are_unchanged() {
        let mut config = min_config();
        config.auth_selection = Some(AuthSelection {
            billing_vault_facts: None,
            auth_method: None,
            credential_id: "oauth:anthropic:work".into(),
            account_ref: Some("acct-1".into()),
            resolved_credential_id: Some("oauth:anthropic:work".into()),
            vault_request: Some(VaultRequest {
                min_ttl_ms: Some(600_000),
                force_refresh: None,
                op: Some(VaultReadOp::GetScoped),
            }),
            account_decision: None,
        });
        let record = WalRecord::RunStarted {
            message_marks: None,
            compaction_plan_version: None,
            sent_by: None,
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("selection-run".into()),
            session: BindIdentity::new("/tmp/project", "runner", "session"),
            ts_ms: None,
            config,
            input: vec![user_msg("hello")].into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        };
        const LEGACY: &str = r#"{"type":"run_started","run_id":"selection-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"auth_selection":{"credential_id":"oauth:anthropic:work","account_ref":"acct-1","resolved_credential_id":"oauth:anthropic:work","vault_request":{"min_ttl_ms":600000,"op":"get_scoped"}}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&record).unwrap()).unwrap(),
            LEGACY
        );
        let decoded: WalRecord = serde_json::from_str(LEGACY).unwrap();
        assert_eq!(decoded, record);
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), LEGACY.as_bytes());
    }

    /// A selection that records how its account was chosen adds exactly the one key, after
    /// every older one, and reads back to the same bytes; a path this build does not name (a
    /// later build's) decodes and keeps its spelling.
    #[test]
    fn an_account_decision_adds_one_key_and_keeps_its_bytes() {
        const STORED: &str = r#"{"credential_id":"oauth:anthropic:work","account_ref":"acct-1","vault_request":{"min_ttl_ms":600000,"op":"get_scoped"},"account_decision":{"path":"routed","admission_id":"run-1","decision_id":"acd-1"}}"#;
        let selection = AuthSelection {
            billing_vault_facts: None,
            auth_method: None,
            credential_id: "oauth:anthropic:work".into(),
            account_ref: Some("acct-1".into()),
            resolved_credential_id: None,
            vault_request: Some(VaultRequest {
                min_ttl_ms: Some(600_000),
                force_refresh: None,
                op: Some(VaultReadOp::GetScoped),
            }),
            account_decision: Some(AccountDecision {
                conversation_key: None,
                conversation_key_source: None,
                path: ACCOUNT_DECISION_ROUTED.into(),
                admission_id: Some("run-1".into()),
                decision_id: Some("acd-1".into()),
            }),
        };
        assert_eq!(serde_json::to_string(&selection).unwrap(), STORED);
        let decoded: AuthSelection = serde_json::from_str(STORED).unwrap();
        assert_eq!(decoded, selection);

        const FUTURE: &str =
            r#"{"credential_id":"oauth:anthropic","account_decision":{"path":"some_future_path"}}"#;
        let decoded: AuthSelection = serde_json::from_str(FUTURE).unwrap();
        assert_eq!(
            decoded.account_decision.as_ref().map(|d| d.path.as_str()),
            Some("some_future_path")
        );
        assert_eq!(serde_json::to_string(&decoded).unwrap(), FUTURE);
    }

    #[test]
    fn model_step_finished_bytes_are_pinned() {
        // Captured from the serializer before steps recorded the service tier the provider
        // reported. A step whose response reported no tier must keep exactly these bytes, and
        // these bytes must keep decoding to the same record.
        let full = WalRecord::ModelStepFinished {
            raw_finish_reason: None,
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(3),
            assistant_message: (AssistantMessage {
                message_id: MessageId("msg-3".into()),
                content: vec![ContentBlock::Text { text: "ok".into() }],
            })
            .into(),
            usage: Usage {
                input_tokens: Some(120),
                cached_input_tokens: Some(0),
                cache_write_tokens: Some(40),
                cache_write_5m_tokens: None,
                cache_write_1h_tokens: None,
                output_tokens: Some(9),
                reasoning_tokens: Some(2),
            },
            finish_reason: Some(FinishReason::Stop),
            retries_used: 1,
            service_tier_used: None,
            speed_used: None,
        };
        let bare = WalRecord::ModelStepFinished {
            raw_finish_reason: None,
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(0),
            assistant_message: (AssistantMessage {
                message_id: MessageId("m".into()),
                content: vec![],
            })
            .into(),
            usage: Usage::default(),
            finish_reason: None,
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        };
        const FULL: &str = r#"{"type":"model_step_finished","step_id":3,"assistant_message":{"message_id":"msg-3","content":[{"type":"text","text":"ok"}]},"usage":{"input_tokens":120,"cached_input_tokens":0,"cache_write_tokens":40,"output_tokens":9,"reasoning_tokens":2},"finish_reason":"stop","retries_used":1}"#;
        const BARE: &str = r#"{"type":"model_step_finished","step_id":0,"assistant_message":{"message_id":"m","content":[]},"usage":{},"retries_used":0}"#;
        for (record, pinned) in [(full, FULL), (bare, BARE)] {
            assert_eq!(
                String::from_utf8(serde_json::to_vec(&record).unwrap()).unwrap(),
                pinned
            );
            let decoded: WalRecord = serde_json::from_str(pinned).unwrap();
            assert_eq!(decoded, record);
            assert_eq!(serde_json::to_vec(&decoded).unwrap(), pinned.as_bytes());
        }
    }

    #[test]
    fn cache_write_lifetimes_roundtrip_in_step_wal_frame() {
        let bytes = r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"synthetic","content":[]},"usage":{"cache_write_tokens":30,"cache_write_5m_tokens":10,"cache_write_1h_tokens":20},"retries_used":0}"#;
        let record: WalRecord = serde_json::from_str(bytes).unwrap();
        let encoded = crate::framing::encode(7, 3, &record).unwrap();
        assert_eq!(encoded[4], crate::framing::FRAMING_VERSION);
        let crate::framing::DecodeStep::Record { framed, .. } =
            crate::framing::decode_next(&encoded)
        else {
            panic!("usage frame must decode");
        };
        assert_eq!(serde_json::to_string(&framed.record).unwrap(), bytes);
    }

    #[test]
    fn old_records_default_retries_used_to_zero() {
        let model = WalRecord::ModelStepFinished {
            raw_finish_reason: None,
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(1),
            assistant_message: (AssistantMessage {
                message_id: MessageId("m".into()),
                content: vec![ContentBlock::Text { text: "ok".into() }],
            })
            .into(),
            usage: Usage::default(),
            finish_reason: None,
            retries_used: 7,
            service_tier_used: None,
            speed_used: None,
        };
        let terminal = WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 9,
        };
        for record in [model, terminal] {
            let mut value = serde_json::to_value(record).expect("serialize current record");
            value
                .as_object_mut()
                .expect("tagged record")
                .remove("retries_used");
            let decoded: WalRecord = serde_json::from_value(value).expect("old record decodes");
            match decoded {
                WalRecord::ModelStepFinished { retries_used, .. }
                | WalRecord::RunFinished { retries_used, .. } => assert_eq!(retries_used, 0),
                other => panic!("unexpected record: {other:?}"),
            }
        }
    }

    #[test]
    fn pre_fix_run_started_omits_ts_ms_and_decodes_as_none() {
        let record = WalRecord::RunStarted {
            message_marks: None,
            compaction_plan_version: None,
            sent_by: None,
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("legacy-run".into()),
            session: BindIdentity::new("/tmp/project", "runner", "session"),
            ts_ms: None,
            config: min_config(),
            input: vec![user_msg("hello")].into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        };
        // Captured from the pre-project-id serializer, not generated from the new shape.
        const LEGACY: &str = r#"{"type":"run_started","run_id":"legacy-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        assert_eq!(serde_json::to_vec(&record).unwrap(), LEGACY.as_bytes());
        let mut observed = record.clone();
        if let WalRecord::RunStarted { project_id, .. } = &mut observed {
            *project_id = Some("pj-a".into());
        }
        let bytes = serde_json::to_vec(&observed).unwrap();
        assert_eq!(bytes.len() - LEGACY.len(), br#","project_id":"pj-a""#.len());
        assert_eq!(
            serde_json::from_slice::<WalRecord>(&bytes).unwrap(),
            observed
        );
        let mut pre_fix = serde_json::to_value(&record).expect("serialize legacy shape");
        pre_fix
            .as_object_mut()
            .expect("tagged record")
            .remove("ts_ms");

        let decoded: WalRecord = serde_json::from_value(pre_fix.clone()).expect("decode old WAL");
        assert!(matches!(decoded, WalRecord::RunStarted { ts_ms: None, .. }));
        let reencoded = serde_json::to_value(decoded).expect("re-encode old WAL");
        assert_eq!(
            reencoded, pre_fix,
            "old record shape must remain byte-compatible"
        );
    }

    #[test]
    fn pre_work_class_run_started_bytes_are_pinned() {
        let record = WalRecord::RunStarted {
            message_marks: None,
            compaction_plan_version: None,
            sent_by: None,
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("legacy-run".into()),
            session: BindIdentity::new("/tmp/project", "runner", "session"),
            ts_ms: None,
            config: min_config(),
            input: vec![user_msg("hello")].into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        };
        // Captured before RunConfig gained work_class; an omitted value must remain byte-identical.
        const PRE_WORK_CLASS: &str = r#"{"type":"run_started","run_id":"legacy-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        assert_eq!(
            serde_json::to_vec(&record).unwrap(),
            PRE_WORK_CLASS.as_bytes()
        );
    }

    /// Captured from the serializer before `RunConfig` gained `context_management`. Both a
    /// minimal config and one carrying every other optional field must keep exactly these
    /// bytes, so a session admitted before the field existed replays and re-serializes
    /// unchanged, and an absent value costs no bytes on any record.
    #[test]
    fn pre_context_management_run_started_bytes_are_pinned() {
        let record = |config: Box<RunConfig>| WalRecord::RunStarted {
            message_marks: None,
            compaction_plan_version: None,
            sent_by: None,
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("legacy-run".into()),
            session: BindIdentity::new("/tmp/project", "runner", "session"),
            ts_ms: None,
            config,
            input: vec![user_msg("hello")].into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        };
        const MINIMAL: &str = r#"{"type":"run_started","run_id":"legacy-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        let minimal = record(min_config());
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&minimal).unwrap()).unwrap(),
            MINIMAL
        );
        let decoded: WalRecord = serde_json::from_str(MINIMAL).unwrap();
        assert_eq!(decoded, minimal);

        let mut populated = min_config();
        populated.tools_were_omitted = true;
        populated.context_limit = Some(200_000);
        populated.work_class = Some("mason".into());
        populated.stop_when = vec![StopCondition::StepCount { n: 7 }];
        let populated = record(populated);
        const POPULATED: &str = r#"{"type":"run_started","run_id":"legacy-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"tools_were_omitted":true,"context_limit":200000,"work_class":"mason","stop_when":[{"type":"step_count","n":7}]},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&populated).unwrap()).unwrap(),
            POPULATED
        );
        let decoded: WalRecord = serde_json::from_str(POPULATED).unwrap();
        assert_eq!(decoded, populated);
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), POPULATED.as_bytes());
    }

    /// Captured from the serializer before `RunConfig` gained the caller's requested reasoning
    /// `variant`. A record without a variant, minimal or carrying every other optional field,
    /// must keep exactly these bytes, so a session admitted before the field existed replays and
    /// re-serializes unchanged and a send without a variant costs no bytes on any record.
    #[test]
    fn pre_variant_run_started_bytes_are_pinned() {
        let record = |config: Box<RunConfig>| WalRecord::RunStarted {
            message_marks: None,
            compaction_plan_version: None,
            sent_by: None,
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("legacy-run".into()),
            session: BindIdentity::new("/tmp/project", "runner", "session"),
            ts_ms: None,
            config,
            input: vec![user_msg("hello")].into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        };
        const MINIMAL: &str = r#"{"type":"run_started","run_id":"legacy-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        let minimal = record(min_config());
        assert_eq!(serde_json::to_vec(&minimal).unwrap(), MINIMAL.as_bytes());
        let decoded: WalRecord = serde_json::from_str(MINIMAL).unwrap();
        assert_eq!(decoded, minimal);
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), MINIMAL.as_bytes());

        let mut populated = min_config();
        populated.tools_were_omitted = true;
        populated.context_limit = Some(200_000);
        populated.work_class = Some("mason".into());
        populated.stop_when = vec![StopCondition::StepCount { n: 7 }];
        // Absent context management costs no bytes; stated so the pin covers every optional
        // field that existed before `variant`.
        populated.context_management = None;
        populated.frozen_render_config =
            serde_json::json!({"family_id": "openai_responses_chatgpt", "reasoning_effort": "max"});
        let populated = record(populated);
        const POPULATED: &str = r#"{"type":"run_started","run_id":"legacy-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"frozen_render_config":{"family_id":"openai_responses_chatgpt","reasoning_effort":"max"},"tools_were_omitted":true,"context_limit":200000,"work_class":"mason","stop_when":[{"type":"step_count","n":7}]},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        assert_eq!(
            serde_json::to_vec(&populated).unwrap(),
            POPULATED.as_bytes()
        );
        let decoded: WalRecord = serde_json::from_str(POPULATED).unwrap();
        assert_eq!(decoded, populated);
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), POPULATED.as_bytes());
    }

    /// Captured from the serializer before `RunConfig` gained the caller's requested
    /// `service_tier`. A record without a tier, minimal or carrying every other optional field
    /// (a variant and context management included), must keep exactly these bytes: a session
    /// admitted before the field existed replays and re-serializes unchanged, and a send without
    /// a tier costs no bytes on any record.
    #[test]
    fn pre_service_tier_run_started_bytes_are_pinned() {
        let record = |config: Box<RunConfig>| WalRecord::RunStarted {
            message_marks: None,
            compaction_plan_version: None,
            sent_by: None,
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("legacy-run".into()),
            session: BindIdentity::new("/tmp/project", "runner", "session"),
            ts_ms: Some(1_790_000_000_000),
            config,
            input: vec![user_msg("hello")].into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        };
        const MINIMAL: &str = r#"{"type":"run_started","run_id":"legacy-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        let minimal = record(min_config());
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&minimal).unwrap()).unwrap(),
            MINIMAL
        );
        let decoded: WalRecord = serde_json::from_str(MINIMAL).unwrap();
        assert_eq!(decoded, minimal);
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), MINIMAL.as_bytes());

        let mut populated = min_config();
        populated.tools_were_omitted = true;
        populated.context_limit = Some(200_000);
        populated.work_class = Some("mason".into());
        populated.variant = Some("high".into());
        populated.stop_when = vec![StopCondition::StepCount { n: 7 }];
        populated.context_management = Some(ContextManagement::None);
        populated.frozen_render_config = serde_json::json!({
            "family_id": "openai_responses_chatgpt",
            "service_tier": "priority"
        });
        let populated = record(populated);
        const POPULATED: &str = r#"{"type":"run_started","run_id":"legacy-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"frozen_render_config":{"family_id":"openai_responses_chatgpt","service_tier":"priority"},"tools_were_omitted":true,"context_limit":200000,"work_class":"mason","variant":"high","stop_when":[{"type":"step_count","n":7}],"context_management":"none"},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&populated).unwrap()).unwrap(),
            POPULATED
        );
        let decoded: WalRecord = serde_json::from_str(POPULATED).unwrap();
        assert_eq!(decoded, populated);
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), POPULATED.as_bytes());
    }

    /// Records this build reads but never writes: a `run_finished` with reason
    /// `transform_unavailable`, and `{"transform": ...}` as `context_management` on a
    /// `RunStarted` config and on a queued request. Older WALs carry these bytes. Each must keep
    /// decoding to the same value and re-encoding to the same bytes; a reader that refused them
    /// would make the whole session unreadable.
    #[test]
    fn records_from_the_removed_transform_route_still_decode_and_keep_their_bytes() {
        const FINISHED: &str = r#"{"type":"run_finished","reason":"transform_unavailable","ts_ms":1790000000000,"usage":{},"error":{"class":"permanent","message":"transform unavailable: route closed"},"retries_used":0}"#;
        const STARTED: &str = r#"{"type":"run_started","run_id":"mc-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"context_limit":200000,"context_management":{"transform":"magic-context"}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        const QUEUED: &str = r#"{"type":"queued","submission_id":"sub-mc","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[],"context_management":{"transform":"magic-context"}}}"#;

        for pinned in [FINISHED, STARTED, QUEUED] {
            let decoded: WalRecord = serde_json::from_str(pinned)
                .unwrap_or_else(|error| panic!("{pinned} must still decode: {error}"));
            assert_eq!(
                String::from_utf8(serde_json::to_vec(&decoded).unwrap()).unwrap(),
                pinned,
                "a pinned record must re-encode to exactly the bytes it was read from"
            );
        }

        // The values really came through, rather than decoding into something that happens to
        // re-encode the same way.
        let WalRecord::RunFinished { reason, error, .. } =
            serde_json::from_str::<WalRecord>(FINISHED).unwrap()
        else {
            panic!("expected RunFinished");
        };
        assert_eq!(reason, RunFinishReason::TransformUnavailable);
        assert_eq!(error.map(|error| error.class), Some(ErrorClass::Permanent));
        let WalRecord::RunStarted { config, .. } =
            serde_json::from_str::<WalRecord>(STARTED).unwrap()
        else {
            panic!("expected RunStarted");
        };
        assert_eq!(
            config.context_management,
            Some(ContextManagement::Transform("magic-context".into()))
        );
        let WalRecord::Queued { request, .. } = serde_json::from_str::<WalRecord>(QUEUED).unwrap()
        else {
            panic!("expected Queued");
        };
        assert_eq!(
            request.context_management,
            Some(ContextManagement::Transform("magic-context".into()))
        );
    }

    /// A `RunStarted` whose frozen render config carries a prompt-cache policy, captured before a
    /// session could warm its cache between requests. Warming adds no field to the episode
    /// record: an Anthropic 1h marker policy and an OpenAI 24h hint policy both keep these bytes.
    #[test]
    fn pre_cache_warm_run_started_bytes_are_pinned() {
        let record = |frozen_render_config: serde_json::Value| {
            let mut config = min_config();
            config.frozen_render_config = frozen_render_config;
            WalRecord::RunStarted {
                message_marks: None,
                compaction_plan_version: None,
                sent_by: None,
                written_at_ms: None,
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: Some(1),
                manifest: None,
                scope: None,
                send_id: Some("send-1".into()),
                request_identity_digest: None,
                project_id: None,
                principal: None,
                run_id: RunId("cache-run".into()),
                session: BindIdentity::new("/tmp/project", "runner", "session"),
                ts_ms: Some(1_790_000_000_000),
                config,
                input: vec![user_msg("hello")].into(),
                origin: RunOrigin::Fresh,
                submission_id: None,
            }
        };
        for (frozen, pinned) in [
            (
                serde_json::json!({
                    "family_id": "anthropic_messages",
                    "cache": {"version": 1, "class": "markers", "syntax": "anthropic_cache_control",
                              "ttl": "1h", "max_breakpoints": 4, "lookback_blocks": 20}
                }),
                r#"{"type":"run_started","run_id":"cache-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"frozen_render_config":{"cache":{"class":"markers","lookback_blocks":20,"max_breakpoints":4,"syntax":"anthropic_cache_control","ttl":"1h","version":1},"family_id":"anthropic_messages"}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"send_id":"send-1","marker_order_version":1}"#,
            ),
            (
                serde_json::json!({
                    "family_id": "openai_responses",
                    "cache": {"version": 1, "class": "hints", "cache_key": "k", "retention": "24h"}
                }),
                r#"{"type":"run_started","run_id":"cache-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"frozen_render_config":{"cache":{"cache_key":"k","class":"hints","retention":"24h","version":1},"family_id":"openai_responses"}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"send_id":"send-1","marker_order_version":1}"#,
            ),
        ] {
            let record = record(frozen);
            assert_eq!(
                String::from_utf8(serde_json::to_vec(&record).unwrap()).unwrap(),
                pinned
            );
            let decoded: WalRecord = serde_json::from_str(pinned).unwrap();
            assert_eq!(decoded, record);
        }
    }

    /// A requested tier is recorded under `service_tier` in its wire spelling and reads back as
    /// the same tier; absent it costs nothing (pinned above).
    #[test]
    fn a_recorded_service_tier_round_trips() {
        let mut config = min_config();
        config.service_tier = Some(broca_protocol::request::ServiceTier::Priority);
        let encoded = serde_json::to_value(&config).unwrap();
        assert_eq!(encoded["service_tier"], "priority");
        let decoded: RunConfig = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, *config);
    }

    /// A RunStarted stored with a recorded vault request must read back and re-serialize to
    /// exactly the bytes it was stored with. Captured from the serializer while
    /// `VaultRequest` was `{ min_ttl_ms, force_refresh: bool }`, before the request gained an
    /// operation name: a later field on the request has to stay absent on a record that
    /// predates it rather than acquiring a default value.
    #[test]
    fn a_stored_run_started_with_a_vault_request_reserializes_to_its_stored_bytes() {
        const STORED: &str = r#"{"type":"run_started","run_id":"vault-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"auth_selection":{"credential_id":"apikey:deepseek","resolved_credential_id":"apikey:deepseek","vault_request":{"min_ttl_ms":600000,"force_refresh":false}}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        let decoded: WalRecord = serde_json::from_str(STORED).expect("decode stored RunStarted");
        match &decoded {
            WalRecord::RunStarted { config, .. } => assert!(
                config
                    .auth_selection
                    .as_ref()
                    .is_some_and(|selection| selection.vault_request.is_some()),
                "the pin must carry a vault request, or it proves nothing about one"
            ),
            other => panic!("expected RunStarted, got {other:?}"),
        }
        assert_eq!(serde_json::to_string(&decoded).unwrap(), STORED);
    }

    /// A RunStarted written by a LATER build, whose vault request names an operation this
    /// build does not know, still decodes -- a rollback must not make the session unreadable --
    /// and re-serializes to exactly its stored bytes, the unknown name included. Reading it
    /// never claims the request was a `credential.get`.
    #[test]
    fn a_run_started_with_a_future_vault_op_decodes_and_keeps_its_bytes() {
        const STORED: &str = r#"{"type":"run_started","run_id":"vault-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"auth_selection":{"credential_id":"apikey:deepseek","vault_request":{"min_ttl_ms":600000,"op":"some_future_op"}}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        let decoded: WalRecord =
            serde_json::from_str(STORED).expect("a future op must not make RunStarted undecodable");
        let WalRecord::RunStarted { config, .. } = &decoded else {
            panic!("expected RunStarted, got {decoded:?}");
        };
        let request = config
            .auth_selection
            .as_ref()
            .and_then(|selection| selection.vault_request.clone())
            .expect("the stored request survives decoding");
        assert_eq!(
            request.op,
            Some(VaultReadOp::Unknown("some_future_op".into()))
        );
        assert_eq!(
            request.op_or_legacy(),
            VaultReadOp::Unknown("some_future_op".into()),
            "an unknown op must not be read as credential.get"
        );
        assert_eq!(serde_json::to_string(&decoded).unwrap(), STORED);
    }

    /// A request records what was sent: a handle read spells out its false `force_refresh`
    /// and names `get`; a scoped read, which has no refresh parameter, omits it and names
    /// `get_scoped`; a record from before the operation name existed reads as `get`.
    #[test]
    fn vault_request_records_the_operation_and_only_the_parameters_it_sent() {
        let handle_read = VaultRequest {
            min_ttl_ms: Some(600_000),
            force_refresh: Some(false),
            op: Some(VaultReadOp::Get),
        };
        assert_eq!(
            serde_json::to_string(&handle_read).unwrap(),
            r#"{"min_ttl_ms":600000,"force_refresh":false,"op":"get"}"#
        );
        let scoped_read = VaultRequest {
            min_ttl_ms: Some(600_000),
            force_refresh: None,
            op: Some(VaultReadOp::GetScoped),
        };
        assert_eq!(
            serde_json::to_string(&scoped_read).unwrap(),
            r#"{"min_ttl_ms":600000,"op":"get_scoped"}"#
        );
        let legacy: VaultRequest =
            serde_json::from_str(r#"{"min_ttl_ms":600000,"force_refresh":false}"#).unwrap();
        assert_eq!(legacy.op, None);
        assert_eq!(legacy.op_or_legacy(), VaultReadOp::Get);
        assert_eq!(scoped_read.op_or_legacy(), VaultReadOp::GetScoped);
    }

    #[test]
    fn every_v2_record_round_trips() {
        let provider_err = ProviderError {
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Auth,
            message: "needs reauth".into(),
            status: Some(401),
            retry_after_secs: None,
            provider_code: Some("invalid_token".into()),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        };
        let records = vec![
            WalRecord::RunStarted {
                message_marks: None,
                compaction_plan_version: None,
                sent_by: None,
                written_at_ms: None,
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                run_id: RunId("r1".into()),
                session: BindIdentity::new("/root", "runner", "s1"),
                ts_ms: None,
                config: min_config(),
                input: vec![user_msg("hi")].into(),
                origin: RunOrigin::Fresh,
                submission_id: Some(SubmissionId("sub1".into())),
            },
            WalRecord::ModelStepFinished {
                raw_finish_reason: None,
                fallback: Default::default(),
                written_at_ms: None,
                tags: None,
                hooks: None,
                step_id: StepId(1),
                assistant_message: AssistantMessage {
                    message_id: MessageId("m1".into()),
                    content: vec![ContentBlock::Text { text: "ok".into() }],
                }
                .into(),
                usage: Usage::default(),
                finish_reason: Some(FinishReason::Stop),
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
            WalRecord::StepStarted { step_id: StepId(1) },
            WalRecord::ToolDispatchIntent {
                dispatched_at_ms: None,
                hooks: None,
                batch_id: BatchId("b1".into()),
                tool_call_id: ToolCallId("t1".into()),
                tool_name: Some("read".into()),
                args: json!({"path": "x"}).into(),
                module_id: None,
                schema_pin: None,
                approval_digest: None,
                call_key: None,
            },
            WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Error,
                usage: Usage::default(),
                error: Some(provider_err.clone()),
                retries_used: 0,
            },
            WalRecord::RunPaused {
                account_limit: None,
                run_id: RunId("r1".into()),
                ts_ms: None,
                reason: PauseReason::AuthRequired,
                error: Some(provider_err),
            },
            WalRecord::RunConfigRefrozen {
                renderer_version: 2,
                ts_ms: None,
                frozen_render_config: json!({"project_id": "project-1"}),
            },
            WalRecord::RunAuthRefrozen {
                ts_ms: Some(123),
                auth_selection: AuthSelection {
                    billing_vault_facts: None,
                    auth_method: None,
                    credential_id: "chatgpt:openai".into(),
                    account_ref: Some("account-1".into()),
                    resolved_credential_id: None,
                    vault_request: None,
                    account_decision: None,
                },
            },
            WalRecord::BudgetFired {
                tags: None,
                run_id: RunId("r1".into()),
                kind: BudgetKind::Nudge,
                threshold: 20,
                ordinal: 5,
                caller_text: Some("nudge text".into()),
            },
            WalRecord::Queued {
                message_marks: None,
                sent_by: None,
                written_at_ms: None,
                retention: None,
                ts_ms: None,
                hooks: None,
                title: None,
                keep_warm: None,
                conversation_key: None,
                send_id: None,
                delivery: None,
                submission_id: SubmissionId("sub2".into()),
                request: Box::new(SubmissionRequest {
                    message_marks: Default::default(),
                    account_route: None,
                    route_decision_id: None,
                    service_tier_level: None,
                    staged_uploads: None,
                    content: Vec::new(),
                    directives: None,
                    on_restart: None,
                    keep_warm: None,
                    scope: Default::default(),
                    project_id: None,
                    principal: None,
                    model: ModelHandle::new("anthropic", "claude-opus-4-8"),
                    input: vec![user_msg("queued prompt")],
                    tools: Some(vec![]),
                    policy_allowed: Default::default(),
                    tool_choice: ToolChoice::Auto,
                    generation: GenerationConfig::default(),
                    cache: None,
                    budget: None,
                    auth: None,
                    context_limit: None,
                    work_class: None,
                    variant: None,
                    service_tier: None,
                    stop_when: vec![],
                    context_management: None,
                    inherit_frozen: Default::default(),
                    structured_output: None,
                    client_message_id: None,
                }),
                request_identity_digest: None,
                principal: None,
            },
            WalRecord::Retracted {
                submission_id: SubmissionId("sub2".into()),
                failure: None,
            },
            WalRecord::Retracted {
                submission_id: SubmissionId("sub3".into()),
                failure: Some(QueuedAdmissionFailure {
                    run_id: RunId("r3".into()),
                    error: ProviderError {
                        provider_details: None,
                        provider_subcode: None,
                        refusal: None,
                        class: ErrorClass::Permanent,
                        message: "unknown model".into(),
                        status: None,
                        retry_after_secs: None,
                        provider_code: Some("queued_admission_failed".into()),
                        resets_at_ms: None,
                        resets_at_source: None,
                        limit_window: None,
                        account_ref: None,
                        credential_id: None,
                        affordable_max_tokens: None,
                    },
                }),
            },
        ];
        for rec in &records {
            round_trips(rec);
        }
    }

    /// The exact bytes a terminal `RunFinished` and a `RunPaused` carrying a typed error were
    /// written with before any account-limit fields existed on `ProviderError`. Old WALs hold
    /// these bytes, so both records must still decode from them and re-encode to them
    /// unchanged: a new optional error field that serialized when absent would change every
    /// terminal record broca writes and every consumer pinned to them.
    #[test]
    fn terminal_error_records_keep_their_pinned_bytes() {
        const FINISHED: &str = r#"{"type":"run_finished","reason":"error","ts_ms":1790000000000,"usage":{},"error":{"class":"transient","message":"429: rate limited","status":429,"retry_after_secs":353271.0,"provider_code":"rate_limit_error"},"retries_used":1}"#;
        const PAUSED: &str = r#"{"type":"run_paused","run_id":"r1","ts_ms":1790000000001,"reason":"auth_required","error":{"class":"auth_required","message":"needs reauth","status":401,"provider_code":"authentication_error"}}"#;
        let finished = WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: Some(1_790_000_000_000),
            reason: RunFinishReason::Error,
            usage: Usage::default(),
            error: Some(ProviderError {
                provider_details: None,
                provider_subcode: None,
                refusal: None,
                class: ErrorClass::Transient,
                message: "429: rate limited".into(),
                status: Some(429),
                retry_after_secs: Some(353271.0),
                provider_code: Some("rate_limit_error".into()),
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
                affordable_max_tokens: None,
            }),
            retries_used: 1,
        };
        let paused = WalRecord::RunPaused {
            account_limit: None,
            run_id: RunId("r1".into()),
            ts_ms: Some(1_790_000_000_001),
            reason: PauseReason::AuthRequired,
            error: Some(ProviderError {
                provider_details: None,
                provider_subcode: None,
                refusal: None,
                class: ErrorClass::Auth,
                message: "needs reauth".into(),
                status: Some(401),
                retry_after_secs: None,
                provider_code: Some("authentication_error".into()),
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
                affordable_max_tokens: None,
            }),
        };
        for (record, pinned) in [(finished, FINISHED), (paused, PAUSED)] {
            assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
            let decoded: WalRecord = serde_json::from_str(pinned).unwrap();
            assert_eq!(decoded, record);
            assert_eq!(serde_json::to_string(&decoded).unwrap(), pinned);
        }
    }

    /// The exact bytes of an account-limit terminal. Routing decodes these keys to wall one
    /// provider account until `resets_at_ms`, so their names, order and number forms are a
    /// contract with that decoder.
    #[test]
    fn account_limit_terminal_error_has_pinned_bytes() {
        const FINISHED: &str = r#"{"type":"run_finished","reason":"error","ts_ms":1790387965000,"usage":{},"error":{"class":"transient","message":"429: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"This request would exceed your account's rate limit. Please try again later.\"},\"request_id\":\"req_011CfRDPYNrfveajQccKCJ7o\"}","status":429,"retry_after_secs":352835.0,"provider_code":"account_limit_exhausted","resets_at_ms":1790740800000,"limit_window":"seven_day","account_ref":"d4bc9dbc"},"retries_used":0}"#;
        let record = WalRecord::RunFinished { interrupt_reason: None, ts_ms: Some(1_790_387_965_000),
        reason: RunFinishReason::Error,
        usage: Usage::default(),
        error: Some(ProviderError {
        provider_details: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Transient,
            message: r#"429: {"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later."},"request_id":"req_011CfRDPYNrfveajQccKCJ7o"}"#.into(),
            status: Some(429),
            retry_after_secs: Some(352_835.0),
            provider_code: Some(broca_protocol::error::ACCOUNT_LIMIT_EXHAUSTED.into()),
            resets_at_ms: Some(1_790_740_800_000),
            resets_at_source: None,
            limit_window: Some("seven_day".into()),
            account_ref: Some("d4bc9dbc".into()),
            credential_id: None,
            affordable_max_tokens: None,
        }),
        retries_used: 0, };
        assert_eq!(serde_json::to_string(&record).unwrap(), FINISHED);
        assert_eq!(serde_json::from_str::<WalRecord>(FINISHED).unwrap(), record);
    }

    /// The exact bytes of the terminal errors that name a credential: a billing refusal with
    /// and without the run's `account_ref`, an HTTP 429 rate-limit terminal with no provider
    /// code, and an auth pause that carries an `account_ref`. Old WALs hold these bytes, and a
    /// later optional error field must leave every one of them unchanged when it is absent.
    #[test]
    fn credential_scoped_terminal_error_bytes_are_pinned() {
        const BILLING_WITH_ACCOUNT: &str = r#"{"type":"run_finished","reason":"error","ts_ms":1790000000002,"usage":{},"error":{"class":"permanent","message":"403: spending limit","status":403,"provider_code":"billing_exhausted","account_ref":"acct-x"},"retries_used":0}"#;
        const BILLING_WITHOUT_ACCOUNT: &str = r#"{"type":"run_finished","reason":"error","ts_ms":1790000000003,"usage":{},"error":{"class":"permanent","message":"403: spending limit","status":403,"provider_code":"billing_exhausted"},"retries_used":0}"#;
        const RATE_LIMITED: &str = r#"{"type":"run_finished","reason":"error","ts_ms":1790000000004,"usage":{},"error":{"class":"transient","message":"429: slow down","status":429,"retry_after_secs":30.0},"retries_used":3}"#;
        const PAUSED_WITH_ACCOUNT: &str = r#"{"type":"run_paused","run_id":"r2","ts_ms":1790000000005,"reason":"auth_required","error":{"class":"auth_required","message":"needs reauth","status":401,"account_ref":"acct-y"}}"#;
        let error = |class,
                     message: &str,
                     status,
                     retry_after_secs,
                     code: Option<&str>,
                     account: Option<&str>| ProviderError {
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            class,
            message: message.into(),
            status: Some(status),
            retry_after_secs,
            provider_code: code.map(str::to_owned),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: account.map(str::to_owned),
            credential_id: None,
            affordable_max_tokens: None,
        };
        let finished = |ts_ms, error, retries_used| WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: Some(ts_ms),
            reason: RunFinishReason::Error,
            usage: Usage::default(),
            error: Some(error),
            retries_used,
        };
        let cases = [
            (
                finished(
                    1_790_000_000_002,
                    error(
                        ErrorClass::Permanent,
                        "403: spending limit",
                        403,
                        None,
                        Some("billing_exhausted"),
                        Some("acct-x"),
                    ),
                    0,
                ),
                BILLING_WITH_ACCOUNT,
            ),
            (
                finished(
                    1_790_000_000_003,
                    error(
                        ErrorClass::Permanent,
                        "403: spending limit",
                        403,
                        None,
                        Some("billing_exhausted"),
                        None,
                    ),
                    0,
                ),
                BILLING_WITHOUT_ACCOUNT,
            ),
            (
                finished(
                    1_790_000_000_004,
                    error(
                        ErrorClass::Transient,
                        "429: slow down",
                        429,
                        Some(30.0),
                        None,
                        None,
                    ),
                    3,
                ),
                RATE_LIMITED,
            ),
            (
                WalRecord::RunPaused {
                    account_limit: None,
                    run_id: RunId("r2".into()),
                    ts_ms: Some(1_790_000_000_005),
                    reason: PauseReason::AuthRequired,
                    error: Some(error(
                        ErrorClass::Auth,
                        "needs reauth",
                        401,
                        None,
                        None,
                        Some("acct-y"),
                    )),
                },
                PAUSED_WITH_ACCOUNT,
            ),
        ];
        for (record, pinned) in cases {
            assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
            let decoded: WalRecord = serde_json::from_str(pinned).unwrap();
            assert_eq!(decoded, record);
            assert_eq!(serde_json::to_string(&decoded).unwrap(), pinned);
        }
    }

    #[test]
    fn run_finished_round_trip_preserves_provider_unavailable_error_class() {
        let record = WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Error,
            usage: Usage::default(),
            error: Some(ProviderError {
                provider_details: None,
                provider_subcode: None,
                refusal: None,
                class: ErrorClass::ProviderUnavailable,
                message: "provider stream disconnected".into(),
                status: None,
                retry_after_secs: None,
                provider_code: None,
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
                affordable_max_tokens: None,
            }),
            retries_used: 0,
        };

        let bytes = serde_json::to_vec(&record).expect("serialize WAL record");
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap()["error"]["class"],
            "provider_unavailable",
            "the durable record retains the producer-local diagnostic class"
        );

        let decoded: WalRecord = serde_json::from_slice(&bytes).expect("deserialize WAL record");
        let WalRecord::RunFinished {
            error: Some(error), ..
        } = decoded
        else {
            panic!("expected terminal error in RunFinished");
        };
        assert_eq!(error.class, ErrorClass::ProviderUnavailable);
    }

    #[test]
    fn legacy_retracted_record_defaults_to_no_failure() {
        let record: WalRecord = serde_json::from_value(json!({
            "type": "retracted",
            "submission_id": "sub-old"
        }))
        .expect("decode legacy retract");
        assert_eq!(
            record,
            WalRecord::Retracted {
                submission_id: SubmissionId("sub-old".into()),
                failure: None,
            }
        );
    }

    #[test]
    fn run_config_stop_when_round_trips_and_defaults_when_absent() {
        let mut config = *min_config();
        config.stop_when = vec![
            StopCondition::StepCount { n: 3 },
            StopCondition::HasToolCall {
                name: Some("edit".into()),
            },
            StopCondition::LoopFinished,
        ];
        let encoded = serde_json::to_value(&config).expect("serialize stop predicates");
        assert!(encoded.get("stop_when").is_some());
        let decoded: RunConfig =
            serde_json::from_value(encoded.clone()).expect("deserialize stop predicates");
        assert_eq!(decoded.stop_when, config.stop_when);

        let mut legacy = encoded;
        legacy
            .as_object_mut()
            .expect("run config object")
            .remove("stop_when");
        let decoded: RunConfig =
            serde_json::from_value(legacy).expect("deserialize pre-stop-policy config");
        assert!(
            decoded.stop_when.is_empty(),
            "an old record keeps the historical empty policy and default safety cap"
        );
    }

    #[test]
    fn run_started_origin_defaults_to_fresh_when_absent() {
        // An older-shaped RunStarted (no `origin`, no `submission_id`) must read back
        // with origin = Fresh and submission_id = None.
        let older = json!({
            "type": "run_started",
            "run_id": "r1",
            "session": BindIdentity::new("/root", "runner", "s1"),
            "config": *min_config(),
            "input": [user_msg("hi")],
        });
        let rec: WalRecord = serde_json::from_value(older).expect("deserialize older shape");
        match rec {
            WalRecord::RunStarted {
                origin,
                submission_id,
                ..
            } => {
                assert_eq!(origin, RunOrigin::Fresh, "absent origin defaults to Fresh");
                assert_eq!(submission_id, None, "absent submission_id defaults to None");
            }
            other => panic!("expected RunStarted, got {other:?}"),
        }
    }

    #[test]
    fn seeded_from_origins_round_trip_and_bare_payload_is_compatible() {
        let import = RunOrigin::SeededFrom {
            origin_kind: SeedOriginKind::Import,
            source_ref: "cc:session-1".into(),
            origin_device_pseudonym: None,
            base_seq: None,
        };
        let fork = RunOrigin::SeededFrom {
            origin_kind: SeedOriginKind::Fork,
            source_ref: "ck:lineage-1".into(),
            origin_device_pseudonym: Some([7; 16]),
            base_seq: Some(42),
        };
        for origin in [&import, &fork] {
            let bytes = serde_json::to_vec(origin).expect("serialize origin");
            let back: RunOrigin = serde_json::from_slice(&bytes).expect("deserialize origin");
            assert_eq!(origin, &back);
        }

        let bare: RunOrigin = serde_json::from_value(json!({"kind": "seeded_from"}))
            .expect("deserialize reserved bare SeededFrom shape");
        assert_eq!(
            bare,
            RunOrigin::SeededFrom {
                origin_kind: SeedOriginKind::Import,
                source_ref: String::new(),
                origin_device_pseudonym: None,
                base_seq: None,
            }
        );
    }

    #[test]
    fn run_config_auth_selection_defaults_to_none_when_absent() {
        // A RunConfig written before `auth_selection` was added must read back with
        // auth_selection = None. This proves the field can be optional-with-default (no
        // schema version bump needed): a pre-existing record stays valid, and the path that
        // never sets it is unchanged.
        // Serialize a current config, then REMOVE the auth_selection key to simulate a
        // record written before the field existed.
        let mut config_json = serde_json::to_value(&*min_config()).unwrap();
        config_json
            .as_object_mut()
            .unwrap()
            .remove("auth_selection");
        assert!(
            config_json.get("auth_selection").is_none(),
            "precondition: the field is absent in the pre-auth shape"
        );
        let older = json!({
            "type": "run_started",
            "run_id": "r1",
            "session": BindIdentity::new("/root", "runner", "s1"),
            "config": config_json,
            "input": [user_msg("hi")],
        });
        let rec: WalRecord = serde_json::from_value(older).expect("deserialize pre-auth shape");
        match rec {
            WalRecord::RunStarted { config, .. } => {
                assert_eq!(
                    config.auth_selection, None,
                    "absent auth_selection defaults to None"
                );
            }
            other => panic!("expected RunStarted, got {other:?}"),
        }
    }

    #[test]
    fn run_config_freezes_only_the_resolved_auth_pin_effect() {
        let mut config = *min_config();
        config.auth_selection = Some(AuthSelection {
            billing_vault_facts: None,
            auth_method: None,
            credential_id: "apikey:openai:astro".into(),
            account_ref: None,
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        });

        let frozen_bytes = serde_json::to_vec(&config).unwrap();
        let frozen_value: Value = serde_json::from_slice(&frozen_bytes).unwrap();
        assert_eq!(
            frozen_value.get("auth_selection"),
            Some(&json!({"credential_id": "apikey:openai:astro"}))
        );
        assert!(
            frozen_value.get("auth").is_none(),
            "RunConfig freezes the credential id, not the caller's admission-only pin"
        );

        let decoded: RunConfig = serde_json::from_slice(&frozen_bytes).unwrap();
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), frozen_bytes);
    }

    #[test]
    fn run_config_budget_round_trips() {
        let mut config = *min_config();
        config.budget = Some(BudgetConfig {
            max_input_tokens: None,
            nudges: vec![broca_protocol::BudgetNudge {
                at_tool_calls: 20,
                text: "slow down".into(),
            }],
            finalize_at_tool_calls: 30,
            finalize_text: "answer now".into(),
        });
        let decoded: RunConfig =
            serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
        assert_eq!(decoded, config);
    }

    #[test]
    fn run_config_budget_defaults_to_none_when_absent() {
        let mut config_json = serde_json::to_value(&*min_config()).unwrap();
        config_json.as_object_mut().unwrap().remove("budget");
        let config: RunConfig = serde_json::from_value(config_json).unwrap();
        assert_eq!(config.budget, None);
    }

    #[test]
    fn run_config_context_limit_round_trips() {
        let mut config = *min_config();
        config.context_limit = Some(200_000);
        let decoded: RunConfig =
            serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
        assert_eq!(decoded, config);
    }

    #[test]
    fn run_config_context_limit_defaults_to_none_and_preserves_legacy_bytes() {
        let legacy_bytes = serde_json::to_vec(&*min_config()).unwrap();
        let legacy_value: Value = serde_json::from_slice(&legacy_bytes).unwrap();
        assert!(
            legacy_value.get("context_limit").is_none(),
            "a None context limit must retain the pre-field WAL shape"
        );

        let decoded: RunConfig = serde_json::from_slice(&legacy_bytes).unwrap();
        assert_eq!(decoded.context_limit, None);
        assert_eq!(
            serde_json::to_vec(&decoded).unwrap(),
            legacy_bytes,
            "decoding and re-encoding an older RunConfig must preserve its bytes"
        );
    }

    #[test]
    fn run_config_tool_module_is_optional_and_round_trips() {
        let mut config = *min_config();
        config.tools = Arc::new(vec![Tool {
            name: "edit".into(),
            description: "edit files".into(),
            input_schema: json!({"type": "object"}),
            module: Some("aft".into()),
            execution_mode: broca_protocol::request::ExecutionMode::Parallel,
            reply: None,
        }]);

        let encoded = serde_json::to_value(&config).unwrap();
        assert_eq!(encoded["tools"][0]["module"], "aft");
        let decoded: RunConfig = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(decoded, config);

        let mut older = encoded;
        older["tools"][0].as_object_mut().unwrap().remove("module");
        let decoded: RunConfig = serde_json::from_value(older).unwrap();
        assert_eq!(decoded.tools[0].module, None);
    }

    #[test]
    fn model_step_finished_preserves_signed_tool_call() {
        let rec = WalRecord::ModelStepFinished {
            raw_finish_reason: None,
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(1),
            assistant_message: (AssistantMessage {
                message_id: MessageId("m1".into()),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId("c1".into()),
                    tool_name: "read".into(),
                    input: json!({"path": "x"}),
                    thought_signature: Some("gemini-signed-call".into()),
                })],
            })
            .into(),
            usage: Usage::default(),
            finish_reason: Some(FinishReason::ToolCalls),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        };

        round_trips(&rec);
        let encoded = serde_json::to_string(&rec).expect("serialize signed record");
        assert!(encoded.contains("\"thought_signature\":\"gemini-signed-call\""));
    }

    #[test]
    fn unsigned_tool_call_keeps_pre_signature_bytes_and_reads_as_none() {
        let call = ToolCall {
            tool_call_id: ToolCallId("c1".into()),
            tool_name: "read".into(),
            input: json!({"path": "x"}),
            thought_signature: None,
        };
        let encoded_call = serde_json::to_vec(&call).expect("serialize unsigned call");
        assert_eq!(
            encoded_call, br#"{"tool_call_id":"c1","tool_name":"read","input":{"path":"x"}}"#,
            "an absent optional signature must not change pre-field bytes"
        );
        let decoded_call: ToolCall =
            serde_json::from_slice(&encoded_call).expect("deserialize pre-field call");
        assert_eq!(decoded_call, call);

        let rec = WalRecord::ModelStepFinished {
            raw_finish_reason: None,
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(1),
            assistant_message: (AssistantMessage {
                message_id: MessageId("m1".into()),
                content: vec![ContentBlock::ToolCall(call)],
            })
            .into(),
            usage: Usage::default(),
            finish_reason: None,
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        };
        round_trips(&rec);
        assert!(
            !serde_json::to_string(&rec)
                .expect("serialize unsigned record")
                .contains("thought_signature"),
            "unsigned WAL records must retain their pre-field shape"
        );
    }

    #[test]
    fn budget_fired_caller_text_is_additive_and_defaults_to_none() {
        let legacy = serde_json::json!({
            "type": "budget_fired",
            "run_id": "r1",
            "kind": "nudge",
            "threshold": 2,
            "ordinal": 4,
        });
        let decoded: WalRecord =
            serde_json::from_value(legacy.clone()).expect("legacy budget firing");
        assert!(matches!(
            decoded,
            WalRecord::BudgetFired {
                caller_text: None,
                ..
            }
        ));
        assert_eq!(
            serde_json::to_value(decoded).expect("reencode legacy budget firing"),
            legacy,
            "an absent replay field must preserve the prior record shape"
        );
    }

    #[test]
    fn enrichment_optionals_default_to_none_when_absent() {
        // ModelStepFinished without finish_reason, ToolDispatchIntent without
        // tool_name, RunFinished without error — all read back as None.
        let step: WalRecord = serde_json::from_value(json!({
            "type": "model_step_finished",
            "step_id": 1,
            "assistant_message": { "message_id": "m1", "content": [] },
            "usage": Usage::default(),
        }))
        .unwrap();
        assert!(
            matches!(
                step,
                WalRecord::ModelStepFinished {
                    finish_reason: None,
                    ..
                }
            ),
            "absent finish_reason defaults to None"
        );

        let intent: WalRecord = serde_json::from_value(json!({
            "type": "tool_dispatch_intent",
            "batch_id": "b1",
            "tool_call_id": "t1",
            "args": {},
        }))
        .unwrap();
        assert!(
            matches!(
                intent,
                WalRecord::ToolDispatchIntent {
                    tool_name: None,
                    ..
                }
            ),
            "absent tool_name defaults to None"
        );

        let finished: WalRecord = serde_json::from_value(json!({
            "type": "run_finished",
            "reason": "completed",
            "usage": Usage::default(),
        }))
        .unwrap();
        assert!(
            matches!(finished, WalRecord::RunFinished { error: None, .. }),
            "absent error defaults to None"
        );
    }

    #[test]
    fn auth_selection_account_ref_is_additive_and_defaults_to_none() {
        // A selection written before `account_ref` was added (only a credential id) reads back
        // with account_ref = None and re-encodes byte-identically — the field is serde-skipped
        // when absent, so a legacy frozen selection keeps its exact shape (the same additive
        // contract as BudgetFired's caller_text).
        let legacy_selection = json!({ "credential_id": "oauth:anthropic" });
        let decoded: AuthSelection =
            serde_json::from_value(legacy_selection.clone()).expect("decode legacy selection");
        assert_eq!(decoded.credential_id, "oauth:anthropic");
        assert_eq!(
            decoded.account_ref, None,
            "absent account_ref defaults to None"
        );
        assert_eq!(
            serde_json::to_value(&decoded).expect("reencode legacy selection"),
            legacy_selection,
            "an absent account_ref must preserve the prior selection shape"
        );

        // A vault-served account id round-trips verbatim (never derived, never hashed).
        let stamped = json!({ "credential_id": "chatgpt:openai", "account_ref": "acct-uuid-123" });
        let decoded: AuthSelection =
            serde_json::from_value(stamped.clone()).expect("decode stamped selection");
        assert_eq!(decoded.account_ref.as_deref(), Some("acct-uuid-123"));
        assert_eq!(
            serde_json::to_value(&decoded).expect("reencode stamped selection"),
            stamped
        );
    }

    #[test]
    fn legacy_run_started_selection_decodes_without_account_ref() {
        // A RunStarted frozen before account_ref existed (selection carries only credential_id)
        // decodes with account_ref None, so resume/replay of an older WAL is unaffected.
        let mut config_json = serde_json::to_value(&*min_config()).unwrap();
        config_json.as_object_mut().unwrap().insert(
            "auth_selection".into(),
            json!({ "credential_id": "oauth:anthropic" }),
        );
        let legacy_run = json!({
            "type": "run_started",
            "run_id": "r1",
            "session": BindIdentity::new("/root", "runner", "s1"),
            "config": config_json,
            "input": [user_msg("hi")],
        });
        let rec: WalRecord = serde_json::from_value(legacy_run).expect("decode legacy run_started");
        match &rec {
            WalRecord::RunStarted { config, .. } => {
                let selection = config.auth_selection.as_ref().expect("selection present");
                assert_eq!(selection.credential_id, "oauth:anthropic");
                assert_eq!(
                    selection.account_ref, None,
                    "a legacy frozen selection has no account_ref"
                );
            }
            other => panic!("expected RunStarted, got {other:?}"),
        }
    }

    /// A record type written by a newer build decodes instead of failing the whole WAL. The
    /// payload deliberately reuses field names known records carry (`step_id`, `usage`,
    /// `reason`), so a decoder that fell through to a known variant would show up here.
    #[test]
    fn a_record_type_this_build_does_not_know_decodes_as_unknown() {
        let newer = br#"{"type":"attempt_digest_from_a_newer_build","step_id":3,"attempt":2,"status":529,"reason":"completed","usage":{"output_tokens":7}}"#;
        let record: WalRecord = serde_json::from_slice(newer).expect("unknown type decodes");
        assert_eq!(record, WalRecord::Unknown);

        // A payload with no tag at all is still corruption, not an unknown record.
        assert!(serde_json::from_slice::<WalRecord>(br#"{"step_id":3}"#).is_err());
    }

    #[test]
    fn pre_account_stamp_attempt_bytes_remain_unchanged() {
        let bytes = r#"{"type":"model_attempt_finished","step_id":2,"attempt":1,"request_body_len":12,"request_body_sha256":"aa","http_status":200,"response_body_len":3,"response_body_sha256":"bb","outcome":"complete","ts_ms":5}"#;
        let record: WalRecord = serde_json::from_str(bytes).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), bytes);
    }

    #[test]
    fn contentful_attempt_progress_bytes_are_pinned_before_gap_metrics() {
        // Keep the full content-bearing shape stable when optional measurements are added.
        let bytes = br#"{"type":"model_attempt_finished","step_id":2,"attempt":1,"request_body_len":12,"request_body_sha256":"aa","http_status":200,"response_body_len":3,"response_body_sha256":"bb","outcome":"complete","ts_ms":50,"first_content_at_ms":10,"last_content_at_ms":20,"content_bytes":3,"non_content_bytes":4,"non_content_bytes_after_last_content":0,"events_after_last_content":0}"#;
        let record: WalRecord = serde_json::from_slice(bytes).unwrap();
        match &record {
            WalRecord::ModelAttemptFinished {
                content_progress, ..
            } => {
                assert_eq!(content_progress.max_content_gap_ms, None);
                assert_eq!(content_progress.max_content_gap_ended_at_ms, None);
            }
            other => panic!("expected model attempt record, got {other:?}"),
        }
        assert_eq!(serde_json::to_vec(&record).unwrap(), bytes.to_vec());
    }

    /// The attempt record's bytes are pinned, and every outcome this build writes round-trips.
    #[test]
    fn model_attempt_finished_bytes_are_pinned_and_every_outcome_round_trips() {
        let stored = br#"{"type":"model_attempt_finished","step_id":2,"attempt":1,"request_body_len":12,"request_body_sha256":"aa","http_status":503,"content_type":"text/plain","response_body_len":1048576,"response_body_sha256":"bb","response_body_cap":1048576,"outcome":"http_error","ts_ms":5}"#;
        let record: WalRecord = serde_json::from_slice(stored).unwrap();
        assert!(record.is_informational());
        assert_eq!(serde_json::to_vec(&record).unwrap(), stored.to_vec());

        let interior_gap = br#"{"type":"model_attempt_finished","step_id":2,"attempt":1,"request_body_len":12,"request_body_sha256":"aa","http_status":200,"response_body_len":3,"response_body_sha256":"bb","outcome":"complete","ts_ms":50,"first_content_at_ms":10,"last_content_at_ms":50,"max_content_gap_ms":30,"max_content_gap_ended_at_ms":40}"#;
        let record: WalRecord = serde_json::from_slice(interior_gap).unwrap();
        match &record {
            WalRecord::ModelAttemptFinished {
                content_progress, ..
            } => {
                assert_eq!(content_progress.max_content_gap_ms, Some(30));
                assert_eq!(content_progress.max_content_gap_ended_at_ms, Some(40));
            }
            other => panic!("expected model attempt record, got {other:?}"),
        }
        assert_eq!(serde_json::to_vec(&record).unwrap(), interior_gap.to_vec());

        // Absent optionals write no bytes: an attempt that never received a status line.
        let no_status = br#"{"type":"model_attempt_finished","step_id":2,"attempt":0,"request_body_len":12,"request_body_sha256":"aa","response_body_len":0,"response_body_sha256":"e3b0","outcome":"connect_failed","ts_ms":5}"#;
        let record: WalRecord = serde_json::from_slice(no_status).unwrap();
        assert_eq!(serde_json::to_vec(&record).unwrap(), no_status.to_vec());

        for name in [
            "complete",
            "eof_before_completion",
            "chunk_error",
            "idle_timeout",
            "content_timeout",
            "byte_limit",
            "decode_error",
            "rate_floor",
            "cancelled",
            "sealed_in_flight",
            "http_error",
            "provider_error",
            "connect_failed",
            "header_timeout",
            "no_status",
        ] {
            let outcome: AttemptOutcome = serde_json::from_value(json!(name)).unwrap();
            assert!(
                !matches!(outcome, AttemptOutcome::Unknown(_)),
                "{name} is known"
            );
            assert_eq!(serde_json::to_value(&outcome).unwrap(), json!(name));
        }
    }

    /// An outcome a later build writes decodes, and keeps its exact string: re-serializing the
    /// record writes back the bytes that were read, as `VaultReadOp` does for operations.
    #[test]
    fn an_attempt_outcome_this_build_does_not_know_decodes_and_keeps_its_string() {
        let stored = br#"{"type":"model_attempt_finished","step_id":2,"attempt":0,"request_body_len":12,"request_body_sha256":"aa","http_status":200,"response_body_len":3,"response_body_sha256":"cc","outcome":"tls_renegotiation_refused","ts_ms":5}"#;
        let record: WalRecord = serde_json::from_slice(stored).expect("unknown outcome decodes");
        match &record {
            WalRecord::ModelAttemptFinished { outcome, .. } => assert_eq!(
                outcome,
                &AttemptOutcome::Unknown("tls_renegotiation_refused".into())
            ),
            other => panic!("expected the attempt record, got {other:?}"),
        }
        assert_eq!(serde_json::to_vec(&record).unwrap(), stored.to_vec());
    }

    /// `Unknown` holds none of the original record, so it must never be written back as if
    /// it were one: serializing it is an error rather than `{"type":"unknown"}`.
    #[test]
    fn an_unknown_record_refuses_to_serialize() {
        assert!(serde_json::to_vec(&WalRecord::Unknown).is_err());
        assert!(serde_json::to_value(&WalRecord::Unknown).is_err());
    }

    #[test]
    fn pre_conversation_key_bytes_are_pinned() {
        let queued = r#"{"type":"queued","submission_id":"sub-r","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]},"request_identity_digest":"v1:abc","send_id":"turn","delivery":"interrupt"}"#;
        let record: WalRecord = serde_json::from_str(queued).unwrap();
        assert!(matches!(record, WalRecord::Queued { .. }));
        assert_eq!(serde_json::to_string(&record).unwrap(), queued);
        let decision = r#"{"path":"routed","admission_id":"a","decision_id":"d"}"#;
        let record: AccountDecision = serde_json::from_str(decision).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), decision);
    }

    /// Optional fields must not change the bytes of records that omit them.
    /// Decode literal bytes so the fixture cannot move together with the encoder.
    #[test]
    fn run_started_and_queued_bytes_are_pinned() {
        const RUN_STARTED_POPULATED: &str = r#"{"type":"run_started","run_id":"run-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"project_id":"pj-a","ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"submission_id":"sub-run-7"}"#;
        const RUN_STARTED_MINIMAL: &str = r#"{"type":"run_started","run_id":"run-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        const QUEUED_POPULATED: &str = r#"{"type":"queued","submission_id":"sub-run-8","request":{"project_id":"pj-a","model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]},"request_identity_digest":"v1:abc"}"#;
        const QUEUED_MINIMAL: &str = r#"{"type":"queued","submission_id":"sub-run-8","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}"#;

        for pinned in [
            RUN_STARTED_POPULATED,
            RUN_STARTED_MINIMAL,
            QUEUED_POPULATED,
            QUEUED_MINIMAL,
        ] {
            let decoded: WalRecord = serde_json::from_str(pinned).expect("pinned record decodes");
            assert_eq!(
                String::from_utf8(serde_json::to_vec(&decoded).unwrap()).unwrap(),
                pinned,
                "a pinned record must re-encode to exactly the bytes it was read from"
            );
        }

        // The populated shapes really carried their optional fields through the decode, so
        // the byte comparison above covered them rather than a record that dropped them.
        let decoded: WalRecord = serde_json::from_str(RUN_STARTED_POPULATED).unwrap();
        let WalRecord::RunStarted {
            project_id,
            ts_ms,
            submission_id,
            ..
        } = decoded
        else {
            panic!("expected RunStarted, got {decoded:?}");
        };
        assert_eq!(project_id.as_deref(), Some("pj-a"));
        assert_eq!(ts_ms, Some(1_790_000_000_000));
        assert_eq!(submission_id, Some(SubmissionId("sub-run-7".into())));
        let decoded: WalRecord = serde_json::from_str(QUEUED_POPULATED).unwrap();
        let WalRecord::Queued {
            request,
            request_identity_digest,
            ..
        } = decoded
        else {
            panic!("expected Queued, got {decoded:?}");
        };
        assert_eq!(request.project_id.as_deref(), Some("pj-a"));
        assert_eq!(request_identity_digest.as_deref(), Some("v1:abc"));
    }

    /// Every optional field `RunStarted` and `Queued` carried before the send key was written on
    /// them, populated at once (the principal included), plus the shapes with none of them. A
    /// session written by an older build holds exactly these bytes, so recording the send key
    /// must leave them decoding and re-encoding to themselves: an absent key costs no bytes.
    ///
    /// Built only by decoding, never by a struct literal, so adding a field to either record
    /// does not change this test's source.
    #[test]
    fn run_started_and_queued_bytes_before_the_send_key_are_pinned() {
        const RUN_STARTED_ALL_OPTIONAL: &str = r#"{"type":"run_started","run_id":"run-sid-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"project_id":"pj-a","principal":{"kind":"reserved","module_id":"prefrontal-core"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"submission_id":"sub-run-sid-7"}"#;
        const RUN_STARTED_NO_OPTIONAL: &str = r#"{"type":"run_started","run_id":"run-sid-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        const QUEUED_ALL_OPTIONAL: &str = r#"{"type":"queued","submission_id":"sub-run-sid-8","request":{"project_id":"pj-a","model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]},"request_identity_digest":"v1:abc","principal":{"kind":"direct"}}"#;
        const QUEUED_NO_OPTIONAL: &str = r#"{"type":"queued","submission_id":"sub-run-sid-8","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}"#;

        for pinned in [
            RUN_STARTED_ALL_OPTIONAL,
            RUN_STARTED_NO_OPTIONAL,
            QUEUED_ALL_OPTIONAL,
            QUEUED_NO_OPTIONAL,
        ] {
            let decoded: WalRecord = serde_json::from_str(pinned).expect("pinned record decodes");
            assert!(
                !matches!(decoded, WalRecord::Unknown),
                "a pinned record must decode as itself: {pinned}"
            );
            assert_eq!(
                String::from_utf8(serde_json::to_vec(&decoded).unwrap()).unwrap(),
                pinned,
                "a pinned record must re-encode to exactly the bytes it was read from"
            );
        }

        // The principal really came through the decode on both records, so the byte
        // comparison above covered it rather than a record that dropped it.
        let decoded: WalRecord = serde_json::from_str(RUN_STARTED_ALL_OPTIONAL).unwrap();
        let WalRecord::RunStarted { principal, .. } = decoded else {
            panic!("expected RunStarted, got {decoded:?}");
        };
        assert!(principal.is_some());
        let decoded: WalRecord = serde_json::from_str(QUEUED_ALL_OPTIONAL).unwrap();
        let WalRecord::Queued { principal, .. } = decoded else {
            panic!("expected Queued, got {decoded:?}");
        };
        assert!(principal.is_some());
    }

    /// The bytes `RunStarted` and `Queued` are written with when they carry the send key: the
    /// key fields come last, after every field an older build already knows. An older reader
    /// ignores them (serde skips unknown fields), so these records stay frame version 1.
    #[test]
    fn run_started_and_queued_bytes_with_the_send_key_are_pinned() {
        const RUN_STARTED_KEYED: &str = r#"{"type":"run_started","run_id":"run-sid-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"submission_id":"sub-run-sid-7","send_id":"turn-7","request_identity_digest":"v1:abc"}"#;
        const QUEUED_KEYED: &str = r#"{"type":"queued","submission_id":"sub-run-sid-8","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]},"request_identity_digest":"v1:abc","send_id":"turn-8"}"#;
        for pinned in [RUN_STARTED_KEYED, QUEUED_KEYED] {
            let decoded: WalRecord = serde_json::from_str(pinned).expect("pinned record decodes");
            assert_eq!(
                String::from_utf8(serde_json::to_vec(&decoded).unwrap()).unwrap(),
                pinned,
                "a pinned record must re-encode to exactly the bytes it was read from"
            );
        }
        let decoded: WalRecord = serde_json::from_str(RUN_STARTED_KEYED).unwrap();
        let WalRecord::RunStarted {
            send_id,
            request_identity_digest,
            ..
        } = decoded
        else {
            panic!("expected RunStarted, got {decoded:?}");
        };
        assert_eq!(send_id.as_deref(), Some("turn-7"));
        assert_eq!(request_identity_digest.as_deref(), Some("v1:abc"));
        let decoded: WalRecord = serde_json::from_str(QUEUED_KEYED).unwrap();
        let WalRecord::Queued { send_id, .. } = decoded else {
            panic!("expected Queued, got {decoded:?}");
        };
        assert_eq!(send_id.as_deref(), Some("turn-8"));
    }

    /// `RunStarted` with every field it has today populated at once, as the complete framed
    /// bytes an ordinary (version 1) frame holds. The tests above pin each optional field, but
    /// in separate records; this one also pins their relative order in one record and the frame
    /// around it, so a field added later can be shown to leave a record that does not use it
    /// byte-identical, frame included.
    ///
    /// The digest is a literal computed outside this crate (SHA-256 over the version byte 1,
    /// seq 1 and fence 1 as u64 LE, then the payload), so it cannot move together with the
    /// encoder. Built only by decoding, so adding a field does not change this test's source.
    #[test]
    fn pre_marker_order_run_started_frame_bytes_are_pinned() {
        const RUN_STARTED_EVERY_FIELD: &str = r#"{"type":"run_started","run_id":"run-all-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"project_id":"pj-a","principal":{"kind":"reserved","module_id":"prefrontal-core"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"submission_id":"sub-run-all-7","send_id":"turn-7","request_identity_digest":"v1:abc"}"#;
        const EXPECTED_DIGEST: [u8; 32] = [
            0x05, 0x3c, 0xba, 0x5b, 0xbc, 0x0b, 0xc3, 0x50, 0x44, 0xa5, 0xb5, 0x65, 0xdd, 0x34,
            0xc7, 0x8e, 0xef, 0x60, 0xea, 0xb3, 0x32, 0xb0, 0x87, 0xc1, 0x64, 0x7f, 0xb7, 0x07,
            0x43, 0x6c, 0xbe, 0xf0,
        ];

        let decoded: WalRecord =
            serde_json::from_str(RUN_STARTED_EVERY_FIELD).expect("pinned record decodes");
        let WalRecord::RunStarted {
            project_id,
            principal,
            ts_ms,
            submission_id,
            send_id,
            request_identity_digest,
            marker_order_version,
            ..
        } = &decoded
        else {
            panic!("expected RunStarted, got {decoded:?}");
        };
        assert_eq!(*marker_order_version, None);
        // Every optional field really came through the decode, so the byte comparison below
        // covers it rather than a record that dropped it.
        assert!(project_id.is_some() && principal.is_some() && ts_ms.is_some());
        assert!(submission_id.is_some() && send_id.is_some());
        assert!(request_identity_digest.is_some());

        let bytes = crate::framing::encode(1, 1, &decoded).unwrap();
        let payload = RUN_STARTED_EVERY_FIELD.as_bytes();
        assert_eq!(
            &bytes[0..4],
            &(payload.len() as u32).to_le_bytes(),
            "length"
        );
        assert_eq!(bytes[4], 1, "an ordinary record frame");
        assert_eq!(&bytes[5..13], &1u64.to_le_bytes(), "seq");
        assert_eq!(&bytes[13..21], &1u64.to_le_bytes(), "fence");
        assert_eq!(&bytes[21..53], &EXPECTED_DIGEST[..], "digest");
        assert_eq!(
            String::from_utf8(bytes[53..].to_vec()).unwrap(),
            RUN_STARTED_EVERY_FIELD,
            "payload"
        );
        let mut versioned = decoded.clone();
        if let WalRecord::RunStarted {
            marker_order_version,
            ..
        } = &mut versioned
        {
            *marker_order_version = Some(1);
        }
        assert!(versioned.required_features().is_empty());
        let versioned_bytes = crate::framing::encode(1, 1, &versioned).unwrap();
        assert_eq!(versioned_bytes[4], 1);
        let roundtrip: WalRecord = serde_json::from_slice(&versioned_bytes[53..]).unwrap();
        assert_eq!(roundtrip, versioned);
        assert!(String::from_utf8(versioned_bytes[53..].to_vec())
            .unwrap()
            .ends_with("\"marker_order_version\":1}"));
    }

    /// `RunStarted` with every field it has before the frozen plan manifest existed, as the
    /// complete framed bytes: once with every field that needs no reader feature (an ordinary
    /// version-1 frame), and once also carrying a frozen scope (a version-3 frame that requires
    /// `scope/v1`). A field added later must leave both byte-identical when it is absent, frame
    /// and digest included.
    ///
    /// The digests are literals computed outside this crate (SHA-256 over the version byte, seq 1
    /// and fence 1 as u64 LE, then the payload), so they cannot move together with the encoder.
    /// Both records are built only by decoding, so adding a field does not change this source.
    #[test]
    fn pre_manifest_run_started_frame_bytes_are_pinned() {
        const UNGATED: &str = r#"{"type":"run_started","run_id":"run-all-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"project_id":"pj-a","principal":{"kind":"reserved","module_id":"prefrontal-core"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"submission_id":"sub-run-all-7","send_id":"turn-7","request_identity_digest":"v1:abc","marker_order_version":1}"#;
        const UNGATED_DIGEST: [u8; 32] = [
            0x71, 0x9b, 0x20, 0xa3, 0x62, 0x09, 0xbe, 0xf1, 0x45, 0x3f, 0xab, 0x46, 0x20, 0xd9,
            0xa3, 0x3d, 0x6e, 0xd9, 0x64, 0x3c, 0x2a, 0x43, 0x58, 0x49, 0xdb, 0xd9, 0x5b, 0x58,
            0x8a, 0x29, 0x0d, 0x99,
        ];
        const SCOPED_PAYLOAD: &str = r#"{"requires":["scope/v1"],"record":{"type":"run_started","run_id":"run-all-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"project_id":"pj-a","principal":{"kind":"reserved","module_id":"prefrontal-core"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"submission_id":"sub-run-all-7","send_id":"turn-7","request_identity_digest":"v1:abc","scope":{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"head-1","scope_epoch":3},"marker_order_version":1}}"#;
        const SCOPED_DIGEST: [u8; 32] = [
            0x47, 0x88, 0xe5, 0xba, 0xc3, 0xd1, 0xb3, 0x88, 0xb1, 0xa1, 0xa1, 0x12, 0x72, 0xa3,
            0x52, 0x5b, 0x5b, 0xc3, 0x7f, 0x0e, 0x2f, 0xa8, 0x10, 0xa5, 0xb0, 0xe6, 0xb4, 0x79,
            0x8b, 0x9d, 0xb2, 0xb6,
        ];

        let ungated: WalRecord = serde_json::from_str(UNGATED).expect("pinned record decodes");
        let scoped_record = SCOPED_PAYLOAD
            .strip_prefix(r#"{"requires":["scope/v1"],"record":"#)
            .and_then(|rest| rest.strip_suffix('}'))
            .expect("the gated payload wraps the record");
        let scoped: WalRecord =
            serde_json::from_str(scoped_record).expect("pinned scoped record decodes");
        for (record, ver, payload, digest) in [
            (&ungated, 1u8, UNGATED, UNGATED_DIGEST),
            (&scoped, 3u8, SCOPED_PAYLOAD, SCOPED_DIGEST),
        ] {
            let WalRecord::RunStarted {
                project_id,
                principal,
                ts_ms,
                submission_id,
                send_id,
                request_identity_digest,
                marker_order_version,
                ..
            } = record
            else {
                panic!("expected RunStarted, got {record:?}");
            };
            // Every optional field really came through the decode, so the byte comparison
            // below covers it rather than a record that dropped it.
            assert!(project_id.is_some() && principal.is_some() && ts_ms.is_some());
            assert!(submission_id.is_some() && send_id.is_some());
            assert!(request_identity_digest.is_some() && marker_order_version.is_some());

            let bytes = crate::framing::encode(1, 1, record).unwrap();
            assert_eq!(
                &bytes[0..4],
                &(payload.len() as u32).to_le_bytes(),
                "length"
            );
            assert_eq!(bytes[4], ver, "frame version");
            assert_eq!(&bytes[5..13], &1u64.to_le_bytes(), "seq");
            assert_eq!(&bytes[13..21], &1u64.to_le_bytes(), "fence");
            assert_eq!(&bytes[21..53], &digest[..], "digest");
            assert_eq!(
                String::from_utf8(bytes[53..].to_vec()).unwrap(),
                payload,
                "payload"
            );
        }
        let WalRecord::RunStarted { scope, .. } = &scoped else {
            unreachable!()
        };
        assert!(scope.is_some(), "the scoped record kept its scope");
    }

    /// Existing scoped sessions must retain their exact WAL encoding when optional fields
    /// for an automation's sender and flow identifier are absent. The fixture and digest are
    /// independent literals so the encoder cannot silently move the expected bytes with it.
    #[test]
    fn pre_flow_scoped_run_started_bytes_are_pinned() {
        let payload =
            include_str!("../../../broca-wal/tests/fixtures/pre_flow_scoped_run_started.json")
                .trim_end();
        let envelope: serde_json::Value = serde_json::from_str(payload).unwrap();
        let record: WalRecord = serde_json::from_value(envelope["record"].clone()).unwrap();
        let bytes = crate::framing::encode(1, 1, &record).unwrap();
        assert_eq!(bytes[4], 3);
        assert_eq!(&bytes[5..13], &1u64.to_le_bytes());
        assert_eq!(&bytes[13..21], &1u64.to_le_bytes());
        assert_eq!(
            &bytes[21..53],
            &[
                0x47, 0x88, 0xe5, 0xba, 0xc3, 0xd1, 0xb3, 0x88, 0xb1, 0xa1, 0xa1, 0x12, 0x72, 0xa3,
                0x52, 0x5b, 0x5b, 0xc3, 0x7f, 0x0e, 0x2f, 0xa8, 0x10, 0xa5, 0xb0, 0xe6, 0xb4, 0x79,
                0x8b, 0x9d, 0xb2, 0xb6,
            ]
        );
        assert_eq!(&bytes[0..4], &(payload.len() as u32).to_le_bytes());
        assert_eq!(&bytes[53..], payload.as_bytes());
    }

    /// `ToolDispatchIntent` with every field it has before it recorded the module a call was
    /// dispatched to, as complete framed bytes: once with every optional field present and once
    /// with none. Both are ordinary version-1 frames. A field added later must leave both
    /// byte-identical when it is absent, frame and digest included.
    ///
    /// The digests are literals computed outside this crate (SHA-256 over the version byte, seq 1
    /// and fence 1 as u64 LE, then the payload), so they cannot move together with the encoder.
    /// Both records are built only by decoding, so adding a field does not change this source.
    #[test]
    fn pre_dispatch_module_tool_dispatch_intent_frame_bytes_are_pinned() {
        const EVERY_FIELD: &str = r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t1","tool_name":"edit","args":{"f":"a"}}"#;
        const EVERY_FIELD_DIGEST: [u8; 32] = [
            0x02, 0x47, 0x46, 0x47, 0x3f, 0x70, 0x65, 0x47, 0x41, 0xc3, 0x98, 0xd6, 0x5f, 0x82,
            0xc4, 0x9a, 0x10, 0x74, 0xb9, 0x85, 0x1e, 0x86, 0x0a, 0x1d, 0x82, 0x37, 0x22, 0x59,
            0xfc, 0x2c, 0x6b, 0x01,
        ];
        const NO_OPTIONAL: &str =
            r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t2","args":null}"#;
        const NO_OPTIONAL_DIGEST: [u8; 32] = [
            0xbe, 0x34, 0xc1, 0xb3, 0x3d, 0x68, 0x23, 0x3f, 0x8a, 0x56, 0x7d, 0x9f, 0x5a, 0xa2,
            0xc3, 0xd5, 0xe9, 0xd8, 0x1b, 0xce, 0x6b, 0x57, 0x55, 0x84, 0x5a, 0xa7, 0x88, 0x18,
            0x28, 0x09, 0x81, 0xa7,
        ];

        for (payload, digest, named) in [
            (EVERY_FIELD, EVERY_FIELD_DIGEST, true),
            (NO_OPTIONAL, NO_OPTIONAL_DIGEST, false),
        ] {
            let record: WalRecord = serde_json::from_str(payload).expect("pinned record decodes");
            let WalRecord::ToolDispatchIntent { tool_name, .. } = &record else {
                panic!("expected ToolDispatchIntent, got {record:?}");
            };
            // The optional name really came through the decode (or really stayed absent), so
            // the byte comparison below covers it.
            assert_eq!(tool_name.is_some(), named);
            assert!(record.required_features().is_empty());

            let bytes = crate::framing::encode(1, 1, &record).unwrap();
            assert_eq!(
                &bytes[0..4],
                &(payload.len() as u32).to_le_bytes(),
                "length"
            );
            assert_eq!(bytes[4], 1, "frame version");
            assert_eq!(&bytes[5..13], &1u64.to_le_bytes(), "seq");
            assert_eq!(&bytes[13..21], &1u64.to_le_bytes(), "fence");
            assert_eq!(&bytes[21..53], &digest[..], "digest");
            assert_eq!(
                String::from_utf8(bytes[53..].to_vec()).unwrap(),
                payload,
                "payload"
            );
        }
    }
}

#[cfg(test)]
mod bill_attempt_tests {
    use super::*;

    const ID: &str = "00112233445566778899aabbccddeeff";
    const RESERVATION: &str = r#"{"run_id":"r","step_id":1,"attempt":0,"send_index":1,"warm":false,"reserved_at_ms":100,"provider":"opencode-go","endpoint_host":"example.com","model_sent":"model","credential":{"credential_ref":"apikey:opencode-go","account_ref":"account","auth_selection":{"credential_id":"apikey:opencode-go","resolved_credential_id":"apikey:opencode-go"},"auth_method":"apikey","provider_ids":["opencode-go","kimi-for-coding"],"plan_tier":"max_5x","plan_tier_source":"operator"},"session":{"project_root":"/project","harness":"runner","session":"s","session_ref":"ref"},"run":{"flow_id":"flow","work_class":"interactive","context_limit":200000,"service_tier_requested":"priority","speed_requested":"fast"},"request_body_bytes":123,"compressed":true,"input_tokens_estimate":20}"#;
    const ENVELOPE: &str = r#"{"sent":true,"response_bytes":99,"outcome":"complete","sent_at_ms":101,"finished_at_ms":200,"http_status":200,"finish_reason":"stop","model_used":"model","usage":{"input_tokens":10,"cached_input_tokens":20,"cache_write_tokens":30,"output_tokens":40,"reasoning_tokens":5},"cache_write_5m_tokens":0,"cache_write_1h_tokens":30,"reasoning_included_in_output":true,"usage_raw":{"future_vendor_key":{"count":9}},"vendor_request_id":"vendor-id","service_tier_used":"default","speed_used":"standard"}"#;
    const MULTI_ENVELOPE: &str = r#"{"sent":true,"response_bytes":99,"outcome":"complete","sent_at_ms":101,"finished_at_ms":200,"http_status":200,"finish_reason":"stop","usage_by_model":[{"model_used":"source-model","usage":{"input_tokens":10,"cache_write_tokens":30,"output_tokens":40,"reasoning_tokens":5},"cache_write_5m_tokens":0,"cache_write_1h_tokens":30},{"model_used":"fallback-model","usage":{"input_tokens":7,"cached_input_tokens":11,"output_tokens":9}}],"reasoning_included_in_output":true,"usage_raw":{"iterations":[{"model":"source-model"},{"model":"fallback-model"}]},"vendor_request_id":"vendor-id","service_tier_used":"default","speed_used":"standard"}"#;
    const LEGACY_ATTEMPT: &str = r#"{"type":"model_attempt_finished","step_id":2,"attempt":0,"request_body_len":12,"request_body_sha256":"aa","response_body_len":0,"response_body_sha256":"e3b0","outcome":"connect_failed","ts_ms":5}"#;
    const LEGACY_WARM: &str = r#"{"type":"cache_warmed","run_id":"r","step_id":2,"request_body_sha256":"aa","warm_body_sha256":"bb","usage":{},"sent_at_ms":1790000000000}"#;

    #[test]
    fn bill_attempt_compression_refused_pinned_bytes() {
        use sha2::{Digest, Sha256};
        const ENVELOPE: &str = r#"{"sent":true,"response_bytes":0,"outcome":"compression_refused","sent_at_ms":101,"finished_at_ms":200,"http_status":415}"#;
        let envelope: AttemptEnvelope = serde_json::from_str(ENVELOPE).unwrap();
        assert_eq!(envelope.outcome, BillingAttemptOutcome::CompressionRefused);
        assert_eq!(serde_json::to_string(&envelope).unwrap(), ENVELOPE);
        let json = format!(
            r#"{{"type":"model_attempt_finished","attempt_id":"{ID}","reservation":{RESERVATION},"envelope":{ENVELOPE},"step_id":1,"attempt":0,"request_body_len":123,"request_body_sha256":"aa","http_status":415,"response_body_len":0,"response_body_sha256":"e3b0","outcome":"http_error","ts_ms":200}}"#
        );
        let record: WalRecord = serde_json::from_str(&json).unwrap();
        let payload = serde_json::to_vec(&record).unwrap();
        assert_eq!(payload, json.as_bytes());
        let frame = crate::framing::encode_v4(
            broca_wal_reader::frame::Metadata {
                seq: 1,
                fence: 1,
                appended_ms: 200,
                writer_id: [1; 16],
            },
            &[],
            &record,
        )
        .unwrap();
        let crate::framing::DecodeStep::Record { framed, consumed } =
            crate::framing::decode_next(&frame)
        else {
            panic!("compression refusal frame must decode");
        };
        assert_eq!(consumed, frame.len());
        assert_eq!(framed.record, record);
        if let Some(output) = std::env::var_os("BROCA_COMPRESSION_SAMPLE_DIR") {
            let output = std::path::PathBuf::from(output);
            std::fs::create_dir_all(&output).unwrap();
            std::fs::write(output.join("compression_refused.json"), &payload).unwrap();
            std::fs::write(output.join("compression_refused.frame"), &frame).unwrap();
            std::fs::write(
                output.join("SHA256SUMS"),
                format!(
                    "{:x}  compression_refused.json\n{:x}  compression_refused.frame\n",
                    Sha256::digest(&payload),
                    Sha256::digest(&frame),
                ),
            )
            .unwrap();
        }
    }

    pub(super) fn golden_records() -> Vec<String> {
        let mut records = vec![
            format!(
                r#"{{"type":"attempt_reserved","attempt_id":"{ID}","reservation":{RESERVATION}}}"#
            ),
            format!(
                r#"{{"type":"attempt_released","attempt_id":"{ID}","reservation":{RESERVATION},"envelope":{{"sent":false,"response_bytes":0,"outcome":"cancelled","finished_at_ms":200}}}}"#
            ),
            format!(
                r#"{{"type":"attempt_corrected","attempt_id":"{ID}","correction_id":"ffeeddccbbaa99887766554433221100","reason":"usage_reconciled","reservation":{RESERVATION},"envelope":{ENVELOPE}}}"#
            ),
            format!(
                r#"{{"type":"attempt_finished","attempt_id":"{ID}","reservation":{RESERVATION},"envelope":{{"sent":"unknown","response_bytes":0,"outcome":"unknown","finished_at_ms":200}}}}"#
            ),
        ];
        for (kind, legacy) in [
            ("model_attempt_finished", LEGACY_ATTEMPT),
            ("cache_warmed", LEGACY_WARM),
        ] {
            let current = if kind == "model_attempt_finished" {
                legacy
                    .replace("connect_failed", "complete")
                    .replace("\"step_id\":2", "\"step_id\":1")
                    .replace("\"ts_ms\":5", "\"ts_ms\":200")
                    .replace("\"request_body_len\":12", "\"request_body_len\":123")
                    .replace("\"response_body_len\":0", "\"response_body_len\":99")
            } else {
                legacy
                    .replace("\"step_id\":2", "\"step_id\":1")
                    .replace("1790000000000", "101")
            };
            let reservation = if kind == "cache_warmed" {
                RESERVATION.replace("\"warm\":false", "\"warm\":true")
            } else {
                RESERVATION.to_owned()
            };
            let suffix = current
                .strip_prefix(&format!(r#"{{"type":"{kind}","#))
                .unwrap();
            let envelope = if kind == "model_attempt_finished" {
                MULTI_ENVELOPE
            } else {
                ENVELOPE
            };
            records.push(format!(r#"{{"type":"{kind}","attempt_id":"{ID}","reservation":{reservation},"envelope":{envelope},{suffix}"#));
        }
        records
    }

    #[test]
    fn bill_attempt_golden_records_cover_every_field() {
        for golden in golden_records() {
            let record: WalRecord = serde_json::from_str(&golden).unwrap();
            assert_ne!(record, WalRecord::Unknown);
            assert_eq!(serde_json::to_string(&record).unwrap(), golden);
            assert_eq!(
                record.required_features(),
                &[crate::framing::BILL_ATTEMPTS_FEATURE]
            );
            assert!(record.is_informational());
            let value = serde_json::to_value(&record).unwrap();
            assert_eq!(value["reservation"]["run"]["speed_requested"], "fast");
            assert_eq!(value["reservation"]["credential"]["auth_method"], "apikey");
            assert!(value["reservation"]["credential"].get("kind").is_none());
            assert!(value["reservation"]["credential"]
                .get("billing_model")
                .is_none());
            if let WalRecord::ModelAttemptFinished {
                attempt,
                reservation: Some(reservation),
                ..
            } = record
            {
                assert_eq!(attempt, reservation.attempt);
            }
        }
    }

    #[test]
    fn bill_attempt_decoder_enforces_usage_arithmetic_and_model_attribution() {
        let single: Value = serde_json::from_str(ENVELOPE).unwrap();
        let multi: Value = serde_json::from_str(MULTI_ENVELOPE).unwrap();
        let rejected = |value: Value, expected: &str| {
            let error = serde_json::from_value::<AttemptEnvelope>(value)
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "{error}");
        };
        for field in ["response_bytes", "sent", "outcome"] {
            let mut value = single.clone();
            value.as_object_mut().unwrap().remove(field);
            rejected(value, field);
        }
        let mut value = single.clone();
        value["cache_write_1h_tokens"] = serde_json::json!(31);
        rejected(value, "cache_write_tokens");
        let mut value = single.clone();
        value["cache_write_5m_tokens"] = serde_json::json!(u64::MAX);
        rejected(value, "cache_write_tokens");
        let mut value = single.clone();
        value["usage"]
            .as_object_mut()
            .unwrap()
            .remove("cache_write_tokens");
        rejected(value, "cache_write_tokens");
        for mut value in [single.clone(), multi.clone()] {
            value
                .as_object_mut()
                .unwrap()
                .remove("reasoning_included_in_output");
            rejected(value, "reasoning_included_in_output");
        }
        let mut value = multi.clone();
        value["usage"] = serde_json::json!({"output_tokens":9});
        rejected(value, "no top-level");
        let mut value = multi.clone();
        value["usage_by_model"][1]["model_used"] = serde_json::json!("source-model");
        rejected(value, "coalesce");
        let mut value = multi.clone();
        value["usage_by_model"].as_array_mut().unwrap().pop();
        rejected(value, "at least two models");
        let mut value = multi.clone();
        value["usage_by_model"][0]["cache_write_1h_tokens"] = serde_json::json!(31);
        rejected(value, "cache_write_tokens");
        let mut value = single.clone();
        value["outcome"] = serde_json::json!("unknown");
        rejected(value, "must not carry usage");
        let mut value = single.clone();
        value["sent"] = serde_json::json!(false);
        rejected(value, "sent false");
        let mut value = single.clone();
        value["outcome"] = serde_json::json!("future-outcome");
        rejected(value, "unknown variant");
        let decoded: AttemptEnvelope = serde_json::from_value(multi).unwrap();
        let models = decoded.usage_by_model.unwrap();
        assert_eq!(
            models
                .iter()
                .map(|m| m.model_used.as_str())
                .collect::<Vec<_>>(),
            ["source-model", "fallback-model"]
        );
        assert!(decoded.usage.is_none());
        assert_eq!(models[0].usage.input_tokens, Some(10));
    }

    #[test]
    fn bill_attempt_recovery_terminal_is_unknown_and_never_released_when_uncertain() {
        let records = golden_records();
        let recovery: Value = serde_json::from_str(&records[3]).unwrap();
        for sent in [
            serde_json::json!(true),
            serde_json::json!(false),
            serde_json::json!("unknown"),
        ] {
            let mut value = recovery.clone();
            value["envelope"]["sent"] = sent;
            let record: WalRecord = serde_json::from_value(value).unwrap();
            assert!(matches!(record, WalRecord::AttemptFinished { .. }));
            assert!(record.is_informational());
        }
        let mut value = recovery.clone();
        value["envelope"]["outcome"] = serde_json::json!("complete");
        assert!(serde_json::from_value::<WalRecord>(value)
            .unwrap_err()
            .to_string()
            .contains("requires outcome unknown"));
        let mut value = recovery.clone();
        value["envelope"]["usage"] = serde_json::json!({});
        assert!(serde_json::from_value::<WalRecord>(value)
            .unwrap_err()
            .to_string()
            .contains("must not carry usage"));
        let mut value = recovery;
        value["type"] = serde_json::json!("attempt_released");
        assert!(serde_json::from_value::<WalRecord>(value)
            .unwrap_err()
            .to_string()
            .contains("provably unsent"));
    }

    #[test]
    fn bill_attempt_corrections_have_independent_ids_and_closed_reasons() {
        let value: Value = serde_json::from_str(&golden_records()[2]).unwrap();
        assert!(value.get("corrects").is_none());
        assert_ne!(value["attempt_id"], value["correction_id"]);
        for reason in [
            "usage_reconciled",
            "provider_evidence_corrected",
            "credential_attribution_corrected",
        ] {
            let mut corrected = value.clone();
            corrected["reason"] = serde_json::json!(reason);
            assert!(serde_json::from_value::<WalRecord>(corrected).is_ok());
        }
        for field in ["correction_id", "reason"] {
            let mut corrected = value.clone();
            corrected.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<WalRecord>(corrected)
                .unwrap_err()
                .to_string()
                .contains(field));
        }
        let mut delta = value.clone();
        delta["envelope"] = serde_json::json!({"usage":{"output_tokens":5}});
        assert!(serde_json::from_value::<WalRecord>(delta).is_err());
        let mut corrected = value;
        corrected["reason"] = serde_json::json!("arbitrary");
        assert!(serde_json::from_value::<WalRecord>(corrected)
            .unwrap_err()
            .to_string()
            .contains("unknown variant"));
    }

    #[test]
    fn bill_attempt_absent_fields_stay_absent_and_reported_zero_stays_zero() {
        for (json, expected) in [
            (
                r#"{"sent":"unknown","response_bytes":0,"outcome":"unknown"}"#,
                r#"{"sent":"unknown","response_bytes":0,"outcome":"unknown"}"#,
            ),
            (
                r#"{"sent":true,"response_bytes":1,"outcome":"complete","usage":{}}"#,
                r#"{"sent":true,"response_bytes":1,"outcome":"complete","usage":{}}"#,
            ),
            (
                r#"{"sent":true,"response_bytes":0,"outcome":"http_error","usage":{"input_tokens":0},"cache_write_5m_tokens":0,"reasoning_included_in_output":false}"#,
                r#"{"sent":true,"response_bytes":0,"outcome":"http_error","usage":{"input_tokens":0},"cache_write_5m_tokens":0,"reasoning_included_in_output":false}"#,
            ),
        ] {
            let envelope: AttemptEnvelope = serde_json::from_str(json).unwrap();
            assert_eq!(serde_json::to_string(&envelope).unwrap(), expected);
        }
        let run: AttemptRun = serde_json::from_str("{}").unwrap();
        assert_eq!(serde_json::to_string(&run).unwrap(), "{}");
        let credential: AttemptCredential = serde_json::from_str("{}").unwrap();
        assert_eq!(serde_json::to_string(&credential).unwrap(), "{}");
        let mut reservation: Value = serde_json::from_str(RESERVATION).unwrap();
        reservation
            .as_object_mut()
            .unwrap()
            .remove("input_tokens_estimate");
        let decoded: AttemptReservation = serde_json::from_value(reservation.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), reservation);
    }

    #[test]
    fn bill_attempt_vault_and_provider_metadata_vocabularies_remain_open() {
        let credential_json = serde_json::json!({
            "credential_ref":"opaque-served-id", "auth_method":"future-auth",
            "provider_ids":["future-provider"], "plan_tier":"future-plan",
            "plan_tier_source":"future-source"
        });
        let credential: AttemptCredential =
            serde_json::from_value(credential_json.clone()).unwrap();
        assert_eq!(serde_json::to_value(credential).unwrap(), credential_json);
        let credential: AttemptCredential = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(credential.plan_tier, None);
        assert!(serde_json::to_value(credential)
            .unwrap()
            .get("plan_tier")
            .is_none());
        let mut value: Value = serde_json::from_str(ENVELOPE).unwrap();
        value["service_tier_used"] = serde_json::json!("future-tier");
        value["speed_used"] = serde_json::json!("future-speed");
        let envelope: AttemptEnvelope = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(envelope).unwrap(), value);
    }

    #[test]
    fn bill_attempt_legacy_attempt_and_warm_bytes_are_unchanged() {
        for pinned in [LEGACY_ATTEMPT, LEGACY_WARM] {
            let record: WalRecord = serde_json::from_str(pinned).unwrap();
            assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
            assert!(record.required_features().is_empty());
            let frame = crate::framing::encode(1, 1, &record).unwrap();
            assert_eq!(&frame[crate::framing::HEADER_LEN..], pinned.as_bytes());
        }
    }

    #[test]
    fn bill_attempt_and_correction_ids_are_validated_by_name() {
        let bytes = [0xab; 16];
        assert_eq!(
            AttemptId::from_bytes(bytes).as_str(),
            "abababababababababababababababab"
        );
        for invalid in [
            "",
            "0011",
            "00112233445566778899AABBCCDDEEFF",
            "g0112233445566778899aabbccddeeff",
            "00112233445566778899aabbccddeeff00",
        ] {
            for kind in golden_records() {
                let mut value: Value = serde_json::from_str(&kind).unwrap();
                value["attempt_id"] = Value::String(invalid.into());
                let err = serde_json::from_value::<WalRecord>(value).unwrap_err();
                assert!(err.to_string().contains("attempt_id"), "{err}");
            }
            let mut value: Value = serde_json::from_str(&golden_records()[2]).unwrap();
            value["correction_id"] = Value::String(invalid.into());
            let err = serde_json::from_value::<WalRecord>(value).unwrap_err();
            assert!(err.to_string().contains("correction_id"), "{err}");
        }
        for invalid in [Value::Null, serde_json::json!(123), serde_json::json!(true)] {
            for golden in golden_records() {
                let mut value: Value = serde_json::from_str(&golden).unwrap();
                value["attempt_id"] = invalid.clone();
                let err = serde_json::from_value::<WalRecord>(value).unwrap_err();
                assert!(err.to_string().contains("attempt_id"), "{err}");
            }
        }
    }

    #[test]
    fn bill_attempt_each_optional_billing_field_requires_the_feature() {
        for legacy in [LEGACY_ATTEMPT, LEGACY_WARM] {
            for (field, value) in [
                ("attempt_id", serde_json::json!(ID)),
                ("reservation", serde_json::from_str(RESERVATION).unwrap()),
                ("envelope", serde_json::from_str(ENVELOPE).unwrap()),
            ] {
                let mut record: Value = serde_json::from_str(legacy).unwrap();
                record[field] = value;
                let record: WalRecord = serde_json::from_value(record).unwrap();
                assert_eq!(
                    record.required_features(),
                    &[crate::framing::BILL_ATTEMPTS_FEATURE]
                );
            }
        }
    }

    #[test]
    fn bill_attempt_samples_use_the_writer_and_reader_codecs() {
        use broca_wal_reader::{
            features::Policy,
            frame::{decode_next_with_policy, DecodeStep, Metadata},
        };
        use sha2::{Digest, Sha256};
        let output = std::env::var_os("BROCA_ATTEMPT_SAMPLE_DIR").map(std::path::PathBuf::from);
        let mut sums = String::new();
        for golden in golden_records() {
            let record: WalRecord = serde_json::from_str(&golden).unwrap();
            let bytes = crate::framing::encode_v4(
                Metadata {
                    seq: 1,
                    fence: 1,
                    appended_ms: 200,
                    writer_id: [1; 16],
                },
                &[],
                &record,
            )
            .unwrap();
            let DecodeStep::Frame(frame) =
                decode_next_with_policy(&bytes, crate::framing::KNOWN_FEATURES, Policy::All)
                    .unwrap()
            else {
                panic!("reader did not decode billing sample");
            };
            assert_eq!(frame.consumed, bytes.len());
            assert_eq!(frame.payload, golden.as_bytes());
            assert_eq!(frame.header.metadata.appended_ms, 200);
            assert_eq!(
                serde_json::from_slice::<WalRecord>(frame.payload).unwrap(),
                record
            );
            assert_eq!(
                decode_next_with_policy(&bytes, &[], Policy::All).unwrap_err(),
                broca_wal_reader::Error::NewerWriter {
                    features: vec![crate::framing::BILL_ATTEMPTS_FEATURE.into()],
                }
            );
            let kind = serde_json::from_str::<Value>(&golden).unwrap()["type"]
                .as_str()
                .unwrap()
                .to_owned();
            if let Some(output) = &output {
                std::fs::create_dir_all(output).unwrap();
                let decoded = serde_json::to_vec_pretty(&serde_json::json!({
                    "seq":frame.header.metadata.seq,
                    "fence":frame.header.metadata.fence,
                    "appended_ms":frame.header.metadata.appended_ms,
                    "writer_id":frame.header.metadata.writer_id,
                    "requires":frame.header.requires,
                    "record":serde_json::from_slice::<Value>(frame.payload).unwrap(),
                }))
                .unwrap();
                for (name, contents) in [
                    (format!("{kind}.json"), frame.payload),
                    (format!("{kind}.wal"), bytes.as_slice()),
                    (format!("{kind}.decoded.json"), decoded.as_slice()),
                ] {
                    std::fs::write(output.join(&name), contents).unwrap();
                    sums.push_str(&format!("{:x}  {name}\n", Sha256::digest(contents)));
                }
            }
        }
        if let Some(output) = output {
            std::fs::write(output.join("SHA256SUMS"), sums).unwrap();
        }
    }
}

#[path = "pinned_bytes_tests.rs"]
mod pinned_bytes_tests;

#[path = "step_transform_bytes_tests.rs"]
mod step_transform_bytes_tests;

#[path = "step_transform_tests.rs"]
mod step_transform_tests;

#[path = "plan_manifest_tests.rs"]
mod plan_manifest;
