use serde_json::{json, Value};
use subc_config::{RefusalCode, Schema};

use super::{review, READERS, SCHEMA_ONLY};

fn schema() -> Schema {
    Schema::new(serde_json::from_str(super::super::SCHEMA).unwrap()).unwrap()
}

/// A key the schema gains without a reader here would reach broca unreviewed.
#[test]
fn every_schema_key_has_one_reviewer() {
    let schema = schema();
    let mut keys: Vec<&str> = schema.normalized()["properties"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    let mut reviewed: Vec<&str> = READERS
        .iter()
        .flat_map(|reader| reader.keys.iter().copied())
        .chain(SCHEMA_ONLY.iter().copied())
        .collect();
    reviewed.sort_unstable();
    assert_eq!(reviewed, keys);
}

#[test]
fn the_defaults_pass_review() {
    let tuple = review(&schema(), &json!({})).unwrap();
    assert_eq!(tuple.effective, schema().defaults());
}

#[test]
fn a_view_a_reader_refuses_is_refused_at_its_key() {
    let refusal = review(
        &schema(),
        &json!({"account_selection": {"routing_module": "   "}}),
    )
    .unwrap_err();
    assert_eq!(refusal.code, RefusalCode::ValueInvalid);
    assert_eq!(refusal.path, "/account_selection");
}

#[test]
fn a_reader_reading_two_keys_names_the_one_the_view_sets() {
    let providers: Vec<Value> = (0..17).map(|n| json!(format!("p{n}"))).collect();
    let refusal = review(&schema(), &json!({"private_content_providers": providers}));
    // Whether the schema or the reader refuses, the refusal names the key the view set.
    assert_eq!(refusal.unwrap_err().path, "/private_content_providers");
}

#[test]
fn a_schema_refusal_passes_through() {
    let view = json!({"no_such_key": true});
    let context = subc_config::ValidationContext::new("broca", None);
    assert_eq!(
        review(&schema(), &view).unwrap_err(),
        schema()
            .validate_layer(subc_config::Layer::User, &view, &context)
            .unwrap_err()
    );
}

#[test]
fn a_valid_view_passes_with_its_sources() {
    let tuple = review(
        &schema(),
        &json!({"service_tier": {"enabled": true}, "chatgpt_force_https": true}),
    )
    .unwrap();
    assert_eq!(tuple.effective["service_tier"]["enabled"], json!(true));
    assert!(tuple.sources.iter().any(
        |(path, layer)| path.starts_with("/service_tier") && *layer == subc_config::Layer::User
    ));
}

mod requests {
    use serde_json::{json, Value};
    use subc_config::{ApplyKind, Layer};
    use subc_protocol::config::{
        ConfigLayer, ConfigLayerSnapshot, ConfigReviewRequest, ConfigStatus, ConfigView,
    };

    use super::super::review_request;
    use super::schema;

    fn user_file(text: Option<&str>) -> ConfigLayerSnapshot {
        ConfigLayerSnapshot::file(
            "broca",
            ConfigLayer::User,
            "/synthetic/broca.jsonc",
            text.map(|text| text.as_bytes().to_vec()),
        )
    }

    /// The view approved from `user`, as the daemon would supply it as a review's base.
    fn approved(user: Value) -> ConfigView {
        let tuple = super::super::review(&schema(), &user).unwrap();
        ConfigView::new(
            1,
            tuple.effective,
            tuple.sources,
            Vec::new(),
            ConfigStatus::Active,
        )
    }

    fn changed_paths(reply: &subc_protocol::config::ConfigReviewReply) -> Vec<&str> {
        reply
            .fields
            .iter()
            .map(|field| field.path.as_str())
            .collect()
    }

    #[test]
    fn an_edit_reports_the_fields_it_changes_with_their_apply_kind_and_sources() {
        let base = approved(json!({}));
        let request = ConfigReviewRequest::new(
            1,
            vec![user_file(Some(
                "{\n  // outage switch\n  \"chatgpt_force_https\": true,\n  \"service_tier\": {\"enabled\": true},\n}\n",
            ))],
            Some(base),
        );
        let reply = review_request(&schema(), &request);
        assert!(reply.refusals.is_empty(), "{:?}", reply.refusals);
        let mut paths = changed_paths(&reply);
        paths.sort_unstable();
        assert_eq!(paths, ["/chatgpt_force_https", "/service_tier/enabled"]);
        for field in &reply.fields {
            assert_eq!(field.old_source, Layer::Default);
            assert_eq!(field.new_source, Layer::User);
            let expected = match field.path.as_str() {
                "/chatgpt_force_https" => ApplyKind::Live,
                _ => ApplyKind::ModuleRestart,
            };
            assert_eq!(field.apply, expected, "{}", field.path);
        }
    }

    #[test]
    fn the_approved_file_again_changes_nothing() {
        let user = json!({"service_tier": {"enabled": true}});
        let request = ConfigReviewRequest::new(
            1,
            vec![user_file(Some(&user.to_string()))],
            Some(approved(user)),
        );
        let reply = review_request(&schema(), &request);
        assert!(
            reply.fields.is_empty() && reply.refusals.is_empty(),
            "{reply:?}"
        );
    }

    #[test]
    fn with_no_base_every_field_is_new_from_null() {
        let reply = review_request(
            &schema(),
            &ConfigReviewRequest::new(0, vec![user_file(None)], None),
        );
        assert!(reply.refusals.is_empty(), "{:?}", reply.refusals);
        assert!(!reply.fields.is_empty());
        for field in &reply.fields {
            assert_eq!(
                serde_json::to_value(&field.old).unwrap(),
                serde_json::to_value(subc_config::RecordedValue::new(Value::Null)).unwrap(),
                "{}",
                field.path
            );
        }
    }

    /// The daemon checks a reply's paths against the leaves of its views: a map's entries and a
    /// tagged choice's members, never the object above them, and an empty map at its own path.
    #[test]
    fn fields_are_the_leaves_the_views_name() {
        let reply = review_request(
            &schema(),
            &ConfigReviewRequest::new(0, vec![user_file(None)], None),
        );
        let paths = changed_paths(&reply);
        for leaf in [
            "/max_same_account_wait/kind",
            "/max_same_account_wait/minutes",
            "/context_management/default/kind",
            "/anthropic_fallbacks/refusal_resend/models/claude-opus-5",
            "/providers",
        ] {
            assert!(paths.contains(&leaf), "{leaf} missing from {paths:?}");
        }
        for path in &paths {
            assert!(
                !paths
                    .iter()
                    .any(|other| other.starts_with(&format!("{path}/"))),
                "{path} is an object above other fields"
            );
        }
    }

    /// A change inside a map or a tagged choice is reported at the leaves it changes, with the
    /// apply kind of the setting they belong to; a member the new choice drops reads null, and
    /// map entries the file doesn't name keep their defaults.
    #[test]
    fn changes_inside_maps_and_choices_name_their_leaves() {
        let request = ConfigReviewRequest::new(
            1,
            vec![user_file(Some(
                r#"{"max_same_account_wait": {"kind": "max"},
                    "anthropic_fallbacks": {"refusal_resend": {"models": {"claude-opus-5": "claude-opus-4-8", "claude-haiku-5-5": "claude-opus-4-8"}}}}"#,
            ))],
            Some(approved(json!({}))),
        );
        let reply = review_request(&schema(), &request);
        assert!(reply.refusals.is_empty(), "{:?}", reply.refusals);
        let field = |path: &str| {
            reply
                .fields
                .iter()
                .find(|field| field.path == path)
                .unwrap_or_else(|| panic!("{path} missing from {:?}", changed_paths(&reply)))
        };
        let kind = field("/max_same_account_wait/kind");
        assert_eq!(kind.apply, ApplyKind::ModuleRestart);
        assert_eq!(kind.new_source, Layer::User);
        let minutes = field("/max_same_account_wait/minutes");
        assert_eq!(
            serde_json::to_value(&minutes.new).unwrap(),
            serde_json::to_value(subc_config::RecordedValue::new(Value::Null)).unwrap()
        );
        let added = field("/anthropic_fallbacks/refusal_resend/models/claude-haiku-5-5");
        assert_eq!(added.apply, ApplyKind::ModuleRestart);
        assert_eq!(added.old_source, Layer::Default);
        // The entry the file repeats changes only its source; the others aren't fields at all.
        assert_eq!(
            field("/anthropic_fallbacks/refusal_resend/models/claude-opus-5").new_source,
            Layer::User
        );
        assert!(!changed_paths(&reply)
            .contains(&"/anthropic_fallbacks/refusal_resend/models/claude-fable-5"));
    }

    #[test]
    fn a_file_in_an_older_shape_is_refused_not_converted() {
        let request = ConfigReviewRequest::new(
            1,
            vec![user_file(Some(r#"{"max_same_account_wait": 30}"#))],
            Some(approved(json!({}))),
        );
        let reply = review_request(&schema(), &request);
        assert!(reply.fields.is_empty());
        assert_eq!(reply.refusals.len(), 1, "{reply:?}");
    }

    #[test]
    fn a_reader_refusal_names_its_key() {
        let request = ConfigReviewRequest::new(
            1,
            vec![user_file(Some(
                r#"{"account_selection": {"routing_module": "   "}}"#,
            ))],
            None,
        );
        let reply = review_request(&schema(), &request);
        assert_eq!(reply.refusals[0].code, "value_invalid");
        assert_eq!(reply.refusals[0].paths, ["/account_selection"]);
    }

    #[test]
    fn unreadable_bytes_and_other_layers_are_refused() {
        let unreadable = ConfigReviewRequest::new(1, vec![user_file(Some("{not json"))], None);
        assert_eq!(
            review_request(&schema(), &unreadable).refusals[0].code,
            "value_invalid"
        );
        let mut project = user_file(Some("{}"));
        project.layer = ConfigLayer::Project;
        let other = ConfigReviewRequest::new(1, vec![project], None);
        assert_eq!(
            review_request(&schema(), &other).refusals[0].code,
            "layer_forbidden"
        );
    }
}
