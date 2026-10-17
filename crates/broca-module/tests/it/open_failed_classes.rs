use broca_protocol::ErrorClass;

/// Keep the production construction inventory closed: every added site must choose a class.
#[test]
fn every_open_failed_site_chooses_a_class() {
    let source = include_str!("../../src/serve.rs");
    let marker = ["ServeError::", "Open("].concat();
    let sites = [
        ("final_message_of: missing RunStarted", "None"),
        ("final_message_of: replay failed", "None"),
        ("final_message_of: missing assistant", "None"),
        ("From<OpenError>: erased cause", "None"),
        ("From<OpenError>: typed cause", "Some(class)"),
        (
            "cancel_at: unsupported",
            "Some(broca_protocol::ErrorClass::Permanent)",
        ),
        (
            "head: unsupported",
            "Some(broca_protocol::ErrorClass::Permanent)",
        ),
        (
            "warm: unsupported",
            "Some(broca_protocol::ErrorClass::Permanent)",
        ),
        (
            "run_result: unsupported",
            "Some(broca_protocol::ErrorClass::Permanent)",
        ),
        (
            "baseline: unsupported",
            "Some(broca_protocol::ErrorClass::Permanent)",
        ),
        (
            "model_read: invalid source",
            "Some(broca_protocol::ErrorClass::Permanent)",
        ),
        ("send: erased cause", "None"),
        ("send: typed admission", "Some(class)"),
        ("retract: erased cause", "None"),
        ("subscribe: erased cause", "None"),
        (
            "read: incomplete late continuation",
            "Some(broca_protocol::ErrorClass::Transient)",
        ),
        ("warm: erased cause", "None"),
        // Cancellation can fail while reading the cut JSON that freezes a session.
        // The actor passes text here, not the I/O kind needed to prove retryability.
        ("cancel: erased move-authority cause", "None"),
    ];
    let mut expressions = Vec::new();
    for tail in source.split(&marker).skip(1) {
        let mut depth = 1;
        let mut end = None;
        for (i, ch) in tail.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => depth -= 1,
                _ => {}
            }
            if depth == 0 {
                end = Some(i);
                break;
            }
        }
        let expression: String = tail[..end.expect("balanced Open expression")]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        // Display matches the variant rather than constructing it.
        if expression == "s,_" {
            continue;
        }
        expressions.push(expression);
    }
    assert_eq!(
        expressions.len(),
        sites.len(),
        "an Open construction was added or removed; update its classification decision"
    );
    for ((site, class), expression) in sites.iter().zip(expressions) {
        assert!(
            expression
                .trim_end_matches(',')
                .ends_with(&format!(",{class}")),
            "{site}: expected {class}, found {expression}"
        );
    }
}

#[test]
fn typed_open_errors_preserve_all_shared_classes() {
    for class in [
        ErrorClass::Transient,
        ErrorClass::Permanent,
        ErrorClass::Auth,
        ErrorClass::ContextOverflow,
    ] {
        let error = broca_module::ServeError::from(broca_module::registry::OpenError::Classified {
            reason: "known cause".into(),
            class,
        });
        assert!(
            matches!(error, broca_module::ServeError::Open(_, Some(actual)) if actual == class)
        );
        assert_eq!(error.to_string(), "open failed: known cause");
    }
}
