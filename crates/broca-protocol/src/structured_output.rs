//! Structured output: a run asked for a JSON value shaped by a JSON Schema.
//!
//! The request type, its admission checks and the result type come from the shared role
//! crate (`cortexkit_role_llm_runner::structured_output`). This module adds what broca owns:
//! the draft 2020-12 validator, and the settings a run freezes at admission
//! ([`FrozenStructuredOutput`]), which the provider renders every request of the run from and
//! the runner reads back when the run ends.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use cortexkit_role_llm_runner::structured_output::{
    bound_errors, check_schema, enforcement, reasons, validation, Mode, SkippedSibling,
    StructuredOutput, StructuredOutputResult, DEFAULT_NAME, FIELD, MAX_SCHEMA_JCS_BYTES,
    SUBMIT_RESULT_TOOL,
};

/// The description of the `submit_result` tool. Fixed text: it is part of every request of a
/// `submit_tool` run, so changing it changes the rendered bytes of runs admitted afterwards.
pub const SUBMIT_RESULT_DESCRIPTION: &str = "Submit the final result of the task. Calling this \
     tool ends the task: call it exactly once, when the work is done, with the result as its \
     input.";

/// The tool result the runner records for the counted `submit_result` call, so the call never
/// dangles in the transcript. The run has ended by the time it is written.
pub const SUBMIT_RESULT_RECORDED: &str = "submit_result recorded; the task has ended.";

/// The message prefix of the never-executed result of a call that shared a step with the
/// counted `submit_result` call. The refusal code the role contract names leads the message,
/// as the input-token cap's `budget_exhausted` does.
pub const SKIPPED_AFTER_SUBMIT_MESSAGE: &str =
    "skipped_after_submit: tool not executed; submit_result ended the run in the same step";

/// The refusal code carried by a skipped sibling's tool result.
pub use cortexkit_role_llm_runner::errors::tool_result_reasons::SKIPPED_AFTER_SUBMIT;

/// How a run's schema reaches the provider, decided at admission from the route's catalog
/// support, its wire family and the family's subset profile. A durable compiler-refusal
/// record can downgrade it; every subsequent request, including resume, uses that decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaDelivery {
    /// The family's schema encoding in its strict (enforced) form.
    Strict,
    /// The family's schema encoding without its strict flag: the schema is outside the route's
    /// subset profile, so asking the provider to enforce it could be refused.
    NonStrict,
    /// No provider-side schema constraint. A `final_text` schema goes into the system prompt as
    /// instructions; a `submit_tool` schema is only the submit tool's input schema.
    Unconstrained,
}

/// Everything a run admitted with structured output freezes on the record that starts it,
/// inside its frozen render config. Every request of the run renders from these values, and
/// the run's result reports `mode`, `enforcement` and `reason` from them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenStructuredOutput {
    pub mode: Mode,
    /// The effective name: the request's `name`, or `result`.
    pub name: String,
    /// The RFC 8785 (JCS) bytes of the schema, as text.
    pub schema_jcs: String,
    /// `enforced` or `best_effort`.
    pub enforcement: String,
    /// Why the run is `best_effort`; absent when `enforced`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The name of the route's subset profile; absent when the route has no schema encoding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    pub delivery: SchemaDelivery,
}

impl FrozenStructuredOutput {
    /// Anthropic's JSON output schema has no non-strict form. After a compilation refusal,
    /// final-text runs use prompt instructions while submit-tool runs omit the strict flag.
    pub fn downgrade_grammar(&mut self) {
        self.enforcement = enforcement::BEST_EFFORT.to_owned();
        self.reason = Some("grammar_too_large".to_owned());
        self.delivery = SchemaDelivery::Unconstrained;
    }

    pub fn downgrade_render_config(config: &mut Value) {
        if let Some(mut so) = Self::from_frozen_render_config(Some(config)) {
            so.downgrade_grammar();
            config["structured_output"] =
                serde_json::to_value(so).expect("frozen schema serializes");
        }
    }

    /// The frozen schema as a JSON value. The bytes were produced from a JSON value at
    /// admission, so they always parse.
    pub fn schema(&self) -> Value {
        serde_json::from_str(&self.schema_jcs).unwrap_or(Value::Null)
    }

    /// Read the frozen settings out of a run's frozen render config, where they live under
    /// `structured_output`. `None` for a run admitted without structured output.
    pub fn from_frozen_render_config(frozen: Option<&Value>) -> Option<Self> {
        frozen
            .and_then(|frozen| frozen.get("structured_output"))
            .and_then(|value| serde_json::from_value(value.clone()).ok())
    }

    /// Whether `request` asks for exactly what this run froze: the same mode, effective name
    /// and schema bytes. A steer joining the run with anything else is refused.
    pub fn matches(&self, request: &StructuredOutput) -> bool {
        self.mode == request.mode
            && self.name == request.effective_name()
            && request
                .schema_jcs()
                .is_ok_and(|bytes| bytes == self.schema_jcs.as_bytes())
    }
}

/// The JCS text of a request's schema. The role crate's decoder has already measured these
/// bytes, so they encode.
pub fn schema_jcs_text(request: &StructuredOutput) -> Result<String, String> {
    let bytes = request.schema_jcs()?;
    String::from_utf8(bytes).map_err(|_| format!("{FIELD}.schema"))
}

/// A compiled draft 2020-12 validator for a run's schema.
///
/// Built with the `jsonschema` crate without its HTTP and file retrievers, so a reference that
/// leaves the document can never be fetched; admission refuses such references before this is
/// built anyway. `format` is an annotation and is never asserted. Unknown keywords are ignored.
pub struct SchemaValidator(jsonschema::Validator);

impl SchemaValidator {
    /// Compile `schema`. An error means the runner cannot compile it, which admission refuses
    /// naming `structured_output.schema`.
    pub fn compile(schema: &Value) -> Result<Self, String> {
        jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .should_validate_formats(false)
            .build(schema)
            .map(Self)
            .map_err(|error| error.to_string())
    }

    /// Every way `value` fails the schema, each message prefixed with the JSON Pointer of the
    /// failing instance location in URI fragment form (`#` is the whole value, `#/a/0` its
    /// member). Empty when the value validates. Not yet bounded; see [`bound_errors`].
    pub fn errors(&self, value: &Value) -> Vec<String> {
        self.0
            .iter_errors(value)
            .map(|error| format!("#{}: {error}", error.instance_path().as_str()))
            .collect()
    }
}

/// Check everything about a request that does not depend on the session: the role crate's
/// member, reference, dialect and size rules (already applied when the request was decoded
/// with `SendRequest::structured_output`), and that broca can compile the schema. Returns the
/// field of the `invalid_params` refusal.
pub fn check_compiles(request: &StructuredOutput) -> Result<(), String> {
    check_schema(&request.schema).map_err(|_| format!("{FIELD}.schema"))?;
    if request.schema_jcs()?.len() > MAX_SCHEMA_JCS_BYTES {
        return Err(format!("{FIELD}.schema"));
    }
    SchemaValidator::compile(&request.schema)
        .map(|_| ())
        .map_err(|_| format!("{FIELD}.schema"))
}

/// The result of a run admitted with structured output, as `run.result` and `run.status`
/// report it.
///
/// `submitted` is the counted `submit_result` call's input for a `submit_tool` run that
/// completed by calling it; `final_text` is the joined final text of a `final_text` run that
/// completed. `skipped` lists the calls of the submitting step that were never dispatched.
/// A run that did not complete, or completed without producing a value, reports `absent`.
pub fn result(
    frozen: &FrozenStructuredOutput,
    completed: bool,
    submitted: Option<&Value>,
    final_text: Option<&str>,
    skipped: Vec<SkippedSibling>,
) -> StructuredOutputResult {
    let base = |validation: &str| {
        let mut result =
            StructuredOutputResult::new(frozen.mode.as_str(), &frozen.enforcement, validation);
        if let Some(reason) = &frozen.reason {
            result = result.with_reason(reason);
        }
        result
    };
    let finish = |result: StructuredOutputResult, skipped: Vec<SkippedSibling>| {
        if frozen.mode == Mode::SubmitTool {
            result.with_skipped_siblings(skipped)
        } else {
            result
        }
    };
    // What the model produced, and whether it parsed as JSON. Final text that does not parse
    // still produced something: it fails validation with a parse error and carries no value.
    let produced: Option<Result<Value, String>> = if !completed {
        None
    } else if frozen.mode == Mode::SubmitTool {
        submitted.cloned().map(Ok)
    } else {
        final_text.map(|text| {
            serde_json::from_str::<Value>(text)
                .map_err(|error| format!("#: the final text is not a JSON value: {error}"))
        })
    };
    let skipped = if completed { skipped } else { Vec::new() };
    let Some(produced) = produced else {
        return finish(base(validation::ABSENT), skipped);
    };
    let value = match produced {
        Ok(value) => value,
        Err(parse_error) => {
            return finish(
                base(validation::FAILED).with_errors(vec![parse_error]),
                skipped,
            )
        }
    };
    let errors = match SchemaValidator::compile(&frozen.schema()) {
        Ok(validator) => validator.errors(&value),
        // Admission compiled this exact schema, so this needs a validator change between
        // admission and the end of the run. Report it rather than claim a pass.
        Err(error) => vec![format!("#: the frozen schema no longer compiles: {error}")],
    };
    if errors.is_empty() {
        finish(base(validation::PASSED).with_value(value), skipped)
    } else {
        finish(
            base(validation::FAILED)
                .with_value(value)
                .with_errors(errors),
            skipped,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn frozen(mode: Mode) -> FrozenStructuredOutput {
        FrozenStructuredOutput {
            mode,
            name: DEFAULT_NAME.into(),
            schema_jcs: r#"{"properties":{"answer":{"type":"string"}},"required":["answer"],"type":"object"}"#.into(),
            enforcement: enforcement::ENFORCED.into(),
            reason: None,
            profile: Some("xai".into()),
            delivery: SchemaDelivery::Strict,
        }
    }

    #[test]
    fn a_conforming_value_passes_and_a_wrong_one_names_its_pointer() {
        let passed = result(
            &frozen(Mode::SubmitTool),
            true,
            Some(&json!({"answer": "x"})),
            None,
            vec![SkippedSibling::new("c2", "read")],
        );
        assert_eq!(passed.validation, validation::PASSED);
        assert_eq!(passed.value, Some(json!({"answer": "x"})));
        assert_eq!(passed.skipped_siblings.as_ref().unwrap().len(), 1);
        assert_eq!(passed.well_formed(), Ok(()));
        let failed = result(
            &frozen(Mode::SubmitTool),
            true,
            Some(&json!({"answer": 42})),
            None,
            vec![],
        );
        assert_eq!(failed.validation, validation::FAILED);
        assert_eq!(failed.value, Some(json!({"answer": 42})));
        let errors = failed.errors.as_ref().unwrap();
        assert!(errors[0].starts_with("#/answer: "), "{errors:?}");
        assert_eq!(failed.well_formed(), Ok(()));
    }

    #[test]
    fn final_text_that_is_not_json_fails_without_a_value() {
        let output = result(&frozen(Mode::FinalText), true, None, Some("prose"), vec![]);
        assert_eq!(output.validation, validation::FAILED);
        assert_eq!(output.value, None);
        assert_eq!(output.skipped_siblings, None);
        assert_eq!(output.well_formed(), Ok(()));
    }

    #[test]
    fn a_run_that_did_not_complete_is_absent_and_lists_no_siblings() {
        let output = result(
            &frozen(Mode::SubmitTool),
            false,
            Some(&json!({"answer": "x"})),
            None,
            vec![SkippedSibling::new("c2", "read")],
        );
        assert_eq!(output.validation, validation::ABSENT);
        assert_eq!(output.value, None);
        assert_eq!(output.skipped_siblings, Some(vec![]));
        assert_eq!(output.well_formed(), Ok(()));
    }

    #[test]
    fn format_is_an_annotation_and_unknown_keywords_are_ignored() {
        let validator = SchemaValidator::compile(
            &json!({"type": "string", "format": "email", "x-unknown": {"type": "integer"}}),
        )
        .unwrap();
        assert!(validator.errors(&json!("not an email")).is_empty());
    }

    #[test]
    fn schemas_that_do_not_compile_or_leave_the_document_are_refused_by_field() {
        for schema in [
            json!({"type": 12}),
            json!({"$ref": "https://example.com/s.json"}),
            json!({"$schema": "http://json-schema.org/draft-07/schema#"}),
        ] {
            assert_eq!(
                check_compiles(&StructuredOutput::new(Mode::FinalText, schema.clone())),
                Err("structured_output.schema".into()),
                "{schema}"
            );
        }
        assert_eq!(
            check_compiles(&StructuredOutput::new(
                Mode::SubmitTool,
                json!({"$defs": {"a": {"type": "string"}}, "$ref": "#/$defs/a"})
            )),
            Ok(())
        );
    }
}
