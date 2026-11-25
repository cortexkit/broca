//! A run at a prefix rebuild takes the current definition of a versioned tool whose provider
//! still serves the major the session froze (llm-runner's `tool_versions` group). The prompt is
//! written afresh there anyway, so the newer description or schema costs no extra rewrite.
//! The run's start then records the manifest and tools it froze, so its resume and the
//! session's later runs keep them. Like any change to the tools a request renders, it also
//! ends the reuse of earlier signed reasoning on routes that bind it to the request prefix:
//! later requests leave that reasoning out (see `crate::run::signed_floor`).

use std::sync::Arc;

use broca_core::tool_plane::CompatibleTool;
use broca_protocol::request::Tool;
use broca_records::{record::FetchOutcome, record::PlanManifest};

/// `manifest` and `offered` (the run's tools, in the order the model sees them) with each tool
/// in `changes` given its current description and input schema, and its frozen pin replaced
/// by the current one. Everything else about the tool, and every other tool, stays as frozen.
/// `None` when nothing in `changes` names a tool of the manifest.
pub fn take_compatible_tools(
    manifest: &PlanManifest,
    offered: &Arc<Vec<Tool>>,
    changes: &[CompatibleTool],
) -> Option<(PlanManifest, Arc<Vec<Tool>>)> {
    let change_of = |provider: Option<&str>, name: &str| {
        changes
            .iter()
            .find(|change| Some(change.provider.as_str()) == provider && change.tool.name == name)
    };
    let take = |tools: &mut Vec<Tool>, provider: Option<&str>| {
        let mut took = false;
        for tool in tools {
            if let Some(change) = change_of(provider.or(tool.module.as_deref()), &tool.name) {
                tool.description.clone_from(&change.tool.description);
                tool.input_schema.clone_from(&change.tool.input_schema);
                took = true;
            }
        }
        took
    };
    let mut manifest = manifest.clone();
    if !take(Arc::make_mut(&mut manifest.tools), None) {
        return None;
    }
    for fetch in &mut manifest.fetches {
        let FetchOutcome::Answered {
            tools,
            declarations_frozen,
            ..
        } = &mut fetch.outcome
        else {
            continue;
        };
        // A fetch's answered tools carry no module: the fetch is the provider's.
        take(Arc::make_mut(tools), Some(&fetch.provider));
        let pins = declarations_frozen
            .as_deref_mut()
            .and_then(|declarations| declarations.schema_pins.as_mut());
        for (name, pin) in pins.into_iter().flatten() {
            if let Some(change) = change_of(Some(&fetch.provider), name) {
                *pin = change.pin.clone();
            }
        }
    }
    let mut offered = Arc::clone(offered);
    take(Arc::make_mut(&mut offered), None);
    Some((manifest, offered))
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_records::{
        record::DispatchSchemaPin, record::FrozenProviderDeclarations, record::ProviderFetch,
    };
    use serde_json::json;

    fn tool(name: &str, module: Option<&str>, description: &str) -> Tool {
        let mut tool: Tool = serde_json::from_value(json!({
            "name": name,
            "description": description,
            "input_schema": {"type": "object"},
        }))
        .unwrap();
        tool.module = module.map(str::to_owned);
        tool
    }

    fn pin(digest: char, major: u64) -> DispatchSchemaPin {
        DispatchSchemaPin {
            schema_digest: digest.to_string().repeat(64),
            semantics: 1,
            major: Some(major),
        }
    }

    fn manifest() -> PlanManifest {
        PlanManifest {
            rich_ui: None,
            plan: broca_records::record::OpaqueJson::from_json_text(json!({}).to_string()).unwrap(),
            plan_identity: "plan-1".into(),
            composition_digest: "composition".into(),
            tools: Arc::new(vec![
                tool("read", Some("aft"), "Read a file."),
                tool("bash", Some("aft"), "Run a command."),
            ]),
            system_text: String::new(),
            system_digest: "system".into(),
            join_version: 1,
            role_versions: Default::default(),
            declarations: Vec::new(),
            fetches: vec![ProviderFetch {
                provider: "aft".into(),
                outcome: FetchOutcome::Answered {
                    catalog_digest: "one".into(),
                    tools: Arc::new(vec![
                        tool("read", None, "Read a file."),
                        tool("bash", None, "Run a command."),
                    ]),
                    text: None,
                    result_ops: None,
                    reply: None,
                    declarations_frozen: Some(Box::new(FrozenProviderDeclarations {
                        schema_pins: Some(
                            [("read".into(), pin('a', 2)), ("bash".into(), pin('b', 1))].into(),
                        ),
                        ..Default::default()
                    })),
                    reference_slots: None,
                },
            }],
        }
    }

    #[test]
    fn a_compatible_tool_takes_its_current_definition_and_pin_everywhere_it_is_frozen() {
        let manifest = manifest();
        let offered = Arc::new(vec![tool("read", Some("aft"), "Read a file.")]);
        let mut current = tool("read", Some("aft"), "Read a file, or part of one.");
        current.input_schema =
            json!({"type": "object", "properties": {"lines": {"type": "string"}}}).into();
        let change = CompatibleTool {
            provider: "aft".into(),
            tool: current.clone(),
            pin: pin('c', 2),
        };
        let (taken, offered) = take_compatible_tools(&manifest, &offered, &[change]).unwrap();
        assert_eq!(taken.tools[0], current);
        assert_eq!(taken.tools[1], manifest.tools[1]);
        assert_eq!(offered[0], current);
        let FetchOutcome::Answered {
            tools,
            declarations_frozen,
            ..
        } = &taken.fetches[0].outcome
        else {
            unreachable!()
        };
        assert_eq!(tools[0].description, current.description);
        assert_eq!(tools[0].module, None);
        let pins = declarations_frozen
            .as_ref()
            .unwrap()
            .schema_pins
            .as_ref()
            .unwrap();
        assert_eq!(pins["read"], pin('c', 2));
        assert_eq!(pins["bash"], pin('b', 1));
        assert_eq!(taken.plan_identity, manifest.plan_identity);
    }

    #[test]
    fn a_change_naming_no_frozen_tool_changes_nothing() {
        let manifest = manifest();
        let offered = Arc::clone(&manifest.tools);
        for change in [
            CompatibleTool {
                provider: "aft".into(),
                tool: tool("grep", Some("aft"), "Search."),
                pin: pin('d', 1),
            },
            CompatibleTool {
                provider: "other".into(),
                tool: tool("read", Some("other"), "Read elsewhere."),
                pin: pin('e', 2),
            },
        ] {
            assert_eq!(take_compatible_tools(&manifest, &offered, &[change]), None);
        }
    }
}
