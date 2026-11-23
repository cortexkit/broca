//! A run's frozen config read back from its start record and the setup parts the start names:
//! the whole config a resume or a new run's admission needs, or only the render and tag facts a
//! read or a status walk needs. A reader decodes only the parts it asks for, since the tools and
//! the plan manifest's parts are the largest a start names.

use serde_json::Value;

use broca_protocol::request::Tool;
use broca_session_store::record::{PartKind, RunStart};

use super::{decode, LoopError};
use crate::parts::Parts;

/// A run's config members, as `RunStarted.config` spells them, and its plan manifest's setup
/// parts, in order.
pub type ConfigMembers = (serde_json::Map<String, Value>, Vec<(PartKind, Value)>);

/// Which of a run start's setup parts [`run_config_members`] reads. A part it doesn't read is
/// never decoded: the plan manifest's parts (provider answers, the plan, system text) and the
/// tools are the largest a start names, and most readers want none of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupParts {
    /// Every part: the config's and the plan manifest's.
    All,
    /// The config's parts only: its tools, response format, global system text and render
    /// config. The manifest comes back empty.
    Config,
    /// The render config only, for a reader of the run's frozen render and tag facts. The
    /// config comes back without its tools, response format and global system text, and the
    /// manifest empty.
    RenderConfig,
}

/// A run start's frozen config: the members of its `config` object and the setup parts of its
/// plan manifest, in order. `part` reads a setup part's value; `wanted` says which parts to read
/// at all. The render config comes back whole, with the start's `render_identity` joined
/// ([`RunStart::render_config`](broca_session_store::record::RunStart::render_config)).
/// `not_object` names a config member, or a render config half, that isn't a JSON object.
///
/// The inverse conversion (`today.rs`) rebuilds the record from this, and the serve executor
/// rebuilds the config a new run's admission reads (`run_config_of_start`), so both read a run's
/// start the same way. The config's parts come first (its tools, then the response format every
/// run config has, then its global system text and render config), then the manifest's. So a
/// tools part before the response format is the config's, and one after it is the manifest's.
pub fn run_config_members<E>(
    start: &broca_session_store::record::RunStart,
    wanted: SetupParts,
    mut part: impl FnMut(&broca_session_store::record::PartRef) -> Result<Value, E>,
    not_object: impl Fn() -> E,
) -> Result<ConfigMembers, E> {
    let choices = match start.run_choices.as_ref() {
        None => serde_json::Map::new(),
        Some(Value::Object(choices)) => choices.clone(),
        Some(_) => return Err(not_object()),
    };
    let mut config = match choices.get("config") {
        None => serde_json::Map::new(),
        Some(Value::Object(config)) => config.clone(),
        Some(_) => return Err(not_object()),
    };
    fn json(value: impl serde::Serialize) -> Value {
        serde_json::to_value(value).expect("record values serialize")
    }
    fn set(
        config: &mut serde_json::Map<String, Value>,
        key: &str,
        value: Option<impl serde::Serialize>,
    ) {
        if let Some(value) = value {
            config.insert(key.into(), json(value));
        }
    }
    config.insert("model".into(), json(&start.model));
    config.insert("renderer_version".into(), json(start.renderer_version));
    set(&mut config, "context_limit", start.context_limit);
    set(&mut config, "service_tier", start.service_tier.as_ref());
    set(
        &mut config,
        "route_decision_id",
        start.route_decision_id.as_ref(),
    );
    set(&mut config, "auth_selection", start.auth_selection.as_ref());
    let mut manifest = Vec::new();
    let mut past_config = false;
    for setup in &start.setup {
        let member = match setup.kind {
            PartKind::Tools if !past_config => Some("tools"),
            PartKind::ResponseFormat => {
                past_config = true;
                Some("response_format")
            }
            PartKind::GlobalSystem => Some("global_system"),
            PartKind::RenderConfig => Some("frozen_render_config"),
            _ => None,
        };
        let read = match (wanted, member) {
            (SetupParts::All, _) => true,
            (SetupParts::Config, member) => member.is_some(),
            (SetupParts::RenderConfig, member) => member == Some("frozen_render_config"),
        };
        if !read {
            continue;
        }
        let mut value = part(setup)?;
        if setup.kind == PartKind::RenderConfig {
            // The run's own identity members go back on top of the shared part; a start whose
            // identity can't be joined reads as a config that isn't an object.
            value = start
                .render_config(std::sync::Arc::new(value))
                .map(std::sync::Arc::unwrap_or_clone)
                .map_err(|_| not_object())?;
        }
        match member {
            Some(member) => {
                config.insert(member.into(), value);
            }
            None => manifest.push((setup.kind, value)),
        }
    }
    Ok((config, manifest))
}

/// The run's frozen config, rebuilt from its start record and setup parts as
/// [`run_config_members`] reads them.
pub fn run_config_of_start(
    start: &broca_session_store::record::RunStart,
    parts: &dyn Parts,
) -> Result<broca_records::record::RunConfig, LoopError> {
    let (config, _) = run_config_members(
        start,
        SetupParts::Config,
        |part| crate::parts::value(parts, part).map_err(LoopError::Parts),
        || LoopError::Parts("a run start's carried config is not an object".into()),
    )?;
    serde_json::from_value(Value::Object(config)).map_err(|e| LoopError::Parts(e.to_string()))
}

/// The run's frozen config as it renders and bills now: its start's, with the render config and
/// the auth selection replaced by the latest ones the run refroze after its start (the fold's
/// `refrozen_render` and `refrozen_auth`), as `frozen_admission` reads them.
///
/// A resume is admitted from this, so a run that moved to another account at a usage limit
/// resumes on the account it moved to, with that account's render config. A resume never asks
/// routing again; admitting it from the start's config would instead send it back to the
/// account that refused it, and its provider would render a config the run no longer froze.
pub fn run_config_of_run(
    run: &broca_session_store::fold::operations::Run,
    parts: &dyn Parts,
) -> Result<broca_records::record::RunConfig, LoopError> {
    let start = run.start.as_ref().ok_or(LoopError::Unsupported(
        "a run whose start isn't in the fold",
    ))?;
    let mut config = run_config_of_start(start, parts)?;
    if let Some(refrozen) = &run.refrozen_render {
        config.frozen_render_config = refrozen["render_config"].clone();
        if let Some(version) = decode::<u32>(Some(refrozen["renderer_version"].clone()))? {
            config.renderer_version = version;
        }
    }
    if let Some(selection) = &run.refrozen_auth {
        config.auth_selection = decode(Some(selection.clone()))?;
    }
    Ok(config)
}

/// The facts of a run's frozen config that its reads and status walk use: how it numbers and
/// cleans tagged text, its provider's render config (which holds a structured-output run's
/// mode), its wake directive and its auth selection. Read by [`run_frozen_facts`], which
/// decodes only the start's render config part. Each member reads as [`run_config_of_start`]
/// reads it.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct FrozenFacts {
    #[serde(default)]
    pub render_tags: bool,
    #[serde(default)]
    pub computed_tags: Option<broca_records::record::ComputedTagRules>,
    #[serde(default)]
    pub frozen_render_config: Value,
    #[serde(default)]
    pub turn_directives_v1: Option<Vec<broca_protocol::request::DirectiveName>>,
    #[serde(default)]
    pub auth_selection: Option<broca_records::record::AuthSelection>,
}

impl FrozenFacts {
    /// The run's computed tag rules ([`broca_core::tags::computed_rules`] over its config).
    pub fn computed_rules(&self) -> Option<broca_protocol::tags::ComputedRules> {
        broca_core::tags::rules_for(
            self.computed_tags,
            self.render_tags,
            &self.frozen_render_config,
        )
    }
}

/// [`FrozenFacts`] of the run `start` starts, decoding only its render config part.
pub fn run_frozen_facts(
    start: &broca_session_store::record::RunStart,
    parts: &dyn Parts,
) -> Result<FrozenFacts, LoopError> {
    let (config, _) = run_config_members(
        start,
        SetupParts::RenderConfig,
        |part| crate::parts::value(parts, part).map_err(LoopError::Parts),
        || LoopError::Parts("a run start's carried config is not an object".into()),
    )?;
    serde_json::from_value(Value::Object(config)).map_err(|e| LoopError::Parts(e.to_string()))
}

/// The [`broca_core::signed_floor::tool_set_digest`] of the tools and render config `start`
/// froze, read as [`admission_of_start`](super::admission_of_start) reads them; `None` for a seeded run.
pub(super) fn tool_set_of_start(
    start: &RunStart,
    parts: &dyn Parts,
) -> Result<Option<String>, LoopError> {
    if start.seeded == Some(true) {
        return Ok(None);
    }
    let part = |kind: PartKind| start.setup.iter().find(|part| part.kind == kind);
    let tools = part(PartKind::Tools)
        .map(|part| crate::parts::shared::<Vec<Tool>, _>(parts, part))
        .transpose()
        .map_err(LoopError::Parts)?;
    let render_config = part(PartKind::RenderConfig)
        .map(|part| crate::parts::shared::<Value, _>(parts, part))
        .transpose()
        .map_err(LoopError::Parts)?
        .map(|render_config| start.render_config(render_config))
        .transpose()
        .map_err(|error| LoopError::Parts(error.to_string()))?;
    Ok(Some(broca_core::signed_floor::tool_set_digest(
        tools.as_deref().map_or(&[], Vec::as_slice),
        render_config.as_deref().unwrap_or(&Value::Null),
    )))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use broca_session_store::record::PartRef;
    use serde_json::json;

    use super::*;
    use crate::parts::MemoryParts;

    /// Parts in memory that note the kind of every part whose value is read.
    #[derive(Default)]
    struct Noting {
        inner: MemoryParts,
        read: Mutex<Vec<PartKind>>,
    }

    impl Parts for Noting {
        fn put(&mut self, kind: PartKind, value: &Value) -> Result<PartRef, String> {
            self.inner.put(kind, value)
        }

        fn get(&self, part: &PartRef) -> Result<Value, String> {
            self.read.lock().unwrap().push(part.kind);
            self.inner.get(part)
        }
    }

    impl Noting {
        fn take(&self) -> Vec<PartKind> {
            std::mem::take(&mut self.read.lock().unwrap())
        }
    }

    /// A planned run's start: the config's tools, response format, global system text and render
    /// config as parts, then a plan manifest's tools, provider answer, plan, system text and
    /// declarations, with tag rules, a wake directive and an auth selection in its config.
    fn planned_start(parts: &mut Noting) -> RunStart {
        let mut config =
            match serde_json::to_value(broca_core::import::import_marker_config("digest", 40))
                .unwrap()
            {
                Value::Object(config) => config,
                _ => unreachable!("a run config serializes as an object"),
            };
        let response_format = config.remove("response_format").unwrap();
        config.remove("tools");
        config.remove("frozen_render_config");
        config.insert("render_tags".into(), json!(true));
        config.insert("computed_tags".into(), json!({"checkpoint": 7}));
        config.insert("turn_directives_v1".into(), json!(["wake"]));
        let tools =
            json!([{"name": "read", "description": "reads", "input_schema": {"type": "object"}}]);
        let setup = [
            (PartKind::Tools, tools.clone()),
            (PartKind::ResponseFormat, response_format),
            (PartKind::GlobalSystem, json!([])),
            (
                PartKind::RenderConfig,
                json!({"family": "anthropic_messages", "beta": ["x"]}),
            ),
            (PartKind::Tools, tools),
            (
                PartKind::ProviderAnswer,
                json!({"provider": "aft", "tools": []}),
            ),
            (PartKind::Plan, json!({"plan_id": "plan-1"})),
            (PartKind::SystemText, json!("you are a worker")),
            (PartKind::Declarations, json!([])),
        ]
        .into_iter()
        .map(|(kind, value)| parts.put(kind, &value).unwrap())
        .collect();
        RunStart {
            run_id: "run-1".into(),
            model: broca_protocol::ModelHandle::new("anthropic", "claude-opus-4-8"),
            setup,
            renderer_version: 3,
            submission_id: None,
            send_id: None,
            request_identity_digest: None,
            ts_ms: Some(9_000),
            written_at_ms: Some(9_000),
            session_class: None,
            retention_secs: None,
            title: None,
            context_limit: None,
            service_tier: None,
            route_decision_id: None,
            compaction_plan_version: None,
            auth_selection: Some(json!({"credential_id": "oauth:anthropic:acct1"})),
            run_choices: Some(json!({ "config": config })),
            origin: None,
            principal: None,
            scope: None,
            hooks: None,
            keep_warm: None,
            on_restart: None,
            seeded: None,
            refusal_schedule: None,
            imported_ids: None,
            render_identity: None,
            message_marks: None,
            named_speaker: None,
            kept_notice: None,
        }
    }

    /// Each reader decodes only the parts it asks for: the frozen facts only the render config,
    /// the run config only the config's own parts, and the inverse conversion's read every
    /// part. The facts read as the run config reads the same members.
    #[test]
    fn readers_decode_only_the_setup_parts_they_ask_for() {
        let mut parts = Noting::default();
        let start = planned_start(&mut parts);

        let facts = run_frozen_facts(&start, &parts).unwrap();
        assert_eq!(parts.take(), vec![PartKind::RenderConfig]);

        let config = run_config_of_start(&start, &parts).unwrap();
        assert_eq!(
            parts.take(),
            vec![
                PartKind::Tools,
                PartKind::ResponseFormat,
                PartKind::GlobalSystem,
                PartKind::RenderConfig,
            ]
        );
        assert_eq!(config.tools.len(), 1);

        let (all, manifest) = run_config_members(
            &start,
            SetupParts::All,
            |part| parts.get(part),
            || "not an object".to_string(),
        )
        .unwrap();
        assert_eq!(parts.take().len(), start.setup.len());
        assert_eq!(manifest.len(), 5);
        let (config_only, no_manifest) = run_config_members(
            &start,
            SetupParts::Config,
            |part| parts.get(part),
            || "not an object".to_string(),
        )
        .unwrap();
        assert_eq!(
            config_only, all,
            "the config's members don't depend on the manifest"
        );
        assert!(no_manifest.is_empty());

        assert!(facts.render_tags && config.render_tags);
        assert_eq!(facts.computed_tags, config.computed_tags);
        assert_eq!(facts.frozen_render_config, config.frozen_render_config);
        assert_eq!(facts.turn_directives_v1, config.turn_directives_v1);
        assert_eq!(facts.auth_selection, config.auth_selection);
        assert_eq!(
            facts.computed_rules(),
            broca_core::tags::computed_rules(&config)
        );
        assert!(facts.computed_rules().is_some_and(|rules| rules.render));
    }

    /// `start` without its plan manifest. The admission reader rebuilds the manifest, which
    /// this fixture's stand-in manifest parts don't make; the render config doesn't depend on it.
    fn without_manifest(start: &RunStart) -> RunStart {
        let mut start = start.clone();
        if let Some(plan) = start
            .setup
            .iter()
            .position(|part| part.kind == PartKind::Plan)
        {
            start.setup.truncate(plan);
        }
        start
    }

    /// A start that keeps its identity beside a shared render config part reads, through every
    /// reader, the same whole config as a start whose part holds all of it.
    #[test]
    fn every_reader_joins_the_run_identity_onto_the_shared_part() {
        let mut parts = Noting::default();
        let mut whole = planned_start(&mut parts);
        // The split start keeps the part `planned_start` stored; the whole one names a part
        // that holds the identity too.
        let mut split = whole.clone();
        let identity = json!({"prompt_cache_key": "session-key"});
        let mut whole_config = json!({"family": "anthropic_messages", "beta": ["x"]});
        whole_config["prompt_cache_key"] = identity["prompt_cache_key"].clone();
        for part in &mut whole.setup {
            if part.kind == PartKind::RenderConfig {
                *part = parts.put(PartKind::RenderConfig, &whole_config).unwrap();
            }
        }
        split.render_identity = Some(identity);

        let read = |start: &RunStart| {
            (
                run_frozen_facts(start, &parts)
                    .unwrap()
                    .frozen_render_config,
                run_config_of_start(start, &parts)
                    .unwrap()
                    .frozen_render_config,
                Value::clone(
                    &crate::run::admission_of_start(&without_manifest(start), &parts)
                        .unwrap()
                        .render_config,
                ),
            )
        };
        let (facts, config, admission) = read(&split);
        assert_eq!(facts, whole_config);
        assert_eq!(config, whole_config);
        assert_eq!(admission, whole_config);
        assert_eq!(read(&whole), (facts, config, admission));
    }

    /// A run's identity stays out of its render config part: two runs whose configs differ only
    /// in identity store one part, each start keeps its own identity, and the run holds the
    /// whole config, the same one its resume reads back.
    #[test]
    fn runs_differing_only_in_identity_share_one_render_config_part() {
        let mut parts = Noting::default();
        let base = without_manifest(&planned_start(&mut parts));
        let shared = json!({"family": "anthropic_messages", "beta": ["x"]});
        let mut digests = Vec::new();
        for key in ["key-a", "key-b"] {
            let mut whole = shared.clone();
            whole["prompt_cache_key"] = json!(key);
            let mut start = base.clone();
            for part in &mut start.setup {
                if part.kind == PartKind::RenderConfig {
                    *part = parts.put(PartKind::RenderConfig, &whole).unwrap();
                }
            }
            let mut admission = crate::run::admission_of_start(&start, &parts).unwrap();
            let setup = crate::run::setup::store_setup(&mut parts, &mut admission).unwrap();
            assert_eq!(*admission.render_config, whole);
            assert_eq!(
                setup.render_identity,
                Some(json!({"prompt_cache_key": key}))
            );
            let part = setup
                .parts
                .iter()
                .find(|part| part.kind == PartKind::RenderConfig)
                .unwrap();
            assert_eq!(parts.get(part).unwrap(), shared);
            digests.push(part.digest);
            let stored = RunStart {
                setup: setup.parts.clone(),
                render_identity: setup.render_identity.clone(),
                ..start
            };
            let resumed = crate::run::admission_of_start(&stored, &parts).unwrap();
            assert_eq!(*resumed.render_config, whole);
        }
        assert_eq!(digests[0], digests[1]);
    }
}
