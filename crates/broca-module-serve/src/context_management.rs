//! Per-session context management: the process defaults, and the admission rule that binds a
//! session to one value.
//!
//! The rule, from `docs/context-management-design.md`:
//!
//! - A session's FIRST episode binds it. The caller's value wins; with none, the first match of:
//!   the `broca.jsonc` default for the session's `project_root`, the `broca.jsonc` global
//!   default, then `"none"`.
//! - A later episode that omits `context_management` inherits the frozen value. One carrying a DIFFERENT
//!   value is refused permanently, the way a changed system prompt is.
//! - `"compaction"` is refused until the mode exists. Never downgraded to `"none"`.
//! - A resumed episode uses its frozen value; process configuration never reaches it.
//!
//! - A `RunStarted` without `context_management` (written by a build that had no such field)
//!   counts as `"none"`.
//!
//! `{"transform": "<module id>"}` is a context-management value broca does not serve. It still
//! decodes, because older `RunStarted` records carry it. Admission, resume and boot refuse it by
//! name with `context_management_unavailable`, rather than running the session as `"none"`,
//! which would let it reach the window its caller believed was managed.
//!
//! In `broca.jsonc`:
//!
//! ```jsonc
//! {"context_management": {
//!     "default": "none",
//!     "projects": {"/workspace/scratch": "none"}
//! }}
//! ```

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use broca_core::RunError;
use broca_protocol::request::ContextManagement;
use serde::Deserialize;

/// The `context_management` section of `broca.jsonc`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContextManagementConfig {
    global: Option<ContextManagement>,
    projects: BTreeMap<String, ContextManagement>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Section {
    #[serde(default)]
    default: Option<ContextManagement>,
    #[serde(default)]
    projects: BTreeMap<String, ContextManagement>,
}

/// What the session's durable history says about its context management, as admission sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriorContextManagement<'a> {
    /// No episode yet (or only an import seed): this admission binds the session.
    FirstAdmission,
    /// The last episode froze this value.
    Frozen(&'a ContextManagement),
    /// The last episode's `RunStarted` carries no `context_management` at all.
    Legacy,
}

impl<'a> PriorContextManagement<'a> {
    /// Classify the last episode's frozen config. An import seed is not an admission: the
    /// session's first harness episode after it is the one that binds.
    pub fn from_last_config(config: Option<&'a broca_wal::RunConfig>) -> Self {
        match config {
            None => Self::FirstAdmission,
            Some(config) if is_import_seed(config) => Self::FirstAdmission,
            Some(config) => match config.context_management.as_ref() {
                Some(value) => Self::Frozen(value),
                None => Self::Legacy,
            },
        }
    }
}

/// Whether `config` is the placeholder `session.import` writes for an imported history: model
/// `import/import` with a string `import_request_digest` option, the same test broca-core's
/// loop applies. Such a seed never ran a provider, so it chose no context management.
fn is_import_seed(config: &broca_wal::RunConfig) -> bool {
    config.model.provider_module_id == "import"
        && config.model.model_id == "import"
        && config
            .provider_options
            .get("import_request_digest")
            .is_some_and(serde_json::Value::is_string)
}

/// Compare project roots without letting a trailing slash decide whether a default applies.
fn project_key(root: &str) -> &str {
    let trimmed = root.trim_end_matches('/');
    if trimmed.is_empty() && root.starts_with('/') {
        "/"
    } else {
        trimmed
    }
}

impl ContextManagementConfig {
    /// Build directly from parts. Values are validated exactly as a loaded file's are.
    pub fn new(
        global: Option<ContextManagement>,
        projects: BTreeMap<String, ContextManagement>,
    ) -> Result<Self, String> {
        if let Some(value) = &global {
            validate_configured("context_management.default", value)?;
        }
        let mut keyed = BTreeMap::new();
        for (root, value) in projects {
            validate_configured(&format!("context_management.projects[{root:?}]"), &value)?;
            keyed.insert(project_key(&root).to_string(), value);
        }
        Ok(Self {
            global,
            projects: keyed,
        })
    }

    /// Load from the config-home `broca.jsonc`, through the same ladder as its other sections.
    pub fn load_default_path() -> Result<Self, String> {
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            crate::config_home::ConfigHome::Resolved { path, .. } => Self::load(&path),
            crate::config_home::ConfigHome::Unresolvable => Ok(Self::default()),
        }
    }

    /// A missing file means no configured defaults; an unreadable or malformed one refuses boot.
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(error) => return Err(format!("read broca config {}: {error}", path.display())),
        };
        Self::parse(&text)
            .map_err(|error| format!("parse broca config {}: {error}", path.display()))
    }

    /// Parse the `context_management` section of a `broca.jsonc` text. Other root keys belong
    /// to other loaders and are ignored; inside this section every key and value is strict.
    pub fn parse(text: &str) -> Result<Self, String> {
        let stripped = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let object: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&stripped).map_err(|error| error.to_string())?;
        let Some(section) = object.get("context_management") else {
            return Ok(Self::default());
        };
        let section: Section = serde_json::from_value(section.clone())
            .map_err(|error| format!("context_management {section}: {error}"))?;
        Self::new(section.default, section.projects)
    }

    /// The value a NEW session in `project_root` gets when its first send names none.
    pub fn default_for(&self, project_root: &str) -> ContextManagement {
        self.projects
            .get(project_key(project_root))
            .or(self.global.as_ref())
            .cloned()
            .unwrap_or(ContextManagement::None)
    }

    /// Decide the value a NEW episode is admitted and frozen under, or refuse it.
    pub fn admit(
        &self,
        requested: Option<&ContextManagement>,
        prior: PriorContextManagement<'_>,
        project_root: &str,
    ) -> Result<ContextManagement, RunError> {
        let resolved = match prior {
            PriorContextManagement::FirstAdmission => requested
                .cloned()
                .unwrap_or_else(|| self.default_for(project_root)),
            PriorContextManagement::Frozen(frozen) => bound_value(frozen.clone(), requested)?,
            // A session whose last `RunStarted` carries no `context_management` binds to "none",
            // and from this episode on that value is frozen like any other. `broca.jsonc`
            // defaults never reach it: they apply only at a session's first admission.
            PriorContextManagement::Legacy => bound_value(ContextManagement::None, requested)?,
        };
        self.check_available(&resolved)?;
        Ok(resolved)
    }

    /// The value a RESUMED episode runs under: the frozen one, or `"none"` when its `RunStarted`
    /// carries no `context_management`. The caller still checks the result with
    /// [`Self::check_available`], so a frozen value this build cannot serve is refused.
    pub fn resumed(&self, frozen: Option<&ContextManagement>) -> ContextManagement {
        frozen.cloned().unwrap_or(ContextManagement::None)
    }

    /// Decide the value an episode runs under, or refuse it before anything runs.
    ///
    /// A new episode (`fresh_episode`: fresh, or appended to an idle lineage) is admitted under
    /// [`Self::admit`]. A resumed episode keeps the value its last episode froze, and is refused
    /// when this build cannot serve it (a frozen transform is refused, see the module doc).
    /// `last_config` is the session's last episode config.
    pub fn for_episode(
        &self,
        fresh_episode: bool,
        requested: Option<&ContextManagement>,
        last_config: Option<&broca_wal::RunConfig>,
        project_root: &str,
    ) -> Result<ContextManagement, RunError> {
        if fresh_episode {
            return self.admit(
                requested,
                PriorContextManagement::from_last_config(last_config),
                project_root,
            );
        }
        let value = self.resumed(last_config.and_then(|config| config.context_management.as_ref()));
        self.check_available(&value)?;
        Ok(value)
    }

    /// Refuse a value this process cannot serve.
    pub fn check_available(&self, value: &ContextManagement) -> Result<(), RunError> {
        match value {
            ContextManagement::None => Ok(()),
            ContextManagement::Compaction => Err(RunError::ContextManagementUnavailable {
                requested: value.clone(),
                reason: "the compaction mode is not available yet; send \"none\", and nothing \
                         was run"
                    .into(),
            }),
            ContextManagement::Transform(module) => Err(RunError::ContextManagementUnavailable {
                requested: value.clone(),
                reason: format!(
                    "broca no longer runs whole-array prompt transforms, so transform module \
                     '{module}' cannot manage this session; nothing was run"
                ),
            }),
        }
    }
}

/// A session already bound to `bound`: an omitted field inherits it, the same value is
/// accepted, and anything else is refused naming both.
fn bound_value(
    bound: ContextManagement,
    requested: Option<&ContextManagement>,
) -> Result<ContextManagement, RunError> {
    match requested {
        Some(requested) if *requested != bound => Err(RunError::ContextManagementChanged {
            frozen: bound,
            requested: requested.clone(),
        }),
        _ => Ok(bound),
    }
}

/// A configured default must be one a session could actually be admitted under, or every new
/// session it reaches would be refused. Checked at boot so the operator hears it once, by name.
fn validate_configured(key: &str, value: &ContextManagement) -> Result<(), String> {
    match value {
        ContextManagement::None => Ok(()),
        ContextManagement::Compaction => Err(format!(
            "{key}: \"compaction\" is not available yet and cannot be a default"
        )),
        ContextManagement::Transform(module) => Err(format!(
            "{key}: {{\"transform\": {module:?}}} names a whole-array prompt transform, which \
             broca no longer runs; use \"none\""
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transform(module: &str) -> ContextManagement {
        ContextManagement::Transform(module.into())
    }

    fn config(text: &str) -> ContextManagementConfig {
        ContextManagementConfig::parse(text).unwrap()
    }

    const LAYERED: &str = r#"{
        // Other loaders own other root keys.
        "providers": {},
        "context_management": {
            "default": "none",
            "projects": {"/work/scratch/": "none",},
        },
    }"#;

    #[test]
    fn nothing_configured_is_none() {
        for text in ["{}", LAYERED] {
            let config = config(text);
            assert_eq!(config.default_for("/work/scratch"), ContextManagement::None);
            assert_eq!(config.default_for("/work/other"), ContextManagement::None);
        }
    }

    #[test]
    fn first_admission_takes_the_callers_value() {
        assert_eq!(
            config(LAYERED)
                .admit(
                    Some(&ContextManagement::None),
                    PriorContextManagement::FirstAdmission,
                    "/work/scratch"
                )
                .unwrap(),
            ContextManagement::None
        );
    }

    #[test]
    fn an_omitted_value_inherits_the_frozen_one() {
        let frozen = ContextManagement::None;
        assert_eq!(
            config(LAYERED)
                .admit(
                    None,
                    PriorContextManagement::Frozen(&frozen),
                    "/work/scratch"
                )
                .unwrap(),
            ContextManagement::None
        );
    }

    #[test]
    fn the_same_value_again_is_accepted() {
        let frozen = ContextManagement::None;
        assert_eq!(
            config(LAYERED)
                .admit(
                    Some(&frozen),
                    PriorContextManagement::Frozen(&frozen),
                    "/work/scratch"
                )
                .unwrap(),
            frozen
        );
    }

    #[test]
    fn a_different_value_on_a_bound_session_is_refused_naming_both() {
        let frozen = ContextManagement::None;
        let error = config(LAYERED)
            .admit(
                Some(&ContextManagement::Compaction),
                PriorContextManagement::Frozen(&frozen),
                "/work/scratch",
            )
            .unwrap_err();
        assert!(matches!(
            &error,
            RunError::ContextManagementChanged { frozen, requested }
                if *frozen == ContextManagement::None
                    && *requested == ContextManagement::Compaction
        ));
        assert_eq!(
            error.admission_error_class(),
            broca_protocol::ErrorClass::Permanent
        );
        let message = error.to_string();
        assert!(message.contains("\"none\""), "{message}");
        assert!(message.contains("\"compaction\""), "{message}");
    }

    #[test]
    fn compaction_is_refused_never_downgraded() {
        let error = config("{}")
            .admit(
                Some(&ContextManagement::Compaction),
                PriorContextManagement::FirstAdmission,
                "/p",
            )
            .unwrap_err();
        assert!(
            matches!(
                &error,
                RunError::ContextManagementUnavailable { requested, .. }
                    if *requested == ContextManagement::Compaction
            ),
            "{error:?}"
        );
        assert!(error.to_string().contains("not available yet"), "{error}");
        assert_eq!(
            error.admission_error_class(),
            broca_protocol::ErrorClass::Permanent
        );
    }

    /// A caller naming a transform is refused by name, never run as `"none"` (see the module
    /// doc).
    #[test]
    fn a_requested_transform_is_refused_by_name_never_run_as_none() {
        let error = config(LAYERED)
            .admit(
                Some(&transform("magic-context")),
                PriorContextManagement::FirstAdmission,
                "/work/scratch",
            )
            .unwrap_err();
        assert!(
            matches!(
                &error,
                RunError::ContextManagementUnavailable { requested, .. }
                    if *requested == transform("magic-context")
            ),
            "{error:?}"
        );
        let message = error.to_string();
        assert!(message.contains("no longer runs"), "{message}");
        assert!(message.contains("'magic-context'"), "{message}");
        assert_eq!(
            error.admission_error_class(),
            broca_protocol::ErrorClass::Permanent
        );
    }

    /// A session frozen onto a transform is refused by name both when a later send inherits the
    /// value and when an episode resumes under it.
    #[test]
    fn a_frozen_transform_is_refused_on_admission_and_on_resume() {
        let config = config(LAYERED);
        let frozen = transform("magic-context");
        let inherited = config
            .admit(
                None,
                PriorContextManagement::Frozen(&frozen),
                "/work/scratch",
            )
            .unwrap_err();
        assert!(
            matches!(
                &inherited,
                RunError::ContextManagementUnavailable { requested, .. } if *requested == frozen
            ),
            "{inherited:?}"
        );
        assert!(
            inherited.to_string().contains("no longer runs"),
            "{inherited}"
        );

        let resumed = config.resumed(Some(&frozen));
        assert_eq!(resumed, frozen, "resume never rewrites the frozen value");
        let error = config.check_available(&resumed).unwrap_err();
        assert!(
            matches!(&error, RunError::ContextManagementUnavailable { .. }),
            "{error:?}"
        );
    }

    /// A session whose last `RunStarted` carries no `context_management` binds to "none", and a
    /// later request for anything else is a change like any other.
    #[test]
    fn a_legacy_session_freezes_none_and_refuses_a_change() {
        let config = config(LAYERED);
        assert_eq!(
            config
                .admit(None, PriorContextManagement::Legacy, "/work/scratch")
                .unwrap(),
            ContextManagement::None,
        );
        let error = config
            .admit(
                Some(&transform("magic-context")),
                PriorContextManagement::Legacy,
                "/work/scratch",
            )
            .unwrap_err();
        assert!(
            matches!(error, RunError::ContextManagementChanged { .. }),
            "{error:?}"
        );
        assert_eq!(config.resumed(None), ContextManagement::None);
    }

    #[test]
    fn malformed_config_refuses_boot_by_name() {
        for (text, needle) in [
            (
                r#"{"context_management":{"default":"compaction"}}"#,
                "context_management.default: \"compaction\" is not available yet",
            ),
            (
                r#"{"context_management":{"default":{"transform":"magic-context"}}}"#,
                "context_management.default: {\"transform\": \"magic-context\"} names a \
                 whole-array prompt transform, which broca no longer runs",
            ),
            (
                r#"{"context_management":{"projects":{"/p":{"transform":" "}}}}"#,
                "context_management.projects[\"/p\"]: {\"transform\": \" \"}",
            ),
            (
                r#"{"context_management":{"default":"summarize"}}"#,
                "context_management",
            ),
            (r#"{"context_management":{"defualt":"none"}}"#, "defualt"),
            (r#"{"context_management":null}"#, "context_management"),
        ] {
            let error = ContextManagementConfig::parse(text).unwrap_err();
            assert!(error.contains(needle), "{text}: {error}");
        }
    }

    #[test]
    fn a_missing_file_has_no_defaults() {
        let path = broca_test_support::scratch_root().join(format!(
            "broca-context-management-missing-{}.jsonc",
            std::process::id()
        ));
        let config = ContextManagementConfig::load(&path).unwrap();
        assert_eq!(config, ContextManagementConfig::default());
    }

    fn episode_config(context_management: Option<ContextManagement>) -> broca_wal::RunConfig {
        broca_wal::RunConfig {
            render_tags: false,
            model: broca_protocol::request::ModelHandle::new("p", "m"),
            tools: Default::default(),
            tool_choice: Default::default(),
            generation: Default::default(),
            response_format: Default::default(),
            provider_options: serde_json::Value::Null,
            cache_tiers: serde_json::Value::Null,
            context_epoch: 0,
            renderer_version: 0,
            frozen_render_config: serde_json::Value::Null,
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            stop_when: Vec::new(),
            context_management,
            variant: None,
            service_tier: None,
        }
    }

    /// A resumed episode runs under its frozen value, and one frozen onto a transform is refused
    /// by name instead of resuming unmanaged.
    #[test]
    fn a_resumed_episode_keeps_its_frozen_value_and_refuses_a_frozen_transform() {
        let config = config(LAYERED);
        let none = episode_config(Some(ContextManagement::None));
        assert_eq!(
            config
                .for_episode(false, None, Some(&none), "/work/scratch")
                .unwrap(),
            ContextManagement::None
        );
        let legacy = episode_config(None);
        assert_eq!(
            config
                .for_episode(false, None, Some(&legacy), "/work/scratch")
                .unwrap(),
            ContextManagement::None,
            "a RunStarted without context_management resumes as none"
        );
        let frozen = episode_config(Some(transform("magic-context")));
        let error = config
            .for_episode(false, None, Some(&frozen), "/work/scratch")
            .unwrap_err();
        assert!(
            matches!(
                &error,
                RunError::ContextManagementUnavailable { requested, .. }
                    if *requested == transform("magic-context")
            ),
            "{error:?}"
        );
        assert!(error.to_string().contains("no longer runs"), "{error}");
        // A new episode on the same session inherits the frozen transform and is refused too.
        let error = config
            .for_episode(true, None, Some(&frozen), "/work/scratch")
            .unwrap_err();
        assert!(
            matches!(&error, RunError::ContextManagementUnavailable { .. }),
            "{error:?}"
        );
    }

    #[test]
    fn an_import_seed_is_not_a_binding_admission() {
        let mut config = broca_wal::RunConfig {
            render_tags: false,
            model: broca_protocol::request::ModelHandle::new("import", "import"),
            tools: Default::default(),
            tool_choice: Default::default(),
            generation: Default::default(),
            response_format: Default::default(),
            provider_options: serde_json::json!({"import_request_digest": "abc"}),
            cache_tiers: serde_json::Value::Null,
            context_epoch: 0,
            renderer_version: 0,
            frozen_render_config: serde_json::Value::Null,
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            stop_when: Vec::new(),
            context_management: None,
            variant: None,
            service_tier: None,
        };
        assert_eq!(
            PriorContextManagement::from_last_config(Some(&config)),
            PriorContextManagement::FirstAdmission
        );
        config.model = broca_protocol::request::ModelHandle::new("p", "m");
        assert_eq!(
            PriorContextManagement::from_last_config(Some(&config)),
            PriorContextManagement::Legacy
        );
        config.context_management = Some(ContextManagement::None);
        assert_eq!(
            PriorContextManagement::from_last_config(Some(&config)),
            PriorContextManagement::Frozen(&ContextManagement::None)
        );
        assert_eq!(
            PriorContextManagement::from_last_config(None),
            PriorContextManagement::FirstAdmission
        );
    }
}
