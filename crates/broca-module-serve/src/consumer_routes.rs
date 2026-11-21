//! The modules broca opens consumer routes to, resolved once from configuration.
//!
//! The serve binary opens its outbound routes from these values, and the manifest declares
//! its `consumes` roles from the SAME values. Keeping both readers on one resolved struct is
//! what stops the declaration and the behaviour drifting: an operator who points
//! `BROCA_TOOL_MODULE` somewhere else changes the dialled module and the declared one together.

use subc_protocol::manifest::ConsumerRole;

use crate::catalog_refresh::FUSIFORM_MODULE_ID;

/// Environment knob naming the default tool-provider module.
pub const TOOL_MODULE_ENV: &str = "BROCA_TOOL_MODULE";

/// The tool-provider module a tool call goes to when the caller gave no `Tool.module` hint
/// and the catalog names no unique provider for that tool.
pub const DEFAULT_TOOL_MODULE: &str = "aft";

/// The credential-vault module's subc id (its `claustrum` management surface), used on the
/// vault auth path (the default) to resolve provider tokens.
///
/// This is a TARGET broca dials, not an allowlist entry, so it has no grace period in either
/// direction: dialing a name the vault has not taken yet fails exactly as hard as dialing one
/// it has left. Changing it is therefore safe only once the vault answers to the new name.
pub const CREDENTIALS_MODULE_ID: &str = "claustrum";

/// Which modules this broca process opens consumer routes to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRoutes {
    /// The default tool module handed to the tool plane.
    pub tool_module: String,
    /// The credential vault, present only when runs resolve credentials through it.
    pub vault_module: Option<String>,
    pub bus_events: bool,
}

impl ConsumerRoutes {
    /// Resolve from the raw knob values. A blank value counts as unset, matching how every
    /// other broca environment knob is read.
    ///
    /// `vault_in_use` is decided by the caller because it depends on the auth source and on
    /// whether a mock provider base URL is set; the vault route is opened only when both say
    /// a real credential lookup will happen.
    pub fn resolve(tool_module: Option<&str>, vault_in_use: bool) -> Self {
        let non_blank =
            |value: Option<&str>| value.filter(|v| !v.trim().is_empty()).map(str::to_string);
        Self {
            tool_module: non_blank(tool_module).unwrap_or_else(|| DEFAULT_TOOL_MODULE.to_string()),
            vault_module: vault_in_use.then(|| CREDENTIALS_MODULE_ID.to_string()),
            bus_events: false,
        }
    }

    /// The manifest `consumes` roles for this configuration.
    ///
    /// The catalog module is always named because the catalog poll runs in every
    /// configuration. The routing module is never named: broca's manifest is sent before its
    /// settings arrive, so it cannot know a routing module the settings name, and it finds one
    /// by discovery at request time otherwise.
    pub fn consumer_roles(&self) -> Vec<ConsumerRole> {
        let mut services = vec![FUSIFORM_MODULE_ID.to_string()];
        services.extend(self.vault_module.iter().cloned());
        if self.bus_events {
            services.push("ckbus".into());
        }
        vec![
            ConsumerRole::ToolClient {
                of: vec![self.tool_module.clone()],
            },
            ConsumerRole::ServiceClient { of: services },
        ]
    }

    pub fn with_bus_events(mut self, enabled: bool) -> Self {
        self.bus_events = enabled;
        self
    }
}

impl Default for ConsumerRoutes {
    /// The unconfigured process: no tool-module override (so the default tool module), and the
    /// vault in use for credentials.
    fn default() -> Self {
        Self::resolve(None, true)
    }
}
