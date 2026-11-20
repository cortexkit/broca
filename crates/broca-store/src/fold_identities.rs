/// A legacy-only operator cannot use a root whose marker does not prove it is legacy.
#[derive(Debug)]
pub struct LegacyOnlyOperator {
    pub operator: String,
    pub root: std::path::PathBuf,
    pub reason: Option<broca_wal::state_format::StateRootError>,
}

impl std::fmt::Display for LegacyOnlyOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} is legacy-only: refusing state root {}",
            self.operator,
            self.root.display()
        )?;
        match &self.reason {
            Some(reason) => write!(f, ": {reason}"),
            None => write!(f, " marked broca-state/2"),
        }
    }
}

impl std::error::Error for LegacyOnlyOperator {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.reason.as_ref().map(|reason| reason as _)
    }
}

/// Classify without preparing the root or opening its legacy database. Unknown and unreadable
/// markers are not evidence of a legacy root, including when legacy files already exist.
pub fn require_legacy_operator_root(
    root: &std::path::Path,
    operator: impl Into<String>,
) -> Result<(), LegacyOnlyOperator> {
    let reason = match broca_wal::state_format::classify_state_root(root) {
        Ok(config) if !config.is_overhaul() => return Ok(()),
        Ok(_) => None,
        Err(reason) => Some(reason),
    };
    Err(LegacyOnlyOperator {
        operator: operator.into(),
        root: root.to_path_buf(),
        reason,
    })
}
