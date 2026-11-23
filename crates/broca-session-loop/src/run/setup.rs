//! Storing a run's setup parts at admission: its tools, response format, system text, render
//! config and plan manifest, each once per deployment, and the run's identity kept apart from
//! the render config part so runs on one route share it.

use super::*;

/// The setup parts a run's start names, as [`store_setup`] stored them, what remains of its
/// manifest, which stays with the run's choices, and the run's identity, which its start keeps
/// beside the shared render config part.
pub(super) struct Setup {
    pub(super) parts: Vec<PartRef>,
    pub(super) manifest_rest: Option<Value>,
    pub(super) render_identity: Option<Value>,
}

/// Stores the run's setup parts, then has the run hold their shared decoded values in place of
/// the ones its send gave: its tools, response format, system text, render config and manifest
/// with its tool list. Its resume and the session's next run (which inherits its tools) read
/// the same parts, so every request of the session renders the same values, and every run of
/// the process on the same parts holds one copy of each. The render config is stored without
/// the run's identity ([`broca_provider::render_identity`]), so runs on one route share its
/// part, and the run holds the part with its identity joined back on top, as its resume reads
/// it. A part that doesn't decode refuses the run.
pub(super) fn store_setup(
    parts: &mut dyn Parts,
    admission: &mut Admission,
) -> Result<Setup, LoopError> {
    let (render_config, render_identity) =
        broca_provider::render_identity::split(&admission.render_config);
    let mut setup = Vec::new();
    // A planned run's system text is part of its manifest; any other run's is its own part.
    let global_system = (admission.manifest.is_none() && !admission.global_system.is_empty())
        .then(|| serde_json::to_value(&admission.global_system))
        .transpose()
        .map_err(|e| LoopError::Parts(e.to_string()))?
        .unwrap_or(Value::Null);
    for (kind, value) in [
        (PartKind::Tools, serde_json::to_value(&admission.tools)),
        (
            PartKind::ResponseFormat,
            serde_json::to_value(&admission.response_format),
        ),
        (PartKind::GlobalSystem, Ok(global_system)),
        (PartKind::RenderConfig, Ok(render_config)),
    ] {
        let value = value.map_err(|e| LoopError::Parts(e.to_string()))?;
        if value.is_null() {
            continue;
        }
        setup.push(parts.put(kind, &value).map_err(LoopError::Parts)?);
    }
    // A manifest's shared members are setup parts after the run config's, in the order the
    // resume reads them back; what remains of it stays with the run's choices.
    let config_parts = setup.len();
    let manifest_rest = match &admission.manifest {
        Some(manifest) => manifest_parts(manifest, parts, &mut setup)?,
        None => None,
    };
    let parts = &*parts;
    fn decoded<T>(parts: &dyn Parts, part: &PartRef) -> Result<Arc<T>, LoopError>
    where
        T: serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        crate::parts::shared(parts, part).map_err(|error| {
            LoopError::Parts(format!("the run's {:?} setup part: {error}", part.kind))
        })
    }
    for part in &setup[..config_parts] {
        match part.kind {
            PartKind::Tools => admission.tools = decoded(parts, part)?,
            PartKind::ResponseFormat => admission.response_format = decoded(parts, part)?,
            PartKind::GlobalSystem => admission.global_system = decoded(parts, part)?,
            PartKind::RenderConfig => {
                admission.render_config = broca_session_store::record::join_render_identity(
                    decoded(parts, part)?,
                    render_identity.as_ref(),
                )
                .map_err(|reason| LoopError::Parts(format!("the run's render config: {reason}")))?
            }
            _ => {}
        }
    }
    if admission.manifest.is_some() {
        // The reference the run's start makes to its manifest, as a later read takes it.
        let reference = broca_session_store::fold::operations::ManifestRef {
            parts: setup[config_parts..].to_vec(),
            rest: manifest_rest.clone(),
        };
        let manifest = stored_manifest(&reference, parts)?;
        // A planned run with a compaction plan version renders its manifest's system text, as
        // `global_system` decided before the manifest was read back.
        if admission.compaction_plan_version.is_some() {
            admission.global_system = manifest_system(&reference, &manifest, parts)?;
        }
        admission.manifest = Some(manifest);
    }
    Ok(Setup {
        parts: setup,
        manifest_rest,
        render_identity,
    })
}
