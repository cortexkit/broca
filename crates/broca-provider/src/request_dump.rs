//! Shared filenames for live request dumps and offline cache exports.

pub fn request_dump_filename(
    step_context: Option<&broca_protocol::StepContext>,
    fallback_run_id: &str,
    fallback_step_index: u64,
) -> String {
    let run_id = step_context
        .map(|ctx| sanitize_filename_component(&ctx.run_id))
        .filter(|run_id| !run_id.is_empty())
        .unwrap_or_else(|| sanitize_filename_component(fallback_run_id));
    let step_index = step_context
        .map(|ctx| ctx.step_index)
        .unwrap_or(fallback_step_index);
    format!("{run_id}-s{step_index}.json")
}

fn sanitize_filename_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-' => ch,
            _ => '_',
        })
        .collect()
}

/// Only names the dump writer can generate are eligible for retention pruning.
/// Other files may share the configured directory and do not belong to broca.
#[cfg(feature = "live")]
pub(crate) fn is_request_dump_filename(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".json") else {
        return false;
    };
    let Some((run_id, step)) = stem.rsplit_once("-s") else {
        return false;
    };
    run_id == sanitize_filename_component(run_id)
        && step
            .parse::<u64>()
            .is_ok_and(|index| index.to_string() == step)
}
