//! One run of a session kept in the session store, through the new loop, over a test's own
//! provider and tool plane: what a test executor's `execute_on_store` does. Include this file
//! with `#[path = "common/store_exec.rs"] mod store_exec;` (or the path from the test's own
//! folder).

#![allow(dead_code)]

use broca_core::{AllowAll, OutputEvent, Provider, ToolPlane};
use broca_module::executor::{StoreRunEnd, StoreRunError, StoreRunJob, StoreRunMode};
use broca_protocol::error::ProviderError;
use tokio::sync::mpsc;

/// Run `job` through the new loop over `provider` and `tools`, with every call allowed and
/// the clock reading 1. With `pause_for_auth`, a fresh run pauses for sign-in at its start, as
/// a run whose credential needs re-authentication does.
pub async fn run_on_store<P, T>(
    job: StoreRunJob,
    out: &mpsc::Sender<OutputEvent>,
    provider: &P,
    tools: &T,
    pause_for_auth: Option<ProviderError>,
) -> StoreRunEnd
where
    P: Provider,
    T: ToolPlane,
{
    use broca_session_loop::writer::Writer as _;
    let StoreRunJob {
        mut req,
        mut control,
        mut writer,
        mut parts,
        mode,
        ..
    } = job;
    if pause_for_auth.is_some() && matches!(mode, StoreRunMode::Fresh) {
        control.pause_for_auth_at_start = pause_for_auth;
    }
    let result = async {
        let now = || 1;
        let seams = broca_session_loop::run::Seams {
            steer_yields: None,
            provider,
            tools,
            bind: req.session.clone(),
            out: Some(out),
            cancel: control.cancel.clone().unwrap_or_default(),
            stop: Default::default(),
            held: None,
            approval: &AllowAll,
            hook_plane: control.hook_plane.clone(),
            now_ms: &now,
            session_address: None,
            compaction_plane: control.compaction_plane.as_deref(),
            compaction_session: control.compaction_session.clone(),
            compaction_changes: true,
            keep_warm: None,
            account_reports: None,
            max_same_account_wait: Default::default(),
            auth_selection: None,
            step_progress: control.step_progress.clone(),
            model_call_ready: None,
            pause_for_auth_at_start: control.pause_for_auth_at_start.clone(),
            account_continue: None,
        };
        match mode {
            StoreRunMode::Fresh => {
                let admission = broca_session_loop::admit::admission(
                    &mut req,
                    &control,
                    provider,
                    writer.operations(),
                    Some(1),
                )
                .map_err(StoreRunError::Admission)?;
                broca_session_loop::run::run(&mut writer, parts.as_mut(), admission, &seams).await
            }
            StoreRunMode::Seal(cause) => {
                broca_session_loop::seal(&mut writer, parts.as_ref(), &seams, cause).await
            }
            StoreRunMode::Resume | StoreRunMode::RecheckSignIn | StoreRunMode::ProbeSignIn => {
                broca_session_loop::run::resume(&mut writer, parts.as_ref(), &seams).await
            }
        }
        .map_err(StoreRunError::Loop)
    }
    .await;
    StoreRunEnd { parts, result }
}

/// The session store for a test registry over `dir`, running each session's runs through
/// `executor`'s `execute_on_store`, with every session encrypted under a fixed key.
pub async fn store_runtime(
    dir: &std::path::Path,
    executor: std::sync::Arc<dyn broca_module::executor::RunExecutor>,
) -> std::sync::Arc<broca_module::store_open::StoreRuntime> {
    std::sync::Arc::new(
        broca_module::store_open::StoreRuntime::open(
            dir,
            broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
            executor,
            std::time::Duration::from_millis(100),
        )
        .await
        .unwrap(),
    )
}

/// The records of `session` in `runtime`, in log order; empty while it doesn't exist.
pub async fn records(
    runtime: &broca_module::store_open::StoreRuntime,
    session: &broca_protocol::BindIdentity,
) -> Vec<broca_session_store::record::Record> {
    match runtime.current_view(session).await {
        Ok(Some(view)) => view
            .open_log_records()
            .await
            .unwrap()
            .into_iter()
            .map(|(_, record)| record)
            .collect(),
        _ => Vec::new(),
    }
}
