//! The settings gate on broca's daemon connection: broca takes its settings from the daemon
//! instead of reading `broca.jsonc`, when the daemon offers that (`config.gate` in HELLO_ACK).
//!
//! broca registers unready, with `config.review` among its control operations, and builds
//! nothing from the settings yet. [`wait_for_settings`] then declares broca's schema and asks
//! for its view (`config.get`). Until a view broca can build from arrives, the connection
//! answers health probes as degraded with the reason, reviews proposed settings, refuses every
//! route bind with the retryable `module_warming`, and asks again whenever the daemon says the
//! settings changed. broca never falls back to the file or to defaults while it waits.
//!
//! With a view in hand, `main` builds every component from it exactly as it would from the
//! file. Then the serve loop sends `catalog.update` with the real provider roles and
//! `ready: true` in one request, and acknowledges the view with `config.applied`
//! ([`Gate::finish_start`]).
//!
//! Later views: one that changes only fields applied in place (today `chatgpt_force_https`) is
//! applied and acknowledged. One that changes a field read at start is left alone: the daemon
//! restarts broca once it approves that view, and the new process starts from it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use subc_config::Schema;
use subc_protocol::config::{
    ConfigAppliedRequest, ConfigApplyResult, ConfigChanged, ConfigDeclareRequest, ConfigGetReply,
    ConfigGetRequest, ConfigReviewRequest, ConfigStatus, ConfigView, CONFIG_CHANGED_OP,
    CONFIG_REVIEW_OP,
};
use subc_protocol::session::{
    HealthStatus, ModuleControlCommand, ModuleControlRequest, ModuleControlRequestFromModule,
    ModuleControlResponse,
};
use subc_protocol::{ErrorBody, Flags, Frame, FrameType, Priority};
use subc_transport::{read_frame, write_frame, FrameIoError};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::oneshot;

use super::Settings;
use crate::ServeIoError;

/// The first correlation id the gate uses for its own requests: above the serve loop's (HELLO
/// is 1) and below the scope describer's (`1 << 40`), so no reply is ever taken for another's.
const FIRST_GATE_CORR: u64 = 1 << 36;

/// How long one of the gate's requests waits for the daemon's answer.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// What a probe reads while broca waits, and why it can't start yet.
const WAITING_FOR_SETTINGS: &str = "waiting for the daemon's settings";

/// The daemon's answer to one of the gate's requests.
#[derive(Debug)]
pub enum Reply {
    /// A `Response` frame's body.
    Done(Vec<u8>),
    /// An `Error` frame's body.
    Refused(ErrorBody),
}

/// The gate's requests on channel 0 of the module connection, matched to their answers by
/// correlation id. Before the serve loop runs, [`wait_for_settings`] reads the answers itself;
/// once the loop runs, it hands each frame here first ([`Gate::on_frame`]).
struct Calls {
    next_corr: AtomicU64,
    pending: Mutex<HashMap<u64, oneshot::Sender<Reply>>>,
    /// The serve loop's outgoing queue, once it runs.
    writer: Mutex<Option<broca_diagnostics::Sender<Frame>>>,
    ver: u8,
}

impl Calls {
    fn new(ver: u8) -> Self {
        Self {
            next_corr: AtomicU64::new(FIRST_GATE_CORR),
            pending: Mutex::new(HashMap::new()),
            writer: Mutex::new(None),
            ver,
        }
    }

    /// A request frame for `body` and the receiver its answer arrives on.
    fn begin(
        &self,
        body: &impl Serialize,
    ) -> Result<(Frame, oneshot::Receiver<Reply>), ServeIoError> {
        let corr = self.next_corr.fetch_add(1, Ordering::Relaxed);
        let body = serde_json::to_vec(body).map_err(|error| ServeIoError(error.to_string()))?;
        let frame = Frame::build_with_version(
            self.ver,
            FrameType::Request,
            control_flags(),
            0,
            0,
            corr,
            body,
        )
        .map_err(|error| ServeIoError(error.to_string()))?;
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(corr, tx);
        Ok((frame, rx))
    }

    /// Takes `frame` when it answers one of the gate's requests.
    fn on_frame(&self, frame: &Frame) -> bool {
        if frame.header.channel != 0
            || !matches!(frame.header.ty, FrameType::Response | FrameType::Error)
        {
            return false;
        }
        let Some(waiter) = self.pending.lock().unwrap().remove(&frame.header.corr) else {
            return false;
        };
        let reply = if frame.header.ty == FrameType::Response {
            Reply::Done(frame.body.clone())
        } else {
            Reply::Refused(
                serde_json::from_slice(&frame.body).unwrap_or_else(|_| ErrorBody {
                    code: "invalid_reply".into(),
                    message: "the daemon's refusal could not be read".into(),
                    detail: None,
                }),
            )
        };
        let _ = waiter.send(reply);
        true
    }

    /// Sends `body` through the serve loop's queue and waits for its answer.
    async fn call(&self, body: &impl Serialize) -> Result<Reply, ServeIoError> {
        let writer = self.writer.lock().unwrap().clone().ok_or_else(|| {
            ServeIoError("the settings gate's request has no connection to go out on".into())
        })?;
        let (frame, rx) = self.begin(body)?;
        let corr = frame.header.corr;
        if writer.send(frame).await.is_err() {
            self.pending.lock().unwrap().remove(&corr);
            return Err(ServeIoError("egress channel closed".into()));
        }
        match tokio::time::timeout(CALL_TIMEOUT, rx).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_)) => Err(ServeIoError(
                "the connection closed before the daemon answered".into(),
            )),
            Err(_) => {
                self.pending.lock().unwrap().remove(&corr);
                Err(ServeIoError("the daemon did not answer within 30 s".into()))
            }
        }
    }
}

fn control_flags() -> Flags {
    Flags::new(false, Priority::Passive, false)
}

/// The settings view broca was started from.
struct Applied {
    effective: Value,
    generation: u64,
    attempt: Option<u64>,
}

/// broca's side of the settings gate on one module connection.
pub struct Gate {
    calls: Calls,
    schema: &'static Schema,
    /// The view broca runs on; `None` when the daemon has no gate and broca read its file.
    applied: Mutex<Option<Applied>>,
}

/// How [`wait_for_settings`] ended.
pub enum GateStart {
    /// A view broca can build from: start from these settings, then serve through the gate.
    Settings(Settings, Arc<Gate>),
    /// The daemon ended the connection, or asked broca to stop, before any view arrived.
    Stopped(crate::ServeEnd),
}

/// Why broca can't start yet, as a probe reports it.
#[derive(Clone, Debug, PartialEq)]
enum Waiting {
    /// No answer to the declaration or the request for the view yet.
    Asking,
    /// The daemon has no settings broca may use yet (`config_gated`, or no view).
    NotApproved,
    /// The view the daemon offers fails a check broca makes before building from it.
    Refused(String),
    /// The daemon refused a request for a reason other than the two above.
    Failed(String),
}

impl Waiting {
    fn detail(&self) -> String {
        match self {
            Self::Asking => WAITING_FOR_SETTINGS.into(),
            Self::NotApproved => format!("{WAITING_FOR_SETTINGS}: none are approved yet"),
            Self::Refused(path) => {
                format!("{WAITING_FOR_SETTINGS}: the offered view is refused at {path}")
            }
            Self::Failed(code) => format!("{WAITING_FOR_SETTINGS}: the daemon refused ({code})"),
        }
    }
}

/// Declare broca's settings and wait for a view it can start from, on a module connection
/// that has just registered unready. `user_file` is the path the declaration names for
/// `broca.jsonc`. Every frame that arrives meanwhile is answered here; nothing in broca is built
/// yet, so none of them can reach a session.
pub async fn wait_for_settings<R, W>(
    reader: &mut R,
    writer: &mut W,
    negotiated_ver: u8,
    user_file: &str,
) -> Result<GateStart, ServeIoError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let schema = super::schema();
    let calls = Calls::new(negotiated_ver);
    let declare =
        ConfigDeclareRequest::new(super::schema_document().clone(), user_file, Vec::new());
    let (frame, mut declared) = calls.begin(&declare)?;
    write(writer, frame).await?;
    let mut asked: Option<oneshot::Receiver<Reply>> = None;
    let mut waiting = Waiting::Asking;
    loop {
        let frame = match read_frame(reader).await {
            Ok(Some(frame)) => frame,
            Ok(None) => return Ok(GateStart::Stopped(crate::ServeEnd::Eof)),
            Err(FrameIoError::Io(error)) if crate::serve::peer_went_away(error.kind()) => {
                return Ok(GateStart::Stopped(crate::ServeEnd::PeerReset))
            }
            Err(error) => return Err(ServeIoError(error.to_string())),
        };
        if calls.on_frame(&frame) {
            if let Ok(reply) = declared.try_recv() {
                match reply {
                    Reply::Done(_) => {
                        let (frame, rx) = calls.begin(&ConfigGetRequest::new())?;
                        write(writer, frame).await?;
                        asked = Some(rx);
                    }
                    Reply::Refused(error) => {
                        tracing::error!(target: "config", code = %error.code, message = %error.message,
                            "the daemon refused broca's settings declaration; broca stays unready");
                        waiting = Waiting::Failed(error.code);
                    }
                }
            }
            let Some(reply) = asked.as_mut().and_then(|rx| rx.try_recv().ok()) else {
                continue;
            };
            asked = None;
            match view_from(reply) {
                Ok(view) => match start_settings(&view) {
                    Ok(settings) => {
                        tracing::info!(target: "config", generation = view.generation,
                            status = view.status.as_str(), "starting from the daemon's settings");
                        crate::transport_config::set_managed(force_https(&view.effective));
                        let gate = Gate {
                            calls,
                            schema,
                            applied: Mutex::new(Some(Applied {
                                effective: view.effective.clone(),
                                generation: view.generation,
                                attempt: view.attempt,
                            })),
                        };
                        return Ok(GateStart::Settings(settings, Arc::new(gate)));
                    }
                    Err(path) => {
                        tracing::warn!(target: "config", generation = view.generation, %path,
                            "the daemon's settings view is refused; broca stays unready");
                        if let Some(attempt) = view.attempt {
                            let nack = ConfigAppliedRequest::new(
                                view.generation,
                                attempt,
                                ConfigApplyResult::Nack,
                                vec![format!("refused at {path}")],
                            );
                            let (frame, _answer) = calls.begin(&nack)?;
                            write(writer, frame).await?;
                        }
                        waiting = Waiting::Refused(path);
                    }
                },
                Err(why) => waiting = why,
            }
            continue;
        }
        match (frame.header.ty, frame.header.channel) {
            (FrameType::Ping, 0) => {
                let pong = Frame::build_with_version(
                    frame.header.ver,
                    FrameType::Pong,
                    frame.header.flags,
                    0,
                    0,
                    frame.header.corr,
                    Vec::new(),
                )
                .map_err(|error| ServeIoError(error.to_string()))?;
                write(writer, pong).await?;
            }
            (FrameType::Goodbye, 0) => return Ok(GateStart::Stopped(crate::ServeEnd::Goodbye)),
            (FrameType::Push, 0) => {
                if op(&frame.body).as_deref() == Some(CONFIG_CHANGED_OP) {
                    if asked.is_none() && waiting != Waiting::Asking {
                        let (frame, rx) = calls.begin(&ConfigGetRequest::new())?;
                        write(writer, frame).await?;
                        asked = Some(rx);
                    }
                } else if let Ok(ModuleControlCommand::Draining { reason, .. }) =
                    serde_json::from_slice(&frame.body)
                {
                    tracing::info!(target: "lifecycle", ?reason,
                        "module draining before broca's settings arrived; nothing to stop");
                    return Ok(GateStart::Stopped(crate::ServeEnd::Goodbye));
                }
            }
            (FrameType::Request, 0) => {
                let mut probe = false;
                let answer = if op(&frame.body).as_deref() == Some(CONFIG_REVIEW_OP) {
                    review_answer(schema, &frame)?
                } else {
                    match serde_json::from_slice::<ModuleControlRequest>(&frame.body) {
                        Ok(ModuleControlRequest::HealthCheck {}) => {
                            probe = true;
                            response(
                                &frame,
                                &ModuleControlResponse::HealthCheck {
                                    status: HealthStatus::Degraded,
                                    detail: Some(waiting.detail()),
                                    metrics: None,
                                },
                            )?
                        }
                        _ => warming(&frame)?,
                    }
                };
                write(writer, answer).await?;
                // A probe that finds broca still refused or failed asks again, so a refusal the
                // daemon lifts without telling broca doesn't keep it waiting.
                if probe
                    && asked.is_none()
                    && matches!(waiting, Waiting::Failed(_) | Waiting::NotApproved)
                {
                    let (frame, rx) = calls.begin(&ConfigGetRequest::new())?;
                    write(writer, frame).await?;
                    asked = Some(rx);
                }
            }
            _ => {}
        }
    }
}

/// The view a `config.get` answer gives broca to start from: its pending view if it has one,
/// else its active one. A view in a state the contract forbids activating is not one.
fn view_from(reply: Reply) -> Result<ConfigView, Waiting> {
    let body = match reply {
        Reply::Done(body) => body,
        Reply::Refused(error) if error.code == subc_protocol::error_codes::CONFIG_GATED => {
            return Err(Waiting::NotApproved)
        }
        Reply::Refused(error) => return Err(Waiting::Failed(error.code)),
    };
    let reply: ConfigGetReply =
        serde_json::from_slice(&body).map_err(|_| Waiting::Failed("invalid_reply".into()))?;
    let view = reply.pending.or(reply.active).ok_or(Waiting::NotApproved)?;
    match view.status {
        ConfigStatus::Active | ConfigStatus::Pending | ConfigStatus::Unstamped => Ok(view),
        _ => Err(Waiting::NotApproved),
    }
}

/// The settings `view` gives, or the key of the first section broca would fail to build from.
fn start_settings(view: &ConfigView) -> Result<Settings, String> {
    let settings = Settings::from_view(&view.effective, "managed settings");
    match super::review::refused_key(&settings, &view.effective) {
        Some(key) => Err(key),
        None => Ok(settings),
    }
}

impl Gate {
    /// The gate for a connection that registered unready on a daemon with no settings gate:
    /// broca read its file, and only becomes ready through it.
    pub fn ungated(negotiated_ver: u8) -> Arc<Self> {
        Arc::new(Self {
            calls: Calls::new(negotiated_ver),
            schema: super::schema(),
            applied: Mutex::new(None),
        })
    }

    /// Whether `frame` is the daemon saying broca's settings changed.
    pub fn is_changed(&self, frame: &Frame) -> bool {
        frame.header.channel == 0 && op(&frame.body).as_deref() == Some(CONFIG_CHANGED_OP)
    }

    /// Takes `frame` when it answers one of the gate's own requests.
    pub fn on_frame(&self, frame: &Frame) -> bool {
        self.calls.on_frame(frame)
    }

    /// broca's answer to a `config.review` request, or `None` for any other channel-0 request.
    pub fn review(&self, frame: &Frame) -> Option<Result<Frame, ServeIoError>> {
        (op(&frame.body).as_deref() == Some(CONFIG_REVIEW_OP))
            .then(|| review_answer(self.schema, frame))
    }

    /// Marks broca ready with its real provider roles, then acknowledges the view it started
    /// from. Runs once the serve loop takes frames, so the daemon's answers reach it.
    pub async fn finish_start(
        &self,
        writer: broca_diagnostics::Sender<Frame>,
        provides: Vec<subc_protocol::manifest::ProviderRole>,
    ) -> Result<(), ServeIoError> {
        *self.calls.writer.lock().unwrap() = Some(writer);
        let update = ModuleControlRequestFromModule::CatalogUpdate {
            provides,
            capabilities: None,
            ready: Some(true),
        };
        if let Reply::Refused(error) = self.calls.call(&update).await? {
            return Err(ServeIoError(format!(
                "the daemon refused broca's readiness: {} — {}",
                error.code, error.message
            )));
        }
        let applied = self
            .applied
            .lock()
            .unwrap()
            .as_ref()
            .map(|applied| (applied.generation, applied.attempt));
        match applied {
            Some((generation, attempt)) => {
                if let Some(attempt) = attempt {
                    self.acknowledge(generation, attempt).await?;
                }
                tracing::info!(target: "config", generation, "ready on the daemon's settings");
            }
            None => tracing::info!(target: "config", "ready on broca.jsonc"),
        }
        Ok(())
    }

    /// Reads the view the daemon now offers after a `config.changed` push, and applies it when
    /// it changes only fields applied in place. Nothing that goes wrong here ends the
    /// connection: broca keeps the settings it runs on, and says so in its log.
    pub async fn on_changed(&self, push: &[u8]) -> Result<(), ServeIoError> {
        if let Err(error) = self.apply_changed(push).await {
            tracing::warn!(target: "config", %error,
                "the changed settings could not be read; broca keeps its settings");
        }
        Ok(())
    }

    async fn apply_changed(&self, push: &[u8]) -> Result<(), ServeIoError> {
        let Ok(changed) = serde_json::from_slice::<ConfigChanged>(push) else {
            return Ok(());
        };
        let newer = |generation: u64| {
            self.applied
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|applied| generation > applied.generation)
        };
        if !newer(changed.generation) {
            return Ok(());
        }
        let view = match view_from(self.calls.call(&ConfigGetRequest::new()).await?) {
            Ok(view) => view,
            Err(why) => {
                tracing::warn!(target: "config", generation = changed.generation, ?why,
                    "the changed settings view could not be read; broca keeps its settings");
                return Ok(());
            }
        };
        let restart = {
            let applied = self.applied.lock().unwrap();
            let Some(applied) = applied
                .as_ref()
                .filter(|applied| view.generation > applied.generation)
            else {
                return Ok(());
            };
            restart_fields_differ(self.schema, &applied.effective, &view.effective)
        };
        if restart {
            // The daemon restarts broca once it approves this view; the new process reads it.
            tracing::info!(target: "config", generation = view.generation,
                "the changed settings take effect at broca's next start");
            return Ok(());
        }
        crate::transport_config::set_managed(force_https(&view.effective));
        *self.applied.lock().unwrap() = Some(Applied {
            effective: view.effective.clone(),
            generation: view.generation,
            attempt: view.attempt,
        });
        if let Some(attempt) = view.attempt {
            self.acknowledge(view.generation, attempt).await?;
        }
        tracing::info!(target: "config", generation = view.generation, "applied changed settings in place");
        Ok(())
    }

    async fn acknowledge(&self, generation: u64, attempt: u64) -> Result<(), ServeIoError> {
        let ack =
            ConfigAppliedRequest::new(generation, attempt, ConfigApplyResult::Ack, Vec::new());
        match self.calls.call(&ack).await? {
            Reply::Done(_) => Ok(()),
            Reply::Refused(error) => {
                // A stale or duplicate outcome is refused and changes nothing on either side.
                tracing::warn!(target: "config", generation, code = %error.code,
                    "the daemon refused broca's settings acknowledgement");
                Ok(())
            }
        }
    }
}

/// Whether any field read only at start differs between `old` and `new`.
fn restart_fields_differ(schema: &Schema, old: &Value, new: &Value) -> bool {
    super::review::fields(schema)
        .into_iter()
        .filter(|(_, kind)| *kind != subc_config::ApplyKind::Live)
        .any(|(pointer, _)| old.pointer(&pointer) != new.pointer(&pointer))
}

fn force_https(effective: &Value) -> bool {
    effective
        .get("chatgpt_force_https")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn review_answer(schema: &Schema, frame: &Frame) -> Result<Frame, ServeIoError> {
    match serde_json::from_slice::<ConfigReviewRequest>(&frame.body) {
        Ok(request) => response(frame, &super::review::review_request(schema, &request)),
        Err(error) => refusal(frame, "invalid_request", &error.to_string()),
    }
}

/// The refusal a route bind, or any other request broca can't take yet, gets while it waits.
fn warming(frame: &Frame) -> Result<Frame, ServeIoError> {
    refusal(
        frame,
        subc_protocol::error_codes::MODULE_WARMING,
        WAITING_FOR_SETTINGS,
    )
}

fn op(body: &[u8]) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct Op {
        op: String,
    }
    serde_json::from_slice::<Op>(body).ok().map(|op| op.op)
}

fn response(request: &Frame, body: &impl Serialize) -> Result<Frame, ServeIoError> {
    let body = serde_json::to_vec(body).map_err(|error| ServeIoError(error.to_string()))?;
    reply_frame(request, FrameType::Response, body)
}

fn refusal(request: &Frame, code: &str, message: &str) -> Result<Frame, ServeIoError> {
    let body = serde_json::to_vec(&ErrorBody {
        code: code.into(),
        message: message.into(),
        detail: None,
    })
    .map_err(|error| ServeIoError(error.to_string()))?;
    reply_frame(request, FrameType::Error, body)
}

fn reply_frame(request: &Frame, ty: FrameType, body: Vec<u8>) -> Result<Frame, ServeIoError> {
    Frame::build_with_version(
        request.header.ver,
        ty,
        control_flags(),
        0,
        0,
        request.header.corr,
        body,
    )
    .map_err(|error| ServeIoError(error.to_string()))
}

async fn write<W: AsyncWrite + Unpin>(writer: &mut W, frame: Frame) -> Result<(), ServeIoError> {
    write_frame(writer, &frame)
        .await
        .map_err(|error| ServeIoError(error.to_string()))?;
    writer
        .flush()
        .await
        .map_err(|error| ServeIoError(error.to_string()))
}

#[cfg(test)]
#[path = "gate_tests.rs"]
mod tests;
