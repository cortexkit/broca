//! broca's module connection between registering and serving, and the claims minted over it.
//!
//! When broca registers over its registration channel (`broca_subc::registration`), every other
//! connection it opens to the daemon first asks for a claim with `claim.mint`, sent on this
//! connection. broca opens most of those connections while it builds itself, before the serve
//! loop reads this connection. So until the loop takes over ([`ModuleConnection::hand_over`]), a
//! mint writes its own request and reads frames until the daemon answers it, keeping every other
//! frame it reads (a health probe, a settings change) for the loop to read first. From the
//! hand-over on, mints go out through the serve loop, which hands their answers back.

use std::io::Cursor;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use broca_subc::registration::{ClaimMinter, DaemonStream, MintAnswer};
use subc_protocol::claim::ClaimMintRequest;
use subc_protocol::session::ModuleControlCommand;
use subc_protocol::{Frame, FrameType};
use subc_transport::{read_frame, write_frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt, Chain, ReadHalf, WriteHalf};

use crate::settings::gate::{Gate, Reply};

/// The reader the serve loop gets: the frames start-up kept, then the connection.
pub type ServeReader = Chain<Cursor<Vec<u8>>, ReadHalf<Box<dyn DaemonStream>>>;

/// broca's registered module connection, read by start-up mints until the serve loop takes it.
pub struct ModuleConnection {
    gate: Arc<Gate>,
    state: tokio::sync::Mutex<State>,
    startup_upgrade: AtomicBool,
}

// The serving generation can keep session state alive. Cold start does not own a serving
// generation yet; closing its startup wire lets the supervisor start it against the new daemon.
pub(crate) fn cold_start_upgrade(frame: &Frame) -> Option<&'static str> {
    if cfg!(any(target_os = "linux", target_os = "macos"))
        && frame.header.channel == 0
        && frame.header.ty == FrameType::Push
        && matches!(
            serde_json::from_slice::<ModuleControlCommand>(&frame.body),
            Ok(ModuleControlCommand::Upgrading { .. })
        )
    {
        tracing::warn!(target: "lifecycle", "daemon upgrade arrived during broca startup; the supervisor will restart broca");
        Some("daemon upgrade interrupted module startup")
    } else {
        None
    }
}

enum State {
    /// Before the serve loop: nobody else reads the connection.
    Startup {
        reader: ReadHalf<Box<dyn DaemonStream>>,
        writer: WriteHalf<Box<dyn DaemonStream>>,
        /// Frames read while waiting for a mint's answer, encoded, for the serve loop to read.
        kept: Vec<u8>,
    },
    /// The serve loop reads and writes the connection.
    Serving,
}

impl ModuleConnection {
    pub fn new(stream: Box<dyn DaemonStream>, gate: Arc<Gate>) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        Self {
            gate,
            startup_upgrade: AtomicBool::new(false),
            state: tokio::sync::Mutex::new(State::Startup {
                reader,
                writer,
                kept: Vec::new(),
            }),
        }
    }

    pub(crate) fn startup_interrupted(&self) -> bool {
        self.startup_upgrade.load(Ordering::Acquire)
    }

    /// Gives the connection to the serve loop: a reader that yields the frames start-up mints
    /// kept before the connection's own, and the writer. A mint still waiting for its answer
    /// finishes first. Panics if called twice: there is one serve loop per connection.
    pub async fn hand_over(&self) -> (ServeReader, WriteHalf<Box<dyn DaemonStream>>) {
        let mut state = self.state.lock().await;
        match std::mem::replace(&mut *state, State::Serving) {
            State::Startup {
                reader,
                writer,
                kept,
            } => (Cursor::new(kept).chain(reader), writer),
            State::Serving => panic!("the module connection was handed over twice"),
        }
    }

    /// One `claim.mint` before the serve loop runs: writes the request, then reads frames until
    /// the daemon answers it.
    async fn startup_call(
        &self,
        reader: &mut ReadHalf<Box<dyn DaemonStream>>,
        writer: &mut WriteHalf<Box<dyn DaemonStream>>,
        kept: &mut Vec<u8>,
        request: &ClaimMintRequest,
    ) -> Result<Reply, String> {
        let (frame, mut answer) = self
            .gate
            .begin_request(request)
            .map_err(|error| error.to_string())?;
        write_frame(writer, &frame)
            .await
            .map_err(|error| format!("claim.mint: {error}"))?;
        writer
            .flush()
            .await
            .map_err(|error| format!("claim.mint: {error}"))?;
        loop {
            if let Ok(reply) = answer.try_recv() {
                return Ok(reply);
            }
            let frame = read_frame(reader)
                .await
                .map_err(|error| format!("claim.mint: {error}"))?
                .ok_or_else(|| "claim.mint: the daemon closed broca's connection".to_string())?;
            if let Some(error) = cold_start_upgrade(&frame) {
                self.startup_upgrade.store(true, Ordering::Release);
                return Err(error.into());
            }
            if !self.gate.on_frame(&frame) {
                write_frame(kept, &frame)
                    .await
                    .map_err(|error| format!("claim.mint: keeping a frame: {error}"))?;
            }
        }
    }
}

#[async_trait]
impl ClaimMinter for ModuleConnection {
    async fn call(&self, request: &ClaimMintRequest) -> Result<MintAnswer, String> {
        if self.startup_interrupted() {
            return Err("daemon upgrade interrupted module startup".into());
        }
        let reply = {
            let mut state = self.state.lock().await;
            match &mut *state {
                State::Startup {
                    reader,
                    writer,
                    kept,
                } => Some(self.startup_call(reader, writer, kept, request).await?),
                State::Serving => None,
            }
        };
        let reply = match reply {
            Some(reply) => reply,
            None => self
                .gate
                .request(request)
                .await
                .map_err(|error| error.to_string())?,
        };
        Ok(match reply {
            Reply::Done(body) => MintAnswer::Done(body),
            Reply::Refused(error) => MintAnswer::Refused(error),
        })
    }
}

#[cfg(test)]
#[path = "module_connection_tests.rs"]
mod tests;
