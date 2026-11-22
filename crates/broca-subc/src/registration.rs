//! Registering over the daemon's registration channel, and binding broca's other daemon
//! connections to that registration with single-use claims.
//!
//! The daemon starts a module with a private stream that is already connected to it: on Unix a
//! socket named by `SUBC_REGISTRATION_FD`, on Windows a named pipe. broca takes that stream once,
//! first thing at start ([`adopt`]), runs the connection file's handshake on it and says HELLO
//! there without a launch nonce: the stream itself shows the daemon which spawn is speaking.
//!
//! Every other connection broca opens to the daemon (the tool, vault, catalog and feed
//! connections, and each reconnect of them) then presents a claim as its first frame after the
//! handshake ([`bind`]). A claim is a single-use token the daemon mints for this spawn over the
//! module connection, valid for 30 seconds. A connection bound that way carries no consumer
//! identity on its route opens: the daemon already knows it is broca's.
//!
//! A start with no channel (broca run outside the daemon, by a test rig or by hand) registers over
//! the connection file with its launch nonce, and its connections present that nonce as their
//! consumer identity on route opens. Nothing here applies to it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use subc_protocol::claim::{
    Claim, ClaimMintReply, ClaimMintRequest, ClaimPresentReply, ClaimPresentRequest, ClaimPurpose,
    ClaimRateLimitedDetail,
};
use subc_protocol::{error_codes, ErrorBody, Flags, Frame, FrameType, Priority};
use subc_transport::{read_frame, write_frame};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::connection::{ConnError, ConnResult};

/// A connected byte stream to the daemon: the registration channel, or a dialled socket.
pub trait DaemonStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> DaemonStream for T {}

/// Set once broca has taken its registration channel; never cleared.
static ADOPTED: AtomicBool = AtomicBool::new(false);

/// The module the daemon launched this process as, when it took its channel.
static MODULE_ID: OnceLock<String> = OnceLock::new();

/// Who sends `claim.mint` on the module connection. Replaced once, when the serve loop takes the
/// module connection over from start-up.
static MINTER: RwLock<Option<Arc<dyn ClaimMinter>>> = RwLock::new(None);

/// How long one claim may take to mint, rate-limit waits included.
const MINT_BUDGET: Duration = Duration::from_secs(30);

/// How long the daemon may take to answer a presented claim.
const PRESENT_DEADLINE: Duration = Duration::from_secs(10);

/// Takes the registration channel the daemon started broca with, or `None` when it gave none.
/// Call it before broca spawns any process: until the take, the descriptor can be inherited.
/// It logs nothing, since it runs before the logger exists; [`refusal`] says why a named channel
/// was not taken. An error means the channel could not be made a stream; broca must not start
/// then, and must never fall back to the connection file.
pub fn adopt() -> Result<Option<Box<dyn DaemonStream>>, String> {
    let channel = subc_os::registration_channel::take_registration_channel()
        .map_err(|error| format!("registration channel: {error}"))?;
    let Some(channel) = channel else {
        return Ok(None);
    };
    #[cfg(unix)]
    let stream: Box<dyn DaemonStream> = {
        let stream = channel.into_unix_stream();
        stream
            .set_nonblocking(true)
            .map_err(|error| format!("registration channel: {error}"))?;
        Box::new(
            tokio::net::UnixStream::from_std(stream)
                .map_err(|error| format!("registration channel: {error}"))?,
        )
    };
    #[cfg(windows)]
    let stream: Box<dyn DaemonStream> = Box::new(
        channel
            .into_tokio_pipe_client()
            .map_err(|error| format!("registration channel: {error}"))?,
    );
    if let Some(module_id) = std::env::var(subc_protocol::SUBC_MODULE_ID_ENV)
        .ok()
        .filter(|id| !id.is_empty())
    {
        let _ = MODULE_ID.set(module_id);
    }
    ADOPTED.store(true, Ordering::Release);
    Ok(Some(stream))
}

/// Why the environment named a registration channel that [`adopt`] did not take (a descriptor
/// that was not the daemon's socket, say), safe to log. `None` when none was named or it was
/// taken.
pub fn refusal() -> Option<String> {
    subc_os::registration_channel::registration_refusal().map(|refusal| refusal.to_string())
}

/// The module every connection of this process is bound to by its claim: the module the daemon
/// launched (`SUBC_MODULE_ID`), once broca took its registration channel. `None` before that, or
/// when broca registers with its launch nonce.
pub fn bound_module_id() -> Option<&'static str> {
    if !adopted() {
        return None;
    }
    MODULE_ID.get().map(String::as_str)
}

/// Whether broca registered over its registration channel, so every other connection it opens
/// presents a claim and no consumer identity.
pub fn adopted() -> bool {
    ADOPTED.load(Ordering::Acquire)
}

/// The daemon's answer to one `claim.mint` request.
#[derive(Debug)]
pub enum MintAnswer {
    /// A `Response` frame's body.
    Done(Vec<u8>),
    /// An `Error` frame's body.
    Refused(ErrorBody),
}

/// Sends one `claim.mint` request on the module connection and returns the daemon's answer.
/// [`mint`] retries rate limits around it; an implementation sends once and never reconnects.
#[async_trait]
pub trait ClaimMinter: Send + Sync {
    async fn call(&self, request: &ClaimMintRequest) -> Result<MintAnswer, String>;
}

/// Makes `minter` the one that mints claims from now on.
pub fn install_minter(minter: Arc<dyn ClaimMinter>) {
    *MINTER.write().unwrap_or_else(|e| e.into_inner()) = Some(minter);
}

/// Mints one claim for another connection of this spawn. The daemon's `claim_rate_limited`
/// delay is waited out within [`MINT_BUDGET`]; any other refusal is final for this connection.
pub async fn mint() -> Result<Claim, String> {
    let minter = MINTER
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .ok_or_else(|| "no claim minter: broca has not registered yet".to_string())?;
    mint_with(minter.as_ref()).await
}

async fn mint_with(minter: &dyn ClaimMinter) -> Result<Claim, String> {
    let request = ClaimMintRequest::new(ClaimPurpose::SelfConnection);
    let deadline = tokio::time::Instant::now() + MINT_BUDGET;
    loop {
        let answer = tokio::time::timeout_at(deadline, minter.call(&request))
            .await
            .map_err(|_| "claim.mint: no answer within 30 s".to_string())??;
        match answer {
            MintAnswer::Done(body) => {
                let reply: ClaimMintReply = serde_json::from_slice(&body)
                    .map_err(|error| format!("claim.mint: unreadable reply: {error}"))?;
                return Ok(reply.claim);
            }
            MintAnswer::Refused(error) if error.code == error_codes::CLAIM_RATE_LIMITED => {
                let wait = error
                    .detail
                    .and_then(|detail| {
                        serde_json::from_value::<ClaimRateLimitedDetail>(detail).ok()
                    })
                    .ok_or_else(|| "claim.mint: rate limited with no delay".to_string())?;
                tokio::time::timeout_at(
                    deadline,
                    tokio::time::sleep(Duration::from_millis(wait.retry_after_ms)),
                )
                .await
                .map_err(|_| "claim.mint: rate limited past 30 s".to_string())?;
            }
            MintAnswer::Refused(error) => {
                return Err(format!(
                    "claim.mint refused: {} — {}",
                    error.code, error.message
                ))
            }
        }
    }
}

/// Binds a connection broca just dialled and authenticated to its registration: mints a claim
/// and presents it as the connection's first frame. Does nothing when broca has no registration
/// channel. A refused claim fails the connection, retryable refusals included; it never goes on
/// unbound.
pub async fn bind<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) -> ConnResult<()> {
    if !adopted() {
        return Ok(());
    }
    let claim = mint()
        .await
        .map_err(|error| ConnError::Transport(error.to_string()))?;
    present(stream, claim).await
}

/// Presents `claim` on a fresh connection and reads the daemon's acceptance.
pub async fn present<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    claim: Claim,
) -> ConnResult<()> {
    let body = serde_json::to_vec(&ClaimPresentRequest::new(claim))
        .map_err(|error| ConnError::Protocol(error.to_string()))?;
    // The first frame on the connection, so correlation id 1, as the daemon expects.
    let frame = Frame::build(FrameType::Request, control_flags(), 0, 0, 1, body)
        .map_err(|error| ConnError::Protocol(error.to_string()))?;
    let reply = tokio::time::timeout(PRESENT_DEADLINE, async {
        write_frame(stream, &frame)
            .await
            .map_err(|error| ConnError::Transport(format!("claim.present: {error}")))?;
        read_frame(stream)
            .await
            .map_err(|error| ConnError::Transport(format!("claim.present: {error}")))?
            .ok_or_else(|| {
                ConnError::Transport("claim.present: the daemon closed the connection".into())
            })
    })
    .await
    .map_err(|_| ConnError::Transport("claim.present: no answer within 10 s".into()))??;
    if reply.header.channel != 0 || reply.header.corr != 1 {
        return Err(ConnError::Protocol(
            "claim.present: the reply answers another request".into(),
        ));
    }
    match reply.header.ty {
        FrameType::Response => serde_json::from_slice::<ClaimPresentReply>(&reply.body)
            .map(|_| ())
            .map_err(|error| ConnError::Protocol(format!("claim.present: {error}"))),
        FrameType::Error => {
            let error: ErrorBody = serde_json::from_slice(&reply.body)
                .map_err(|error| ConnError::Protocol(format!("claim.present: {error}")))?;
            Err(ConnError::Transport(format!(
                "claim.present refused: {} — {}",
                error.code, error.message
            )))
        }
        ty => Err(ConnError::Protocol(format!(
            "claim.present: unexpected {ty:?} frame"
        ))),
    }
}

fn control_flags() -> Flags {
    Flags::new(false, Priority::Passive, false)
}

#[cfg(test)]
#[path = "registration_tests.rs"]
mod tests;
