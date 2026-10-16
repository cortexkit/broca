//! ENGRAM's session-door client. Admits a frozen WAL as hex-encoded opaque bytes and reads only
//! sealed cuts, whose pages include the stream's sealed SHA-256 digest. Requests carry a session
//! reference and byte offsets; the caller principal determines the stream, never a request field.
//! Wire provenance: ENGRAM commit 50d65fc70ccc3e0082eaf1bde882ffaf7b2c2bcd on `integrate/session-door`.

use std::{fmt, sync::Arc, time::Duration};

use broca_protocol::BindIdentity;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};
use tokio::{
    sync::Mutex,
    time::{sleep_until, timeout_at, Instant},
};

use crate::connection::{RouteHandle, RouteOpenRetry, SubcConnection};

/// The door caps admits and reads at 1 MiB raw bytes: hex encoding in JSON doubles
/// the payload, which must fit the daemon's 4 MiB frame including its envelope.
pub const MAX_CALL_BYTES: usize = 1_048_576;
const FIRST_BACKOFF: Duration = Duration::from_millis(50);
const MAX_BACKOFF: Duration = Duration::from_secs(1);

/// An unavailable door, retaining the door's refusal code when it supplied one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionDoorError {
    pub door_code: Option<String>,
    pub message: String,
}

impl fmt::Display for SessionDoorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "session-door unavailable")?;
        if let Some(code) = &self.door_code {
            write!(f, " ({code})")?;
        }
        write!(f, ": {}", self.message)
    }
}

impl std::error::Error for SessionDoorError {}

fn unavailable(message: impl Into<String>) -> SessionDoorError {
    SessionDoorError {
        door_code: None,
        message: message.into(),
    }
}

fn refused(error: &subc_protocol::ErrorBody) -> SessionDoorError {
    SessionDoorError {
        door_code: Some(error.code.clone()),
        message: format!("session-door refused: {}", error.message),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DoorCoverage {
    pub durable_offset: u64,
    pub cloud_offset: u64,
}

/// A cut-bound page, including the sealed digest over the entire `[0, stream_end)` stream.
/// The importer must hash all pages and compare with this digest before staging a WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoorRead {
    pub bytes: Vec<u8>,
    pub from_offset: u64,
    pub end_offset: u64,
    pub stream_end: u64,
    pub digest: String,
}

#[derive(Deserialize)]
struct Reply<T> {
    result: T,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReadReply {
    bytes: String,
    from_offset: u64,
    end_offset: u64,
    stream_end: u64,
    digest: String,
}

/// Uses a connection authenticated as the daemon-launched broca module. The daemon stamps that
/// principal on the route bind, and ENGRAM derives `reserved:broca` from it, not from parameters.
/// Each call has a caller-supplied deadline, including route opening and retries.
pub struct SessionDoorClient {
    conn: Arc<SubcConnection>,
    identity: BindIdentity,
    route: Mutex<Option<RouteHandle>>,
}

impl SessionDoorClient {
    pub fn new(conn: Arc<SubcConnection>, identity: BindIdentity) -> Self {
        Self {
            conn,
            identity,
            route: Mutex::new(None),
        }
    }

    /// Ask the door to attempt cloud upload and report the locally durable byte end and the byte
    /// end already confirmed in the cloud. The latter can lag behind local admission.
    pub async fn cover(
        &self,
        session_ref: &str,
        deadline: Instant,
    ) -> Result<DoorCoverage, SessionDoorError> {
        let answer = self
            .call(
                "session.cover",
                json!({"sessionRef": session_ref}),
                deadline,
            )
            .await?
            .map_err(|error| refused(&error))?;
        decode_coverage(answer)
    }

    /// Admit a frozen WAL's complete `[0, endOffset)` bytes, resuming from a fresh cover.
    /// Already durable bytes are not resent. This never waits for cloud coverage or seals a cut.
    pub async fn admit(
        &self,
        session_ref: &str,
        wal: &[u8],
        deadline: Instant,
    ) -> Result<DoorCoverage, SessionDoorError> {
        let mut coverage = self.cover(session_ref, deadline).await?;
        let mut offset = checked_offset(coverage.durable_offset, wal.len())?;
        let mut mismatch_backoff = FIRST_BACKOFF;
        while offset < wal.len() {
            let end = offset.saturating_add(MAX_CALL_BYTES).min(wal.len());
            let answer = self
                .call(
                    "session.admit",
                    json!({
                        "sessionRef": session_ref,
                        "fromOffset": offset as u64,
                        "bytes": encode_hex(&wal[offset..end]),
                    }),
                    deadline,
                )
                .await?;
            match answer {
                Ok(value) => {
                    coverage = decode_coverage(value)?;
                    if coverage.durable_offset != end as u64 {
                        return Err(unavailable(
                            "session.admit returned an unexpected durableOffset",
                        ));
                    }
                    offset = end;
                    mismatch_backoff = FIRST_BACKOFF;
                }
                Err(error) if error.code == "offset_mismatch" => {
                    let expected = error
                        .detail
                        .as_ref()
                        .and_then(|detail| detail.get("expectedOffset"))
                        .and_then(Value::as_u64)
                        .ok_or_else(|| refused(&error))?;
                    coverage = self.cover(session_ref, deadline).await?;
                    // Cover can advance after the refusal, but cannot precede its durable end.
                    if coverage.durable_offset < expected {
                        return Err(refused(&error));
                    }
                    let next = checked_offset(coverage.durable_offset, wal.len())?;
                    if next == offset {
                        backoff(deadline, &mut mismatch_backoff, &error).await?;
                    }
                    offset = next;
                }
                Err(error) => return Err(refused(&error)),
            }
        }
        Ok(coverage)
    }

    /// Read one page of a sealed cut. An unbound read or a missing sealed digest is never accepted.
    pub async fn read(
        &self,
        session_ref: &str,
        from_offset: u64,
        limit: usize,
        cut_id: &str,
        deadline: Instant,
    ) -> Result<DoorRead, SessionDoorError> {
        if limit == 0 || limit > MAX_CALL_BYTES {
            return Err(unavailable(format!(
                "session.read limit must be in 1..={MAX_CALL_BYTES}"
            )));
        }
        let answer = self.call("session.read", json!({
            "sessionRef": session_ref, "fromOffset": from_offset, "limit": limit, "cutId": cut_id,
        }), deadline).await?.map_err(|error| refused(&error))?;
        let reply: ReadReply = decode(answer)?;
        if reply.digest.len() != 64
            || !reply
                .digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(unavailable(
                "session.read returned an invalid sealed digest",
            ));
        }
        let bytes = decode_hex(&reply.bytes)
            .ok_or_else(|| unavailable("session.read returned invalid hex bytes"))?;
        if reply.from_offset != from_offset
            || from_offset > reply.stream_end
            || reply.end_offset
                != from_offset
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| unavailable("session.read offset overflow"))?
            || bytes.len() as u64 != (reply.stream_end - from_offset).min(limit as u64)
        {
            return Err(unavailable(
                "session.read returned inconsistent byte range offsets",
            ));
        }
        Ok(DoorRead {
            bytes,
            from_offset,
            end_offset: reply.end_offset,
            stream_end: reply.stream_end,
            digest: reply.digest,
        })
    }

    async fn call(
        &self,
        method: &str,
        params: Value,
        deadline: Instant,
    ) -> Result<Result<Value, subc_protocol::ErrorBody>, SessionDoorError> {
        if self.conn.consumer_module_id() != Some("broca") {
            return Err(unavailable(
                "session-door requires broca's supervised consumer identity",
            ));
        }
        let body = serde_json::to_vec(&json!({"method": method, "params": params}))
            .map_err(|error| unavailable(format!("encode session-door request: {error}")))?;
        let mut delay = FIRST_BACKOFF;
        loop {
            let answer = self.exchange(body.clone(), deadline).await?;
            // Oversize calls are a client bug, not a transient lease failure.
            // In particular, record_too_large must never be retried.
            match &answer {
                Err(error)
                    if error.code == "lease_unavailable" || error.code == "lease_acquiring" =>
                {
                    backoff(deadline, &mut delay, error).await?;
                }
                _ => return Ok(answer),
            }
        }
    }

    async fn exchange(
        &self,
        body: Vec<u8>,
        deadline: Instant,
    ) -> Result<Result<Value, subc_protocol::ErrorBody>, SessionDoorError> {
        if Instant::now() >= deadline {
            return Err(unavailable("session-door request deadline expired"));
        }
        timeout_at(deadline, async {
            let mut cached = self.route.lock().await;
            let mut lease = RouteLease {
                conn: self.conn.clone(),
                route: cached.take(),
            };
            if lease
                .route
                .is_some_and(|route| !self.conn.route_is_open(&route))
            {
                lease.route = None;
            }
            if lease.route.is_none() {
                lease.route = Some(
                    self.conn
                        .route_open_internal_service_within(
                            "engram",
                            "session-door",
                            &self.identity,
                            RouteOpenRetry {
                                deadline,
                                max_interval: MAX_BACKOFF,
                            },
                        )
                        .await
                        .map_err(|error| unavailable(error.to_string()))?,
                );
            }
            let answer = self
                .conn
                .route_unary_reply(
                    lease.route.as_ref().unwrap(),
                    body,
                    deadline.saturating_duration_since(Instant::now()),
                )
                .await
                .map_err(|error| unavailable(error.to_string()))?;
            *cached = lease.route.take();
            match answer {
                Ok(bytes) => {
                    let reply: Reply<Value> = serde_json::from_slice(&bytes).map_err(|error| {
                        unavailable(format!("decode session-door result envelope: {error}"))
                    })?;
                    Ok(Ok(reply.result))
                }
                Err(error) => Ok(Err(error)),
            }
        })
        .await
        .unwrap_or_else(|_| Err(unavailable("session-door request deadline expired")))
    }
}

impl Drop for SessionDoorClient {
    fn drop(&mut self) {
        drop(RouteLease {
            conn: self.conn.clone(),
            route: self.route.get_mut().take(),
        });
    }
}

// A cancelled or timed-out call must not leave a possibly live route in the cache.
struct RouteLease {
    conn: Arc<SubcConnection>,
    route: Option<RouteHandle>,
}

impl Drop for RouteLease {
    fn drop(&mut self) {
        if let (Some(route), Ok(runtime)) =
            (self.route.take(), tokio::runtime::Handle::try_current())
        {
            let conn = self.conn.clone();
            runtime.spawn(async move {
                let _ = conn.route_close(&route).await;
            });
        }
    }
}

async fn backoff(
    deadline: Instant,
    delay: &mut Duration,
    error: &subc_protocol::ErrorBody,
) -> Result<(), SessionDoorError> {
    let wake = (Instant::now() + *delay).min(deadline);
    sleep_until(wake).await;
    if Instant::now() >= deadline {
        let mut exhausted = refused(error);
        exhausted.message = format!("session-door retry deadline expired after {}", error.code);
        return Err(exhausted);
    }
    *delay = delay.saturating_mul(2).min(MAX_BACKOFF);
    Ok(())
}

fn decode<T: DeserializeOwned>(value: Value) -> Result<T, SessionDoorError> {
    serde_json::from_value(value)
        .map_err(|error| unavailable(format!("decode session-door reply: {error}")))
}

fn decode_coverage(value: Value) -> Result<DoorCoverage, SessionDoorError> {
    let coverage: DoorCoverage = decode(value)?;
    if coverage.cloud_offset > coverage.durable_offset {
        return Err(unavailable(
            "session-door cloudOffset exceeds durableOffset",
        ));
    }
    Ok(coverage)
}

fn checked_offset(offset: u64, end: usize) -> Result<usize, SessionDoorError> {
    let offset = usize::try_from(offset)
        .map_err(|_| unavailable("session-door durableOffset exceeds frozen WAL endOffset"))?;
    if offset > end {
        return Err(unavailable(
            "session-door durableOffset exceeds frozen WAL endOffset",
        ));
    }
    Ok(offset)
}

fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 15) as usize] as char);
    }
    result
}

fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    let (pairs, remainder) = hex.as_bytes().as_chunks::<2>();
    if !remainder.is_empty() {
        return None;
    }
    pairs
        .iter()
        .map(|pair| Some(nibble(pair[0])?.checked_mul(16)? + nibble(pair[1])?))
        .collect()
}

fn nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}
