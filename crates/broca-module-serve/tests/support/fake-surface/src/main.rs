//! A stand-in management surface for the private-content tests.
//!
//! The `llm.classify/v1` route asks cerebellum (`private_content.query`) and Basal
//! (`flow.private_content_providers`) before it may call a provider, so a test needs both to
//! answer. `fake-aft-stub` declares only the tool-provider role, so the daemon refuses a
//! `route.open` aimed at its management surface before any operation is dispatched. This binary
//! is a module that does declare a management surface, and serves those two operations.
//!
//! Configuration, from the environment and the command line:
//!
//! - `SUBC_CONNECTION`: the daemon connection file this process connects to.
//! - `FAKE_AFT_MODULE_ID`, `FAKE_AFT_LAUNCH_NONCE_PATH`: the module identity, taken exactly as
//!   `fake-aft-stub` takes them.
//! - `SURFACE_FIXTURES`: a file holding one fixture or an array of them, each
//!   `{"match": {"method", "scope_ref"?}, "stdout"}`. A request whose method matches (and whose
//!   `scope_ref` equals the fixture's, when it names one) is answered with that fixture's
//!   `stdout`, decoded as JSON and wrapped as `{"result": ...}`; a fixture whose `stdout`
//!   decodes as an `ErrorBody` refuses the request with it instead. A `private_content.query`
//!   with no matching fixture is answered marked, with no sources, when the file names a
//!   `person_browser` fixture, and unmarked when it does not; any other unmatched operation is
//!   refused `unknown_method`.
//! - `SURFACE_REQUESTS`: a file this process appends one line to per request, so a test can
//!   count how often broca asked.

use std::io::Write;
use std::path::PathBuf;

use subc_protocol::manifest::{
    Concurrency, IdentityScope, ManagementOperation, ManagementOperationKind, ModuleManifest,
    ProviderRole,
};
use subc_protocol::{ErrorBody, Flags, Frame, FrameType, ModuleHelloBody, Priority, PROTOCOL_VERSION};
use subc_transport::{authenticate_client, read_frame, write_frame};

const SURFACED_OPS: [&str; 2] = ["private_content.query", "flow.private_content_providers"];

fn main() {
    if let Some(()) = relaunch() {
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build the stand-in runtime");
    if let Err(error) = runtime.block_on(serve()) {
        eprintln!("fake-surface: {error}");
        let args: Vec<String> = std::env::args().collect();
        if args.iter().any(|arg| arg.starts_with("--SURFACE_REQUESTS=")) {
            let _ = append(
                &PathBuf::from(stand_in_value("SURFACE_REQUESTS")),
                &format!("failed: {error}\n"),
            );
        }
        std::process::exit(1);
    }
}

/// The module id and launch nonce this process registers under. A rig may name them directly
/// (`FAKE_AFT_MODULE_ID` and `FAKE_AFT_LAUNCH_NONCE_PATH`, as `fake-aft-stub` takes them), or
/// the daemon may have launched this process as a module named on the command line.
fn identity() -> Result<(String, String), String> {
    if let Ok(module_id) = std::env::var("FAKE_AFT_MODULE_ID") {
        let nonce = std::fs::read_to_string(env("FAKE_AFT_LAUNCH_NONCE_PATH"))
            .map_err(|error| format!("read the launch nonce: {error}"))?;
        return Ok((module_id, nonce.trim().to_owned()));
    }
    let args: Vec<String> = std::env::args().collect();
    let module_id = args
        .iter()
        .position(|arg| arg == "--module-id")
        .and_then(|at| args.get(at + 1))
        .cloned()
        .ok_or("fake-surface needs FAKE_AFT_MODULE_ID or --module-id <id>")?;
    let nonce = match std::env::var("SUBC_LAUNCH_NONCE") {
        Ok(nonce) => nonce,
        Err(_) => read_launch_nonce_descriptor()?,
    };
    Ok((module_id, nonce))
}

/// The launch nonce the daemon hands a module it spawned.
///
/// The environment variable names the descriptor holding it as `<fd>:<inode>`, so a process
/// that inherited only the variable does not read and close whatever sits at that number. The
/// descriptor is a one-shot pipe: it is read once, here.
fn read_launch_nonce_descriptor() -> Result<String, String> {
    use std::io::Read;
    use std::os::unix::io::FromRawFd;
    let named = std::env::var("SUBC_LAUNCH_NONCE_FD").map_err(|_| {
        let seen: Vec<String> = std::env::vars()
            .filter(|(name, _)| name.starts_with("SUBC_"))
            .map(|(name, value)| format!("{name}={value}"))
            .collect();
        format!("fake-surface needs a launch nonce from the daemon; SUBC env: {seen:?}")
    })?;
    let (fd, inode) = named
        .split_once(':')
        .ok_or_else(|| format!("bad SUBC_LAUNCH_NONCE_FD {named:?}"))?;
    let fd: i32 = fd
        .parse()
        .map_err(|_| format!("bad SUBC_LAUNCH_NONCE_FD {named:?}"))?;
    let expected: u64 = inode
        .parse()
        .map_err(|_| format!("bad SUBC_LAUNCH_NONCE_FD {named:?}"))?;
    #[cfg(unix)]
    {
        // SAFETY: this only asks the kernel about an already-open descriptor it gave us.
        let stat = unsafe {
            let mut stat: libc::stat = std::mem::zeroed();
            if libc::fstat(fd, &mut stat) != 0 {
                return Err(format!("descriptor {fd} is not open"));
            }
            stat
        };
        let _ = stat.st_ino;
        if stat.st_ino as u64 != expected || (stat.st_mode & libc::S_IFMT) != libc::S_IFIFO {
            return Err(format!("descriptor {fd} is not the launch pipe"));
        }
    }
    let mut nonce = String::new();
    // SAFETY: the descriptor was just checked to be the pipe the daemon named for this
    // process, and this is its only read.
    let mut pipe = unsafe { std::fs::File::from_raw_fd(fd) };
    pipe.read_to_string(&mut nonce)
        .map_err(|error| format!("read the launch nonce: {error}"))?;
    std::mem::forget(pipe);
    Ok(nonce.trim().to_owned())
}

/// Run the same program again, in the stage the environment asks for.
fn relaunch() -> Option<()> {
    let stage = std::env::var("SURFACE_STAGE").ok()?;
    let bin = std::env::var("SURFACE_BIN").ok()?;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let status = std::process::Command::new(bin)
        .args(args)
        .env("SURFACE_STAGE", stage)
        .env_remove("SURFACE_BIN")
        .status()
        .ok()?;
    std::process::exit(status.code().unwrap_or(1));
}

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("fake-surface needs {name}"))
}

/// A setting the handoff passes on the command line: `<name>=<value>` decoded from hex, so a
/// scratch path with spaces or symbols arrives intact.
fn stand_in_value(name: &str) -> String {
    let prefix = format!("--{name}=");
    let args: Vec<String> = std::env::args().collect();
    let encoded = args
        .iter()
        .find_map(|arg| arg.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("fake-surface needs {prefix}<hex>"));
    let bytes: Vec<u8> = (0..encoded.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&encoded[at..at + 2], 16).expect("hex"))
        .collect();
    String::from_utf8(bytes).expect("utf-8")
}

async fn serve() -> Result<(), String> {
    let (module_id, nonce) = identity()?;
    let fixtures = PathBuf::from(stand_in_value("SURFACE_FIXTURES"));
    let requests = PathBuf::from(stand_in_value("SURFACE_REQUESTS"));
    let info = subc_transport::read_for_client(env("SUBC_CONNECTION"))
        .map_err(|error| format!("read the connection file: {error}"))?;
    let endpoint = info
        .endpoints
        .first()
        .ok_or("the connection file names no endpoint")?;
    let mut stream = tokio::net::TcpStream::connect((endpoint.host.as_str(), endpoint.port))
        .await
        .map_err(|error| format!("connect: {error}"))?;
    authenticate_client(&mut stream, &info, std::time::Duration::from_secs(10))
        .await
        .map_err(|error| format!("authenticate: {error}"))?;
    let hello = ModuleHelloBody {
        manifest: manifest(&module_id),
        protocol_ver: PROTOCOL_VERSION,
        control_ops: None,
        launch_nonce: Some(nonce),
    };
    let body = serde_json::to_vec(&hello).map_err(|error| error.to_string())?;
    write(&mut stream, control(FrameType::Hello, 1, body)).await?;
    // The daemon answers HELLO with HELLO_ACK before it sends anything else.
    let ack = read_frame(&mut stream)
        .await
        .map_err(|error| format!("read HELLO_ACK: {error}"))?
        .ok_or("the daemon closed before HELLO_ACK")?;
    if ack.header.ty != FrameType::HelloAck {
        return Err(format!("expected HELLO_ACK, got {:?}", ack.header.ty));
    }
    append(
        &requests,
        &format!("hello-ack {:?} {}\n", ack.header.ty, String::from_utf8_lossy(&ack.body)),
    )?;
    loop {
        let Some(frame) = read_frame(&mut stream)
            .await
            .map_err(|error| format!("read a request: {error}"))?
        else {
            return Ok(());
        };
        if frame.header.ty == FrameType::Goodbye {
            // The consumer closed its route. Nothing is owed an answer.
            continue;
        }
        if frame.header.channel == 0 && is_route_bind(&frame) {
            // A consumer opened a route to this surface: accept it and serve its channel.
            append(&requests, &format!("route.bind {}\n", String::from_utf8_lossy(&frame.body)))?;
            let accepted = serde_json::to_vec(&serde_json::json!({"op":"route.bind"}))
                .unwrap_or_default();
            write(&mut stream, reply_frame(&frame, FrameType::Response, accepted)).await?;
            continue;
        }
        if frame.header.channel == 0 && frame.header.ty == FrameType::Request {
            // Channel 0 carries the daemon's module control requests, which this surface does
            // not serve: refuse them rather than leaving the daemon waiting. Only a Request is
            // answered — a bound route's own requests are control-channel frames too, and
            // those belong to the loop below.
            append(
                &requests,
                &format!("control-refused {}\n", String::from_utf8_lossy(&frame.body)),
            )?;
            let refused = ErrorBody::new(
                "unknown_method",
                "the stand-in surface serves no module control operation",
            );
            let body = serde_json::to_vec(&refused).unwrap_or_default();
            write(&mut stream, reply_frame(&frame, FrameType::Error, body)).await?;
            continue;
        }
        let request: serde_json::Value =
            serde_json::from_slice(&frame.body).unwrap_or(serde_json::Value::Null);
        let method = request["method"].as_str().unwrap_or_default().to_owned();
        append(
            &requests,
            &format!("{} {}\n", method, request["params"]),
        )?;
        let (ty, body) = match answer(&fixtures, &method, &request["params"]) {
            Ok(result) => (FrameType::Response, result),
            Err(refused) => (FrameType::Error, refused),
        };
        write(&mut stream, reply_frame(&frame, ty, body)).await?;
    }
}

/// Whether this control-channel frame is a consumer's `route.bind`.
fn is_route_bind(frame: &Frame) -> bool {
    serde_json::from_slice::<serde_json::Value>(&frame.body)
        .is_ok_and(|body| body["op"].as_str() == Some("route.bind"))
}

fn control(ty: FrameType, corr: u64, body: Vec<u8>) -> Frame {
    Frame::build(
        ty,
        Flags::new(false, Priority::Interactive, false),
        0,
        0,
        corr,
        body,
    )
    .expect("a control frame fits the wire limit")
}

fn reply_frame(request: &Frame, ty: FrameType, body: Vec<u8>) -> Frame {
    Frame::build(
        ty,
        Flags::new(false, Priority::Interactive, false),
        request.header.channel,
        request.header.epoch,
        request.header.corr,
        body,
    )
    .expect("a reply fits the wire limit")
}

async fn write(stream: &mut (impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin), frame: Frame) -> Result<(), String> {
    write_frame(stream, &frame)
        .await
        .map_err(|error| format!("write a frame: {error}"))
}

fn manifest(module_id: &str) -> ModuleManifest {
    let operations = SURFACED_OPS
        .iter()
        .map(|name| ManagementOperation {
            name: (*name).to_owned(),
            kind: ManagementOperationKind::Query,
            description: None,
        })
        .collect();
    ModuleManifest::builder(module_id, env!("CARGO_PKG_VERSION")).provides(vec![
        ProviderRole::ManagementSurface {
            operations,
            config_schema: serde_json::json!({}),
            observability: Vec::new(),
            identity_scope: Vec::<IdentityScope>::new(),
            concurrency: Concurrency::Serial,
        },
    ])
    .build()
}

fn answer(fixtures: &PathBuf, method: &str, params: &serde_json::Value) -> Result<Vec<u8>, Vec<u8>> {
    if method == "private_content.query" {
        if let Some(field) = params.as_object().and_then(|params| {
            params
                .keys()
                .find(|key| !matches!(key.as_str(), "scope_ref" | "parent_scope_ref"))
        }) {
            return Ok(serde_json::to_vec(&serde_json::json!({
                "outcome": "invalid_request", "context": null, "session": null,
                "detail": {"field": field, "reason_code": "unknown_field"}
            }))
            .unwrap());
        }
    }
    let refused = |code: &str, message: String| {
        serde_json::to_vec(&ErrorBody::new(code, message)).unwrap_or_default()
    };
    let text = std::fs::read_to_string(fixtures).unwrap_or_default();
    // The file holds one fixture or an array of them.
    let document: serde_json::Value = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
    let fixtures: Vec<serde_json::Value> = match document {
        serde_json::Value::Array(items) => items,
        serde_json::Value::Null => text
            .lines()
            .filter(|line| !line.trim().is_empty())
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect(),
        other => vec![other],
    };
    for fixture in fixtures {
        if fixture["match"]["method"].as_str() != Some(method) {
            continue;
        }
        // A fixture may name the scope it answers for. A request for another scope falls
        // through to the default below.
        if fixture["match"]["scope_ref"]
            .as_str()
            .is_some_and(|scope| params["scope_ref"].as_str() != Some(scope))
        {
            continue;
        }
        let raw = fixture["stdout"].as_str().unwrap_or_default();
        let value: serde_json::Value = match serde_json::from_str(raw) {
            Ok(value) => value,
            Err(error) => return Err(refused("internal_error", format!("bad fixture: {error}"))),
        };
        // Cerebellum sends both answers and refusals as bare response bodies. Preserve the
        // fixture bytes so the rig cannot hide a client that expects a result envelope.
        if method == "private_content.query" {
            return Ok(raw.as_bytes().to_vec());
        }
        // A fixture that reads as an error is the module refusing the operation.
        if value.get("code").is_some() {
            return Err(serde_json::to_vec(&value).unwrap_or_default());
        }
        return Ok(serde_json::to_vec(&serde_json::json!({ "result": value })).unwrap_or_default());
    }
    // A cerebellum stand-in answers a scope it has no fixture for. The file naming any
    // `person_browser` fixture marks it as the whole file's answer, which is how a test says
    // that this cerebellum keeps finding more private content.
    if method == "private_content.query" {
        let marked = text.contains("person_browser");
        return Ok(serde_json::to_vec(&serde_json::json!({
            "private_content": marked, "sources": []
        }))
        .unwrap_or_default());
    }
    Err(refused(
        "unknown_method",
        format!("the stand-in surface does not serve {method}"),
    ))
}
fn append(path: &PathBuf, line: &str) -> Result<(), String> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| format!("open the request log: {error}"))?;
    file.write_all(line.as_bytes())
        .map_err(|error| format!("write the request log: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_content_rejects_unknown_params_with_a_normal_refusal_body() {
        let response = answer(
            &PathBuf::new(),
            "private_content.query",
            &serde_json::json!({
                "scope_ref": "run", "owner": {"kind": "reserved", "module_id": "broca"}
            }),
        )
        .unwrap();
        let expected: serde_json::Value = serde_json::from_slice(br#"{"outcome":"invalid_request","context":null,"session":null,"detail":{"field":"owner","reason_code":"unknown_field"}}"#).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response).unwrap(),
            expected
        );
    }

    #[test]
    fn private_content_default_answer_is_bare() {
        let response = answer(
            &PathBuf::new(),
            "private_content.query",
            &serde_json::json!({
                "scope_ref": "run", "parent_scope_ref": "head"
            }),
        )
        .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response).unwrap(),
            serde_json::json!({"private_content": false, "sources": []})
        );
    }
}
