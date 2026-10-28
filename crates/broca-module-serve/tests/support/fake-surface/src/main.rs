//! A stand-in management surface for the private-content tests.
//!
//! The `llm.classify/v1` route asks cerebellum (`private_content.query`) and Basal
//! (`flow.private_content_providers`) before it may call a provider, so a test needs both to
//! answer. `fake-aft-stub` declares only the tool-provider role, so the daemon refuses a
//! `route.open` aimed at its management surface before any operation is dispatched. This binary
//! is a module that declares a management surface, including screenshot resolution and
//! Cingulate card filing.
//!
//! Configuration, from the environment and the command line:
//!
//! - `SUBC_CONNECTION`: the daemon connection file this process connects to.
//! - `FAKE_AFT_MODULE_ID`, `FAKE_AFT_LAUNCH_NONCE_PATH`: the module identity, taken exactly as
//!   `fake-aft-stub` takes them.
//! - `SURFACE_FIXTURES`: a file holding one fixture or an array of them, each
//!   `{"match": {"method", "scope_ref"?}, "stdout"}`. A request whose method matches (and whose
//!   `scope_ref` equals the fixture's, when it names one) is answered with that fixture's
//!   `stdout` bytes after JSON validation. Basal `ErrorBody` fixtures use error frames;
//!   Cerebellum's answers and outcome refusals use response frames. A `private_content.query`
//!   with no matching fixture is answered marked, with no sources, when the file names a
//!   recognized source kind, and unmarked when it does not; any other unmatched operation is
//!   refused `unknown_method`.
//! - `SURFACE_REQUESTS`: a file this process appends one line to per request, so a test can
//!   count how often broca asked.
//! - `SURFACE_SCOPES`: optional full scope set to sync on this module's own connection
//!   after HELLO_ACK. The file need not exist at startup; this process waits up to 60 seconds
//!   for it to appear.

use std::io::Write;
use std::path::PathBuf;

use base64::{engine::general_purpose::STANDARD, Engine};
use subc_protocol::manifest::{
    CapabilityDeclarations, Concurrency, IdentityScope, ManagementOperation, ManagementOperationKind,
    ModuleManifest, ProviderRole,
};
use subc_protocol::{ErrorBody, Flags, Frame, FrameType, ModuleHelloBody, Priority, PROTOCOL_VERSION};
use subc_transport::{authenticate_client, read_frame, write_frame};

const SURFACED_OPS: [&str; 3] = ["private_content.query", "flow.private_content_providers", "image.resolve"];

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
    // A flow caller must connect under Basal's daemon-issued identity, not the scope owner's.
    // The rig uses the optional nonce file to open that additional consumer connection.
    if std::env::args().any(|arg| arg.starts_with("--SURFACE_NONCE_PATH=")) {
        std::fs::write(stand_in_value("SURFACE_NONCE_PATH"), &nonce)
            .map_err(|error| format!("write the consumer nonce: {error}"))?;
    }
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
    if std::env::args().any(|arg| arg.starts_with("--SURFACE_SCOPES=")) {
        let path = PathBuf::from(stand_in_value("SURFACE_SCOPES"));
        let scopes = tokio::time::timeout(std::time::Duration::from_secs(60), async {
            loop {
                match std::fs::read(&path) {
                    Ok(bytes) => break serde_json::from_slice::<serde_json::Value>(&bytes)
                        .map_err(|error| format!("decode scope set: {error}")),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                    Err(error) => break Err(format!("read scope set: {error}")),
                }
            }
        }).await.map_err(|_| "the owner did not publish its scope set".to_owned())??;
        // Keep optional registration fields intact; the daemon, not the stand-in's pinned
        // protocol decoder, validates the owner's project and workdir attribution.
        let body = serde_json::to_vec(&serde_json::json!({
            "op": "scope.sync", "generation": 1, "scopes": scopes
        })).map_err(|error| error.to_string())?;
        write(&mut stream, control(FrameType::Request, 2, body)).await?;
        let reply = read_frame(&mut stream).await
            .map_err(|error| format!("read scope.sync reply: {error}"))?
            .ok_or("the daemon closed before scope.sync replied")?;
        let body_json: serde_json::Value = serde_json::from_slice(&reply.body)
            .map_err(|error| format!("decode scope.sync reply: {error}"))?;
        append(&requests, &format!("{}\n", serde_json::json!({
            "kind": if reply.header.ty == FrameType::Response { "scope_sync_response" } else { "error" },
            "corr": reply.header.corr, "body_json": body_json
        })))?;
        if reply.header.ty != FrameType::Response || reply.header.corr != 2 {
            return Err(format!("scope.sync was not accepted: {body_json}"));
        }
    }
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
            write(&mut stream, reply_frame(&frame, FrameType::Response, accepted, false)).await?;
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
            write(&mut stream, reply_frame(&frame, FrameType::Error, body, false)).await?;
            continue;
        }
        let request: serde_json::Value =
            serde_json::from_slice(&frame.body).unwrap_or(serde_json::Value::Null);
        let method = request["method"].as_str().unwrap_or_default().to_owned();
        append(
            &requests,
            &format!("{} {}\n", method, request["params"]),
        )?;
        // Leave the image request unanswered to exercise the caller's source timeout.
        // Other operations remain available while the image source is down.
        if method == "image.resolve" && fixtures.with_extension("down").exists() {
            continue;
        }
        let (ty, body, binary) = match answer(&fixtures, &method, &request["params"]) {
            Ok(result) => (FrameType::Response, result.body, result.binary),
            Err(refused) => (FrameType::Error, refused, false),
        };
        write(&mut stream, reply_frame(&frame, ty, body, binary)).await?;
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

fn reply_frame(request: &Frame, ty: FrameType, body: Vec<u8>, binary: bool) -> Frame {
    Frame::build(
        ty,
        Flags::new(binary, Priority::Interactive, false),
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
    let mut operations: Vec<_> = SURFACED_OPS
        .iter()
        .map(|name| ManagementOperation {
            name: (*name).to_owned(),
            kind: ManagementOperationKind::Query,
            description: None,
        })
        .collect();
    if module_id == "cingulate" {
        operations.push(ManagementOperation {
            name: "consent.request".into(),
            kind: ManagementOperationKind::Mutate,
            description: None,
        });
    }
    ModuleManifest::builder(module_id, env!("CARGO_PKG_VERSION")).provides(vec![
        ProviderRole::ManagementSurface {
            operations,
            config_schema: serde_json::json!({}),
            observability: Vec::new(),
            identity_scope: Vec::<IdentityScope>::new(),
            concurrency: Concurrency::Serial,
        },
    ])
    .capabilities((module_id == "cingulate").then(|| CapabilityDeclarations {
        provides: vec!["consent-no-expiry/v1".into()],
        requires: Vec::new(),
        must_never_reach: Vec::new(),
    }))
    .build()
}

fn answer(fixtures: &PathBuf, method: &str, params: &serde_json::Value) -> Result<Surfaced, Vec<u8>> {
    if method == "flow.private_content_providers"
        && (params["flow_id"].as_str().is_none()
            || params.as_object().is_none_or(|params| params.keys().any(|key| key != "flow_id")))
    {
        return Err(br#"{"code":"invalid_params","message":"params must contain only flow_id"}"#.to_vec());
    }
    if method == "private_content.query" {
        if let Some(field) = params.as_object().and_then(|params| {
            params
                .keys()
                .find(|key| !matches!(key.as_str(), "scope_ref" | "parent_scope_ref"))
        }) {
            return Ok(Surfaced::plain(
                serde_json::to_vec(&serde_json::json!({
                    "outcome": "invalid_request", "context": null, "session": null,
                    "detail": {"field": field, "reason_code": "unknown_field"}
                }))
                .unwrap(),
            ));
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
        if fixture["match"]["handle"].as_str()
            .is_some_and(|handle| params["handle"].as_str() != Some(handle)) {
            continue;
        }
        let raw = fixture["stdout"].as_str().unwrap_or_default();
        // Cerebellum sends both answers and refusals as bare response bodies. Preserve the
        // fixture bytes so the rig cannot hide a client that expects a result envelope.
        // A resolve success is a binary body (`u32` big-endian head length, head JSON, image
        // bytes): the fixture carries those exact body bytes base64-encoded, and the reply
        // frame carries the binary flag. Every `image.resolve` fixture is a success; an
        // unmatched handle gets the JSON refusal from the default below.
        if method == "image.resolve" {
            let body = STANDARD.decode(raw).map_err(|error| {
                refused("internal_error", format!("bad fixture: {error}"))
            })?;
            return Ok(Surfaced { body, binary: true });
        }
        if method == "private_content.query" {
            return Ok(Surfaced::plain(raw.as_bytes().to_vec()));
        }
        let value: serde_json::Value = match serde_json::from_str(raw) {
            Ok(value) => value,
            Err(error) => return Err(refused("internal_error", format!("bad fixture: {error}"))),
        };
        // Basal refuses with an error frame, while its successful fixture includes result.
        if value.get("code").is_some() {
            return Err(raw.as_bytes().to_vec());
        }
        return Ok(Surfaced::plain(raw.as_bytes().to_vec()));
    }
    // An unmatched private-content query returns marked=true if the fixture file names any
    // recognized source. This lets a test model Cerebellum continuing to find private content.
    if method == "private_content.query" {
        let marked = ["person_browser", "desktop_app", "signed_in_site"]
            .iter().any(|source| text.contains(source));
        return Ok(Surfaced::plain(
            serde_json::to_vec(&serde_json::json!({
                "private_content": marked, "sources": []
            }))
            .unwrap_or_default(),
        ));
    }
    if method == "image.resolve" {
        return Ok(Surfaced::plain(br#"{"outcome":"image_unavailable"}"#.to_vec()));
    }
    Err(refused(
        "unknown_method",
        format!("the stand-in surface does not serve {method}"),
    ))
}

/// A fixture's answer: the reply body, and whether it is a binary body rather than JSON.
#[derive(Debug)]
struct Surfaced {
    body: Vec<u8>,
    binary: bool,
}

impl Surfaced {
    fn plain(body: Vec<u8>) -> Self {
        Self { body, binary: false }
    }
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
    #[test]
    fn cingulate_declares_card_filing_and_no_expiry_without_changing_other_surfaces() {
        let manifest = super::manifest("cingulate");
        assert_eq!(
            manifest.capabilities.as_ref().unwrap().provides,
            vec!["consent-no-expiry/v1"]
        );
        assert!(manifest.provides.iter().any(|role| matches!(
            role, super::ProviderRole::ManagementSurface { operations, .. }
                if operations.iter().any(|op| op.name == "consent.request" && op.kind == super::ManagementOperationKind::Mutate)
        )));
        assert!(super::manifest("cerebellum").capabilities.is_none());
    }

    use super::*;

    #[test]
    fn image_resolution_fixtures_are_scoped_by_handle_and_answer_binary() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("target/scratch/image-fixtures-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("fixtures.json");
        let image = b"abc";
        // SHA-256 of `abc`, so the test needs no hash dependency for three fixture bytes.
        let head = br#"{"content_type":"image/png","length":3,"sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}"#;
        let mut body = (head.len() as u32).to_be_bytes().to_vec();
        body.extend_from_slice(head);
        body.extend_from_slice(image);
        let stdout = STANDARD.encode(&body);
        std::fs::write(&path, serde_json::json!([{
            "match":{"method":"image.resolve","scope_ref":"run","handle":"ih123"},"stdout":stdout
        }]).to_string()).unwrap();
        let answered = answer(&path, "image.resolve", &serde_json::json!({"handle":"ih123","scope_ref":"run"})).unwrap();
        assert!(answered.binary);
        assert_eq!(answered.body, body);
        for params in [serde_json::json!({"handle":"foreign","scope_ref":"run"}), serde_json::json!({"handle":"ih123","scope_ref":"foreign"})] {
            let refused = answer(&path, "image.resolve", &params).unwrap();
            assert!(!refused.binary);
            assert_eq!(refused.body, br#"{"outcome":"image_unavailable"}"#);
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn private_content_rejects_unknown_params_with_a_normal_refusal_body() {
        let response = answer(
            &PathBuf::new(),
            "private_content.query",
            &serde_json::json!({
                "scope_ref": "run", "owner": {"kind": "reserved", "module_id": "broca"}
            }),
        )
        .unwrap()
        .body;
        // Cerebellum fbcc6766, crates/cerebellum/tests/vectors/private-content-query-v1/unknown-field.json.
        // Whole-file SHA-256 (provenance only, not body bytes):
        // 7cc2ec06859a700a91c1e6a1a57643c66ab9af848debcf9b4beb6ecaa7aeb2bc.
        assert_eq!(response, br#"{"context":null,"detail":{"field":"owner","reason_code":"unknown_field"},"outcome":"invalid_request","session":null}"#);
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
        .unwrap()
        .body;
        // Cerebellum fbcc6766, crates/cerebellum/tests/vectors/private-content-query-v1/success-unmarked.json.
        // Whole-file SHA-256 (provenance only, not body bytes):
        // e860efcd7db12298f7af82758e8a72bc66b5e0ede81ada0c7fba40c1aa29acb2.
        assert_eq!(response, br#"{"private_content":false,"sources":[]}"#);
    }

    #[test]
    fn cerebellum_fixtures_preserve_pinned_reply_bytes() {
        // Cerebellum fbcc6766. These reply bodies come from the following vectors; SHA-256s
        // identify whole golden-file documents for provenance only, not the body bytes:
        // crates/cerebellum/tests/vectors/private-content-query-v1/success-marked.json
        // whole-file SHA-256 eb4c452cadd0b5d48e8855db366c599c3dcccb3408da1ea403fa5d8703032d1c
        // crates/cerebellum/tests/vectors/private-content-query-v1/success-unmarked.json
        // whole-file SHA-256 e860efcd7db12298f7af82758e8a72bc66b5e0ede81ada0c7fba40c1aa29acb2
        // crates/cerebellum/tests/vectors/private-content-query-v1/unknown-field.json
        // whole-file SHA-256 7cc2ec06859a700a91c1e6a1a57643c66ab9af848debcf9b4beb6ecaa7aeb2bc
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("target/scratch/cerebellum-fixtures-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let fixture = root.join("fixtures.json");
        for body in [
            r#"{"private_content":true,"sources":["signed_in_site"]}"#,
            r#"{"private_content":false,"sources":[]}"#,
            r#"{"context":null,"detail":{"field":"owner","reason_code":"unknown_field"},"outcome":"invalid_request","session":null}"#,
        ] {
            std::fs::write(&fixture, serde_json::json!([{
                "match": {"method": "private_content.query", "scope_ref": "test-session"}, "stdout": body
            }]).to_string()).unwrap();
            let bytes = answer(&fixture, "private_content.query", &serde_json::json!({"scope_ref": "test-session"})).unwrap().body;
            assert_eq!(bytes, body.as_bytes());
            if body.contains("signed_in_site") {
                let fallback = answer(&fixture, "private_content.query", &serde_json::json!({"scope_ref": "other-scope"})).unwrap().body;
                assert_eq!(fallback, br#"{"private_content":true,"sources":[]}"#);
            }
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn basal_rejects_unknown_params_in_an_error_frame() {
        for params in [
            serde_json::json!({"flow_id": "flow", "owner": {"kind": "reserved", "module_id": "broca"}}),
            serde_json::json!({"flow_id": "flow", "future_field": true}),
        ] {
            let bytes = answer(&PathBuf::new(), "flow.private_content_providers", &params).unwrap_err();
            let error: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(error, serde_json::json!({"code": "invalid_params", "message": "params must contain only flow_id"}));
        }
    }

    #[test]
    fn basal_fixtures_preserve_wrapped_successes_and_error_frames() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("target/scratch/basal-fixtures-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let fixture = root.join("fixtures.json");
        for (body, refused) in [
            (r#"{"result":{"flow_id":"flow","version":7,"code_hash":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","providers":["typesafe","openai"]}}"#, false),
            (r#"{"result":{"flow_id":"flow","version":7,"code_hash":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","providers":[]}}"#, false),
            (r#"{"code":"storage_unavailable","message":"storage is unavailable"}"#, true),
        ] {
            std::fs::write(&fixture, serde_json::json!([{
                "match": {"method": "flow.private_content_providers"}, "stdout": body
            }]).to_string()).unwrap();
            let response = answer(&fixture, "flow.private_content_providers", &serde_json::json!({"flow_id": "flow"}));
            let bytes = if refused { response.unwrap_err() } else { response.unwrap().body };
            assert_eq!(bytes, body.as_bytes());
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
