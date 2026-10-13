//! broca provider module binary.
//!
//! Production mode is still a scaffold. The `--bench-replay` subcommand (feature
//! `bench`) is the cross-process producer used by the latency benchmark: it stands
//! up the REAL subc-transport server handshake and streams a shared deterministic
//! schedule as frames, so the benchmark can measure two genuine OS processes
//! talking over the wire.
#![forbid(unsafe_code)]

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        #[cfg(feature = "bench")]
        Some("--bench-replay") => {
            if let Err(e) = bench_replay::run(&args[2..]).await {
                broca_protocol::diagln!("broca-provider --bench-replay: {e}");
                std::process::exit(1);
            }
        }
        _ => {
            broca_protocol::diagln!("broca-provider: scaffold — the production SSE path is not wired into this binary yet");
        }
    }
}

/// The (A) arm3 producer. Mirrors what a real provider module's transport leg
/// will do (subc-transport server handshake, then frame streaming), but emits
/// the shared `broca-replay` schedule instead of normalized SSE.
#[cfg(feature = "bench")]
mod bench_replay {
    use std::{path::PathBuf, time::Duration};

    use broca_replay::{build_delta_frame, monotonic_ns, pace, Schedule};
    use subc_transport::{
        authenticate_server, connection_file, generate_daemon_id, generate_key, write_frame,
        ConnectionInfo, Endpoint, SCHEMA_VERSION,
    };
    use tokio::{io::AsyncWriteExt, net::TcpListener};

    type BoxError = Box<dyn std::error::Error + Send + Sync>;

    const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(5);

    pub async fn run(args: &[String]) -> Result<(), BoxError> {
        let cfg = Args::parse(args)?;
        let sched = if cfg.flood {
            Schedule::Flood
        } else {
            Schedule::Fixed {
                cadence_ns: cfg.cadence_us.saturating_mul(1000),
            }
        };

        // Bind, then publish a subc connection-file the bench (client) discovers.
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let key = generate_key()?;
        let daemon_id = generate_daemon_id()?;
        let info = ConnectionInfo {
            schema: SCHEMA_VERSION,
            wire_version: None,
            endpoints: vec![Endpoint {
                host: addr.ip().to_string(),
                port: addr.port(),
            }],
            key: key.clone(),
            daemon_id,
            pid: std::process::id(),
            daemon_ver: "broca-provider-bench".to_owned(),
        };
        connection_file::write_atomic(&cfg.connection_file, &info)?;

        // Accept + REAL subc-transport server handshake.
        let (mut stream, _peer) = listener.accept().await?;
        stream.set_nodelay(true)?;
        authenticate_server(
            &mut stream,
            &key,
            &daemon_id,
            "broca-provider-bench",
            HANDSHAKE_DEADLINE,
        )
        .await?;

        // Stream the schedule. emit_ns is stamped on the shared monotonic clock
        // just before each frame goes out.
        let base = monotonic_ns();
        for seq in 0..cfg.count as u64 {
            pace(sched, base, seq);
            let emit = monotonic_ns();
            let last = seq + 1 == cfg.count as u64;
            let frame = build_delta_frame(emit, seq, last, cfg.payload);
            write_frame(&mut stream, &frame).await?;
        }
        stream.flush().await?;
        Ok(())
    }

    struct Args {
        connection_file: PathBuf,
        flood: bool,
        cadence_us: u64,
        count: usize,
        payload: usize,
    }

    impl Args {
        fn parse(args: &[String]) -> Result<Self, BoxError> {
            let mut connection_file: Option<PathBuf> = None;
            let mut flood = false;
            let mut cadence_us: u64 = 1000;
            let mut count: usize = 100_000;
            let mut payload: usize = 48;
            let mut it = args.iter();
            while let Some(a) = it.next() {
                match a.as_str() {
                    "--connection-file" => connection_file = it.next().map(PathBuf::from),
                    "--mode" => flood = it.next().map(|v| v == "flood").unwrap_or(false),
                    "--cadence-us" => {
                        if let Some(v) = it.next() {
                            cadence_us = v.parse()?;
                        }
                    }
                    "--count" => {
                        if let Some(v) = it.next() {
                            count = v.parse()?;
                        }
                    }
                    "--payload" => {
                        if let Some(v) = it.next() {
                            payload = v.parse()?;
                        }
                    }
                    other => return Err(format!("unknown arg {other}").into()),
                }
            }
            Ok(Args {
                connection_file: connection_file.ok_or("missing --connection-file")?,
                flood,
                cadence_us,
                count,
                payload,
            })
        }
    }
}
