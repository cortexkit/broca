use super::*;
use broca_core::{media_request::DurableMediaSeed, Provider, WalStore};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct HttpReceiver {
    url: String,
    accepted: Arc<AtomicUsize>,
    probe: std::net::TcpListener,
    task: tokio::task::JoinHandle<()>,
}

impl HttpReceiver {
    fn start() -> Self {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let probe = listener.try_clone().unwrap();
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let count = accepted.clone();
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                count.fetch_add(1, Ordering::SeqCst);
                let mut request = Vec::new();
                let end = loop {
                    let mut bytes = [0; 4096];
                    let read = socket.read(&mut bytes).await.unwrap();
                    if read == 0 {
                        return;
                    }
                    request.extend_from_slice(&bytes[..read]);
                    if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                        break end + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&request[..end]);
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                while request.len() < end + length {
                    let mut bytes = [0; 4096];
                    let read = socket.read(&mut bytes).await.unwrap();
                    if read == 0 {
                        return;
                    }
                    request.extend_from_slice(&bytes[..read]);
                }
                socket.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.unwrap();
            }
        });
        Self {
            url,
            accepted,
            probe,
            task,
        }
    }

    async fn finish(self) -> usize {
        self.task.abort();
        let _ = self.task.await;
        // Stop and join the receiver before probing the backlog, so its accept loop cannot race
        // the probe. A queued connection is observed even if the receiver was not scheduled.
        match self.probe.accept() {
            Ok(_) => {
                self.accepted.fetch_add(1, Ordering::SeqCst);
            }
            Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock),
        }
        self.accepted.load(Ordering::SeqCst)
    }
}

async fn assert_damaged_admitted_object_has_no_physical_send(delete: bool) {
    let fixture = Fixture::new();
    let receiver = HttpReceiver::start();
    let mut frozen = super::live_tests::frozen(WireFamilyId::AnthropicMessages);
    frozen.base_url = Some(receiver.url.clone());
    let records = super::live_tests::history(&fixture, &frozen);
    let WalRecord::RunStarted {
        session: identity, ..
    } = &records[0]
    else {
        unreachable!()
    };
    let wal_store = broca_core::FileWalStore::new(fixture.root.join("wal"));
    let mut wal = wal_store.open(identity).unwrap();
    for record in &records {
        wal.append(1, record, true).unwrap();
    }
    drop(wal);
    let frames = wal_store.read_records(identity).unwrap();
    let checkpoint = broca_core::AdmissionCheckpointV1::from_frames(&frames).unwrap();
    let Some(DurableMediaSeed::Ready(seed)) = checkpoint.media else {
        panic!("durable admissions carry a media seed")
    };
    let head = checkpoint.at_seq;
    let media = Arc::new(
        LiveMedia::from_checkpoint(
            fixture.store.clone(),
            *seed,
            None,
            frozen.media.clone(),
            Box::new(move |after| {
                assert_eq!(after, head);
                Ok(Vec::new())
            }),
        )
        .unwrap(),
    );
    let provider = crate::transport::LiveProvider::new(
        crate::families::AnthropicMessages,
        frozen,
        crate::wire::AuthHandle::None,
    );
    provider.set_media_request_renderer(Arc::new(move |config, opts, tags, commit| {
        media.render(config, opts, tags, commit)
    }));
    provider.defer_media_tags(false);
    let opts =
        broca_protocol::CallOptions::new(broca_core::replay::replay(&records).unwrap().prompt);
    let before_damage = provider.try_render_request(&opts).unwrap();
    assert_eq!(
        String::from_utf8_lossy(&before_damage)
            .matches(&STANDARD.encode(&fixture.bytes))
            .count(),
        2,
        "both admitted captures are eligible and readable before damage"
    );
    let object = fixture
        .root
        .join("media-durable/objects")
        .join(&fixture.media.sha256);
    if delete {
        std::fs::remove_file(object).unwrap();
    } else {
        std::fs::write(object, b"corrupted after durable admission").unwrap();
    }
    let mut events = provider.step(broca_protocol::StepId(2), &opts).await;
    let errors = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut errors = Vec::new();
        while let Some(event) = events.recv().await {
            if let broca_protocol::NormalizedEvent::Control(
                broca_protocol::ProviderControl::Error { error, .. },
            ) = event
            {
                errors.push(error);
            }
        }
        errors
    })
    .await
    .expect("storage failure terminates before HTTP");
    assert_eq!(errors.len(), 1);
    assert_eq!(
        errors[0].provider_code.as_deref(),
        Some(if delete {
            "media_object_missing"
        } else {
            "media_object_corrupt"
        })
    );
    assert!(errors[0].message.contains(&fixture.media.sha256));
    assert_eq!(
        receiver.finish().await,
        0,
        "a named object refusal must make zero physical HTTP requests"
    );
}

#[tokio::test]
async fn step_refuses_deleted_admitted_media_before_any_physical_http_request() {
    assert_damaged_admitted_object_has_no_physical_send(true).await;
}

#[tokio::test]
async fn step_refuses_corrupted_admitted_media_before_any_physical_http_request() {
    assert_damaged_admitted_object_has_no_physical_send(false).await;
}
