#![cfg(all(feature = "live", feature = "test-harness"))]

use broca_core::Provider;
use broca_provider::{
    families::{anthropic_frozen_config, AnthropicMessages},
    wire::{AuthHandle, CacheClass, CachePolicy, CacheTtl, MarkerSyntax},
    LiveProvider,
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static COUNTS: Cell<[usize; 4]> = const { Cell::new([0; 4]) };
}

struct WarmAllocator;
fn note(size: usize) {
    let _ = ACTIVE.try_with(|active| {
        if active.get() {
            COUNTS.with(|counts| {
                let mut value = counts.get();
                value[0] += 1;
                for (index, sentinel) in [17011, 25013, 32003].into_iter().enumerate() {
                    if size == sentinel {
                        value[index + 1] += 1;
                    }
                }
                counts.set(value);
            });
        }
    });
}

// Forward each operation unchanged to System; only the current test future's polls count.
unsafe impl GlobalAlloc for WarmAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        note(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: WarmAllocator = WarmAllocator;

struct Counted<F> {
    future: Pin<Box<F>>,
}
impl<F: Future> Future for Counted<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                ACTIVE.with(|active| active.set(false));
            }
        }
        ACTIVE.with(|active| active.set(true));
        let _reset = Reset;
        self.future.as_mut().poll(cx)
    }
}
async fn count<F: Future>(future: F) -> (F::Output, [usize; 4]) {
    COUNTS.with(|counts| counts.set([0; 4]));
    let output = Counted {
        future: Box::pin(future),
    }
    .await;
    (output, COUNTS.with(Cell::get))
}

fn provider(url: String) -> LiveProvider<AnthropicMessages> {
    let mut frozen = anthropic_frozen_config("claude-opus-4-8");
    frozen.base_url = Some(url);
    frozen.cache = Some(CachePolicy {
        version: 1,
        class: CacheClass::Markers {
            syntax: MarkerSyntax::AnthropicCacheControl,
            ttl: CacheTtl::OneHour,
            max_breakpoints: 4,
            lookback_blocks: 20,
            tail: None,
            omit_ttl: false,
        },
    });
    let account = "a".repeat(32003);
    LiveProvider::new(AnthropicMessages, frozen, AuthHandle::None)
        .with_account_refs(Some(account.clone()), Some(account))
}

#[tokio::test]
async fn legacy_warm_allocations_match_base_path() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let response = format!(
            r#"{{"model":"{}","usage":{{"input_tokens":3,"cache_read_input_tokens":4000}}}}"#,
            "s".repeat(25013)
        );
        let mut bodies = Vec::new();
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let head_end = loop {
                let mut bytes = [0; 4096];
                let n = socket.read(&mut bytes).await.unwrap();
                assert!(n > 0);
                request.extend_from_slice(&bytes[..n]);
                if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let head = String::from_utf8_lossy(&request[..head_end]);
            let len: usize = head
                .lines()
                .find_map(|l| {
                    let (name, value) = l.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap();
            while request.len() < head_end + len {
                let mut bytes = [0; 4096];
                let n = socket.read(&mut bytes).await.unwrap();
                assert!(n > 0);
                request.extend_from_slice(&bytes[..n]);
            }
            bodies.push(request[head_end..].to_vec());
            socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}", response.len(), response).as_bytes()).await.unwrap();
        }
        bodies
    });
    let provider = provider(url);
    let base = format!(
        r#"{{"model":"{}","max_tokens":64,"messages":[{{"role":"user","content":"q"}}]}}"#,
        "r".repeat(17011)
    );
    let reference =
        count(async { Box::pin(provider.warm_legacy_reference_for_test(base.as_bytes())).await })
            .await;
    let actual = count(async { provider.warm(base.as_bytes()).await }).await;
    assert_eq!(reference.0.unwrap().usage.cached_input_tokens, Some(4000));
    assert_eq!(actual.0.unwrap().usage.cached_input_tokens, Some(4000));
    assert!(
        actual.1[0] > 0,
        "the counter must observe allocations during the warm"
    );
    // Distinct lengths isolate request parsing, response metadata and account copies from
    // unrelated network buffer allocations. The reference is the original legacy path.
    assert_eq!(
        &reference.1[1..],
        &[1, 1, 1],
        "the original warm path visits each marked string once"
    );
    assert_eq!(&actual.1[1..], &reference.1[1..]);
    let bodies = server.await.unwrap();
    assert_eq!(bodies[0], bodies[1]);
}

#[tokio::test]
async fn legacy_provider_warm_has_no_extra_boxed_delegation() {
    let provider = provider("invalid".into());
    // Invalid JSON refuses synchronously; no connection task contributes allocations.
    let reference =
        count(async { Box::pin(provider.warm_legacy_reference_for_test(b"not json")).await }).await;
    let actual = count(async { provider.warm(b"not json").await }).await;
    assert!(reference.0.is_err());
    assert!(actual.0.is_err());
    assert!(actual.1[0] > 0);
    assert_eq!(actual.1, reference.1);
}
