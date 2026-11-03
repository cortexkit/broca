use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

pub fn runner() -> TestRunner {
    TestRunner::new_with_rng(
        Config {
            cases: 24,
            max_shrink_iters: 256,
            failure_persistence: None,
            ..Config::default()
        },
        TestRng::from_seed(RngAlgorithm::ChaCha, &[0x73; 32]),
    )
}

// A parser stuck inside a synchronous push cannot be bounded by an async timeout.
pub fn guarded(test: impl FnOnce() + Send + 'static) {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let _ = tx.send(catch_unwind(AssertUnwindSafe(test)));
    });
    match rx.recv_timeout(broca_test_support::EVENT_HANG_GUARD) {
        Ok(Ok(())) => {}
        Ok(Err(panic)) => resume_unwind(panic),
        Err(error) => panic!("generated parser cases did not finish: {error}"),
    }
}

pub fn chunks<'a>(bytes: &'a [u8], sizes: &'a [usize]) -> impl Iterator<Item = &'a [u8]> {
    assert!(!sizes.is_empty() && sizes.iter().all(|size| *size > 0));
    let mut offset = 0;
    let mut index = 0;
    std::iter::from_fn(move || {
        if offset == bytes.len() {
            return None;
        }
        let end = (offset + sizes[index % sizes.len()]).min(bytes.len());
        let chunk = &bytes[offset..end];
        offset = end;
        index += 1;
        Some(chunk)
    })
}

pub fn damaged(bytes: &[u8], at: usize, count: usize, replacement: &[u8]) -> Vec<Vec<u8>> {
    let at = at % (bytes.len() + 1);
    let end = (at + count).min(bytes.len());
    let mut dropped = bytes.to_vec();
    dropped.drain(at..end);
    let mut duplicated = bytes.to_vec();
    duplicated.splice(at..at, bytes[at..end].iter().copied());
    let mut corrupted = bytes.to_vec();
    corrupted.splice(at..end, replacement.iter().copied());
    let mut invalid_utf8 = bytes.to_vec();
    invalid_utf8.splice(at..at, [0xff, 0xc0, 0x80]);
    vec![
        bytes[..at].to_vec(),
        dropped,
        duplicated,
        corrupted,
        invalid_utf8,
    ]
}
