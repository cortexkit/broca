//! Opt-in WAL access accounting shared by readers and daemon measurements.
//!
//! Start an operation before choosing a seeded or full-replay route. Keep the same
//! operation across fallback and registry handoffs so their work cannot disappear.
//! Operations are cloneable: pass a clone into blocking jobs and call `run` there.
//! `run_future` installs the operation on each poll, even when an executor moves it
//! between threads. Unmeasured production reads allocate no accounting records.
//! WAL source ranges and decodes are counted; archive container indexes, headers
//! and other container metadata are outside this counter's scope.

use std::cell::RefCell;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};

use crate::framing::{DecodeStep, LINEAGE_FRAME_LEN};

/// Seed authentication reads, separate from the requested source ranges.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValidationRead {
    BoundaryFrame,
    LineageGap,
    DurableEnd,
    QualifierTerminal,
    NextRunDelimiter,
}

/// Writer recovery always takes precedence over the offset-based classes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadClass {
    LineageHeader,
    Origin,
    Validation(ValidationRead),
    Source,
    WriterOpen,
}

impl ReadClass {
    /// Classify a source range; offset-zero lineage metadata is not an origin scan.
    pub fn source(offset: u64, length: u64) -> Self {
        if offset != 0 {
            Self::Source
        } else if length <= LINEAGE_FRAME_LEN as u64 {
            Self::LineageHeader
        } else {
            Self::Origin
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalReadAccess {
    pub offset: u64,
    /// Bytes actually returned, including zero for EOF probes.
    pub length: u64,
    pub class: ReadClass,
    pub writer_open: Option<usize>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriterOpenAccess {
    pub path: PathBuf,
    /// Session length when recovery began, before any torn suffix was removed.
    pub session_length: u64,
    pub opens: u64,
    pub bytes: u64,
    pub frames_decoded: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OperationAccess {
    pub name: String,
    pub reads: Vec<WalReadAccess>,
    /// Successful record and lineage decodes, including repeated validation decodes.
    pub frames_decoded: u64,
    /// One entry for each fallback decision, not for each byte read on its route.
    pub seed_fallbacks: Vec<&'static str>,
    pub writer_opens: Vec<WriterOpenAccess>,
}

/// One collector can hold many concurrent, independently named operations.
#[derive(Clone, Default)]
pub struct WalAccessCounter(Arc<Mutex<Vec<OperationAccess>>>);

impl WalAccessCounter {
    pub fn operation(&self, name: impl Into<String>) -> WalAccessOperation {
        let mut operations = self.0.lock().unwrap();
        let index = operations.len();
        operations.push(OperationAccess {
            name: name.into(),
            ..Default::default()
        });
        WalAccessOperation {
            counter: self.clone(),
            index,
        }
    }

    pub fn snapshot(&self) -> Vec<OperationAccess> {
        self.0.lock().unwrap().clone()
    }
}

#[derive(Clone)]
pub struct WalAccessOperation {
    counter: WalAccessCounter,
    index: usize,
}

#[derive(Clone)]
struct Context {
    operation: WalAccessOperation,
    class: Option<ReadClass>,
    writer_open: Option<usize>,
    captured_read: Option<Arc<Mutex<Option<u64>>>>,
    suppress_fallbacks: bool,
}

thread_local! {
    static CURRENT: RefCell<Option<Context>> = const { RefCell::new(None) };
}

fn with_context<T>(context: Option<Context>, work: impl FnOnce() -> T) -> T {
    struct Restore(Option<Context>);
    impl Drop for Restore {
        fn drop(&mut self) {
            CURRENT.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(CURRENT.with(|slot| slot.replace(context)));
    work()
}

fn current() -> Option<Context> {
    CURRENT.with(|slot| slot.borrow().clone())
}

/// Capture the current operation before dispatching a blocking job. The worker
/// restores its previous context even when the job returns an error or unwinds.
pub fn inherit<F: FnOnce() -> T, T>(work: F) -> impl FnOnce() -> T {
    let context = current();
    move || with_context(context, work)
}

impl WalAccessOperation {
    pub fn run<T>(&self, work: impl FnOnce() -> T) -> T {
        with_context(
            Some(Context {
                operation: self.clone(),
                class: None,
                writer_open: None,
                captured_read: None,
                suppress_fallbacks: false,
            }),
            work,
        )
    }

    pub fn run_future<F: Future>(&self, future: F) -> impl Future<Output = F::Output> + use<F> {
        MeasuredFuture {
            operation: self.clone(),
            future: Box::pin(future),
        }
    }
}

struct MeasuredFuture<F> {
    operation: WalAccessOperation,
    future: Pin<Box<F>>,
}

impl<F: Future> Future for MeasuredFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.operation.run(|| this.future.as_mut().poll(cx))
    }
}

/// Forward the spawning operation through a task's first poll, which can enqueue
/// independently inherited blocking reads. The context is dropped after that poll,
/// so a long-lived subscriber never retains the operation's counter.
/// With no active operation, return the original future: callers spawn it directly,
/// without a wrapper allocation or any extra work on its poll path.
pub fn inherit_future<F: Future>(future: F) -> Result<impl Future<Output = F::Output> + use<F>, F> {
    match current() {
        Some(context) => Ok(InheritedFuture {
            context: Some(context),
            future: Box::pin(future),
        }),
        None => Err(future),
    }
}

struct InheritedFuture<F> {
    context: Option<Context>,
    future: Pin<Box<F>>,
}

impl<F: Future> Future for InheritedFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.context.take() {
            Some(context) => with_context(Some(context), || this.future.as_mut().poll(cx)),
            None => this.future.as_mut().poll(cx),
        }
    }
}

/// Attribute both a validation read and its decode to a named purpose.
pub fn validation<T>(kind: ValidationRead, work: impl FnOnce() -> T) -> T {
    let context = current().map(|mut context| {
        if context.writer_open.is_none() {
            context.class = Some(ReadClass::Validation(kind));
        }
        context
    });
    with_context(context, work)
}

/// Instrument an arbitrary seeded reader callback without double-counting a
/// callback that already instruments its underlying file access.
pub fn read_at(
    read: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
    offset: u64,
    length: u64,
) -> std::io::Result<Vec<u8>> {
    let captured = current().map(|_| Arc::new(Mutex::new(None)));
    let context = current().map(|mut context| {
        context.captured_read = captured.clone();
        context
    });
    let result = with_context(context, || read(offset, length));
    let actual = captured
        .and_then(|captured| *captured.lock().unwrap())
        .unwrap_or_else(|| result.as_ref().map_or(0, |bytes| bytes.len() as u64));
    record_read(offset, actual);
    result
}

pub fn read_validation_at(
    kind: ValidationRead,
    read: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
    offset: u64,
    length: u64,
) -> std::io::Result<Vec<u8>> {
    validation(kind, || read_at(read, offset, length))
}

/// Count one logical range even if I/O fails after returning a partial buffer.
pub fn read_to_end(
    mut reader: impl std::io::Read,
    offset: u64,
    bytes: &mut Vec<u8>,
) -> std::io::Result<usize> {
    let before = bytes.len();
    let result = reader.read_to_end(bytes);
    record_read(offset, (bytes.len() - before) as u64);
    result
}

/// Record a logical file range, including short reads and EOF probes.
pub fn record_read(offset: u64, length: u64) {
    let Some(context) = current() else {
        return;
    };
    if let Some(captured) = context.captured_read {
        let mut length_read = captured.lock().unwrap();
        *length_read = Some(length_read.unwrap_or(0) + length);
        return;
    }
    let mut operations = context.operation.counter.0.lock().unwrap();
    let operation = &mut operations[context.operation.index];
    let class = context
        .class
        .unwrap_or_else(|| ReadClass::source(offset, length));
    operation.reads.push(WalReadAccess {
        offset,
        length,
        class,
        writer_open: context.writer_open,
    });
    if let Some(index) = context.writer_open {
        operation.writer_opens[index].bytes += length;
    }
}

/// Use at the decoding boundary, not at semantic replay (which may fold a frame twice).
pub fn decode_next(bytes: &[u8]) -> DecodeStep {
    let step = crate::framing::decode_next(bytes);
    if matches!(step, DecodeStep::Record { .. } | DecodeStep::Lineage { .. }) {
        if let Some(context) = current() {
            let mut operations = context.operation.counter.0.lock().unwrap();
            let operation = &mut operations[context.operation.index];
            operation.frames_decoded += 1;
            if let Some(index) = context.writer_open {
                operation.writer_opens[index].frames_decoded += 1;
            }
        }
    }
    step
}

pub fn seed_fallback(reason: &'static str) {
    if let Some(context) = current().filter(|context| !context.suppress_fallbacks) {
        context.operation.counter.0.lock().unwrap()[context.operation.index]
            .seed_fallbacks
            .push(reason);
    }
}

/// Count the returned decision from an implementation-provided reader, including
/// readers that construct errors without the instrumented constructor. Internal
/// error construction is suppressed so a propagated error is not counted twice.
pub fn fallback_result<T, E>(
    work: impl FnOnce() -> Result<T, E>,
    reason: impl FnOnce(&E) -> &'static str,
) -> Result<T, E> {
    let context = current().map(|mut context| {
        context.suppress_fallbacks = true;
        context
    });
    let result = with_context(context, work);
    if let Err(error) = &result {
        seed_fallback(reason(error));
    }
    result
}

/// Group recovery and subsequent startup replays under one writer-open entry.
/// These reads remain separate from service and resume source reads.
pub fn writer_open<T>(path: &Path, recovery: bool, work: impl FnOnce() -> T) -> T {
    let context = current().map(|mut context| {
        let mut operations = context.operation.counter.0.lock().unwrap();
        let opens = &mut operations[context.operation.index].writer_opens;
        let index = if !recovery {
            opens.iter().rposition(|open| open.path == path)
        } else {
            None
        }
        .unwrap_or_else(|| {
            opens.push(WriterOpenAccess {
                path: path.to_owned(),
                session_length: crate::fs_ops::metadata(path).map_or(0, |meta| meta.len()),
                opens: u64::from(recovery),
                ..Default::default()
            });
            opens.len() - 1
        });
        context.class = Some(ReadClass::WriterOpen);
        context.writer_open = Some(index);
        drop(operations);
        context
    });
    with_context(context, work)
}

/// Counts the bytes returned by a streaming reader without changing its I/O shape.
pub struct AccessReader<R> {
    inner: R,
    offset: u64,
}

impl<R> AccessReader<R> {
    pub fn new(inner: R, offset: u64) -> Self {
        Self { inner, offset }
    }
}

impl<R: std::io::Read> std::io::Read for AccessReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let result = self.inner.read(bytes);
        let length = result.as_ref().copied().unwrap_or(0) as u64;
        record_read(self.offset, length);
        self.offset += length;
        result
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn inherited_future_without_an_operation_is_the_original_future() {
        let future = std::future::ready(17);
        let original_size = std::mem::size_of_val(&future);
        let mut bare = super::inherit_future(future)
            .err()
            .expect("no operation returns the unwrapped future");
        assert_eq!(std::mem::size_of_val(&bare), original_size);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert_eq!(
            std::future::Future::poll(std::pin::Pin::new(&mut bare), &mut cx),
            std::task::Poll::Ready(17)
        );
    }

    #[test]
    fn inherited_future_releases_its_operation_after_the_first_poll() {
        let counter = super::WalAccessCounter::default();
        let operation = counter.operation("spawned read");
        let baseline = std::sync::Arc::strong_count(&counter.0);
        let mut first = true;
        let future = std::future::poll_fn(move |_| {
            super::record_read(7, 3);
            if std::mem::take(&mut first) {
                std::task::Poll::Pending
            } else {
                std::task::Poll::Ready(())
            }
        });
        let mut inherited = Box::pin(
            operation
                .run(|| super::inherit_future(future))
                .ok()
                .unwrap(),
        );
        assert_eq!(std::sync::Arc::strong_count(&counter.0), baseline + 1);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(std::future::Future::poll(inherited.as_mut(), &mut cx).is_pending());
        assert_eq!(std::sync::Arc::strong_count(&counter.0), baseline);
        assert!(std::future::Future::poll(inherited.as_mut(), &mut cx).is_ready());
        assert_eq!(counter.snapshot()[0].reads.len(), 1);
    }
    use super::*;
    use crate::framing::{encode, encode_lineage, WalLineageId};
    use crate::{FileWal, Wal, WalRecord};

    #[test]
    fn range_vocabulary_and_nested_readers_count_once() {
        let counter = WalAccessCounter::default();
        let operation = counter.operation("ranges");
        operation.run(|| {
            let reader = |offset, length| {
                record_read(offset, length);
                Ok(vec![0; length as usize])
            };
            read_at(&reader, 0, LINEAGE_FRAME_LEN as u64).unwrap();
            read_at(&reader, 0, LINEAGE_FRAME_LEN as u64 + 1).unwrap();
            read_validation_at(ValidationRead::BoundaryFrame, &reader, 200, 80).unwrap();
            read_at(&reader, 400, 20).unwrap();
            read_validation_at(ValidationRead::DurableEnd, &|_, _| Ok(Vec::new()), 500, 1).unwrap();
            seed_fallback("fixture refusal");
        });
        let snapshot = counter.snapshot();
        let measured = &snapshot[0];
        assert_eq!(measured.reads.len(), 5);
        assert_eq!(
            measured
                .reads
                .iter()
                .map(|read| read.class)
                .collect::<Vec<_>>(),
            [
                ReadClass::LineageHeader,
                ReadClass::Origin,
                ReadClass::Validation(ValidationRead::BoundaryFrame),
                ReadClass::Source,
                ReadClass::Validation(ValidationRead::DurableEnd),
            ]
        );
        assert_eq!(measured.reads[4].length, 0);
        assert_eq!(measured.seed_fallbacks, ["fixture refusal"]);
    }

    #[test]
    fn failed_ranges_include_bytes_returned_before_the_error() {
        struct FailingRead(bool);
        impl std::io::Read for FailingRead {
            fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
                if self.0 {
                    Err(std::io::Error::other("read failed after partial data"))
                } else {
                    self.0 = true;
                    bytes[0] = 1;
                    Ok(1)
                }
            }
        }
        let counter = WalAccessCounter::default();
        counter.operation("partial failure").run(|| {
            let result = read_validation_at(
                ValidationRead::BoundaryFrame,
                &|offset, _| {
                    let mut bytes = Vec::new();
                    read_to_end(FailingRead(false), offset, &mut bytes)?;
                    Ok(bytes)
                },
                200,
                80,
            );
            assert!(result.is_err());
        });
        let measured = counter.snapshot();
        assert_eq!(
            measured[0].reads,
            [WalReadAccess {
                offset: 200,
                length: 1,
                class: ReadClass::Validation(ValidationRead::BoundaryFrame),
                writer_open: None,
            }]
        );
    }

    #[test]
    fn implementation_provided_fallbacks_are_counted_once_per_decision() {
        let counter = WalAccessCounter::default();
        counter.operation("reader decisions").run(|| {
            let literal: Result<(), _> =
                fallback_result(|| Err("literal reader refusal"), |reason| *reason);
            assert!(literal.is_err());
            let constructed: Result<(), _> = fallback_result(
                || {
                    seed_fallback("instrumented reader refusal");
                    Err("instrumented reader refusal")
                },
                |reason| *reason,
            );
            assert!(constructed.is_err());
            fallback_result(|| Ok::<_, &'static str>(()), |reason| *reason).unwrap();
        });
        assert_eq!(
            counter.snapshot()[0].seed_fallbacks,
            ["literal reader refusal", "instrumented reader refusal"]
        );
    }

    #[test]
    fn operation_handoff_is_isolated_and_restored_on_unwind() {
        let counter = WalAccessCounter::default();
        let outer = counter.operation("outer");
        let inner = counter.operation("inner");
        outer.run(|| {
            let job = inherit(|| record_read(7, 8));
            std::thread::spawn(job).join().unwrap();
            let _ = std::panic::catch_unwind(|| {
                inner.run(|| {
                    record_read(9, 10);
                    panic!("restore the outer operation");
                })
            });
            record_read(11, 12);
        });
        record_read(13, 14);
        let snapshot = counter.snapshot();
        assert_eq!(
            snapshot[0]
                .reads
                .iter()
                .map(|read| read.offset)
                .collect::<Vec<_>>(),
            [7, 11]
        );
        assert_eq!(
            snapshot[1]
                .reads
                .iter()
                .map(|read| read.offset)
                .collect::<Vec<_>>(),
            [9]
        );
    }

    #[test]
    fn future_keeps_its_operation_when_polls_move_threads() {
        let counter = WalAccessCounter::default();
        let operation = counter.operation("moving future");
        let mut polls = 0;
        let mut future = Box::pin(operation.run_future(std::future::poll_fn(move |_| {
            polls += 1;
            record_read(polls, 1);
            if polls == 1 {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })));
        let mut future = std::thread::spawn(move || {
            let mut context = TaskContext::from_waker(std::task::Waker::noop());
            assert!(future.as_mut().poll(&mut context).is_pending());
            record_read(99, 1);
            future
        })
        .join()
        .unwrap();
        std::thread::spawn(move || {
            let mut context = TaskContext::from_waker(std::task::Waker::noop());
            assert!(future.as_mut().poll(&mut context).is_ready());
            record_read(99, 1);
        })
        .join()
        .unwrap();
        let measured = counter.snapshot();
        assert_eq!(
            measured[0]
                .reads
                .iter()
                .map(|read| read.offset)
                .collect::<Vec<_>>(),
            [1, 2]
        );
    }

    #[test]
    fn writer_recovery_and_replay_are_separate_from_source_reads() {
        let root =
            broca_test_support::scratch_root().join(format!("wal-access-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("session.wal");
        let record = WalRecord::TurnFinished {
            step_id: broca_protocol::StepId(1),
        };
        let lineage = WalLineageId::from_bytes([3; 16]);
        let mut bytes = encode_lineage(lineage).to_vec();
        bytes.extend(encode(1, 1, &record).unwrap());
        std::fs::write(&path, &bytes).unwrap();
        let counter = WalAccessCounter::default();
        counter.operation("open and read").run(|| {
            let wal = FileWal::open(&path).unwrap();
            writer_open(&path, false, || wal.replay()).unwrap();
            crate::replay_file(&path, u64::MAX).unwrap();
        });
        let snapshot = counter.snapshot();
        let measured = &snapshot[0];
        let open = &measured.writer_opens[0];
        assert_eq!(measured.writer_opens.len(), 1);
        assert_eq!(open.opens, 1);
        assert_eq!(open.session_length, bytes.len() as u64);
        assert_eq!(open.bytes, bytes.len() as u64 * 2);
        assert_eq!(open.frames_decoded, 4);
        assert_eq!(measured.frames_decoded, 6);
        assert_eq!(
            measured
                .reads
                .iter()
                .filter(|read| read.class == ReadClass::Origin)
                .count(),
            1
        );
        assert!(measured
            .reads
            .iter()
            .filter(|read| read.writer_open.is_some())
            .all(|read| read.class == ReadClass::WriterOpen));
        std::fs::remove_dir_all(root).unwrap();
    }
}
