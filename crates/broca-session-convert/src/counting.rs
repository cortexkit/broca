//! A backend that counts what a read takes from storage: the sealed objects it fetches (segments
//! and directory pages) and the open-log bytes it reads. A read whose cost should stay bounded
//! whatever the session's age is measured by these counts: a whole-history read fetches every
//! segment, a page read only those its page lies in.

use std::ops::Range;
use std::sync::Mutex;

use broca_session_store::backend::{BackendError, SessionAddr, SessionBackend, Version};
use broca_session_store::codec::Id128;

/// What reads took from storage since the counts were last taken.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reads {
    /// The sealed objects fetched, by name, in the order fetched.
    pub objects: Vec<String>,
    /// The bytes of those objects.
    pub object_bytes: u64,
    /// How many times an open log was read, and how many bytes, reads of only its checkpoint
    /// frame aside.
    pub log_reads: u64,
    pub log_bytes: u64,
    /// How many reads took only a log's checkpoint frame (see
    /// [`broca_session_store::log::reads_checkpoint_frame_only`]), and how many bytes.
    pub checkpoint_frame_reads: u64,
    pub checkpoint_frame_bytes: u64,
}

/// `inner`, counting reads.
pub struct Counting<B> {
    pub inner: B,
    reads: Mutex<Reads>,
}

impl<B> Counting<B> {
    pub fn new(inner: B) -> Self {
        Self {
            inner,
            reads: Mutex::default(),
        }
    }

    /// The reads since the last call, which start the count again.
    pub fn take(&self) -> Reads {
        std::mem::take(&mut *self.reads.lock().expect("counts lock"))
    }
}

impl<B: SessionBackend> SessionBackend for Counting<B> {
    async fn read_head(&self, s: &SessionAddr) -> Result<Option<(Vec<u8>, Version)>, BackendError> {
        self.inner.read_head(s).await
    }

    async fn write_head(
        &self,
        s: &SessionAddr,
        head: &[u8],
        expected: Option<Version>,
        next: Version,
    ) -> Result<Version, BackendError> {
        self.inner.write_head(s, head, expected, next).await
    }

    async fn create_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        self.inner.create_log(s, log, at).await
    }

    async fn read_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        range: Range<u64>,
    ) -> Result<Vec<u8>, BackendError> {
        let bytes = self.inner.read_log(s, log, range.clone()).await?;
        // Only a read of exactly the first frame asks for the log's length, to tell it from a
        // read of a log that holds nothing else; a failure to learn it counts a log read.
        let checkpoint_frame = range.start == 0
            && broca_session_store::log::first_frame_len(&bytes)
                .is_some_and(|len| len as usize == bytes.len())
            && match self.inner.log_end(s, log).await {
                Ok(log_len) => broca_session_store::log::reads_checkpoint_frame_only(
                    range.start,
                    &bytes,
                    log_len,
                ),
                Err(_) => false,
            };
        let mut reads = self.reads.lock().expect("counts lock");
        if checkpoint_frame {
            reads.checkpoint_frame_reads += 1;
            reads.checkpoint_frame_bytes += bytes.len() as u64;
        } else {
            reads.log_reads += 1;
            reads.log_bytes += bytes.len() as u64;
        }
        Ok(bytes)
    }

    async fn log_end(&self, s: &SessionAddr, log: &Id128) -> Result<u64, BackendError> {
        self.inner.log_end(s, log).await
    }

    async fn append_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        bytes: &[u8],
        expected_end: u64,
        at: Version,
        barrier: bool,
    ) -> Result<u64, BackendError> {
        self.inner
            .append_log(s, log, bytes, expected_end, at, barrier)
            .await
    }

    async fn truncate_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        to: u64,
        expected_end: u64,
        at: Version,
    ) -> Result<(), BackendError> {
        self.inner.truncate_log(s, log, to, expected_end, at).await
    }

    async fn delete_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        self.inner.delete_log(s, log, at).await
    }

    async fn delete_session(&self, s: &SessionAddr, at: Version) -> Result<(), BackendError> {
        self.inner.delete_session(s, at).await
    }

    async fn put_object(
        &self,
        s: &SessionAddr,
        name: &str,
        bytes: &[u8],
        at: Version,
    ) -> Result<(), BackendError> {
        self.inner.put_object(s, name, bytes, at).await
    }

    async fn sync_objects(
        &self,
        s: &SessionAddr,
        change: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        self.inner.sync_objects(s, change, at).await
    }

    async fn get_object(
        &self,
        s: &SessionAddr,
        name: &str,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        let bytes = self.inner.get_object(s, name).await?;
        let mut reads = self.reads.lock().expect("counts lock");
        reads.objects.push(name.to_owned());
        reads.object_bytes += bytes.as_ref().map_or(0, |bytes| bytes.len() as u64);
        Ok(bytes)
    }

    // A sealed segment is read a range at a time. Forwarding keeps that, so a range costs its
    // own bytes here as in production, not a fetch of the whole object, which the trait's
    // default makes.
    async fn object_size(&self, s: &SessionAddr, name: &str) -> Result<Option<u64>, BackendError> {
        self.inner.object_size(s, name).await
    }

    async fn get_object_range(
        &self,
        s: &SessionAddr,
        name: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        let bytes = self.inner.get_object_range(s, name, range).await?;
        let mut reads = self.reads.lock().expect("counts lock");
        reads.objects.push(name.to_owned());
        reads.object_bytes += bytes.as_ref().map_or(0, |bytes| bytes.len() as u64);
        Ok(bytes)
    }

    async fn delete_object(
        &self,
        s: &SessionAddr,
        name: &str,
        at: Version,
    ) -> Result<(), BackendError> {
        self.inner.delete_object(s, name, at).await
    }

    async fn delete_change(
        &self,
        s: &SessionAddr,
        change: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        self.inner.delete_change(s, change, at).await
    }

    async fn put_setup(&self, name: &str, bytes: &[u8]) -> Result<(), BackendError> {
        self.inner.put_setup(name, bytes).await
    }

    async fn get_setup(&self, name: &str) -> Result<Option<Vec<u8>>, BackendError> {
        self.inner.get_setup(name).await
    }

    async fn list_setup(&self) -> Result<Vec<String>, BackendError> {
        self.inner.list_setup().await
    }
}
