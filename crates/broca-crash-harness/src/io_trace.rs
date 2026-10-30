//! Filesystem-attempt tracing for the power-loss harness. The backend remains
//! responsible for persistence semantics; this layer records even failed probes.
use broca_wal as wal_api;
include!("io_trace_backend.rs");
