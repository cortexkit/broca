//! `memory.report` wire types: what the daemon's open sessions and shared caches hold in
//! memory, sizes and counts only.

use serde::{Deserialize, Serialize};

/// `memory.report` request parameters: none. An absent, null or empty params object all mean the
/// same report; an unknown field is refused.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryReportParams {}

/// The `memory.report` answer: what the daemon's open sessions and shared caches hold in memory,
/// estimated by walking them when the report is asked for.
///
/// Sizes and counts only: it never carries content, message ids or anything else from a
/// transcript. A session is named by its store address.
///
/// Sessions still open in the old write-ahead log are counted but not sized by this report.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryReport {
    /// Every session open in the session store (`broca-session-store`), largest first.
    pub sessions: Vec<MemorySession>,
    /// How many of them had a run in progress: those hold their run's entries and request on
    /// top of what an idle session holds, so compare like with like.
    pub running_sessions: u64,
    /// Sessions open in the session store whose actor didn't answer in time, or was closing;
    /// counted, not sized.
    #[serde(default)]
    pub unanswered_sessions: u64,
    /// Sessions still open in the old write-ahead log are counted but not sized.
    pub legacy_sessions: u64,
    /// The sizes of every session in `sessions` added up, by holder.
    pub totals: MemoryHolders,
    /// The addresses of the ten largest sessions, largest first.
    pub largest: Vec<String>,
    /// The setup parts sessions hold decoded, each with how many sessions share it.
    pub setup_parts: Vec<MemorySetupPart>,
    /// Read-only loads of sessions no actor holds (each a session's open log and the folds built
    /// from it), kept so the next read of the same session needn't load it again.
    pub closed_session_views: MemoryCache,
    /// Facts of runs that finished, kept for status reads.
    pub settled_runs: MemoryCache,
    /// Each setup-part store's held bytes and how often it read the disk since broca started:
    /// one per storage class and mode.
    #[serde(default)]
    pub setup_stores: Vec<MemorySetupStore>,
}

/// One session open in the session store (`broca-session-store`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemorySession {
    /// The session's store address.
    pub address: String,
    /// The session class its first run froze; absent for a session created without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_class: Option<String>,
    /// Messages in its window.
    pub window_messages: u64,
    /// Whether a run was in progress when the report was taken.
    pub run_in_progress: bool,
    /// The id of that run. A restart gate names it before the restart and reads its outcome
    /// after; absent when no run is in progress.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Its estimated bytes by holder.
    pub holders: MemoryHolders,
    /// The rendered body of the request in flight, in bytes. Absent: the body lives in the
    /// provider's transport, which the report doesn't reach yet; `request_body_untracked` says so
    /// while a model call is in flight.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_bytes: Option<u64>,
    /// Why `request_body_bytes` is absent while a model call is in flight.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_untracked: Option<String>,
}

/// Estimated bytes by holder.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryHolders {
    /// The window's messages.
    pub messages: u64,
    /// Each window message's entry facts.
    pub entry_facts: u64,
    /// The open log's records kept in memory.
    pub kept_records: u64,
    /// The kept records' buffer beyond the records it holds.
    pub kept_records_capacity: u64,
    /// The open log's id index.
    pub id_index: u64,
    /// Sealed entries read by id and kept.
    pub sealed_entries: u64,
    /// What a run on the compaction contract keeps of its transcript between steps: the
    /// compaction provider's entries for the window's messages, and the message states they were
    /// built from.
    pub compaction_entries: u64,
    /// The session's image index: what the session's actor keeps to render the media its
    /// records name, such as the media it captured, their aging, and where each medium is
    /// stored. The member keeps the name it had when a per-run media fold held this, so the
    /// report's readers keep reading it.
    pub media_fold: u64,
    /// The request of a model call in flight: its prompt and options, without the tool list,
    /// which `setup_parts` counts.
    pub request_in_flight: u64,
    /// Everything else the session's actor holds.
    pub rest: u64,
    /// The sum of the above.
    pub total: u64,
}

impl MemoryHolders {
    /// The holders with `total` set to their sum.
    pub fn summed(mut self) -> Self {
        self.total = self.messages
            + self.entry_facts
            + self.kept_records
            + self.kept_records_capacity
            + self.id_index
            + self.sealed_entries
            + self.compaction_entries
            + self.media_fold
            + self.request_in_flight
            + self.rest;
        self
    }

    /// Adds `other` holder by holder.
    pub fn add(&mut self, other: &Self) {
        self.messages += other.messages;
        self.entry_facts += other.entry_facts;
        self.kept_records += other.kept_records;
        self.kept_records_capacity += other.kept_records_capacity;
        self.id_index += other.id_index;
        self.sealed_entries += other.sealed_entries;
        self.compaction_entries += other.compaction_entries;
        self.media_fold += other.media_fold;
        self.request_in_flight += other.request_in_flight;
        self.rest += other.rest;
        self.total += other.total;
    }
}

/// One setup part held decoded.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemorySetupPart {
    /// What the part holds: `tools`, `system_text` or `plan_manifest` (the rest of a plan's
    /// frozen manifest).
    pub kind: String,
    /// The estimated bytes of one decoded copy.
    pub decoded_bytes: u64,
    /// Decoded copies held.
    pub decoded_copies: u64,
    /// Open sessions whose runs use it.
    pub sessions: u64,
}

/// One setup-part store: the parts of one storage class's sessions in one mode. It holds the
/// parts read or stored recently up to a byte bound, and reads the others from disk.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemorySetupStore {
    /// The storage class: `normal`, `durable` or `ephemeral`.
    pub class: String,
    /// `encrypted` or `plain`.
    pub mode: String,
    /// Stored parts held in memory, and their bytes.
    pub held_parts: u64,
    pub held_bytes: u64,
    /// Bytes of parts made but not stored yet.
    pub unstored_bytes: u64,
    /// Parts read from disk because memory didn't hold them, and their bytes, since broca
    /// started.
    pub store_reads: u64,
    pub store_read_bytes: u64,
    /// Of those reads, the ones of a part memory held until its byte bound dropped it recently:
    /// reads a larger bound would have saved.
    pub reads_after_eviction: u64,
    /// Parts decoded from their bytes because no holder kept them decoded, and the bytes
    /// decoded, since broca started.
    pub decodes: u64,
    pub decoded_bytes: u64,
}

/// A bounded cache: its caps and what it holds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryCache {
    /// Entries held.
    pub entries: u64,
    /// The most entries it keeps.
    pub max_entries: u64,
    /// The bytes it counts against its byte cap, as it counts them; absent for a cache capped by
    /// entries only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counted_bytes: Option<u64>,
    /// Its byte cap; absent for a cache capped by entries only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    /// The bytes it holds, estimated by walking its entries.
    pub estimated_bytes: u64,
    /// For the closed-session view cache: how many views this process loaded and didn't keep
    /// because each alone was larger than the byte cap. Each read of such a session while it is
    /// closed loads it again. Absent for other caches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub views_over_cap: Option<u64>,
    /// For the closed-session view cache: views loaded from storage since broca started, and
    /// their walked bytes. Absent for other caches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loads: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loaded_bytes: Option<u64>,
    /// Of those loads, the ones that replaced a kept view of the same session because the
    /// session was written since that view loaded. Absent for other caches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reloads: Option<u64>,
}
