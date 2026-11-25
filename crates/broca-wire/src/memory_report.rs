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
    /// How `session.head` of closed sessions was answered from kept heads, why heads weren't
    /// kept, and what the kept heads hold. Absent from a broca that keeps no heads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kept_heads: Option<KeptHeadCounts>,
    /// Subscriptions waiting for a closed session to reopen.
    #[serde(default)]
    pub parked_subscribers: ParkedSubscriberCounts,
    /// Lifetime store actor closes, by reason (`parked` means subscribers kept their streams).
    #[serde(default)]
    pub actor_close_reasons: std::collections::BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParkedSubscriberCounts {
    pub sessions: u64,
    pub subscriptions: u64,
}

/// `session.head` reads of sessions no actor holds since broca started: those a kept head
/// answered, those that loaded the session's view, and why heads weren't kept; and the heads kept
/// now.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeptHeadCounts {
    /// Reads a kept head answered.
    pub hits: u64,
    /// Reads that built the head from the session's view, by why no kept head answered:
    /// `ephemeral` (a session that expires is never kept), `no_position` (the session's position
    /// couldn't be read cheaply), `not_kept` (no head kept at the session's position).
    pub misses: std::collections::BTreeMap<String, u64>,
    /// Heads kept, by where they were built: `read` (by a head read of the closed session) or
    /// `close` (by its actor as it closed idle).
    pub kept: std::collections::BTreeMap<String, u64>,
    /// Heads an idle-closing actor or a read built and didn't keep, by why: `unsettled` (a run
    /// active, paused or waiting), `ephemeral`, `unsynced` (records the actor appended were not yet
    /// durable), `pending` (the head needed to wait on something), `storage` (it needed records
    /// not in memory), `failed`.
    pub not_kept: std::collections::BTreeMap<String, u64>,
    /// The heads kept now: how many, their cap, and their estimated bytes, walked when the
    /// report is asked for.
    #[serde(default)]
    pub held: MemoryCache,
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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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
    /// For the closed-session view cache: reads served a kept view, or a load another read had
    /// already started, instead of loading one. Absent for other caches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hits: Option<u64>,
    /// For the closed-session view cache: the same counts by the read or check that loaded each
    /// view (`run.result`, `session.read`, `expiry` and so on). Absent for other caches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loads_by_reason: Option<std::collections::BTreeMap<String, ViewLoads>>,
    /// For the closed-session view cache: the sessions whose loads read the most bytes since
    /// broca started, most first, at most ten. Absent for other caches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_sessions: Option<Vec<SessionViewLoads>>,
    /// For the closed-session view cache: how many reads an open session's actor couldn't
    /// answer from memory since broca started, by the read and the source line that needed
    /// storage (`session.read @ crates/.../read.rs:532`). Each such read is served from the
    /// session as stored, which loads a view. Absent when there were none, and for other caches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stored_reads: Option<std::collections::BTreeMap<String, u64>>,
}

/// One closed session's view loads since broca started.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionViewLoads {
    /// The session's store address.
    pub address: String,
    /// Views loaded, their walked bytes, and the loads that replaced a view of an older position.
    pub loads: u64,
    pub loaded_bytes: u64,
    pub reloads: u64,
    /// The read or check that loaded its latest view.
    pub reason: String,
}

/// Closed-session views loaded for one reason since broca started.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewLoads {
    /// Views loaded from storage.
    pub loads: u64,
    /// Their walked bytes when they loaded.
    pub loaded_bytes: u64,
    /// Of those, the ones that replaced a kept view of a session written since.
    pub reloads: u64,
    /// Reads for this reason served a kept view, or a load already started, instead of loading.
    #[serde(default)]
    pub hits: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parking_counts_round_trip_and_default_for_older_memory_reports() {
        let report = MemoryReport {
            parked_subscribers: ParkedSubscriberCounts {
                sessions: 1,
                subscriptions: 2,
            },
            actor_close_reasons: [("parked".into(), 7)].into(),
            ..MemoryReport::default()
        };
        let mut wire = serde_json::to_value(&report).unwrap();
        assert_eq!(
            wire["parked_subscribers"],
            serde_json::json!({"sessions": 1, "subscriptions": 2})
        );
        assert_eq!(
            wire["actor_close_reasons"],
            serde_json::json!({"parked": 7})
        );
        assert_eq!(
            serde_json::from_value::<MemoryReport>(wire.clone()).unwrap(),
            report
        );
        let fields = wire.as_object_mut().unwrap();
        fields.remove("parked_subscribers");
        fields.remove("actor_close_reasons");
        assert_eq!(
            serde_json::from_value::<MemoryReport>(wire).unwrap(),
            MemoryReport::default()
        );
    }
}
