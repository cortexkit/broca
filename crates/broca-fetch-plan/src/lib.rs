//! Fetch-plan admission for broca.
//!
//! A session's first `session.send` may carry a fetch plan instead of a tool list and system
//! prompt. Broca then fetches each provider's catalog and system text itself in parallel
//! under one deadline. If an optional tool provider is absent, it recomposes system text
//! against the surviving tools. It checks the answers and freezes exactly what it got
//! in the session's `RunStarted` (`broca_wal::PlanManifest`). Every later episode and every
//! resume renders from that manifest and never fetches again.
//!
//! [`plan::parse_plan`] reads and checks the plan before anything is fetched; [`admit::admit`]
//! fetches and checks the answers through a [`admit::Fetcher`] and returns the manifest or a
//! [`refusal::Refusal`]. Nothing in this crate writes.

pub mod admit;
pub mod plan;
pub mod refusal;

#[cfg(test)]
mod compaction_tests;

pub use admit::{
    admit, baseline_facts, join_system_texts, Admitted, BaselineFacts, DescribeAnswer, FetchError,
    Fetcher, PlanAdmitter, ADMISSION_DEADLINE, CATALOG_OP, DECLARE_OP, JOIN_SEPARATOR,
    JOIN_VERSION,
};
pub use plan::{
    check_later_plan, check_send_members, parse_plan, plan_identity, plan_senders,
    CompactionSupport, Plan, PlanSender, PlanTemplate, SlotType, SENDERS_MEMBER,
};
pub use refusal::{Refusal, Unanswered};
