//! Shared materialized-store conformance tests.
//!
//! The functions in this module are generic over the stable `SessionStore` trait so
//! every backend is checked against the same WAL projection invariants. The module
//! lives in `broca-store` behind a test-only feature because the SQLite crate already
//! owns the reference tests and all required test fixtures; a separate dev crate
//! would add another workspace member without reducing coupling.

use std::sync::atomic::{AtomicU64, Ordering};

use broca_core::{ChargeBasis, ExportCursor, ExportError, ExportIndex};
use broca_protocol::{
    caller::RunFinishReason,
    content::{ContentBlock, Message, Role},
    request::ModelHandle,
    BindIdentity, RunId, Usage,
};
use broca_wal::{AuthSelection, FramedRecord, RunConfig, WalRecord};

pub mod migration_order;

mod billing_floor;
pub use billing_floor::billing_floor_preserves_local_and_new_runs;

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Run the usage-export conformance suite against a fresh store. This is intentionally small
/// but exercises the shared epoch, cursor, immutable-fact, and idempotent-insert contract for
/// every backend.
pub fn assert_export_conformance<S, F>(make_store: F)
where
    S: ExportIndex,
    F: Fn() -> S,
{
    let store = make_store();
    let session = session("export-conformance");
    let run_id = RunId("r".into());
    let mut records = base_records(&session);
    records.push(framed(
        1,
        WalRecord::RunFinished {
            interrupt_reason: None,
            reason: RunFinishReason::Completed,
            ts_ms: Some(2),
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ));
    let resolver = |_: &str, _: &str| (ChargeBasis::Unknown, 1);
    store
        .emit_run(&records, &run_id, &resolver)
        .expect("usage export emission");
    store
        .emit_run(&records, &run_id, &resolver)
        .expect("repeated usage export emission");
    assert_eq!(store.high_water(), 1);
    let page = store.read_page(None, 1000).expect("usage export page read");
    assert_eq!(page.source_epoch, 1);
    assert_eq!(page.high_water, 1);
    assert_eq!(page.facts.len(), 1);
    assert_eq!(page.facts[0].export_seq, 1);
    assert_eq!(page.facts[0].segment.as_session().unwrap().run_id, run_id);
    assert_eq!(
        page.next,
        Some(ExportCursor {
            source_instance_id: page.source_instance_id.clone(),
            source_epoch: 1,
            export_seq: 1
        })
    );
    let exhausted = store
        .read_page(page.next.clone(), 1000)
        .expect("same-source cursor page read");
    assert!(exhausted.facts.is_empty());
    assert_eq!(exhausted.source_instance_id, page.source_instance_id);

    let replaced_source = store
        .read_page(
            Some(ExportCursor {
                source_instance_id: "replaced-export-source".into(),
                source_epoch: page.source_epoch,
                export_seq: 0,
            }),
            1000,
        )
        .expect_err("a cursor from another source instance must reset");
    assert_eq!(
        replaced_source,
        ExportError::EpochChanged {
            source_instance_id: page.source_instance_id.clone(),
            source_epoch: page.source_epoch,
            high_water: page.high_water,
        }
    );

    export_account_ref_round_trips_and_legacy_absent(make_store());
}

pub(crate) fn session(label: &str) -> BindIdentity {
    let n = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
    BindIdentity::new(
        format!(
            "/tmp/broca-store-conformance-{label}-{}-{n}",
            std::process::id()
        ),
        "harness",
        "session",
    )
}

fn framed(seq: u64, record: WalRecord) -> FramedRecord {
    FramedRecord {
        seq,
        fence: 1,
        record,
    }
}

pub(crate) fn run_started(
    seq: u64,
    run_id: &str,
    session: &BindIdentity,
    input: &str,
    model: ModelHandle,
) -> FramedRecord {
    framed(
        seq,
        WalRecord::RunStarted {
            message_marks: None,
            compaction_plan_version: None,
            sent_by: None,
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            ts_ms: None,
            run_id: RunId(run_id.into()),
            session: session.clone(),
            config: Box::new(RunConfig {
                global_system: None,
                account_route: None,
                route_decision_id: None,
                served_provider: None,
                account_limit_pause: false,
                turn_directives_v1: None,
                render_tags: false,
                computed_tags: None,
                model,
                tools: vec![].into(),
                tool_choice: Default::default(),
                generation: Default::default(),
                response_format: Default::default(),
                provider_options: serde_json::Value::Null,
                cache_tiers: serde_json::Value::Null,
                context_epoch: 0,
                renderer_version: 1,
                frozen_render_config: serde_json::Value::Null,
                auth_selection: None,
                tools_were_omitted: false,
                budget: None,
                context_limit: None,
                work_class: None,
                variant: None,
                service_tier: None,
                stop_when: Vec::new(),
                context_management: None,
            }),
            input: vec![Message {
                turn_system: false,
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                tags: Default::default(),
                role: Role::User,
                content: vec![ContentBlock::Text { text: input.into() }],
            }]
            .into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        },
    )
}

/// A run frozen with a vault `account_ref` round-trips it through the export fact verbatim, and a
/// run with no vault account stores the fact WITHOUT an `account_ref` key (the legacy/unlabeled
/// shape) that reads back as `None` — identically on every backend.
pub fn export_account_ref_round_trips_and_legacy_absent<S>(store: S)
where
    S: ExportIndex,
{
    let resolver = |_: &str, _: &str| (ChargeBasis::Unknown, 1);

    // Stamped: the frozen vault account_id rides the fact verbatim.
    let stamped_session = session("export-account-ref");
    let stamped_run = RunId("acct-stamped".into());
    let stamped =
        export_records_with_account(&stamped_session, &stamped_run, Some("acct-uuid-321"));
    store
        .emit_run(&stamped, &stamped_run, &resolver)
        .expect("emit stamped fact");
    let page = store.read_page(None, 1000).expect("read stamped page");
    let stamped_fact = page
        .facts
        .iter()
        .find(|fact| fact.segment.as_session().unwrap().run_id == stamped_run)
        .expect("stamped fact present");
    assert_eq!(
        stamped_fact
            .segment
            .as_session()
            .unwrap()
            .account_ref
            .as_deref(),
        Some("acct-uuid-321"),
        "the frozen vault account_id round-trips verbatim through the export fact"
    );

    // Unlabeled: a credential with no vault account stores no account_ref key, reading back None.
    let bare_session = session("export-account-absent");
    let bare_run = RunId("acct-bare".into());
    let bare = export_records_with_account(&bare_session, &bare_run, None);
    store
        .emit_run(&bare, &bare_run, &resolver)
        .expect("emit bare fact");
    let page = store.read_page(None, 1000).expect("read bare page");
    let bare_fact = page
        .facts
        .iter()
        .find(|fact| fact.segment.as_session().unwrap().run_id == bare_run)
        .expect("bare fact present");
    assert_eq!(
        bare_fact.segment.as_session().unwrap().account_ref,
        None,
        "a run with no vault account reads back with account_ref absent"
    );
    // The stored shape omits the key entirely (legacy rows stay unlabeled, never null/empty):
    // re-serializing the read-back segment must not introduce an account_ref member.
    let reserialized = serde_json::to_value(&bare_fact.segment).expect("reserialize bare segment");
    assert!(
        reserialized.get("account_ref").is_none(),
        "an absent account_ref stays omitted from the stored fact shape"
    );
}

/// A terminal run whose frozen `AuthSelection` carries `account_ref` when `account_ref` is
/// `Some`, and a credential with no vault account when it is `None`.
fn export_records_with_account(
    session: &BindIdentity,
    run_id: &RunId,
    account_ref: Option<&str>,
) -> Vec<FramedRecord> {
    let mut start = run_started(0, &run_id.0, session, "hi", ModelHandle::new("stub", "m"));
    if let WalRecord::RunStarted { config, .. } = &mut start.record {
        config.auth_selection = Some(AuthSelection {
            billing_vault_facts: None,
            auth_method: None,
            credential_id: "chatgpt:openai".into(),
            account_ref: account_ref.map(str::to_string),
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        });
    }
    vec![
        start,
        framed(
            1,
            WalRecord::RunFinished {
                interrupt_reason: None,
                reason: RunFinishReason::Completed,
                ts_ms: Some(2),
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        ),
    ]
}

pub(crate) fn base_records(session: &BindIdentity) -> Vec<FramedRecord> {
    vec![run_started(
        0,
        "r",
        session,
        "hi",
        ModelHandle::new("stub", "m"),
    )]
}
