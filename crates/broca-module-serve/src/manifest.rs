//! The broca module capability manifest.
//!
//! broca registers as a ManagementSurface exposing the session/run control ops as
//! request/response operations, plus `session.subscribe` as a Stream observability surface
//! (a held-open event stream, not a request/response op). subc routes a client to this
//! surface via `route.bind { target: ManagementSurface{ module_id: "broca" } }`.

use crate::catalog_refresh::CATALOG_POLL_INTERVAL;
use crate::consumer_routes::ConsumerRoutes;
use cortexkit_role_routing::declarations::{
    ConsumerDeclaration, ConsumerDeclarations, DemandDefaults, HarnessLimits, RunnerDeclaration,
};
use cortexkit_role_routing::placement::HarnessCapability;
use std::collections::BTreeMap;
use subc_protocol::manifest::Concurrency;
use subc_protocol::manifest::{
    Bindings, CapabilityDeclarations, IdentityBinding, IdentityScope, LaunchNonceSource,
    ManagementOperation, ManagementOperationKind, ManifestProvenance, ModuleManifest,
    ObservabilityKind, ObservabilitySurface, ProviderRole, StorageBinding, StorageKind,
    StorageScope, TrustTier,
};
use subc_protocol::manifest::{
    SelfSignalDeclaration, SelfSignalEffect, SelfSignalKind, SignalAnchor, SignalCadence,
};
use subc_protocol::PROTOCOL_VERSION;

/// The request/response ops, classified Query vs Mutate.
pub const OP_SEND_STATUS: &str = broca_wire::OP_SEND_STATUS;
pub const OP_SESSION_SEND_STATUS: &str = broca_wire::OP_SESSION_SEND_STATUS;
pub const OP_SEND: &str = "session.send";
pub const OP_IMPORT: &str = "session.import";
/// Record a moved head's compaction view as its imported session's applied view, before the
/// session's first step.
pub const OP_INSTALL_VIEW: &str = broca_wire::head_import::OP_SESSION_INSTALL_VIEW;
pub const OP_RETRACT: &str = "session.retract";
/// Restore a session kept in the session store, or every such session, from the files its store
/// holds after a backup came back. Only the operator calls it, through `broca-session
/// restore-from-backup`.
pub const OP_RESTORE_FROM_BACKUP: &str = "session.restore_from_backup";
/// Read which message and part each part number of a session kept in the session store belongs
/// to. Only the operator calls it, through `broca-session tag-map`; it is outside the runner role.
pub const OP_TAG_MAP: &str = broca_wire::tag_map::OP_SESSION_TAG_MAP;
pub const OP_REQUEST_DUMP: &str = broca_wire::request_dump::OP_SESSION_REQUEST_DUMP;
/// Deletes one session kept in the session store that holds no work; operator only, outside the
/// runner role.
pub const OP_SESSION_DELETE: &str = broca_wire::session_delete::OP_SESSION_DELETE;
pub const OP_CANCEL: &str = "run.cancel";
pub const OP_STATUS: &str = "run.status";
pub const OP_RUN_ADMISSION: &str = "run.admission";
pub const OP_RUN_EVENT_GET: &str = "run.event.get";
pub const OP_READ: &str = "session.read";
/// Daemon-scoped read of the credential vault's listing under broca's grants.
pub const OP_VAULT_AUDIT: &str = "vault.audit";
/// Daemon-scoped, operator-only read of what the open sessions and caches hold in memory.
pub const OP_MEMORY_REPORT: &str = "memory.report";
/// The held-open stream observability surface.
pub const OP_SUBSCRIBE: &str = "session.subscribe";
/// The `llm-runner/v1` operations broca exposes: role discovery, session head and baseline, run
/// results and tool-call history, and tag resolution through `session.resolve`.
pub const OP_ROLE_DESCRIBE: &str = broca_wire::OP_ROLE_DESCRIBE;
pub const OP_HEAD: &str = broca_wire::OP_SESSION_HEAD;
pub const OP_RUN_RESULT: &str = broca_wire::OP_RUN_RESULT;
pub const OP_RUN_TOOL_CALLS: &str = broca_wire::OP_RUN_TOOL_CALLS;
pub const OP_BASELINE: &str = broca_wire::OP_SESSION_BASELINE;
pub const OP_REFRESH: &str = cortexkit_role_llm_runner::ops::SESSION_REFRESH;
pub const OP_REFRESH_POLICY: &str = cortexkit_role_llm_runner::ops::SESSION_REFRESH_POLICY;
pub const OP_FLUSH_PREFIX: &str = cortexkit_role_llm_runner::ops::SESSION_FLUSH_PREFIX;
pub const OP_RELEASE: &str = cortexkit_role_llm_runner::ops::SESSION_RELEASE;
/// Re-send a session's most recent model request shaped to generate nothing, keeping the
/// provider's cached prefix alive between a caller's rounds.
pub const OP_WARM: &str = broca_wire::OP_SESSION_WARM;

/// Build broca's manifest for the HELLO registration.
/// Build provenance, present ONLY when the packaging path stamped it.
///
/// This is what a deploy gate reads to answer "is the running artifact the one I built", and it
/// replaces a ladder I otherwise reconstruct by hand after every placement: hunting a string or
/// symbol that discriminates the new build from the old, with a known-present control beside it.
/// That ladder fails in BOTH directions -- a private function inlines away and reads 0/0, and a
/// release that only DELETES code has no discriminator to find at all.
///
/// THE SENTINEL IS THE POINT, not ceremony. `option_env!` reads whatever was in the environment
/// at compile time, so an ambient `CK_BUILD_REV` from any cargo invocation would otherwise mint
/// a provenance claim this build has not earned. Requiring a separate stamp that only
/// `scripts/stage-release.sh` sets means an unstamped build declares NOTHING rather than
/// something plausible -- and a wrong sha is worse than a gap, because a gate treats it as
/// authoritative.
fn build_provenance() -> Option<ManifestProvenance> {
    // Absence of the stamp is the ordinary case: every dev build, every test, every `cargo run`.
    option_env!("CK_BUILD_STAMPED")?;

    // CONSTRUCTED THROUGH THE OWNING CRATE'S HELPER, which normalises sentinels to omission and
    // then validates canonical form: 40 lowercase hex for the sha, 64 for the lock digest. It
    // also fills `wire_crate_version` from subc-protocol's own version const, which closes the
    // gap this file previously recorded as parked -- the value is now READ rather than absent.
    //
    // `store_schema_version` stays None because BROCA HAS NO SCHEMA VERSION TO REPORT. The store
    // migrates by additive `ALTER TABLE ADD COLUMN` guarded by a `PRAGMA table_info` check, with
    // no version row and no migration head, so any string here would be invented rather than
    // read. Do not synthesise one from the column list; it would change silently under an
    // unrelated migration.
    match subc_protocol::manifest::build_provenance(
        option_env!("CK_BUILD_REV"),
        option_env!("CK_BUILD_LOCK_DIGEST"),
        None,
    ) {
        Ok(provenance) => Some(provenance.with_launch_nonce_source(launch_nonce_source())),
        // OMIT, AND SAY SO IN THE LOG. A malformed fact is worse than a gap because a gate treats
        // provenance as authoritative, so dropping it is right -- but dropping it SILENTLY is
        // the failure this whole surface exists to prevent: the artifact would then be
        // indistinguishable from an unstamped dev build, and the operator would go hunting a
        // string discriminator instead of fixing the stamp. The stamp is set by exactly one
        // script, so a form error means that script needs correcting, not this call site.
        Err(error) => {
            tracing::warn!(
                target: "lifecycle",
                "build provenance omitted, the stamp is malformed: {error}. \
                 scripts/stage-release.sh sets CK_BUILD_REV and CK_BUILD_LOCK_DIGEST; a full \
                 `git rev-parse HEAD` and a sha256 of Cargo.lock are the conforming forms."
            );
            None
        }
    }
}

/// Where this process read its launch nonce, as the stamped provenance block reports it: `fd`
/// (the pipe the daemon hands over) or `env` (the environment copy kept while modules move to
/// the pipe). The fleet census reads this to tell a module that has switched to the pipe from
/// one that has not.
///
/// Taken from the accessor's cached answer, the same one HELLO and route opens use, so it names
/// the source of the nonce broca actually presents. `None` when there is no nonce (broca was not
/// started by the daemon) or it could not be read; an absent field means "did not say", which is
/// honest, where a guess would count a module as switched when it is not. `None` too when broca
/// registered over its registration channel: it then presents no nonce at all.
pub fn launch_nonce_source() -> Option<LaunchNonceSource> {
    if broca_subc::registration::adopted() {
        return None;
    }
    let nonce = subc_os::launch_nonce().ok().flatten()?;
    Some(reported_launch_nonce_source(nonce.source()))
}

/// The wire name for an accessor source. Spelled through the accessor's own name rather than a
/// second table, so a source added to the accessor later is reported under its own name instead
/// of being folded into one of the two known today.
fn reported_launch_nonce_source(source: subc_os::LaunchNonceSource) -> LaunchNonceSource {
    LaunchNonceSource::from_wire_name(source.as_str())
}

pub fn manifest(module_id: &str) -> ModuleManifest {
    manifest_configured(module_id, &ConsumerRoutes::default())
}

/// The management request list is also the source of `health.check` request-counter names.
pub(crate) fn management_operations() -> Vec<ManagementOperation> {
    let mut operations = vec![
        ManagementOperation {
            name: broca_wire::diagnostics::OP.into(),
            kind: ManagementOperationKind::Query,
            description: Some("Read bounded, content-free diagnoses of broca's own degraded state.".into()),
        },
        ManagementOperation {
            name: crate::findings_watch::OP_EVENTS_GET.into(),
            kind: ManagementOperationKind::Query,
            description: Some("Fetch a findings_changed event's stored body by its key (7 day retention).".into()),
        },
        ManagementOperation {
            name: cortexkit_role_llm_runner::ops::SESSION_HISTORY.into(),
            kind: ManagementOperationKind::Query,
            description: Some("Page surviving session history as stored entries or display rows.".into()),
        },
        ManagementOperation {
            name: cortexkit_role_llm_runner::ops::SESSION_OUTPUT_REFS.into(),
            kind: ManagementOperationKind::Query,
            description: Some("Read the records of a session-owned output reference set.".into()),
        },
        ManagementOperation {
            name: cortexkit_role_llm_runner::ops::SESSION_OUTPUT_REF_READ.into(),
            kind: ManagementOperationKind::Query,
            description: Some("Read retained bytes of a session-owned output reference.".into()),
        },
        ManagementOperation {
            name: "session.upload_begin".into(),
            kind: ManagementOperationKind::Mutate,
            description: Some("Reserve session-owned attachment staging.".into()),
        },
        ManagementOperation {
            name: "session.upload_append".into(),
            kind: ManagementOperationKind::Mutate,
            description: Some("Append a bounded chunk to session-owned staging.".into()),
        },
        ManagementOperation {
            name: "session.upload_commit".into(),
            kind: ManagementOperationKind::Mutate,
            description: Some("Verify and commit a staged upload without admitting a send.".into()),
        },
        ManagementOperation {
            name: "session.upload_status".into(),
            kind: ManagementOperationKind::Query,
            description: Some("Read the state and received count of a staged upload.".into()),
        },
        ManagementOperation {
            name: OP_SEND_STATUS.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some("Read the durable fate of a send_id without admitting or delivering work.".into()),
        },
        ManagementOperation {
            name: OP_SESSION_SEND_STATUS.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some("Read the durable fate of a send_id without admitting or delivering work.".into()),
        },
        ManagementOperation {
            name: OP_SEND.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "Admit a prompt and run it to a terminal, or re-drive an existing run idempotently by send_id."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_IMPORT.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "Seed an EMPTY lineage with transcript history from another harness; refuses a non-empty lineage."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_RESTORE_FROM_BACKUP.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "Restore a session kept in the new storage, or every such session, after its files came back from a backup; operator only."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_TAG_MAP.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "Page which message and part each part number of a session kept in the new storage belongs to, ids and numbers only; operator only."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_REQUEST_DUMP.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "Rebuild the request each step of one run of a session kept in the new storage sent, and write the bodies that match their recorded digests to a private folder under the state root; operator only."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_SESSION_DELETE.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "Delete one session kept in the new storage that holds no run, queued send or unexported spend, as retention deletes an expired session; operator only."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_INSTALL_VIEW.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "Record the compaction view a moved head's provider applies as its imported session's view, before the session's first step."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_RETRACT.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "Withdraw a queued submission that has not yet started running.".to_string(),
            ),
        },
        ManagementOperation {
            name: OP_CANCEL.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "Stop an in-flight run; the lineage is sealed and is not resumable under the same send_id."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_STATUS.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "Point-read one run's state and terminal outcome, scoped to the owning session."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_RUN_ADMISSION.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some("Read one run's frozen admission metadata without session content.".to_string()),
        },
        ManagementOperation {
            name: OP_RUN_EVENT_GET.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "Fetch a session-owned run event's exact JCS bytes by its claim-check key (24 hour retention)."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_READ.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "Page the durable transcript. Lease-free and write-free, so it serves during a live run."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_ROLE_DESCRIBE.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "Describe the role the route was opened for: on an llm-runner/v1 route its ops, capability groups and read byte cap; on a classifier/v1 or extractor/v1 route its limits and the models it serves."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: cortexkit_role_classifier::ops::CLASSIFY_RUN.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "classifier/v1 routes only: answer a batch of items against a set of questions with one hosted decision model, idempotent by batch_id; no session is involved."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: cortexkit_role_extractor::ops::EXTRACT_RUN.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "extractor/v1 routes only: extract typed fields from a batch with one pinned model, idempotent by batch_id; no session is involved."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: "tool.catalog".into(),
            kind: ManagementOperationKind::Query,
            description: Some("List the sessionless model tools and their invocation modes.".into()),
        },
        ManagementOperation {
            name: "late_results".into(),
            kind: ManagementOperationKind::Query,
            description: Some("Read late model-tool results belonging to the stamped caller.".into()),
        },
        ManagementOperation {
            name: "late_results.ack".into(),
            kind: ManagementOperationKind::Mutate,
            description: Some("Acknowledge the stamped caller's late model-tool results.".into()),
        },
        ManagementOperation {
            name: "permissions.list".into(),
            kind: ManagementOperationKind::Query,
            description: Some("Permissions routes only: list private-content grants for the operator.".into()),
        },
        ManagementOperation {
            name: "permissions.revoke".into(),
            kind: ManagementOperationKind::Mutate,
            description: Some("Permissions routes only: revoke one private-content grant for the operator.".into()),
        },
        ManagementOperation {
            name: OP_HEAD.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "Read the session's lineage, newest ordinal, control head and last run state, without message bodies."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_RUN_RESULT.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "Read one run's state and, once it completed, its final message's text.".to_string(),
            ),
        },
        ManagementOperation {
            name: OP_RUN_TOOL_CALLS.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "Read a run's tool calls in dispatch order, without their result content.".to_string(),
            ),
        },
        ManagementOperation {
            name: OP_BASELINE.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "Read what the session froze at admission. Answered only to the session's owner."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_REFRESH.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "Accept a mid-session plan change and hold it pending, owner only."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_REFRESH_POLICY.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "Change the pending change's rung, owner only, idempotent on send_id."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_FLUSH_PREFIX.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "Drop the frozen prefix and apply any pending change at the next run, owner only."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_RELEASE.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "End the session: delete it once no run or queued send holds it, owner only."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_WARM.to_string(),
            kind: ManagementOperationKind::Mutate,
            description: Some(
                "Keep the provider's prompt cache warm: re-send the session's most recent request with no output, and report what it cost."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_VAULT_AUDIT.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "List the vault credentials broca's grants cover, and name every configured credential the vault did not list."
                    .to_string(),
            ),
        },
        ManagementOperation {
            name: OP_MEMORY_REPORT.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "Report, for the operator, the estimated bytes each open session and shared cache holds in memory: sizes and counts only."
                    .to_string(),
            ),
        },
    ];
    operations.push(ManagementOperation {
        name: cortexkit_role_llm_runner::ops::SESSION_MEDIA_READ.into(),
        kind: ManagementOperationKind::Query,
        description: Some("Read bounded media bytes referenced by the bound session.".into()),
    });
    operations
}

pub(crate) fn registered_management_operation(method: &str) -> bool {
    static NAMES: std::sync::OnceLock<std::collections::HashSet<String>> =
        std::sync::OnceLock::new();
    NAMES
        .get_or_init(|| {
            management_operations()
                .into_iter()
                .map(|op| op.name)
                .collect()
        })
        .contains(method)
}

pub(crate) fn observability_surfaces() -> Vec<ObservabilitySurface> {
    vec![ObservabilitySurface {
        name: OP_SUBSCRIBE.to_string(),
        kind: ObservabilityKind::Stream,
    }]
}

fn routing_declarations() -> ConsumerDeclarations {
    // The ChatGPT backend refuses max_output_tokens, so its harness does not promise
    // output_cap. Routing places it separately using the runner's current serves list.
    let mut capped_capabilities = BROCA_HARNESS_CAPABILITIES.to_vec();
    capped_capabilities.push(HarnessCapability::OutputCap);
    let mut runner = RunnerDeclaration::new();
    runner.follows_account_choice = true;
    runner = runner
        .with_harnesses(BROCA_HARNESSES.map(String::from).to_vec())
        .with_harness_limits(BTreeMap::from([
            (
                BROCA_HARNESS.into(),
                HarnessLimits::new().with_capabilities(capped_capabilities),
            ),
            (
                BROCA_UNCAPPED_HARNESS.into(),
                HarnessLimits::new().with_capabilities(BROCA_HARNESS_CAPABILITIES.to_vec()),
            ),
        ]));
    runner
        .typed_harness_limits()
        .expect("broca declares harness limits")
        .expect("broca's harness facts are valid producer data");
    let declarations = ConsumerDeclarations::new()
        .with_consumers(vec![ConsumerDeclaration::new(
            crate::llm_tools::complete::CONSUMER_ROOT,
        )
        .with_defaults(DemandDefaults::default().with_workload("quick"))])
        .with_runner(runner);
    declarations
        .validate_serving(&[cortexkit_role_routing::capabilities::SELECT_WORKLOAD])
        .expect("broca's routing declaration is valid");
    declarations
}

/// The harness for every route that honors an output cap, which is every route but ChatGPT's.
pub(crate) const BROCA_HARNESS: &str = "broca";

/// The harness for ChatGPT routes, whose backend refuses an output cap.
pub(crate) const BROCA_UNCAPPED_HARNESS: &str = "broca-uncapped";

/// The same programs are named by the routing declaration and the serves conformance check.
#[doc(hidden)]
pub const BROCA_HARNESSES: [&str; 2] = [BROCA_HARNESS, BROCA_UNCAPPED_HARNESS];

/// What both of broca's harnesses can do, as routing places tasks by them (routing contract
/// §16.2). Only [`BROCA_HARNESS`] adds `output_cap`.
const BROCA_HARNESS_CAPABILITIES: [HarnessCapability; 5] = [
    HarnessCapability::OneOff,
    HarnessCapability::ForkAnswer,
    HarnessCapability::SubmitTool,
    HarnessCapability::FinalText,
    HarnessCapability::Plans,
];

/// The manifest for a fully configured process: the resolved consumer routes shape `consumes`.
pub fn manifest_configured(module_id: &str, consumer_routes: &ConsumerRoutes) -> ModuleManifest {
    // BUILT THROUGH THE BUILDER, because `ModuleManifest` is `#[non_exhaustive]` upstream.
    //
    // The attribute is the point rather than an obstacle: a field ADDED upstream can no longer
    // break this file's compile, so the fleet stops paying a migration every time the manifest
    // grows. The cost is that anything this module has an opinion about must be set EXPLICITLY
    // through a named method, and anything unnamed silently takes the upstream default.
    //
    // So each `.method(None)` below is a DECLARATION, not decoration -- without it, a field this
    // module deliberately withholds would be indistinguishable from one nobody considered.
    // subc-protocol 0.19.0 moved trust_tier and bindings OFF the constructor and onto builder
    // methods. They are still DECLARED here, explicitly, for the reason the comment above gives:
    // a builder substitutes a default for anything omitted, so a field this module deliberately
    // sets would become indistinguishable from one nobody considered if it were dropped in the
    // migration. The two-argument form is the upstream shape; the two calls below restore what
    // the four-argument form used to carry positionally.
    ModuleManifest::builder(
        module_id.to_string(),
        env!("CARGO_PKG_VERSION").to_string(),
    )
    .trust_tier(Some(TrustTier::FirstParty))
    .bindings(Some(Bindings {
        // broca owns the per-session log + materialized store (its own schema).
        storage: StorageBinding {
            kind: StorageKind::Sqlite,
            scope: StorageScope::Project,
            owns_schema: true,
        },
        vault_grants: Vec::new(),
        // A run binds to a session: the project + session identity are required.
        identity: IdentityBinding {
            requires: vec![IdentityScope::Project, IdentityScope::Session],
            optional: Vec::new(),
            },
    }))
    .protocol_ver(PROTOCOL_VERSION)
    // CATALOG REFRESH IS ALWAYS DECLARED, and this field was WRONGLY ABSENT until the daemon
    // began serving it from catalog.list, at which point the absence became a fleet-visible
    // claim rather than a local omission.
    //
    // The comment here said broca had nothing to declare yet, and named the log append
    // and the store projection as the eventual first entries. That was wrong on the field's own
    // definition -- "a declared PERIODIC or event-driven behavior that shapes an EXTERNAL
    // surface". Both of those are caller-triggered and internal: they happen because a caller
    // sent a prompt, and they touch broca's own storage. Neither is a self-signal.
    //
    // The behavior that actually qualifies is the one the comment overlooked: broca calls
    // fusiform's `catalog.get` every 30 minutes (CATALOG_POLL_INTERVAL) with NO CALLER ASKING.
    // From fusiform's side that is unattributed periodic load, and an analyst reading their
    // request log without this declaration has to reverse-engineer where it comes from.
    //
    //   Poller       -- fixed cadence, not a keepalive and not tied to a session
    //   Observe      -- a read; the refusal path leaves the served catalog untouched
    //   FixedInterval-- its own signature, findable without an external event grid
    //
    // NOT DECLARED, deliberately: the detached catalog-readiness watches at startup. They are
    // bounded, they stop once the module goes live, and declaring a transient as a standing
    // behavior would misdescribe the steady state this field exists to describe.
    .self_signals(Some(vec![SelfSignalDeclaration {
        name: "catalog_refresh".to_string(),
        kind: SelfSignalKind::Poller,
        effect: SelfSignalEffect::Observe,
        anchored_to: SignalAnchor::FixedInterval,
        // LITERAL, read from the constant rather than retyped as a number. Writing 1_800_000
        // here would let the declaration and the timer drift apart silently, which is the one
        // failure a cadence declaration cannot tolerate: a consumer sizing capacity against a
        // stale figure gets no signal that it is stale.
        cadence: Some(SignalCadence::Literal {
            interval_ms: CATALOG_POLL_INTERVAL.as_millis() as u64,
        }),
        domain: Some("model-catalog".to_string()),
        note: Some(
            "Refreshes the served model catalog from fusiform. Skips the rebuild when the \
             upstream version is unchanged, and refuses rather than persists a response that \
             fails its completeness check."
                .to_string(),
        ),
    }, SelfSignalDeclaration {
        name: "runs_in_flight".to_string(),
        kind: SelfSignalKind::Busy,
        effect: SelfSignalEffect::Observe,
        anchored_to: SignalAnchor::HealthGauges {
            // Open idle/paused actors need no drain time. Opening and closing actors can
            // still hold admission or durable teardown work, so wait for those too.
            // System One batch calls hold no session but are work a restart would cut.
            gauges: vec![
                "runs_in_flight".to_string(),
                "opening".to_string(),
                "closing".to_string(),
                "system_one_batches_in_flight".to_string(),
            ],
        },
        cadence: None,
        domain: Some("runs".to_string()),
        note: Some("Active runs, actors opening or closing, and System One batch calls. Restart-paused runs are idle and do not hold the drain open.".to_string()),
    }]))
    .provenance(build_provenance())
    // The daemon validates a present block at HELLO, and a name outside its grammar
    // (`[a-z0-9-]+/v<N>`: no colon, no dot) refuses the HELLO, so a test checks this block
    // against the daemon's own validator. `provides` routes nothing: `route.open` names a
    // module. It only satisfies another module's `requires` and is matched against another
    // module's `must_never_reach`. Empty `requires` and `must_never_reach` gate nothing.
    .capabilities(Some(CapabilityDeclarations {
        // Which `session.send` delivery modes this build accepts (`queue`, `steer`, `interrupt`)
        // is stated by the `llm-runner/v1` role's `role.describe` groups, not by a capability
        // name here. A caller that finds no `steer` group sends plain queued messages only.
        // `flow-scopes/v1` promises that broca admits a flow only from its attested carrier,
        // never as the flow's owner agent, and refuses tools on flow sends. The daemon routes a
        // flow-scoped bind only to modules that declare it.
        // Agent-run scopes likewise admit only Basal's attested sender, without tools.
        // `scope-workdir/v1` asks the daemon to stamp a scope's session folder and project id
        // on its binds. New scoped sessions take workdir; the inventory preserves existing
        // addresses. Project id never keys a session.
        // `bind-no-project/v1` promises that only scope supplies project attribution.
        provides: vec![
            subc_protocol::session::ACCEPTING_DRAIN_CAPABILITY.to_string(),
            broca_wire::LLM_RUNNER_V1.to_string(),
            "compaction-runner/v1".to_string(),
            broca_wire::agent_data::PROVIDES.to_string(),
            // System One's decision models, on management routes opened declaring the
            // `classifier` role. A caller finds broca by this entry, then picks it for a model
            // its classifier `role.describe` lists.
            cortexkit_role_classifier::PROVIDES.to_string(),
            cortexkit_role_extractor::PROVIDES.to_string(),
            "tool-provider/v1".to_string(),
            "permissions/v1".to_string(),
            subc_protocol::scope::FLOW_SCOPES_CAPABILITY.to_string(),
            subc_protocol::scope::AGENT_RUN_SCOPES_CAPABILITY.to_string(),
            subc_protocol::scope::SCOPE_WORKDIR_CAPABILITY.to_string(),
            subc_protocol::scope::BIND_NO_PROJECT_CAPABILITY.to_string(),
        ],
        requires: Vec::new(),
        must_never_reach: Vec::new(),
    }))
    .flow_operations(Some(crate::management_admission::flow_operations()))
    // A flow triggers on a declared event only. Broca's run events and compaction notices stay
    // undeclared (see the test below); `findings_changed` wakes the self-diagnosis flow.
    .events(Some(vec![subc_protocol::manifest::EventDeclaration::new(
        broca_store::findings_events::FINDINGS_EVENT,
        broca_store::findings_events::FINDINGS_VERSION,
    )
    .with_headers(vec![broca_store::findings_events::EVENT_KEY_HEADER.into()])]))
    .provides(vec![ProviderRole::ManagementSurface {
            // Concurrent ACROSS sessions, FIFO WITHIN one: the registry fans requests out to
            // per-session lineage actors, and each actor processes its own commands in order
            // behind the single-writer lease for the session's log. That is ModuleManaged rather
            // than Serial -- declaring Serial would tell subc to gate every session behind one
            // in-flight call and serialise a fleet that already schedules itself.
            //
            // Both halves are covered by runtime tests:
            //
            // WITHIN-SESSION ORDERING --
            // `delayed_terminal_send_retry_does_not_queue_behind_active_run`.
            //
            // CROSS-SESSION OVERLAP --
            // `concurrent_sessions_sharing_one_tool_module_are_isolated` drives five sessions
            // through one shared tool route, and its ConcurrencyProbe fails if ANY run finishes
            // before all five are admitted. So a global lock that made this surface secretly
            // Serial would be caught, which is what the declaration actually asserts.
            //
            // Run the cross-session test with BROCA_REQUIRE_AIMOCK=1 and BROCA_REQUIRE_SUBC=1.
            // Without these guards, a missing dependency can return early without testing overlap.
            concurrency: Concurrency::ModuleManaged,
            // The request/response ops: send/retract/cancel change state (Mutate); status
            // is a point-read (Query).
            operations: management_operations(),
            config_schema: serde_json::json!({ "type": "object" }),
            // session.subscribe is a held-open STREAM, not a request/response op — it lives
            // in observability as a Stream surface.
            observability: observability_surfaces(),
            // A session is identified by {project_root, harness, session}: the surface is
            // scoped per project + per session.
            identity_scope: vec![IdentityScope::Project, IdentityScope::Session],
        }, ProviderRole::InternalService {
            service_id: broca_wire::agent_data::SERVICE_ID.to_string(),
            transport: subc_protocol::manifest::InternalTransport::Bulk,
            agent_facing: false,
            operations: broca_wire::agent_data::REQUIRED_OPS.iter().map(|op| op.to_string()).collect(),
        }, ProviderRole::ToolProvider {
            // A caller such as Basal resolves a module's tools from
            // this declaration before it reads the route's catalog, so a tool missing here is
            // refused before broca sees the call.
            tools: vec![
                subc_protocol::manifest::Tool {
                    name: crate::llm_tools::CLASSIFY_TOOL_NAME.into(),
                    description: Some("Classify one item with a decision model.".into()),
                    execution_mode: subc_protocol::manifest::ExecutionMode::Pure,
                    schema: cortexkit_llm_tools::classify::input_schema(),
                },
                subc_protocol::manifest::Tool {
                    name: crate::llm_tools::complete::COMPLETE_TOOL_NAME.into(),
                    description: Some(crate::llm_tools::complete::COMPLETE_DESCRIPTION.into()),
                    execution_mode: subc_protocol::manifest::ExecutionMode::Pure,
                    schema: crate::llm_tools::complete::Complete::input_schema(),
                },
                subc_protocol::manifest::Tool {
                    name: crate::llm_tools::extract::NAME.into(),
                    description: Some(crate::llm_tools::extract::DESCRIPTION.into()),
                    execution_mode: subc_protocol::manifest::ExecutionMode::Pure,
                    schema: cortexkit_llm_tools::extract::input_schema(),
                },
            ],
            identity_scope: vec![IdentityScope::Project, IdentityScope::Session],
            concurrency: Concurrency::ModuleManaged,
            emits_push: false,
            sub_supervises: false,
        }])
    // WHAT BROCA OPENS ROUTES TO, built from the same resolved values the serve binary dials,
    // so the declaration follows the configuration rather than a second list of names.
    //
    // `consumes` is a DESCRIPTION. No daemon behaviour depends on it: the daemon does not read
    // it to gate registration or route opening, so declaring a module here cannot delay HELLO
    // and leaving one out cannot stop broca reaching it.
    //
    // The ToolClient role names only the DEFAULT tool module. broca routes each tool call on
    // its own: to the caller's `Tool.module` hint, else to the module that is the unique
    // catalog provider of that tool, else to this default. So broca can reach tool modules
    // this list does not name, and the list cannot enumerate them, because callers choose
    // them at request time.
    .consumes(consumer_routes.consumer_roles())
    .role_declarations(BTreeMap::from([(
        cortexkit_role_routing::ROLE_DECLARATIONS_KEY.to_string(),
        serde_json::to_value(routing_declarations())
            .expect("routing declaration serializes")
            .as_object()
            .expect("routing declaration is an object")
            .clone(),
    )]))
    .provider_declarations(Some(provider_declarations()))
    .build()
}

/// What broca asks of other modules by declaration, which the daemon hands each provider when a
/// first-party module opens a route to it. The vault (claustrum) grants a first-party module the
/// credential categories its entry lists in place of a hand grant: broca reads model logins and
/// keys (`llm-provider`), and reads and lists company gateway keys (`llm-gateway`), each listed
/// key being an account it offers routing. broca's storage key is a single vault record, which a
/// declaration doesn't cover, so it stays a hand grant (see `store_key`).
fn provider_declarations() -> subc_protocol::provider_declarations::ProviderDeclarations {
    let claustrum = serde_json::json!({
        "v": 1,
        "grants": [
            {"category": "llm-provider", "operation": "read"},
            {"category": "llm-gateway", "operation": "read"},
            {"category": "llm-gateway", "operation": "list"},
        ],
    });
    BTreeMap::from([(
        "claustrum".to_string(),
        claustrum
            .as_object()
            .expect("the vault declaration is an object")
            .clone(),
    )])
}

#[cfg(test)]
mod routing_tests {
    use super::*;
    use cortexkit_role_routing::declarations::parse_consumer_declarations_serving;

    #[test]
    fn hello_routing_slot_passes_the_whole_slot_parser() {
        let manifest = serde_json::to_value(manifest("broca")).unwrap();
        let slot = &manifest["role_declarations"][cortexkit_role_routing::ROLE_DECLARATIONS_KEY];
        assert_eq!(*slot, serialized_routing_slot());
        let parsed = parse_consumer_declarations_serving(
            slot,
            &[cortexkit_role_routing::capabilities::SELECT_WORKLOAD],
        )
        .expect("the entire routing slot validates");
        assert_eq!(parsed.consumers.len(), 1);
        assert_eq!(parsed.consumers[0].name, "broca.complete");
        assert_eq!(
            parsed.consumers[0].defaults.workload.as_deref(),
            Some("quick")
        );
        let runner = parsed.runner.as_ref().expect("broca declares its runner");
        assert!(runner.follows_account_choice);
        assert!(runner.starts_sessions);
        assert_eq!(runner.harnesses.as_deref().unwrap(), BROCA_HARNESSES);
        assert_eq!(serde_json::to_value(parsed).unwrap(), *slot);
    }

    fn serialized_routing_slot() -> serde_json::Value {
        let manifest = manifest_configured("broca", &ConsumerRoutes::default());
        serde_json::to_value(manifest).unwrap()["role_declarations"]
            [cortexkit_role_routing::ROLE_DECLARATIONS_KEY]
            .clone()
    }

    /// Placement comes from `role.describe`; the routing slot states only durable harness facts.
    #[test]
    fn routing_slot_declares_the_broca_harnesses_and_only_supported_capabilities() {
        let expected = concat!(
            r#"{"consumers":[{"name":"broca.complete","workload":"quick"}],"runner":{"follows_account_choice":true,"harness_limits":{"#,
            r#""broca":{"capabilities":["one_off","fork_answer","submit_tool","final_text","plans","output_cap"]},"#,
            r#""broca-uncapped":{"capabilities":["one_off","fork_answer","submit_tool","final_text","plans"]}},"#,
            r#""harnesses":["broca","broca-uncapped"]}}"#,
        );
        let slot = serialized_routing_slot();
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&slot).unwrap()).unwrap(),
            expected
        );
        let parsed = parse_consumer_declarations_serving(
            &slot,
            &[cortexkit_role_routing::capabilities::SELECT_WORKLOAD],
        )
        .expect("the entire routing slot validates");
        parsed
            .validate_serving(&[cortexkit_role_routing::capabilities::SELECT_WORKLOAD])
            .expect("the producer declaration validates");
        let limits = parsed
            .runner
            .as_ref()
            .unwrap()
            .typed_harness_limits()
            .expect("broca declares harness limits")
            .expect("the ungated producer validator accepts the harness facts");
        assert!(limits["broca"]
            .capabilities
            .as_ref()
            .unwrap()
            .contains(&HarnessCapability::OutputCap));
        assert!(!limits["broca-uncapped"]
            .capabilities
            .as_ref()
            .unwrap()
            .contains(&HarnessCapability::OutputCap));
    }
}

#[cfg(test)]
mod provenance_tests {
    use super::*;

    /// An UNSTAMPED build must declare no provenance at all.
    ///
    /// This is the property the sentinel exists for. `option_env!` reads the environment at
    /// COMPILE time, so an ambient `CK_BUILD_REV` in any shell that runs cargo would mint a
    /// provenance claim the build never earned -- and a deploy gate reads that claim as
    /// authoritative. Ordinary builds are unstamped, so this asserts the common case declares
    /// nothing rather than something plausible.
    /// The builder migration must be WIRE-NEUTRAL apart from the one intended addition.
    ///
    /// A builder replaces explicit struct fields with defaults for anything not named, so a
    /// field I forgot to call would silently take upstream's choice rather than mine -- and
    /// this manifest is read by the daemon at HELLO, where a quietly different value is a
    /// quietly different contract. Serialising and asserting the top-level key set catches an
    /// omission as a missing key rather than as a behaviour someone notices later.
    #[test]
    fn the_builder_manifest_carries_every_field_this_module_declares() {
        let v = serde_json::to_value(manifest("broca")).expect("manifest serialises");
        let obj = v.as_object().expect("manifest is an object");
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "bindings",
                "capabilities",
                "consumes",
                "events",
                "flow_operations",
                "module_id",
                "module_version",
                "protocol_ver",
                "provider_declarations",
                "provides",
                "role_declarations",
                "self_signals",
                "trust_tier",
            ],
            "the serialised manifest key set moved"
        );
        // provenance is absent BY DECLARATION on an unstamped build and skip_serializing_if,
        // so its absence from the list above is the assertion.
        assert_eq!(
            obj["capabilities"],
            serde_json::json!({
                "provides": ["accepting-drain/v1", "llm-runner/v1", "compaction-runner/v1", "agent-data/v1", "classifier/v1", "extractor/v1", "tool-provider/v1", "permissions/v1", "flow-scopes/v1", "agent-run-scopes/v1", "scope-workdir/v1", "bind-no-project/v1"],
                "requires": [],
                "must_never_reach": [],
            }),
            "broca declares accepting drain, runner, agent-data, classifier, model-tool and permissions roles, flow and agent-run scope handling, scope workdir stamps, and requires or denies nothing"
        );
        //
        // `consumes` has skip_serializing_if = "Vec::is_empty" since subc-protocol 0.19.0, so
        // its presence here is the assertion that broca declares the routes it opens. Its
        // absence would mean the manifest had gone back to claiming broca consumes nothing.
        //
        // This test caught the migration: the four-argument builder became two-argument in
        // the same release, and a field silently dropped in that edit would have been
        // indistinguishable from one nobody considered. That is the whole reason it exists.
        assert_eq!(obj["module_id"], "broca");
        assert_eq!(obj["trust_tier"], "first_party");
        assert_eq!(
            obj["consumes"],
            serde_json::json!([
                { "role": "tool_client", "of": ["aft"] },
                { "role": "service_client", "of": ["fusiform", "claustrum"] },
            ]),
            "the default configuration consumes the default tool module, the catalog and the vault"
        );
    }

    /// broca publishes its run events and its compaction advance notices
    /// (`compaction.advanced`) undeclared. Their names are dotted, and the daemon's HELLO check
    /// once refused a dotted event declaration, which would keep broca from registering at all.
    /// So the manifest declares only `findings_changed`, which a flow triggers on, and no
    /// dotted name; the daemon's own validator accepts it.
    #[test]
    fn the_manifest_declares_only_findings_changed() {
        let encoded = serde_json::to_value(manifest("broca")).expect("manifest serialises");
        let declared: Vec<&str> = encoded
            .get("events")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|event| event.get("name").and_then(serde_json::Value::as_str))
            .collect();
        assert!(
            declared.iter().all(|name| !name.contains('.')),
            "dotted event declarations: {declared:?}"
        );
        assert_eq!(declared, ["findings_changed"]);
        assert_eq!(
            encoded["events"][0],
            serde_json::json!({"name": "findings_changed", "version": 1, "headers": ["event_key"]})
        );
        let hello = |manifest: &serde_json::Value| serde_json::json!({ "manifest": manifest });
        subc_protocol::manifest::validate_hello_event_declarations(&hello(&encoded))
            .expect("the daemon would refuse this HELLO");
        // The control: the daemon's validator refuses an empty name token, so the pass above
        // cannot come from a validator that accepts everything.
        let mut dotted = encoded.clone();
        dotted["events"] = serde_json::json!([
            {"name": "compaction..advanced", "version": 1, "headers": ["session_ref"]}
        ]);
        assert!(
            subc_protocol::manifest::validate_hello_event_declarations(&hello(&dotted)).is_err()
        );
    }

    /// Basal resolves programs' tools from this declaration before opening broca's tool route.
    #[test]
    fn the_manifest_declares_every_tool_the_route_lists() {
        let declared: Vec<(String, serde_json::Value)> = manifest("broca")
            .provides
            .into_iter()
            .filter_map(|role| match role {
                ProviderRole::ToolProvider { tools, .. } => Some(tools),
                _ => None,
            })
            .flatten()
            .map(|tool| (tool.name, tool.schema))
            .collect();
        assert_eq!(
            declared,
            [
                (
                    crate::llm_tools::CLASSIFY_TOOL_NAME.to_owned(),
                    cortexkit_llm_tools::classify::input_schema()
                ),
                (
                    crate::llm_tools::complete::COMPLETE_TOOL_NAME.to_owned(),
                    crate::llm_tools::complete::Complete::input_schema()
                ),
                (
                    "llm_extract".to_owned(),
                    cortexkit_llm_tools::extract::input_schema()
                ),
            ]
        );
    }

    /// broca's vault grants travel as a declaration the vault reads: exactly the categories
    /// broca's hand grants gave it. A malformed declaration makes the daemon refuse HELLO, so
    /// it is checked with the daemon's own validator; the control shows that validator refuses
    /// one, so the pass can't come from a validator that accepts everything.
    #[test]
    fn the_vault_grants_are_declared_in_a_form_the_daemon_accepts() {
        let encoded = serde_json::to_value(manifest("broca")).expect("manifest serialises");
        assert_eq!(
            encoded["provider_declarations"],
            serde_json::json!({"claustrum": {"v": 1, "grants": [
                {"category": "llm-provider", "operation": "read"},
                {"category": "llm-gateway", "operation": "read"},
                {"category": "llm-gateway", "operation": "list"},
            ]}})
        );
        let hello = |manifest: &serde_json::Value| serde_json::json!({ "manifest": manifest });
        subc_protocol::manifest::validate_hello_provider_declarations(&hello(&encoded))
            .expect("the daemon would refuse this HELLO");
        let mut malformed = encoded.clone();
        malformed["provider_declarations"]["claustrum"] = serde_json::json!("grants");
        assert!(
            subc_protocol::manifest::validate_hello_provider_declarations(&hello(&malformed))
                .is_err()
        );
    }

    /// A capability name the daemon's grammar refuses makes it refuse broca's HELLO, which
    /// keeps broca from registering at all. So the declared block is checked with the
    /// daemon's own validator, not a copy of its rules. The control shows the validator does
    /// refuse a module-prefixed name, so a pass here can't come from a validator that
    /// accepts everything.
    #[test]
    fn the_declared_capabilities_pass_the_daemons_grammar() {
        let m = manifest("broca");
        m.validate_capability_grammar()
            .expect("the daemon would refuse this HELLO");
        let caps = m
            .capabilities
            .as_ref()
            .expect("broca declares a capability block");
        assert!(caps.provides.iter().any(|c| c == broca_wire::LLM_RUNNER_V1));
        assert!(caps
            .provides
            .iter()
            .any(|c| c == subc_protocol::scope::FLOW_SCOPES_CAPABILITY));
        for name in &caps.provides {
            assert!(
                subc_protocol::manifest::is_valid_capability_identifier(name),
                "{name} fails the daemon's capability grammar"
            );
        }
        assert!(
            !subc_protocol::manifest::is_valid_capability_identifier("broca:send-delivery/v1"),
            "control: the validator must refuse a module-prefixed name"
        );
    }

    /// The declared cadence must be READ FROM the timer, never retyped beside it.
    ///
    /// A cadence declaration is consumed by analysts sizing load on the surface broca polls,
    /// and its one intolerable failure is silent drift: if someone changes the interval and the
    /// manifest keeps the old number, every downstream reader is confidently wrong with no
    /// signal that anything moved. Asserting equality against the constant makes the drift a
    /// compile-visible test failure rather than a fleet-visible lie.
    #[test]
    fn the_declared_cadence_tracks_the_real_timer() {
        let m = manifest("broca");
        let signals = m.self_signals.as_ref().expect("broca declares its poller");
        let poll = signals
            .iter()
            .find(|s| s.name == "catalog_refresh")
            .expect("the catalog poller is declared");
        // PINNED AS AN INDEPENDENT LITERAL, and the first version of this test was VACUOUS
        // for want of that. It compared the declaration against CATALOG_POLL_INTERVAL -- but
        // the declaration is BUILT from that constant, so both sides moved together and the
        // test would pass even if the interval changed to 15 minutes. A guard drawn from the
        // thing it guards can only ever confirm self-consistency.
        //
        // The two risks are different and need different mechanisms. The DECLARATION reads the
        // constant, so it cannot drift from the timer. This TEST pins the wire value, so
        // changing the timer is a deliberate edit here rather than a silent change under every
        // consumer sizing load against 30 minutes.
        assert_eq!(
            poll.cadence,
            Some(SignalCadence::Literal {
                interval_ms: 1_800_000
            }),
            "the catalog poll interval moved; update this literal deliberately and tell \
             fusiform, who size their served load against the declared cadence"
        );
        // Effect is load-bearing separately: a reader deciding whether this poll can perturb
        // fusiform's served state reads THIS, not the kind.
        assert_eq!(poll.effect, SelfSignalEffect::Observe);
    }

    #[test]
    fn an_unstamped_build_declares_no_provenance() {
        assert!(
            build_provenance().is_none(),
            "a build without CK_BUILD_STAMPED must not mint a provenance claim"
        );
    }

    /// Check the provenance broca actually registers, rather than a separately built upstream
    /// value. A stamped test build exercises the same compile-time inputs as packaging.
    #[test]
    #[ignore = "requires CK_BUILD_STAMPED=1 and canonical CK_BUILD_REV/CK_BUILD_LOCK_DIGEST at compile time"]
    fn provenance_fills_only_what_broca_can_source() {
        let emitted = serde_json::to_value(manifest("broca")).unwrap();
        let provenance = emitted.get("provenance").expect("stamped HELLO provenance");
        assert_eq!(
            provenance["build_git_sha"].as_str(),
            option_env!("CK_BUILD_REV")
        );
        assert_eq!(
            provenance["build_lock_digest"].as_str(),
            option_env!("CK_BUILD_LOCK_DIGEST")
        );
        assert_eq!(
            provenance["wire_crate_version"],
            subc_protocol::SUBC_PROTOCOL_CRATE_VERSION
        );
        assert!(provenance.get("store_schema_version").is_none());
    }

    /// The exact bytes of the provenance block a stamped build sends in HELLO, for fixed inputs.
    ///
    /// The daemon and `ck provenance` read this block as written, so a change in its serialised
    /// form is a wire change even when every field keeps its value. Pinning the bytes means a new
    /// provenance field shows up here as a deliberate edit rather than as a silent addition to
    /// what every stamped build declares.
    #[test]
    fn stamped_provenance_bytes_are_pinned() {
        let sha = "a".repeat(40);
        let lock = "b".repeat(64);
        let provenance = subc_protocol::manifest::build_provenance(Some(&sha), Some(&lock), None)
            .expect("full-length lowercase hex is the canonical form for both");
        let pinned = format!(
            r#"{{"build_git_sha":"{sha}","build_lock_digest":"{lock}","wire_crate_version":"{}"}}"#,
            subc_protocol::SUBC_PROTOCOL_CRATE_VERSION
        );
        assert_eq!(
            serde_json::to_string(&provenance).expect("provenance serialises"),
            pinned,
        );

        // With no nonce source to report the block is byte-identical to the pin: an absent
        // field means "did not say", so a process that has no nonce sends exactly what a
        // build before the field existed sent.
        let unsaid = provenance.clone().with_launch_nonce_source(None);
        assert_eq!(serde_json::to_string(&unsaid).unwrap(), pinned);

        // With a nonce source, the field is appended and nothing else moves.
        let said = provenance.with_launch_nonce_source(Some(LaunchNonceSource::Fd));
        assert_eq!(
            serde_json::to_string(&said).unwrap(),
            format!(
                "{},\"launch_nonce_source\":\"fd\"}}",
                &pinned[..pinned.len() - 1]
            ),
        );
    }

    /// Each accessor source is reported under its own wire name.
    ///
    /// The census counts a module as switched to the pipe only when its provenance says `fd`, so
    /// reporting `fd` for a nonce that came from the environment would mark an unswitched
    /// module done.
    #[test]
    fn each_nonce_source_is_reported_under_its_own_name() {
        assert_eq!(
            reported_launch_nonce_source(subc_os::LaunchNonceSource::Fd),
            LaunchNonceSource::Fd
        );
        assert_eq!(
            reported_launch_nonce_source(subc_os::LaunchNonceSource::Env),
            LaunchNonceSource::Env
        );
    }

    /// A malformed sha is REFUSED; a sentinel is an honest omission. Both arms, because
    /// collapsing them is the plausible mistake.
    ///
    /// This is the arm that decides what a dirty-tree build declares. `scripts/stage-release.sh`
    /// appended `-dirty` to the sha and shipped it as a commit-match-only claim; under canonical
    /// forms that is a FORM ERROR, so the whole provenance block would be omitted instead. The
    /// script now refuses to stage a dirty tree, making this input unreachable from the release
    /// path -- and this test is what keeps that reasoning visible if a suffix is reintroduced.
    #[test]
    fn a_malformed_sha_errors_while_a_sentinel_omits() {
        let lock = "b".repeat(64);

        let suffixed = format!("{}-dirty", "a".repeat(40));
        assert!(
            subc_protocol::manifest::build_provenance(Some(&suffixed), Some(&lock), None).is_err(),
            "a -dirty suffix is a real value in a non-canonical form, so it must error"
        );

        let abbreviated = "a".repeat(8);
        assert!(
            subc_protocol::manifest::build_provenance(Some(&abbreviated), Some(&lock), None)
                .is_err(),
            "an abbreviated sha must error rather than be discarded as if absent"
        );

        let sentinel =
            subc_protocol::manifest::build_provenance(Some("unknown"), Some(&lock), None)
                .expect("a sentinel normalises to omission before form checks");
        assert_eq!(
            sentinel.build_git_sha, None,
            "a sentinel is an honest gap, not a form error"
        );
    }

    /// The store has no schema version to report. Even a stamped manifest must omit the field,
    /// not invent a version from its columns or emit a null that consumers could mistake for one.
    #[test]
    #[ignore = "requires CK_BUILD_STAMPED=1 and canonical CK_BUILD_REV/CK_BUILD_LOCK_DIGEST at compile time"]
    fn unsourced_provenance_fields_are_not_synthesised() {
        let emitted = serde_json::to_value(manifest("broca")).unwrap();
        let provenance = emitted["provenance"]
            .as_object()
            .expect("stamped HELLO provenance");
        assert_eq!(
            provenance.keys().map(String::as_str).collect::<Vec<_>>(),
            if launch_nonce_source().is_some() {
                vec![
                    "build_git_sha",
                    "build_lock_digest",
                    "launch_nonce_source",
                    "wire_crate_version",
                ]
            } else {
                vec!["build_git_sha", "build_lock_digest", "wire_crate_version"]
            }
        );
        assert!(!provenance.contains_key("store_schema_version"));
    }
}

#[cfg(test)]
mod consumes_tests {
    use super::*;
    use subc_protocol::manifest::ConsumerRole;

    fn declared(routes: &ConsumerRoutes) -> (Vec<String>, Vec<String>) {
        let m = manifest_configured("broca", routes);
        let mut tools = Vec::new();
        let mut services = Vec::new();
        for role in m.consumes {
            match role {
                ConsumerRole::ToolClient { of } => tools.extend(of),
                ConsumerRole::ServiceClient { of } => services.extend(of),
                ConsumerRole::LlmClient { .. } => panic!("broca declares no LLM client role"),
            }
        }
        (tools, services)
    }

    /// The declared ids must be the ids the serve binary dials, for a NON-default input.
    ///
    /// The serve binary opens the tool plane with `routes.tool_module`, the vault with
    /// `routes.vault_module` and the catalog with `FUSIFORM_MODULE_ID`. Changing an input away from its default is what makes this a
    /// drift guard: a manifest that hard-coded the defaults would still pass on defaults.
    #[test]
    fn declared_consumers_follow_the_route_opening_configuration() {
        let routes = ConsumerRoutes::resolve(Some("custom"), true);
        let (tools, services) = declared(&routes);
        assert_eq!(tools, vec![routes.tool_module.clone()]);
        assert_eq!(tools, vec!["custom".to_string()]);
        let mut expected = vec![crate::catalog_refresh::FUSIFORM_MODULE_ID.to_string()];
        expected.extend(routes.vault_module.clone());
        assert_eq!(services, expected);
        assert_eq!(services, vec!["fusiform", "claustrum"]);
    }

    /// Each configuration-dependent entry appears and disappears with its knob.
    #[test]
    fn optional_consumers_track_their_knobs() {
        // A mock provider leaves the vault unopened: only the
        // catalog remains a service dependency.
        let (tools, services) = declared(&ConsumerRoutes::resolve(None, false));
        assert_eq!(tools, vec!["aft"]);
        assert_eq!(services, vec!["fusiform"]);

        // A blank knob is unset, as it is for the route-opening code.
        let (tools, services) = declared(&ConsumerRoutes::resolve(Some("  "), false));
        assert_eq!(tools, vec!["aft"]);
        assert_eq!(services, vec!["fusiform"]);

        let (_, services) = declared(&ConsumerRoutes::resolve(None, true));
        assert_eq!(services, vec!["fusiform", "claustrum"]);
    }
}

#[cfg(test)]
mod agent_run_writer_tests {
    #[test]
    fn writer_advertises_agent_run_scopes() {
        let manifest = super::manifest("broca");
        assert!(manifest
            .capabilities
            .unwrap()
            .provides
            .iter()
            .any(|name| name == subc_protocol::scope::AGENT_RUN_SCOPES_CAPABILITY));
    }
}
