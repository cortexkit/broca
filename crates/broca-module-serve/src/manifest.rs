//! The broca module capability manifest.
//!
//! broca registers as a ManagementSurface exposing the session/run control ops as
//! request/response operations, plus `session.subscribe` as a Stream observability surface
//! (a held-open event stream, not a request/response op). subc routes a client to this
//! surface via `route.bind { target: ManagementSurface{ module_id: "broca" } }`.

use crate::catalog_refresh::CATALOG_POLL_INTERVAL;
use crate::consumer_routes::ConsumerRoutes;
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
pub const OP_SEND: &str = "session.send";
pub const OP_IMPORT: &str = "session.import";
pub const OP_RETRACT: &str = "session.retract";
pub const OP_CANCEL: &str = "run.cancel";
pub const OP_STATUS: &str = "run.status";
pub const OP_RUN_EVENT_GET: &str = "run.event.get";
pub const OP_READ: &str = "session.read";
/// Daemon-scoped read of the credential vault's listing under broca's grants.
pub const OP_VAULT_AUDIT: &str = "vault.audit";
/// The held-open stream observability surface.
pub const OP_SUBSCRIBE: &str = "session.subscribe";
/// The `llm-runner/v1` operations broca exposes: role discovery, session head and baseline, run
/// results and tool-call history, and tag resolution through `session.resolve`.
pub const OP_ROLE_DESCRIBE: &str = broca_wire::OP_ROLE_DESCRIBE;
pub const OP_HEAD: &str = broca_wire::OP_SESSION_HEAD;
pub const OP_RUN_RESULT: &str = broca_wire::OP_RUN_RESULT;
pub const OP_RUN_TOOL_CALLS: &str = broca_wire::OP_RUN_TOOL_CALLS;
pub const OP_BASELINE: &str = broca_wire::OP_SESSION_BASELINE;
pub const OP_RESOLVE: &str = broca_wire::OP_SESSION_RESOLVE;
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
/// honest, where a guess would count a module as switched when it is not.
pub fn launch_nonce_source() -> Option<LaunchNonceSource> {
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
    manifest_with_archive(module_id, crate::wal_archive::WalArchiveConfig::default())
}

pub fn manifest_with_archive(
    module_id: &str,
    archive: crate::wal_archive::WalArchiveConfig,
) -> ModuleManifest {
    manifest_configured(module_id, archive, &ConsumerRoutes::default())
}

/// The management request list is also the source of `health.check` request-counter names.
pub(crate) fn management_operations() -> Vec<ManagementOperation> {
    vec![
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
                "Describe the role the route was opened for: on an llm-runner/v1 route its ops, capability groups and read byte cap; on a classifier/v1 route its limits and the models it serves."
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
            name: OP_RESOLVE.to_string(),
            kind: ManagementOperationKind::Query,
            description: Some(
                "Answer the stored text of one runner-tagged part, or a character range of it."
                    .to_string(),
            ),
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
    ]
}

pub(crate) fn observability_surfaces() -> Vec<ObservabilitySurface> {
    vec![ObservabilitySurface {
        name: OP_SUBSCRIBE.to_string(),
        kind: ObservabilityKind::Stream,
    }]
}

/// The manifest for a fully configured process: the archive timer's configuration shapes
/// `self_signals`, and the resolved consumer routes shape `consumes`.
pub fn manifest_configured(
    module_id: &str,
    archive: crate::wal_archive::WalArchiveConfig,
    consumer_routes: &ConsumerRoutes,
) -> ModuleManifest {
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
        // broca owns the per-session WAL + materialized store (its own schema).
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
    // CATALOG REFRESH IS ALWAYS DECLARED, and this field was WRONGLY ABSENT until the daemon began serving it
    // from catalog.list, at which point the absence became a fleet-visible claim rather than a
    // local omission.
    //
    // The prior comment here said broca had nothing to declare yet, and named the WAL append
    // and the store projection as the eventual first entries. That was wrong on the field's own
    // definition -- "a declared PERIODIC or event-driven behavior that shapes an EXTERNAL
    // surface". Both of those are caller-triggered and internal: they happen because a caller
    // sent a prompt, and they touch broca's own storage. Neither is a self-signal.
    //
    // The behavior that actually qualifies is the one the old comment overlooked: broca calls
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
    .self_signals(Some({
        let mut signals = vec![SelfSignalDeclaration {
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
    }];
        // An absent retention creates no timer, so it must make no standing signal claim.
        if archive.enabled() {
            signals.push(SelfSignalDeclaration {
                name: "wal_archive_fold".to_string(),
                kind: SelfSignalKind::Poller,
                effect: SelfSignalEffect::Mutate,
                anchored_to: SignalAnchor::FixedInterval,
                cadence: Some(SignalCadence::Literal {
                    interval_ms: crate::wal_archive::WAL_ARCHIVE_INTERVAL.as_millis() as u64,
                }),
                domain: Some("wal-archive".to_string()),
                note: Some("Archives idle session WALs after a full-day initial delay; skips held archive-pass leases.".to_string()),
            });
        }
        signals
    }))
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
        provides: vec![
            broca_wire::LLM_RUNNER_V1.to_string(),
            broca_wire::agent_data::PROVIDES.to_string(),
            // System One's decision models, on management routes opened declaring the
            // `classifier` role. A caller finds broca by this entry, then picks it for a model
            // its classifier `role.describe` lists.
            cortexkit_role_classifier::PROVIDES.to_string(),
            "tool-provider/v1".to_string(),
            "permissions/v1".to_string(),
            subc_protocol::scope::FLOW_SCOPES_CAPABILITY.to_string(),
            subc_protocol::scope::AGENT_RUN_SCOPES_CAPABILITY.to_string(),
        ],
        requires: Vec::new(),
        must_never_reach: Vec::new(),
    }))
    .provides(vec![ProviderRole::ManagementSurface {
            // Concurrent ACROSS sessions, FIFO WITHIN one: the registry fans requests out to
            // per-session lineage actors, and each actor processes its own commands in order
            // behind the single-writer WAL lease. That is ModuleManaged rather than Serial --
            // declaring Serial would tell subc to gate every session behind one in-flight call
            // and serialise a fleet that already schedules itself.
            //
            // BOTH HALVES ARE PINNED, and the cross-session half was verified by mutation
            // rather than by reading:
            //
            // WITHIN-SESSION ORDERING --
            // `delayed_terminal_send_retry_does_not_queue_behind_active_run`.
            //
            // CROSS-SESSION OVERLAP -- `concurrent_sessions_sharing_one_tool_module_are_isolated`
            // drives five sessions through one shared tool route, and its ConcurrencyProbe fails
            // if ANY run finishes before all five are admitted. Replacing its tokio::join! with
            // sequential awaits reddens it BY NAME. So a global lock that made this surface
            // secretly Serial would be caught, which is what the declaration actually asserts.
            //
            // RUN THAT MUTATION WITH BROCA_REQUIRE_AIMOCK=1 AND BROCA_REQUIRE_SUBC=1. Unguarded,
            // the rig returns early when a dependency is missing and reports a pass having
            // executed nothing -- which is exactly how this comment briefly claimed the
            // OPPOSITE, off a 15s 'pass' that was startup timeouts rather than serialised
            // sessions. The guards exist so an experiment cannot silently test nothing, and an
            // unguarded mutation run is evidence in neither direction.
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
            tools: vec![subc_protocol::manifest::Tool {
                name: crate::llm_tools::CLASSIFY_TOOL_NAME.into(),
                description: Some("Classify one item with a decision model.".into()),
                execution_mode: subc_protocol::manifest::ExecutionMode::Pure,
                schema: cortexkit_llm_tools::classify::input_schema(),
            }],
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
    .build()
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
                "module_id",
                "module_version",
                "protocol_ver",
                "provides",
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
                "provides": ["llm-runner/v1", "agent-data/v1", "classifier/v1", "tool-provider/v1", "permissions/v1", "flow-scopes/v1", "agent-run-scopes/v1"],
                "requires": [],
                "must_never_reach": [],
            }),
            "broca declares runner, agent-data, classifier, model-tool and permissions roles, flow and agent-run scope handling, and requires or denies nothing"
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
    /// refuses a dotted event declaration, so declaring one would keep broca from registering
    /// at all. So the manifest declares no events, and in particular no dotted name; the
    /// daemon's own validator accepts it.
    #[test]
    fn the_manifest_declares_no_dotted_event_names() {
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
        assert!(
            encoded.get("events").is_none(),
            "broca publishes its events undeclared: {declared:?}"
        );
        let hello = |manifest: &serde_json::Value| serde_json::json!({ "manifest": manifest });
        subc_protocol::manifest::validate_hello_event_declarations(&hello(&encoded))
            .expect("the daemon would refuse this HELLO");
        // The control: the daemon's validator does refuse a dotted name, so the pass above
        // cannot come from a validator that accepts everything.
        let mut dotted = encoded.clone();
        dotted["events"] = serde_json::json!([
            {"name": "compaction.advanced", "version": 1, "headers": ["session_ref"]}
        ]);
        assert!(
            subc_protocol::manifest::validate_hello_event_declarations(&hello(&dotted)).is_err()
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
        // test passed happily with the interval mutated to 15 minutes. A guard drawn from the
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

    /// What the provenance helper does with broca's inputs, asserted on VALUES not on text.
    ///
    /// Replaces a text search over this file, and the replacement is the point. The old test
    /// grepped the production half for `wire_crate_version: None` -- itself already the repaired
    /// form of a test that had been outright unfailable, because it originally searched the WHOLE
    /// file and the literal inside its own assertion satisfied it (issue #9, confirmed by
    /// mutation). Even scoped correctly, a text search pins the SPELLING of a decision rather
    /// than its OUTCOME, so it could not survive the construction moving into a helper.
    ///
    /// Two facts pinned, one of which is a live behaviour change:
    ///
    ///   - `wire_crate_version` IS NOW SOURCED. It stayed absent for as long as subc-protocol
    ///     exported no version const; the helper fills it from that crate's own const, so the
    ///     parked reason is DISCHARGED rather than still pending.
    ///   - `store_schema_version` STAYS ABSENT, because broca has no schema version to report.
    ///     Tempting to synthesise from the column list, which would then change silently under
    ///     an unrelated additive migration.
    #[test]
    fn provenance_fills_only_what_broca_can_source() {
        let sha = "a".repeat(40);
        let lock = "b".repeat(64);
        let provenance = subc_protocol::manifest::build_provenance(Some(&sha), Some(&lock), None)
            .expect("full-length lowercase hex is the canonical form for both");

        assert_eq!(provenance.build_git_sha.as_deref(), Some(sha.as_str()));
        assert_eq!(provenance.build_lock_digest.as_deref(), Some(lock.as_str()));
        assert!(
            provenance.wire_crate_version.is_some(),
            "the helper sources this from subc-protocol's own version const"
        );
        assert_eq!(
            provenance.store_schema_version, None,
            "broca passes None: it has no schema version to report"
        );
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

    /// The stamped block carries the process's nonce source.
    ///
    /// Checked as source text, because the block exists only in builds that
    /// `scripts/stage-release.sh` stamps at compile time, and no test build is stamped. The value mapping is tested on values
    /// above and through the real process accessor in `tests/launch_nonce.rs`; this pins that
    /// the stamped block is the one that uses it.
    #[test]
    fn the_stamped_block_reports_the_nonce_source() {
        let text = include_str!("manifest.rs");
        let (production, _tests) = text
            .split_once("#[cfg(test)]")
            .expect("manifest.rs must contain a #[cfg(test)] boundary to split on");
        assert!(
            production.contains(
                "Ok(provenance) => Some(provenance.with_launch_nonce_source(launch_nonce_source())),"
            ),
            "build_provenance must attach the accessor's cached nonce source to the stamped block"
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

    /// The one field broca cannot honestly source stays absent even when stamped.
    ///
    /// Pinned because both are tempting to synthesise: a wire crate version could be read from
    /// the wrong crate, and a store schema version could be derived from the column list -- which
    /// would then change silently under an unrelated additive migration. Absent is the truthful
    /// value until each has a real source.
    ///
    /// THE SEARCH IS SCOPED TO THE PRODUCTION HALF OF THIS FILE, and that is the whole test.
    ///
    /// It originally searched the WHOLE file, which made it unfailable: the literal it looks for
    /// appears inside its own assertion, so the corpus contained the answer regardless of what
    /// the field said. Setting `wire_crate_version: Some("0.13.0".to_string())` left it GREEN.
    /// Reported from outside (issue #9) and confirmed by mutation before this rewrite.
    ///
    /// A guard whose own source is part of the corpus it searches satisfies itself. Splitting at
    /// the `#[cfg(test)]` boundary is what makes the corpus the thing under test rather than the
    /// test plus the thing.
    #[test]
    fn unsourced_provenance_fields_are_not_synthesised() {
        let text = include_str!("manifest.rs");
        let (production, _tests) = text
            .split_once("#[cfg(test)]")
            .expect("manifest.rs must contain a #[cfg(test)] boundary to split on");

        // An empty or truncated split is an instrument failure, not a clean result: a file that
        // opened with #[cfg(test)] would yield an empty production half and pass every `!contains`
        // check below by having nothing to find.
        assert!(
            production.contains("fn build_provenance()"),
            "the production half must contain build_provenance; the split is wrong, not the code"
        );

        // wire_crate_version is no longer checked here: it is now SOURCED by the helper, and the
        // value assertion above is the honest test of that. Only the field broca still cannot
        // source belongs in a call-site text check.
        // Anchored on the CALL, not on a bare `None`. A loose `contains("None,")` would match
        // any of a dozen unrelated lines in this file and pass whatever the third argument said.
        assert!(
            production.contains("option_env!(\"CK_BUILD_LOCK_DIGEST\"),\n        None,\n    ) {"),
            "build_provenance must pass None as store_schema_version until the store has a real \
             version to report; deriving one from the column list would change silently under an \
             unrelated additive migration"
        );
    }
}

#[cfg(test)]
mod archive_tests {
    use super::*;
    #[test]
    fn archive_signal_follows_configuration() {
        let off = manifest("broca");
        assert!(!off
            .self_signals
            .unwrap()
            .iter()
            .any(|s| s.name == "wal_archive_fold"));
        let config =
            crate::wal_archive::WalArchiveConfig::parse("{\"wal_archive_idle_retention_days\":30}")
                .unwrap();
        let on = manifest_with_archive("broca", config);
        let signals = on.self_signals.unwrap();
        let fold = signals
            .iter()
            .find(|s| s.name == "wal_archive_fold")
            .unwrap();
        assert_eq!(fold.kind, SelfSignalKind::Poller);
        assert_eq!(fold.effect, SelfSignalEffect::Mutate);
        assert_eq!(fold.anchored_to, SignalAnchor::FixedInterval);
        assert_eq!(
            fold.cadence,
            Some(SignalCadence::Literal {
                interval_ms: 86_400_000
            })
        );
    }
}

#[cfg(test)]
mod consumes_tests {
    use super::*;
    use subc_protocol::manifest::ConsumerRole;

    fn declared(routes: &ConsumerRoutes) -> (Vec<String>, Vec<String>) {
        let m = manifest_configured(
            "broca",
            crate::wal_archive::WalArchiveConfig::default(),
            routes,
        );
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

        // The routing module, under the id `account_selection` configures, is declared beside
        // the vault whenever account selection is active.
        let selection = crate::account_routing::AccountSelectionConfig::parse(
            r#"{"account_selection":{"routing_module":"routing-b"}}"#,
        )
        .unwrap();
        let routes =
            ConsumerRoutes::resolve(Some("custom"), true).with_account_selection(&selection);
        let (_, services) = declared(&routes);
        assert_eq!(routes.routing_module.as_deref(), Some("routing-b"));
        assert_eq!(services, vec!["fusiform", "claustrum", "routing-b"]);
    }

    /// broca never opens a route to routing with `legacy_static` on, or without the vault (the
    /// mock providers resolve no accounts), so it declares none then.
    #[test]
    fn the_routing_module_is_declared_only_while_account_selection_is_active() {
        let legacy = crate::account_routing::AccountSelectionConfig::parse(
            r#"{"account_selection":{"routing_module":"routing-b","legacy_static":true}}"#,
        )
        .unwrap();
        let (_, services) =
            declared(&ConsumerRoutes::resolve(None, true).with_account_selection(&legacy));
        assert_eq!(services, vec!["fusiform", "claustrum"]);

        let active = crate::account_routing::AccountSelectionConfig::default();
        let (_, services) =
            declared(&ConsumerRoutes::resolve(None, false).with_account_selection(&active));
        assert_eq!(services, vec!["fusiform"]);

        let (_, services) =
            declared(&ConsumerRoutes::resolve(None, true).with_account_selection(&active));
        assert_eq!(
            services,
            vec!["fusiform", "claustrum", "prefrontal-routing"]
        );
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
