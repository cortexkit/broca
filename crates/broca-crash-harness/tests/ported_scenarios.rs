//! The crash-cut scenarios that broca-core's `tests/it/resume_exec.rs` and
//! `tests/it/terminal_reason_crash_cut.rs` ran with their own truncate-and-resume code, now run on
//! the shared harness.
//!
//! Each scenario is killed at every record boundary of its uninterrupted log, resumed through
//! the real run loop, and checked against every resume invariant, including the resume record.
//! What each original test asserted is kept: exactly-once dispatch, byte-identical re-render of
//! every step the resume requests, the uninterrupted finish (or Interrupted on an indeterminate
//! cut), and convergence of a resume killed inside itself. Two kills are real SIGKILLs of a
//! spawned runner, one inside a tool dispatch and one inside a provider call.

mod scenarios;

use broca_crash_harness::{InFlight, KillPoint, KillTrigger, RunnerHandle, Suite};
use broca_protocol::ToolCallId;
use broca_wal::WalRecord;

broca_crash_harness::crash_harness_child!(scenarios::by_name);

#[tokio::test]
async fn ported_crash_cut_scenarios_hold_every_invariant_at_every_boundary() {
    let mut suite = Suite::new("ported crash-cut scenarios");
    let mut truncations = 0;

    for scenario in scenarios::ALL {
        let handle = RunnerHandle::new(*scenario).await;
        // Properties of each uninterrupted run that its scenario exists to exercise (the budget
        // nudge fires, the third request carries repetition guidance, the render embeds the
        // run identity), checked first so the cuts below are known to cover them.
        match scenario.name {
            "antigravity_identity" => assert!(
                String::from_utf8_lossy(&handle.reference.sent[&0]).contains("identity:r:1"),
                "the reference render must embed run_id=r and step_index=1"
            ),
            "budget_nudge" => assert!(
                handle
                    .reference
                    .log
                    .records()
                    .iter()
                    .any(|record| matches!(record, WalRecord::BudgetFired { .. })),
                "the reference run must fire its budget nudge"
            ),
            "repetition_guidance" => {
                assert!(
                    String::from_utf8_lossy(&handle.reference.sent[&2])
                        .contains("read has now failed 2 times with identical arguments"),
                    "the uninterrupted third request must carry repetition guidance"
                );
                assert!(
                    handle
                        .reference
                        .log
                        .records()
                        .iter()
                        .all(|record| match record {
                            WalRecord::ToolResult { result, .. } =>
                                !serde_json::to_string(&result.output)
                                    .unwrap()
                                    .contains("Tool repetition guidance"),
                            _ => true,
                        }),
                    "guidance must not alter the tool-owned log output"
                );
            }
            _ => {}
        }
        // `inherited_tool_surface` is also cut as a torn append after each boundary; the other
        // scenarios are cut at clean record boundaries only, to keep the suite quick.
        let torn = scenario.name == "inherited_tool_surface";
        let kills = handle.every_record_boundary(torn).await;
        assert_eq!(kills.len(), handle.record_boundaries(torn).len());
        truncations += kills.len();
        assert!(
            kills.iter().any(|kill| kill.after_sync == Some(true))
                && kills.iter().any(|kill| kill.after_sync == Some(false)),
            "{}: the cuts must land both just after a sync and inside an unsynced batch",
            scenario.name
        );
        suite.record(kills);
    }

    // A resume killed inside itself converges. Kill after the first tool result, resume, then
    // cut that resume at every boundary of what it appended and resume again from each.
    let handle = RunnerHandle::new(scenarios::TWO_EDITS).await;
    let after_first_result = handle
        .reference
        .log
        .records()
        .iter()
        .position(|record| matches!(record, WalRecord::ToolResult { .. }))
        .expect("the run records a tool result")
        + 1;
    let kill = handle
        .kill_at(
            KillPoint::RecordBoundary {
                records: after_first_result,
                torn: false,
            },
            KillTrigger::Truncate,
        )
        .await;
    let nested = handle.every_boundary_inside_resume(&kill).await;
    assert!(!nested.is_empty());
    truncations += 1 + nested.len();
    suite.record(std::iter::once(kill).chain(nested));

    // Real process kills: state that lives only in the runner's memory at the kill is gone,
    // and the resume must still hold every invariant.
    let handle = RunnerHandle::new(scenarios::INHERITED_TOOL_SURFACE).await;
    let (mid_dispatch, _) = handle
        .kill_resume_check(
            KillPoint::InFlight(InFlight::ToolDispatch(ToolCallId("t1".into()))),
            KillTrigger::ProcessKill,
        )
        .await;
    let (mid_provider_call, resumed) = handle
        .kill_resume_check(
            KillPoint::InFlight(InFlight::ProviderStep(2)),
            KillTrigger::ProcessKill,
        )
        .await;
    assert_eq!(
        resumed.provider.request_count(),
        1,
        "the step the runner died inside is requested again, once"
    );
    suite.record([mid_dispatch, mid_provider_call]);

    let methods = suite.finish();
    assert_eq!(methods["Truncation"], truncations, "{methods:?}");
    assert_eq!(methods["Sigkill"], 2, "{methods:?}");
}
