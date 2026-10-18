//! All of this crate's integration tests that can share one process build into this single test
//! binary. Each former `tests/<name>.rs` is the module `<name>` here, so its tests are named
//! `<name>::<test>` (run one file's tests with `cargo test -p broca-module-serve --test it <name>::`).
//!
//! One binary rather than one per file because every test executable is linked separately and, on
//! macOS, is assessed by the code-signing check the first time it runs, so per-file binaries made
//! the suite slow to build and slow to start.
//!
//! Some files keep binaries of their own, because sharing a process would change what they check:
//! - `manifest_older_build` and `steer_older_build` make this process forget a WAL feature
//!   (`broca_wal::forget_feature_for_test`), which every other WAL writer in the process would see.
//! - `launch_nonce` needs the launch-nonce accessor's process-wide cache fresh, and re-runs its own
//!   binary by exact test name.
//! - `operation_counters` asserts exact values of process-wide operation counters, which any other
//!   test in the process would move.
//! - `in_use_models_still_serve` replaces the process-global catalog with its own mid-test, while
//!   `wire` installs the vendored snapshot there.
//! - `fleet_log_record_shape` installs the process-global log subscriber and expects to be the only
//!   one.
//! - `scope_real_daemon_e2e` replays scope admission mutations against real daemons, independently
//!   of the unrelated provider and vault fixtures in this target.
//!
//! The two wire health probes also need fresh lifetime projection metrics. They
//! re-execute their exact test names in a child, so this target still runs them.
//!
//! Several rigs here remove `SUBC_MODULE_ID` and `SUBC_LAUNCH_NONCE` from this process's own
//! environment. That is safe to share: the removal only ever moves toward the state every test here
//! wants, and nothing restores the variables.

#[path = "../common/mod.rs"]
mod common;
#[path = "../delivery_rig/mod.rs"]
mod delivery_rig;
#[path = "../step_transform_rig/mod.rs"]
mod step_transform_rig;

mod account_selection_real_daemon;
mod anthropic_default_cache_render;
mod anthropic_model_switch_live;
mod anthropic_thinking_binding_live;
mod anthropic_thinking_echo_live;
#[cfg(unix)]
mod approval_gate;
mod cache_shared_prefix_render;
mod captured_replay_e2e;
mod chatgpt_websocket_live;
mod early_args_boot;
mod engram_descriptor;
mod graceful_stop_real_daemon;
mod input_token_cap;
mod launch_nonce_e2e;
mod llm_runner_role;
mod log_levels_are_deliberate;
mod meta_prompt_cache_key_render;
#[cfg(unix)]
mod placed_aft_preset;
mod plan_admission;
mod plan_fetch_real_daemon;
mod preset_real_daemon;
mod prompt_blocks_render;
mod provider_override_boot;
mod renderer_version_resume_real_daemon;
mod reply_deadline_real_daemon;
mod retention_consent_wire;
mod scope_carrier;
mod scope_stamp_source;
mod seal_line_is_filterable_e2e;
mod send_delivery;
mod session_client;
mod single_turn_module_e2e;
mod state_root_isolation;
mod step_transforms;
mod structured_output;
mod transform_route_removed;
mod wal_known_features;
mod wire;
