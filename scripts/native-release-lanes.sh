#!/usr/bin/env bash
# Shared by the macOS operator runner and scripts/native-release-windows.sh.
# The caller supplies run <lane> <command...> and stages its own test rigs.

native_release_lane_exclusion() {
  case "$1:$2" in
    windows:daemon-upgrade)
      printf '%s\n' "SUBC help: \`ck daemon upgrade --in-place\` replaces the daemon with the executable at its startup path (Linux and macOS)."
      ;;
    *) return 1 ;;
  esac
}

# Exercise legacy and tool-result/v1 image replies from a stand-in for AFT.
run real-daemon cargo test --locked -p broca-module-serve --features test-clock,test-loopback-credentials --test it --test tool_result_v1_real_daemon
# Exercise broca-subc's tool, restart, resume, cancel and scope paths against ck-subc.
run subc-real-daemon cargo test --locked -p broca-subc --test it
# Verify scoped session calls from broca-session and worker admission under a real daemon.
run scope-real-daemon cargo test --locked -p broca-module-serve --features test-clock --test scope_real_daemon_e2e
# The upgrade cases need a daemon with upgrade hooks and a broca that can delay reconnecting.
run conformance cargo test --locked -p broca-module-serve --features test-clock --test llm_runner_conformance -- --skip held_late_results::daemon_upgrade
# Media capture is off in shipping builds, so check the media shapes with it enabled separately.
run conformance-media cargo test --locked -p broca-module-serve --features test-clock,test-attachments --test llm_runner_conformance ck_broca_passes_the_llm_runner_conformance_suite -- --exact
run daemon-upgrade cargo test --locked -p broca-module-serve --features test-clock,test-hooks --test llm_runner_conformance -- held_late_results::daemon_upgrade
run media-acceptance cargo test --locked -p broca-module-serve --features test-clock,test-attachments --test it media_
run run-events-bus cargo test --locked -p broca-module-serve --features test-clock --test run_events_bus
run agent-data cargo test --locked -p broca-module-serve --features test-clock --test agent_data
run llm-tools cargo test --locked -p broca-module-serve --features test-clock --test llm_tools_route --test llm_tools_private_content --test llm_tools_consent --test llm_tools_held --test llm_tools_permissions --test llm_tools_crash --test llm_tools_cerebellum
run classifier-conformance cargo test --locked -p broca-module-serve --features test-clock --test classifier_conformance
run extractor-conformance env CARGO_INCREMENTAL=0 cargo test --locked -p broca-module-serve --features test-clock --test extractor_conformance -- --nocapture
