#!/usr/bin/env python3
"""Check ckdev-mutate reports for missing Windows mutations and expected test failures.

Selection rules match CK_MUTATE_REV in .github/workflows/mutations.yml.
"""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tomllib
import unittest
from unittest import mock


def eligible_windows(controls):
    eligible = {}
    for row in controls:
        platforms = row.get("platforms")
        if platforms is not None and (not isinstance(platforms, list) or not platforms):
            raise ValueError(f"invalid platforms for {row['id']}")
        # ckdev-mutate uses Rust's OS value "windows", not GitHub's "Windows".
        if platforms is None or "windows" in platforms:
            eligible[row["id"]] = row
    return eligible


def shard_controls(controls, index, count):
    if not 1 <= index <= count:
        raise ValueError("shard must satisfy 1 <= index <= count")
    # ckdev-mutate shards ID-sorted selected rows before platform filtering.
    ordered = sorted(controls, key=lambda row: row["id"])
    return [row for position, row in enumerate(ordered) if position % count == index - 1]


def normalized_control(row):
    # ckdev-mutate compares parsed rows with the base catalogue. An omitted
    # serde default must equal its explicit spelling, or this verifier would
    # demand an unchanged row that the runner correctly did not select.
    return {
        "edits": [], "output_normalize": [], "only": False,
        "timeout_s": 600, "build_timeout_s": 1800, **row,
    }


def normalized_prebuilds(prebuilds):
    return [{"timeout_s": 1800, **prebuild} for prebuild in prebuilds]


def select_diff(controls, previous, changed_paths, prebuild_changed=False):
    old = {row["id"]: normalized_control(row) for row in previous}
    selected = []
    for row in controls:
        edits = row.get("edits", []) or [{"file": row.get("file")}]
        paths = {row.get("test_file"), *(edit["file"] for edit in edits)}
        if prebuild_changed or paths & changed_paths or old.get(row["id"]) != normalized_control(row):
            selected.append(row)
    return selected


def load_selected(catalogue_path, base=None):
    catalogue = tomllib.loads(Path(catalogue_path).read_text(encoding="utf-8"))
    controls = catalogue.get("control", [])
    if base is None:
        return controls
    changed = subprocess.run(
        ["git", "diff", "--name-only", "-z", base, "HEAD", "--"],
        check=True, stdout=subprocess.PIPE,
    ).stdout
    changed_paths = set()
    for path in changed.split(b"\0"):
        if path:
            try:
                changed_paths.add(path.decode("utf-8"))
            except UnicodeDecodeError:
                # ckdev-mutate also ignores non-UTF-8 Git paths.
                continue
    previous = subprocess.run(
        ["git", "show", f"{base}:{catalogue_path}"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if previous.returncode:
        if catalogue_path not in changed_paths:
            raise ValueError("base catalogue missing and catalogue not added in diff")
        old = {}
    else:
        old = tomllib.loads(previous.stdout.decode("utf-8"))
    return select_diff(
        controls, old.get("control", []), changed_paths,
        normalized_prebuilds(catalogue.get("prebuild", [])) != normalized_prebuilds(old.get("prebuild", [])),
    )


def verify(controls, results):
    eligible = eligible_windows(controls)
    known = {row["id"] for row in controls}
    observed = {}
    for result in results:
        row_id = result["id"]
        if row_id not in known or row_id in observed:
            raise ValueError(f"unknown or duplicate report row: {row_id}")
        observed[row_id] = result
    missing = eligible.keys() - observed.keys()
    if missing:
        raise ValueError(f"eligible rows filtered out: {', '.join(sorted(missing))}")
    replayed = []
    for row_id, row in eligible.items():
        result = observed[row_id]
        if result["outcome"] not in {"CAUGHT", "CAUGHT_BROADLY", "HUB"}:
            raise ValueError(f"eligible row {row_id}: {result['outcome']}")
        red = result["red"]
        if not isinstance(red, list) or not all(isinstance(name, str) for name in red):
            raise ValueError(f"invalid red test names for {row_id}")
        if not set(row["expect_red"]) <= set(red):
            raise ValueError(f"expected red tests not observed for {row_id}")
        replayed.append({"id": row_id, "red": red})
    return replayed


class CoverageTests(unittest.TestCase):
    def setUp(self):
        self.controls = [
            {"id": "portable", "expect_red": ["tests::portable"]},
            {"id": "native", "platforms": ["windows"], "expect_red": ["tests::native"]},
            {"id": "unix", "platforms": ["macos", "linux"], "expect_red": ["tests::unix"]},
        ]
        self.results = [
            {"id": "portable", "outcome": "CAUGHT", "red": ["tests::portable"]},
            {"id": "native", "outcome": "CAUGHT", "red": ["tests::native"]},
            {"id": "unix", "outcome": "SKIPPED_PLATFORM", "red": []},
        ]

    def test_pinned_platform_rule(self):
        self.assertEqual(set(eligible_windows(self.controls)), {"portable", "native"})
        with self.assertRaises(ValueError):
            eligible_windows([{"id": "invalid", "platforms": []}])

    def test_unix_approval_gate_controls_are_not_claimed_on_windows(self):
        root = Path(__file__).resolve().parents[1]
        catalog = tomllib.loads((root / "mutations.toml").read_text())
        controls = [row for row in catalog["control"]
                    if row.get("test_file") == "crates/broca-module-serve/tests/it/approval_gate.rs"]
        self.assertEqual({row["id"] for row in controls},
                         {"serve-boot-terminal-withdrawal-debt", "serve-installs-elicitation-plane"})
        for row in controls:
            with self.subTest(control=row["id"]):
                # Order is not meaningful; scripts/verify-mutation-platforms.py checks the claim itself.
                self.assertEqual(sorted(row.get("platforms") or []), ["linux", "macos"])
                self.assertTrue(all(name.startswith("approval_gate::") for name in row["expect_red"]))
        self.assertEqual(eligible_windows(controls), {})

    def test_pinned_shards_sort_before_platform_filtering(self):
        controls = [
            {"id": "d"}, {"id": "c"}, {"id": "b"},
            {"id": "a", "platforms": ["linux"]},
        ]
        shards = [shard_controls(controls, index, 4) for index in range(1, 5)]
        self.assertEqual([[row["id"] for row in shard] for shard in shards], [["a"], ["b"], ["c"], ["d"]])
        self.assertEqual([list(eligible_windows(shard)) for shard in shards], [[], ["b"], ["c"], ["d"]])
        with self.assertRaises(ValueError):
            shard_controls(controls, 0, 4)

    def test_unix_retention_conformance_controls_are_not_claimed_on_windows(self):
        root = Path(__file__).resolve().parents[1]
        catalog = tomllib.loads((root / "mutations.toml").read_text())
        controls = [row for row in catalog["control"]
                    if row.get("test_file") == "crates/broca-module-serve/tests/llm_runner_conformance/retention.rs"]
        self.assertEqual({row["id"] for row in controls}, {
            "retention-writer-admission", "retention-own-tombstone-before-older",
            "retention-tombstone-never-shadows-fresh-lineage-real-daemon",
            "retention-real-daemon-marker-title", "retention-shared-clock-advances-daemon",
            "retention-size-admission-typed-refusal",
        })
        for row in controls:
            with self.subTest(control=row["id"]):
                # Order is not meaningful; scripts/verify-mutation-platforms.py checks the claim itself.
                self.assertEqual(sorted(row.get("platforms") or []), ["linux", "macos"])
                self.assertTrue(all(name.startswith("retention::ck_broca_") for name in row["expect_red"]))
        self.assertEqual(eligible_windows(controls), {})

    def test_reports_actual_red_names_for_every_eligible_row(self):
        for outcome in ("CAUGHT", "CAUGHT_BROADLY", "HUB"):
            with self.subTest(outcome=outcome):
                self.results[1]["outcome"] = outcome
                self.assertEqual(verify(self.controls, self.results), [
                    {"id": "portable", "red": ["tests::portable"]},
                    {"id": "native", "red": ["tests::native"]},
                ])

    def test_rejects_filtered_eligible_row(self):
        with self.assertRaisesRegex(ValueError, "eligible rows filtered out: native"):
            verify(self.controls, [self.results[0], self.results[2]])

    def test_rejects_skipped_eligible_row(self):
        self.results[1]["outcome"] = "SKIPPED_PLATFORM"
        with self.assertRaisesRegex(ValueError, "eligible row native: SKIPPED_PLATFORM"):
            verify(self.controls, self.results)

    def test_rejects_non_replay_successes(self):
        for outcome in ("EQUIVALENT", "EQUIVALENT_CAUGHT", "UNREACHABLE", "DESK_ONLY"):
            with self.subTest(outcome=outcome):
                self.results[1]["outcome"] = outcome
                with self.assertRaisesRegex(ValueError, f"eligible row native: {outcome}"):
                    verify(self.controls, self.results)

    def test_requires_observed_expected_red_test(self):
        self.results[1]["red"] = ["tests::unrelated"]
        with self.assertRaisesRegex(ValueError, "expected red tests not observed for native"):
            verify(self.controls, self.results)

    def test_rejects_duplicate_report_rows(self):
        with self.assertRaisesRegex(ValueError, "unknown or duplicate report row: native"):
            verify(self.controls, self.results + [self.results[1]])

    def test_rejects_unknown_report_rows(self):
        with self.assertRaisesRegex(ValueError, "unknown or duplicate report row: unknown"):
            verify(self.controls, self.results + [{"id": "unknown"}])

    def test_rejects_invalid_red_names(self):
        self.results[1]["red"] = "tests::native"
        with self.assertRaisesRegex(ValueError, "invalid red test names for native"):
            verify(self.controls, self.results)


class SelectionTests(unittest.TestCase):
    def setUp(self):
        self.controls = [
            {"id": "native", "file": "src/native.rs", "test_file": "tests/native.rs", "expect_red": ["tests::native"]},
            {"id": "portable", "file": "src/portable.rs", "test_file": "tests/portable.rs", "expect_red": ["tests::portable"]},
        ]

    def test_touched_rows_require_complete_red_report_without_untouched_rows(self):
        selected = select_diff(self.controls, self.controls, {"src/portable.rs"})
        # The touched row starts in shard 1 even though it was second in the full catalogue.
        assigned = shard_controls(selected, 1, 4)
        self.assertEqual([row["id"] for row in assigned], ["portable"])
        result = {"id": "portable", "outcome": "CAUGHT", "red": ["tests::portable"]}
        self.assertEqual(verify(assigned, [result]), [{"id": "portable", "red": ["tests::portable"]}])
        with self.assertRaisesRegex(ValueError, "eligible rows filtered out: portable"):
            verify(assigned, [])
        result["outcome"] = "SKIPPED_PLATFORM"
        with self.assertRaisesRegex(ValueError, "eligible row portable: SKIPPED_PLATFORM"):
            verify(assigned, [result])

    def test_test_file_and_each_edit_path_select_rows_but_helpers_do_not(self):
        self.controls[0].pop("file")
        self.controls[0]["edits"] = [{"file": "src/a.rs"}, {"file": "src/b.rs"}]
        for path in ("src/a.rs", "src/b.rs", "tests/native.rs"):
            with self.subTest(path=path):
                selected = select_diff(self.controls, self.controls, {path})
                self.assertEqual([row["id"] for row in selected], ["native"])
        self.assertEqual(select_diff(self.controls, self.controls, {"scripts/helper.py"}), [])

    def test_changed_and_added_rows_are_selected_but_deleted_rows_are_not(self):
        previous = [dict(row) for row in self.controls]
        previous.append({"id": "deleted"})
        self.controls[0]["guards"] = "changed property"
        self.controls.append({"id": "added"})
        selected = select_diff(self.controls, previous, set())
        self.assertEqual([row["id"] for row in selected], ["native", "added"])

    def test_explicit_serde_defaults_do_not_select_unchanged_rows(self):
        previous = [dict(row) for row in self.controls]
        self.controls[0].update({"edits": [], "output_normalize": [], "only": False, "timeout_s": 600, "build_timeout_s": 1800})
        self.assertEqual(select_diff(self.controls, previous, set()), [])
        self.controls[0]["only"] = True
        self.assertEqual([row["id"] for row in select_diff(self.controls, previous, set())], ["native"])
        self.assertEqual(normalized_prebuilds([{"name": "rig", "command": ["build"]}]), [{"name": "rig", "command": ["build"], "timeout_s": 1800}])

    def test_changed_prebuilds_select_all_rows(self):
        self.assertEqual(select_diff(self.controls, self.controls, set(), prebuild_changed=True), self.controls)

    def test_empty_touched_selection_is_valid_but_cannot_accept_extra_rows(self):
        selected = select_diff(self.controls, self.controls, set())
        self.assertEqual(verify(shard_controls(selected, 1, 4), []), [])
        with self.assertRaisesRegex(ValueError, "unknown or duplicate report row: native"):
            verify(selected, [{"id": "native"}])

    def test_load_selection_uses_committed_diff_and_ignores_non_utf8_paths(self):
        catalogue = '[[control]]\nid = "native"\nfile = "src/native.rs"\ntest_file = "tests/native.rs"\n'
        responses = [
            subprocess.CompletedProcess([], 0, b"src/native.rs\0invalid-\xff\0"),
            subprocess.CompletedProcess([], 0, catalogue.encode()),
        ]
        with mock.patch.object(Path, "read_text", return_value=catalogue), mock.patch.object(subprocess, "run", side_effect=responses) as git:
            selected = load_selected("mutations.toml", "base-sha")
        self.assertEqual([row["id"] for row in selected], ["native"])
        self.assertEqual(git.call_args_list[0].args[0], ["git", "diff", "--name-only", "-z", "base-sha", "HEAD", "--"])
        self.assertEqual(git.call_args_list[1].args[0], ["git", "show", "base-sha:mutations.toml"])

    def test_missing_base_catalogue_is_allowed_only_when_catalogue_changed(self):
        catalogue = '[[control]]\nid = "native"\n'
        for changed in (b"mutations.toml\0", b""):
            responses = [subprocess.CompletedProcess([], 0, changed), subprocess.CompletedProcess([], 1, b"", b"missing")]
            with mock.patch.object(Path, "read_text", return_value=catalogue), mock.patch.object(subprocess, "run", side_effect=responses):
                if changed:
                    self.assertEqual([row["id"] for row in load_selected("mutations.toml", "base")], ["native"])
                else:
                    with self.assertRaisesRegex(ValueError, "base catalogue missing and catalogue not added in diff"):
                        load_selected("mutations.toml", "base")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalogue", default="mutations.toml")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--all", action="store_true")
    mode.add_argument("--diff", metavar="BASE")
    parser.add_argument("--shard", default="1/1")
    parser.add_argument("--write-selection", metavar="PATH")
    parser.add_argument("--selection", metavar="PATH")
    parser.add_argument("--report", default="target/mutations/windows-replay.json")
    args = parser.parse_args()
    if args.write_selection:
        if not args.all and args.diff is None:
            parser.error("--write-selection requires --all or --diff BASE")
        index, count = map(int, args.shard.split("/"))
        selected = shard_controls(load_selected(args.catalogue, args.diff), index, count)
        selection = {
            "mode": "diff" if args.diff is not None else "all",
            "base": args.diff,
            "head": subprocess.run(["git", "rev-parse", "HEAD"], check=True, stdout=subprocess.PIPE, text=True).stdout.strip(),
            "shard": args.shard,
            "runner_rev": os.environ["CK_MUTATE_REV"],
            "selected_ids": [row["id"] for row in selected],
        }
        Path(args.write_selection).write_text(json.dumps(selection, indent=2) + "\n", encoding="utf-8")
        print(f"Selected {len(selected)} catalogue rows ({selection['mode']}, shard {args.shard}).")
        return
    if not args.selection:
        parser.error("coverage verification requires --selection PATH")
    if sys.platform != "win32":
        raise SystemExit("Windows mutation coverage requires a native Windows replay")
    selection = json.loads(Path(args.selection).read_text(encoding="utf-8"))
    with open(args.catalogue, "rb") as source:
        controls = {row["id"]: row for row in tomllib.load(source).get("control", [])}
    ids = selection["selected_ids"]
    if len(ids) != len(set(ids)) or any(row_id not in controls for row_id in ids):
        raise SystemExit("selection contains unknown or duplicate catalogue IDs")
    if selection["runner_rev"] != os.environ["CK_MUTATE_REV"]:
        raise SystemExit("selection runner revision differs from replay runner revision")
    results = json.loads(Path(args.report).read_text(encoding="utf-8"))
    try:
        replayed = verify([controls[row_id] for row_id in ids], results)
    except ValueError as error:
        raise SystemExit(str(error)) from error
    coverage = {**selection, "platform": "windows", "replayed": replayed}
    Path("target/mutations/windows-coverage.json").write_text(json.dumps(coverage, indent=2) + "\n", encoding="utf-8")
    shard = selection["shard"]
    lines = [f"## Native Windows mutation coverage ({selection['mode']}, shard {shard})", "", "| Row ID | Observed red tests |", "| --- | --- |"]
    for row in replayed:
        print(f"{row['id']}: {', '.join(row['red'])}")
        lines.append(f"| {row['id']} | {', '.join(row['red'])} |")
    lines.append(f"\nReplayed {len(replayed)} Windows-eligible selected rows assigned to this shard.")
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as summary:
        summary.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    if sys.argv[1:2] == ["--self-test"]:
        unittest.main(argv=[sys.argv[0], *sys.argv[2:]], verbosity=2)
    else:
        main()
