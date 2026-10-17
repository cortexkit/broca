#!/usr/bin/env python3
"""Guard production Tokio features and transport-free non-live dependency graphs."""

import json
from pathlib import Path
import re
import subprocess
import sys
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parent.parent

# Members whose whole design is to stay free of Tokio declare it nowhere, not even for tests.
# The shared ChatGPT policy crate is embedded by other gateways and its own guard
# (crates/cortexkit-chatgpt-policy/tests/dependency_policy.rs) refuses any dependency beyond
# its JSON, HTTP-date and base64 decoders. A member listed here must declare no Tokio at all;
# the feature-agreement rule below exists so test builds that do pull Tokio agree, and a
# Tokio-free member cannot disagree.
TOKIO_FREE_MEMBERS = {"cortexkit-chatgpt-policy", "broca-owner-only"}


def cargo_output(*args):
    result = subprocess.run(
        ["cargo", *args], cwd=ROOT, check=True, text=True, capture_output=True,
        timeout=120,
    )
    return result.stdout


def tree(*args):
    output = cargo_output("tree", "--locked", *args)
    if not output.strip():
        raise ValueError("cargo tree returned an empty graph for " + " ".join(args))
    return output


class DependencyGraphProof(unittest.TestCase):
    def test_production_tokio_excludes_test_util(self):
        # broca-module-serve owns ck-broca. Dev dependencies must be excluded:
        # test-util synchronizes Tokio's clock even when production never pauses it.
        output = tree(
            "-p", "broca-module-serve", "-e", "features,no-dev", "-i", "tokio",
            "--target", "all",
        )
        self.assertRegex(output, r"\Atokio v\d", "inverse tree must reach Tokio")
        self.assertIn("broca-module-serve v", output)
        self.assertNotIn('tokio feature "test-util"', output)

    def test_dev_tokio_includes_test_util(self):
        output = tree(
            "-p", "broca-module-serve", "-e", "features", "-i", "tokio",
            "--target", "all",
        )
        self.assertRegex(output, r"\Atokio v\d", "inverse tree must reach Tokio")
        self.assertIn('tokio feature "test-util"', output)

    def test_every_member_declares_identical_dev_tokio(self):
        # Cargo resolves inherited declarations for us, avoiding a TOML parser
        # dependency or a requirement for Python 3.11's tomllib on CI runners.
        metadata = json.loads(cargo_output(
            "metadata", "--locked", "--no-deps", "--format-version", "1",
        ))
        members = set(metadata["workspace_members"])
        self.assertTrue(members, "cargo metadata returned no workspace members")
        packages = [p for p in metadata["packages"] if p["id"] in members]
        self.assertEqual({p["id"] for p in packages}, members)
        expected_features = None
        for package in packages:
            with self.subTest(member=package["name"]):
                if package["name"] in TOKIO_FREE_MEMBERS:
                    declared = [d for d in package["dependencies"] if d["name"] == "tokio"]
                    self.assertEqual(declared, [], "a Tokio-free member declares Tokio")
                    continue
                dev_tokio = [
                    d for d in package["dependencies"]
                    if d["name"] == "tokio" and d["kind"] == "dev"
                ]
                self.assertEqual(len(dev_tokio), 1, "member must declare dev Tokio")
                features = set(dev_tokio[0]["features"])
                self.assertIn("test-util", features)
                if expected_features is None:
                    expected_features = features
                self.assertEqual(features, expected_features)
                self.assertFalse(dev_tokio[0]["optional"])
        print("dependency feature check: {} workspace members checked".format(len(packages)))

    def test_non_live_normal_graphs_exclude_live_transport(self):
        # These packages must not acquire the provider's optional networking stack
        # through feature unification or a workspace-hack dependency.
        for package in ["broca-protocol", "broca-wal", "broca-provider", "broca-catalog"]:
            with self.subTest(package=package):
                output = tree("-p", package, "-e", "normal,build", "--target", "all")
                self.assertRegex(output, r"\A" + re.escape(package) + r" v\d")
                self.assertIsNone(re.search(
                    r"\b(?:reqwest|rustls|ring|hyper|tungstenite|tokio-tungstenite) v",
                    output,
                ))

    def test_cargo_tree_refuses_empty_graph(self):
        # Absence checks must not turn a successful but empty Cargo response into
        # a claim about dependencies they never examined.
        for output in ["", " \n\t"]:
            with self.subTest(output=repr(output)):
                result = subprocess.CompletedProcess(["cargo", "tree"], 0, output, "")
                with mock.patch("subprocess.run", return_value=result):
                    with self.assertRaisesRegex(ValueError, "cargo tree returned an empty graph"):
                        tree("-p", "broca-provider", "-e", "normal,build")

    def test_cargo_tree_failure_is_not_an_empty_graph_pass(self):
        error = subprocess.CalledProcessError(1, ["cargo", "tree"], stderr="Cargo failed")
        with mock.patch("subprocess.run", side_effect=error):
            with self.assertRaises(subprocess.CalledProcessError):
                tree("-p", "broca-provider", "-e", "normal,build")


if __name__ == "__main__":
    print("Python " + sys.version.split()[0], flush=True)
    print(cargo_output("--version").strip(), flush=True)
    unittest.main(verbosity=2)
