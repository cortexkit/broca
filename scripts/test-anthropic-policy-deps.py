#!/usr/bin/env python3
"""Keep the shared Anthropic policy safe for transport-free gateway consumers."""

import os
from pathlib import Path
import subprocess
import sys
import tomllib
import unittest


class AnthropicPolicyDependencies(unittest.TestCase):
    def test_dependency_graph_stays_transport_free(self):
        root = Path(__file__).resolve().parent.parent
        manifest = tomllib.loads(
            (root / "crates/cortexkit-anthropic-policy/Cargo.toml").read_text()
        )
        self.assertNotIn("features", manifest, "Shared policy must have no feature flags")
        # Check declared dependencies first so this test reports a forbidden dependency
        # even when Cargo would stop because the lockfile is out of date.
        for section in ("dependencies", "dev-dependencies", "build-dependencies"):
            self.assertLessEqual(set(manifest.get(section, {})), {"serde_json", "httpdate"})
        self.assertNotIn("target", manifest, "Target-specific dependencies need review")
        cargo = os.environ.get("CARGO", "cargo")
        result = subprocess.run(
            [cargo, "tree", "--locked", "--offline", "-p", "cortexkit-anthropic-policy",
             "--edges", "all", "--prefix", "none", "--format", "{p}"],
            cwd=root, text=True, capture_output=True, timeout=120,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        packages = {line.split()[0] for line in result.stdout.splitlines() if line.strip()}
        self.assertIn("cortexkit-anthropic-policy", packages, "Cargo tree must examine the crate")
        self.assertLessEqual(packages, {
            "cortexkit-anthropic-policy", "serde_json", "httpdate", "serde", "serde_core",
            "serde_derive", "proc-macro2", "quote", "syn", "unicode-ident", "itoa", "memchr", "zmij",
        }, result.stdout)
        print(f"Checked {len(packages)} packages in cortexkit-anthropic-policy's cargo tree")


if __name__ == "__main__":
    # Accept an exact test name for the mutation runner; ordinary invocation runs all tests.
    unittest.main(argv=[sys.argv[0], *sys.argv[1:]], verbosity=2)
