#!/usr/bin/env python3
"""Exercise release admission at staging and at the final signed placement copy."""

import importlib.util
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
CLOCK_MARKER = "BROCA_TEST_CLOCK_RETENTION_CLOCK_OVERRIDE_V1"
RETRY_MARKER = "BROCA_TEST_HARNESS_VIRTUAL_RETRY_WAITS_V1"
ATTACHMENTS_MARKER = "broca-nonshipping-attachments-v1"

spec = importlib.util.spec_from_file_location("placement_fixture", ROOT / "scripts/test-place-release-gate.py")
placement = importlib.util.module_from_spec(spec)
spec.loader.exec_module(placement)


class CatalogApiFence(unittest.TestCase):
    def test_snapshot_installers_require_test_support(self):
        source = (ROOT / "crates/broca-catalog/src/runtime.rs").read_text()
        for name in ("install_for_test", "install_parsed_for_test"):
            with self.subTest(api=name):
                # Inspect the attributes on the API itself, not a feature mention elsewhere.
                declaration = re.search(r"((?:\s*#\[[^\n]+\]\n)+)\s*pub fn " + name + r"\(", source)
                self.assertIsNotNone(declaration, name)
                self.assertIn('#[cfg(any(test, feature = "test-support"))]', declaration[1], name)

    def test_catalog_test_support_is_forbidden_without_debug_assertions(self):
        source = (ROOT / "crates/broca-catalog/src/runtime.rs").read_text()
        prohibition = ('#[cfg(all(feature = "test-support", not(debug_assertions)))]\n'
                       'compile_error!("catalog test-support cannot be enabled in a release build");')
        self.assertTrue(prohibition in source, "catalog test-support needs a release compile prohibition")


class RetryMarkerStaging(unittest.TestCase):
    def probe(self, content):
        with tempfile.TemporaryDirectory(prefix="broca-retry-marker-") as tmp:
            artifact = Path(tmp) / "artifact"
            artifact.write_bytes(content)
            return subprocess.run(
                ["bash", str(ROOT / "scripts/stage-release.sh"), "--probe-only", str(artifact)],
                cwd=ROOT, env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"),
                capture_output=True, text=True, timeout=30,
            )

    def test_test_harness_marker_refuses_artifact(self):
        result = self.probe(b"ordinary control\0" + RETRY_MARKER.encode() + b"\0")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("artifact contains the test-harness override marker", result.stderr)

    def test_test_attachments_marker_refuses_artifact(self):
        result = self.probe(b"ordinary control\0" + ATTACHMENTS_MARKER.encode() + b"\0")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("artifact contains the test-attachments override marker", result.stderr)

    def test_normal_artifact_passes_both_marker_checks(self):
        result = self.probe(b"ordinary production artifact\0")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("test-clock absent (planted strings control reads 1)", result.stdout)
        self.assertIn("test-harness absent (planted strings control reads 1)", result.stdout)
        self.assertIn("test-attachments absent (planted strings control reads 1)", result.stdout)

    def test_retry_path_carries_the_marker(self):
        source = (ROOT / "crates/broca-provider/src/transport.rs").read_text()
        sleep = source.split("async fn sleep(&self, delay: Duration) {", 1)[1].split("tokio::time::sleep(delay)", 1)[0]
        self.assertIn('#[cfg(feature = "test-harness")]\n        tracing::info!("' + RETRY_MARKER + '");', sleep)


class ClientMarkerStaging(unittest.TestCase):
    def test_staging_refuses_test_harness_client(self):
        source = (ROOT / "scripts/stage-release.sh").read_text()
        start = source.index("for client in broca-session broca-run; do", source.index("# Clients are signed in private scratch too:"))
        block = source[start:source.index("# THE CURRENT-ARTIFACT MANIFEST.", start)]
        with tempfile.TemporaryDirectory(prefix="broca-client-marker-") as tmp:
            root = Path(tmp)
            (root / "target/release").mkdir(parents=True)
            (root / "scratch").mkdir()
            (root / "seam").mkdir()
            for client in ("broca-session", "broca-run"):
                (root / "target/release" / client).write_text(RETRY_MARKER + "\n")
            result = subprocess.run(
                ["bash", "-c", 'set -euo pipefail; source "$ROOT/scripts/nonshipping-markers.sh"; '
                 'uname() { echo Linux; }; publish_verified() { cp "$1" "$2"; }; ' + block],
                cwd=root, env=dict(os.environ, ROOT=str(ROOT), BUILD_SCRATCH=str(root / "scratch"),
                                  SEAM=str(root / "seam"), COMMIT="fixture"),
                capture_output=True, text=True, timeout=30,
            )
            self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
            self.assertIn("artifact contains the test-harness override marker", result.stderr)
            self.assertEqual(list((root / "seam").iterdir()), [])


class ReleaseMarkerPlacement(placement.GateBase):
    def setUp(self):
        super().setUp()
        placement.stub_binary(self.dest, version="0.3.148", signed="adhoc,runtime")
        placement.stub_binary(self.staged, features=[])
        self.before = self.dest.read_bytes()

    def assert_refused(self, result, name="ck-broca"):
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("placement gate check 'nonshipping-marker' failed", result.stderr)
        self.assertIn(f"file  {self.bindir / name}.place.", result.stderr)
        self.assertEqual(self.dest.read_bytes(), self.before)
        self.assertFalse(self.rollback.exists())
        self.assertFalse(any(".place." in p.name for p in self.bindir.iterdir()))

    def test_direct_placement_refuses_nonshipping_markers(self):
        for marker in (CLOCK_MARKER, RETRY_MARKER, ATTACHMENTS_MARKER):
            with self.subTest(marker=marker):
                placement.stub_binary(self.staged, features=[])
                with self.staged.open("ab") as output:
                    output.write(b"\n# " + marker.encode() + b"\n")
                self.assert_refused(self.place("--skip-gate"))

    def test_marker_added_by_signing_is_refused(self):
        signer = self.fake_bin / "codesign"
        source = signer.read_text().replace(
            'data += MARK + f"flags={flags} entitlements={ents}\\n".encode()',
            'if os.path.basename(path).startswith("ck-broca.place."):\n'
            '        data += b"\\n# ' + RETRY_MARKER + '\\n"\n'
            '    data += MARK + f"flags={flags} entitlements={ents}\\n".encode()',
        )
        signer.write_text(source)
        self.assertNotIn(RETRY_MARKER.encode(), self.staged.read_bytes())
        self.assert_refused(self.place())

    def test_client_marker_refuses_before_any_binary_is_placed(self):
        client = self.tmp / "broca-run.staged"
        client.write_bytes(client.read_bytes().replace(
            b"# identity", b"# " + RETRY_MARKER.encode() + b"\n# identity",
        ))
        self.assert_refused(self.place(), name="broca-run")

    def test_normal_placement_passes_marker_checks(self):
        result = self.place()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("test-harness absent (planted strings control reads 1)", result.stdout)
        self.assertIn("test-attachments absent (planted strings control reads 1)", result.stdout)
        self.assertNotEqual(self.dest.read_bytes(), self.before)


if __name__ == "__main__":
    unittest.main(verbosity=2)
