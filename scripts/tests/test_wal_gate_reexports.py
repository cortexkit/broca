"""Keep source-based WAL feature discovery aligned with Rust re-exports."""

import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]


class WalFeatureReexports(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("wal_gate_reexports", ROOT / "scripts/lib/wal_gate.py")
        cls.gate = importlib.util.module_from_spec(spec)
        # Isolate import-time discovery so each test controls the source tree it reads.
        initial = 'pub const KNOWN_FEATURES: &[&str] = &[A_FEATURE];\npub const A_FEATURE: &str = "a/v1";\n'
        with mock.patch.object(Path, "read_text", return_value=initial):
            spec.loader.exec_module(cls.gate)

    def setUp(self):
        scratch = ROOT / "target/scratch"
        scratch.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.wal = self.root / "broca-wal/src"
        records = self.root / "broca-records/src"
        self.wal.mkdir(parents=True)
        records.mkdir(parents=True)
        self.framing = self.wal / "framing.rs"
        self.framing.write_text(
            'pub const KNOWN_FEATURES: &[&str] = &[A_FEATURE, crate::recovery::FEATURE];\n'
            'pub use broca_records::features::{A_FEATURE};\n'
        )
        (self.wal / "recovery.rs").write_text('pub use broca_records::recovery::FEATURE;\n')
        (records / "features.rs").write_text(
            'pub const A_FEATURE: &str = "a/v1";\n'
            'pub const UNEXPORTED_FEATURE: &str = "not-exported/v1";\n'
        )
        (records / "recovery.rs").write_text('pub const FEATURE: &str = "recovery/v1";\n')
        patch = mock.patch.object(self.gate, "FRAMING_RS", self.framing)
        patch.start()
        self.addCleanup(patch.stop)

    def test_reexported_feature_constants_keep_the_complete_table(self):
        self.assertEqual(
            self.gate.broca_known_features(self.framing.read_text(), self.gate._broca_wal_module_source),
            frozenset({"a/v1", "recovery/v1"}),
        )

    def test_unexported_or_missing_record_constants_are_refused(self):
        for constant in ("UNEXPORTED_FEATURE", "MISSING_FEATURE"):
            with self.subTest(constant=constant):
                source = self.framing.read_text().replace("&[A_FEATURE,", f"&[{constant},")
                with self.assertRaises(ValueError):
                    self.gate.broca_known_features(source, self.gate._broca_wal_module_source)


if __name__ == "__main__":
    unittest.main()
