#!/usr/bin/env python3
"""Overlay only the diagnostic allocator and load server onto an archived broca release.

Extract `git archive v0.3.199` inside the task worktree or a job-private directory,
then pass that directory here. Dependencies and production sources stay pinned to
that release; the optional path dependency adds no registry packages.
"""
import argparse
from pathlib import Path
import re
import shutil

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('historical', type=Path)
args = parser.parse_args()
source = Path(__file__).resolve().parents[1]
root = args.historical.resolve()
assert root != source
shutil.copytree(source / 'crates/broca-heap-profile', root / 'crates/broca-heap-profile')
shutil.copy2(source / 'crates/broca-module-serve/src/bin/memory-load-server.rs', root / 'crates/broca-module-serve/src/bin/memory-load-server.rs')
manifest = root / 'Cargo.toml'
text = manifest.read_text().replace('members = [', 'members = [\n    "crates/broca-heap-profile",', 1)
manifest.write_text(text)
manifest = root / 'crates/broca-module-serve/Cargo.toml'
text = manifest.read_text().replace('[dependencies]', '[dependencies]\nbroca-heap-profile = { path = "../broca-heap-profile", optional = true }', 1)
text = text.replace('[features]', '[features]\nheap-profile = ["dep:broca-heap-profile"]', 1)
text += '\n[[bin]]\nname = "memory-load-server"\npath = "src/bin/memory-load-server.rs"\nrequired-features = ["heap-profile"]\n'
manifest.write_text(text)
lock = root / 'Cargo.lock'
text = lock.read_text()
version = re.search(r'\[workspace.package\]\nversion = "([^"]+)"', (root / 'Cargo.toml').read_text())[1]
assert version == '0.3.199', f'overlay is validated only for v0.3.199, got {version}'
# That release discovers session refs from disk; the indexed lookup hook arrived later.
server = root / 'crates/broca-module-serve/src/bin/memory-load-server.rs'
server.write_text(server.read_text().replace('    wal.set_session_ref_index(store.clone());\n', '', 1))
text = text.replace('[[package]]\nname = "broca-module"', f'[[package]]\nname = "broca-heap-profile"\nversion = "{version}"\ndependencies = [\n "serde",\n "serde_json",\n "tokio",\n]\n\n[[package]]\nname = "broca-module"', 1)
prefix, remainder = text.split('name = "broca-module-serve"', 1)
remainder = remainder.replace(' "broca-fetch-plan",', ' "broca-fetch-plan",\n "broca-heap-profile",', 1)
lock.write_text(prefix + 'name = "broca-module-serve"' + remainder)
print(f'Overlaid measurement-only sources onto broca {version} at {root}')
