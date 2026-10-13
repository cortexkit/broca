import os, re, json, sys
from pathlib import Path
from lib import state_root
from lib.archive_read import ArchivedWal, ArchiveRefusal, logical_wals
# The state root is OVERRIDABLE, because a census that can only ever read the live tree cannot
# be tested. Proving the brace fix below needed a fixture, and the hardcoded path silently
# scanned production instead -- a test that reports on the wrong tree and looks like it passed.
# Takes --state-root as the sibling scripts do, and otherwise resolves the root as the daemon does.
def _state_root():
    for i, arg in enumerate(sys.argv):
        if arg == "--state-root" and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return str(state_root.default_state_root())

WAL = os.path.join(_state_root(), "wal")

def fnv(s):
    h = 0xcbf29ce484222325
    for b in s.encode():
        h ^= b; h = (h * 0x100000001b3) & 0xFFFFFFFFFFFFFFFF
    return f"{h:016x}"

def triple(path):
    try:
        if isinstance(path, ArchivedWal):
            raw = path.read_bytes()[:8000]
        else:
            with path.open("rb") as source:
                raw = source.read(8000)
        raw = raw.decode("utf-8", "replace")
    except OSError:
        return None
    i = raw.find('"session":{')
    if i < 0: return None
    # raw_decode RESPECTS QUOTED STRINGS; brace counting does not.
    #
    # This used to walk the text counting { and }, so a project_root containing a CLOSING BRACE
    # -- `/work/brace}root`, a legal directory name, and BindIdentity::validate_keyable rejects
    # only U+001F -- drove depth to zero mid-string. json.loads then got truncated JSON, raised,
    # and the WAL was dropped from a census whose whole job is finding WALs nobody can reach.
    try:
        return json.JSONDecoder().raw_decode(raw[i + 10:])[0]
    except ValueError:
        return None

try:
    # Both live roots share addresses, but older tombstones cannot hide a fresh WAL.
    files = logical_wals(Path(WAL))
    # Verify archived bytes before any census output, including empty members.
    sizes = {f.name: len(f.read_bytes()) if isinstance(f, ArchivedWal) else f.stat().st_size
             for f in files}
except (ArchiveRefusal, OSError) as error:
    print(f"REFUSING: {error}", file=sys.stderr)
    sys.exit(2)
empty = {f.name for f in files if sizes[f.name] == 0}
# A WAL carrying ONLY its lineage frame has no records, so it has no bind identity to read --
# it is a lineage minted and never written, not a parse failure. broca-wal pins that frame at
# LINEAGE_FRAME_LEN = 69 bytes (framing.rs), and sweep-empty-wals.py protects the same class from
# deletion for the same reason. Counting these as unreadable made the refusal below fire on 37
# ordinary production files the first time it ran, which is how a true guard gets deleted.
nonempty = [f for f in files if sizes[f.name] > 69]
scanned = moved = stranded = 0
pairs = []
unparsed = []
for f in nonempty:
    t = triple(f)
    if not t:
        # COUNT WHAT IS SKIPPED. The zero-scan refusal below cannot see a PARTIAL failure: one
        # readable session keeps `scanned` nonzero while another is dropped silently, which is
        # precisely how the brace bug hid -- a census reporting STRANDED=1 when the answer was 2,
        # exiting 0. A guard against "parsed nothing" is not a guard against "parsed some".
        unparsed.append(f.name)
        continue
    scanned += 1
    root = t.get("project_root", "")
    resolved = os.path.realpath(root)
    if resolved == root: continue
    moved += 1
    sib = fnv(f"{resolved}\u001f{t['harness']}\u001f{t['session']}") + ".wal"
    if sib in empty:
        stranded += 1
        pairs.append((f.name, sib, root, resolved, t["session"][:60]))
print(f"    wals={len(files)} empty={len(empty)} scanned={scanned} root_moved={moved} STRANDED={stranded}")
if scanned == 0:
    print("    REFUSING: parsed zero triples — the probe cannot fire, so a 0 result means nothing")
    sys.exit(2)
if unparsed:
    print(f"    REFUSING: {len(unparsed)} non-empty WAL(s) had no readable bind identity, so this")
    print("      census covers less than the tree and its STRANDED count is a lower bound:")
    for name in unparsed[:10]:
        print(f"        {name}")
    sys.exit(2)
for p in pairs[:10]:
    print(f"      {p[0]} -> empty {p[1]}  {p[2]} => {p[3]}  [{p[4]}]")

# ENUMERATE BY MOVED ROOT, NOT BY PLACEHOLDER. The stranded count below is a
# strict subset of root_moved: a placeholder only exists where somebody already
# TRIED to resume, so counting placeholders finds exactly the sessions a user has
# already noticed are broken -- a confirmation instrument wearing a discovery
# instrument's clothes. Measured on the rename this was written for: 65 sessions
# had moved, 24 had been walked into. Stopping at the placeholders would have
# declared the recovery complete and left 1,339 KB unaddressable, with no way to
# find it afterwards.
#
# WHY THE FAIL-FIRST CONTROL IS NOT OPTIONAL, from the night this was written:
# a synthetic stranded pair planted with json.dumps() defaults did NOT fire this
# census, and the census looked broken. The plant was malformed -- json.dumps emits
# `"session": {` with a space, while real records carry no space, so the marker below
# never matched. A census that reports 0 is worth nothing until it has been seen to
# report 1 on a pair built the way the writer builds them.
#
# THE WINDOW: this only detects a move while realpath(embedded_root) still differs
# from the embedded root -- i.e. while a compatibility symlink or the old directory
# survives. Remove the symlink and the pairing evaporates: the embedded root resolves
# to itself and the store keeps no other evidence that the session moved. Run this
# DURING a rename window, not as an always-available tool.
