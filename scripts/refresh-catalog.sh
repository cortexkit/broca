#!/usr/bin/env bash
#
# Refresh the vendored models.dev catalog snapshot and its derived artifacts.
#
# This is the ONLY supported way to update the catalog. It regenerates three files
# that must always move together, so they can never drift:
#   crates/broca-catalog/data/models.dev.json        — the vendored snapshot
#   crates/broca-catalog/data/models.dev.meta.json   — provenance (sha256, counts, time)
#   crates/broca-catalog/data/resolution-report.json — per-provider family resolution
#
# The resolution report is the review gate: after running this, the report diff shows
# exactly which providers changed wire family. CI (the resolution_report test) fails
# if the snapshot moves without the report being regenerated, so a silent family
# re-route cannot merge. REVIEW THE REPORT DIFF before committing a refresh.
#
# Usage: scripts/refresh-catalog.sh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
data_dir="$repo_root/crates/broca-catalog/data"
snapshot="$data_dir/models.dev.json"
meta="$data_dir/models.dev.meta.json"
source_url="https://models.dev/api.json"

command -v python3 >/dev/null || { echo "error: python3 is required" >&2; exit 1; }

echo "→ fetching $source_url"
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
curl -fsSL --max-time 60 "$source_url" -o "$tmp"

# Normalize to a deterministic, diff-friendly form (sorted keys, 2-space indent) and
# regenerate provenance. A fetched snapshot that doesn't parse aborts here.
echo "→ normalizing snapshot + regenerating provenance"
python3 - "$tmp" "$snapshot" "$meta" "$source_url" <<'PY'
import json, hashlib, sys, datetime, os

tmp, snapshot, meta, source_url = sys.argv[1:5]
with open(tmp, "rb") as f:
    raw = f.read()
d = json.loads(raw)  # aborts on malformed fetch

# Deterministic snapshot: sorted keys, 2-space indent, trailing newline.
with open(snapshot, "w", encoding="utf-8") as f:
    json.dump(d, f, indent=2, sort_keys=True, ensure_ascii=False)
    f.write("\n")

# sha256 over the NORMALIZED bytes we actually vendored.
with open(snapshot, "rb") as f:
    normalized = f.read()
sha = hashlib.sha256(normalized).hexdigest()
n_models = sum(len(p.get("models") or {}) for p in d.values())

# Keep fetched_at STABLE when the upstream content is unchanged, so a no-op refresh
# produces a zero diff (otherwise a fresh timestamp every run would be spurious noise
# that hides whether anything real moved). Only stamp a new time when sha256 changes.
prior = {}
if os.path.exists(meta):
    try:
        prior = json.load(open(meta))
    except Exception:
        prior = {}
if prior.get("sha256") == sha:
    fetched_at = prior.get("fetched_at")
else:
    fetched_at = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

meta_obj = {
    "source_url": source_url,
    "fetched_at": fetched_at,
    "sha256": sha,
    "provider_count": len(d),
    "model_count": n_models,
    "note": "Vendored models.dev catalog snapshot. Refresh via scripts/refresh-catalog.sh (explicit, human/CI-run) — never fetched at build or run time.",
}
with open(meta, "w", encoding="utf-8") as f:
    json.dump(meta_obj, f, indent=2, sort_keys=True)
    f.write("\n")
changed = "UNCHANGED" if prior.get("sha256") == sha else "CHANGED"
print(f"  providers={len(d)} models={n_models} sha256={sha[:12]}… ({changed})")
PY

# Regenerate the per-provider resolution report from the new snapshot.
echo "→ regenerating resolution report"
( cd "$repo_root" && BROCA_UPDATE_REPORT=1 cargo test -p broca-catalog --test it resolution_report:: >/dev/null 2>&1 )

# Human-readable summary of how providers now classify.
echo "→ resolution summary"
python3 - "$data_dir/resolution-report.json" <<'PY'
import json, sys
from collections import Counter
rows = json.load(open(sys.argv[1]))
src = Counter(r["resolution_source"] for r in rows)
served = sum(1 for r in rows if r["resolution_source"] != "pending")
print(f"  SERVED {served} / PENDING {src['pending']}  (total {len(rows)})")
print(f"  by source: {dict(src)}")
fam = Counter(r["resolved_family"] for r in rows if r["resolved_family"])
print(f"  by family: {dict(fam)}")
PY

echo
echo "Done. REVIEW THE DIFF before committing — especially data/resolution-report.json:"
echo "  git diff --stat crates/broca-catalog/data/"
echo "Any provider whose resolved_family or resolution_source changed is a deliberate"
echo "re-route that must be understood before it merges."
