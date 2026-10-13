#!/usr/bin/env python3
"""Regenerate data/in-use-models.json from the billing export lane.

WHY THIS EXISTS: the manifest it writes is the input to a catalog guard
(`every_model_production_uses_still_resolves`) that refuses a manifest older than 30 days. That
guard fired on 2026-09-10 against a manifest generated 2026-08-11 -- correctly, and the repository
had NO WAY TO REGENERATE IT. It had been produced by hand, so honouring the guard meant
re-deriving a SQL query, a recency window and an output shape from scratch, every 30 days, forever.

A guard whose remedy is archaeology gets its limit raised instead. That is not a prediction: the
guard's own failure text says "Do not raise the limit to make this pass", which is a sentence you
only write when you can feel the pull. This script is what makes obeying it cheap.

WINDOW: 30 days, matching the guard's own staleness limit so the manifest describes the same period
the guard promises to cover. The original hand-built manifest spanned 2026-07-18 to 2026-08-11,
which is the same ~30-day shape; this preserves that intent rather than inventing a new one.

USAGE: scripts/refresh-in-use-models.py [--days N] [--check]
  --check  report what WOULD change and exit 1 if the manifest is stale, writing nothing.
"""

import argparse
import datetime
import json
import os
import sqlite3
import sys
from pathlib import Path

from lib import state_root

WINDOW_DAYS = 30
MANIFEST = Path("crates/broca-module-serve/data/in-use-models.json")

# Both roots, newest first. The state root moved on 2026-08-06 and the old path still exists on
# some machines; picking the wrong one silently produces a manifest from a frozen database.
# The first is the daemon's own default rule (BROCA_STATE_ROOT, else the data home).
ROOTS = [
    state_root.default_state_root(),
    Path.home() / ".local/share/cortexkit/run/broca-state",
]


def resolve_db() -> Path:
    found = [r / "run-index.db" for r in ROOTS if (r / "run-index.db").exists()]
    if not found:
        sys.exit("refresh-in-use-models: no run-index.db under any known state root")
    if len(found) > 1:
        # REFUSE rather than pick. Two roots means one of them is a leftover, and choosing the
        # wrong one yields a plausible manifest describing a fleet that stopped running months ago.
        sys.exit(
            "refresh-in-use-models: TWO state roots hold a run-index.db:\n  "
            + "\n  ".join(str(p) for p in found)
            + "\nRemove the stale one, or say which is live. Refusing to guess."
        )
    return found[0]


def collect(db: Path, days: int):
    cutoff_ms = int(
        (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)).timestamp()
        * 1000
    )
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    # occurred_at_ms and the rest are FIELDS INSIDE segment_json, not columns. Filtering them as
    # columns makes sqlite error; with stderr redirected that reads as "no rows" rather than "bad
    # query", which cost me two wrong readings on 2026-09-09.
    rows = con.execute(
        """
        SELECT json_extract(segment_json,'$.provider'),
               json_extract(segment_json,'$.model'),
               json_extract(segment_json,'$.auth_selection'),
               COUNT(*),
               MAX(json_extract(segment_json,'$.occurred_at_ms'))
        FROM export_facts
        WHERE json_extract(segment_json,'$.occurred_at_ms') >= ?
        GROUP BY 1,2,3
        ORDER BY 4 DESC
        """,
        (cutoff_ms,),
    ).fetchall()
    con.close()

    pairs = []
    missing_auth = 0
    for provider, model, auth, facts, last_ms in rows:
        if not provider or not model:
            continue
        # The lane stores a full credential id (`oauth:xai`, `apikey:deepseek:acct`); the manifest
        # wants the METHOD, which is the part before the first colon.
        method = auth.split(":", 1)[0] if auth else None
        if not method:
            missing_auth += 1
            continue
        pairs.append(
            {
                "provider": provider,
                "model": model,
                "auth_method": method,
                "facts": facts,
                "last_used": datetime.datetime.fromtimestamp(
                    last_ms / 1000, datetime.timezone.utc
                ).strftime("%Y-%m-%d"),
            }
        )

    # THE FIELD-NAME GUARD, and it is here because I tripped exactly this. My first version read
    # `$.auth_method`, which does not exist -- the field is `auth_selection` -- so every row came
    # back None and the script cheerfully wrote 44 pairs with auth_method "unknown". A plausible
    # manifest, entirely wrong, and the only reason it did not ship is that the consuming test
    # refuses an unknown method.
    #
    # Some rows legitimately lack an auth selection (the opencode path froze none), so a few
    # missing is normal and ALL missing means the extraction is broken. Refuse that case here
    # rather than relying on a downstream test to notice.
    if pairs and missing_auth > len(pairs):
        sys.exit(
            f"refresh-in-use-models: {missing_auth} rows had no auth selection against only "
            f"{len(pairs)} that did. That ratio means the field extraction is wrong, not that "
            "the fleet ran unauthenticated. Check the json_extract path against a real row."
        )
    if not pairs and rows:
        sys.exit(
            f"refresh-in-use-models: {len(rows)} lane rows and ZERO usable pairs -- every row "
            "lacked an auth selection. The extraction path is wrong."
        )
    if missing_auth:
        print(f"    skipped       {missing_auth} rows with no frozen auth selection")
    return pairs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=WINDOW_DAYS)
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()

    if not MANIFEST.parent.exists():
        sys.exit(f"refresh-in-use-models: run from the repo root; {MANIFEST.parent} not found")

    db = resolve_db()
    pairs = collect(db, args.days)

    # THE VACUITY GUARD. An empty result writes a manifest that makes the catalog guard pass while
    # covering nothing -- the guard's own "an empty manifest would make this test vacuous"
    # assertion, one layer upstream where it can still be prevented.
    if not pairs:
        sys.exit(
            f"refresh-in-use-models: ZERO pairs in the last {args.days} days from {db}.\n"
            "That is a broken query or a dead database, not a fleet that ran nothing. "
            "Refusing to write an empty manifest."
        )

    doc = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
        "window_days": args.days,
        "source": "export_facts",
        "pairs": pairs,
    }

    old = json.loads(MANIFEST.read_text()) if MANIFEST.exists() else {"pairs": []}
    was = {(p["provider"], p["model"]) for p in old["pairs"]}
    now = {(p["provider"], p["model"]) for p in pairs}

    print(f"    source        {db}")
    print(f"    window        {args.days} days")
    print(f"    pairs         {len(old['pairs'])} -> {len(pairs)}")
    print(f"    facts covered {sum(p['facts'] for p in pairs)}")
    for provider, model in sorted(now - was):
        print(f"      + {provider}/{model}")
    for provider, model in sorted(was - now):
        print(f"      - {provider}/{model}  (no longer used in window)")

    if args.check:
        # COMPARE CONTENT, NOT THE DATE STAMP.
        #
        # This compared generated_at against today, which is a FRESHNESS test wearing a content
        # test's name: doc["generated_at"] is always today, so it answered "was the manifest
        # written today" and reported `current` for every same-day change. The diff it needs was
        # already computed and PRINTED six lines above and then ignored.
        #
        # It was wrong on live data the day it was written: a pinned run that afternoon added
        # openai/gpt-5.4-mini/apikey to production, the manifest did not have it, and --check
        # exited 0. The guard that consumes this manifest therefore had no coverage of that
        # combination while its own checker called the file current.
        #
        # `facts` and `last_used` are DELIBERATELY EXCLUDED from the identity: they move with
        # ordinary traffic, so including them would make --check permanently stale and train an
        # operator to ignore it. What the downstream guard actually asserts is that every
        # (provider, model, auth_method) in production still resolves, so that triple plus the
        # window is the whole content question.
        def identity(document):
            return (
                document.get("window_days"),
                sorted(
                    (pair["provider"], pair["model"], pair.get("auth_method"))
                    for pair in document.get("pairs", [])
                ),
            )

        stale = identity(old) != identity(doc)
        print("    check: manifest differs" if stale else "    check: current")
        return 1 if stale else 0

    MANIFEST.write_text(json.dumps(doc, indent=2) + "\n")
    print(f"    wrote {MANIFEST}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
