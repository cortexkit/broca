#!/usr/bin/env python3
"""Project pinned, public Fusiform catalog data into offline routing test rows."""

import argparse
from collections import Counter
from hashlib import sha256
import json
from pathlib import Path
import subprocess


COMMIT = "119258da721c5fac0e197d38f318e62100f8598e"
MODULE = "crates/fusiform-module/"
HERE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fusiform_checkout", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    sources = {}

    def source(path):
        content = subprocess.check_output(
            ["git", "-C", str(args.fusiform_checkout), "show", f"{COMMIT}:{path}"]
        )
        sources[path] = sha256(content).hexdigest()
        return content

    def document(name):
        return json.loads(source(MODULE + "data/" + name + ".json"))

    seed = document("models-dev-seed")
    rules = document("wire-families")["rules"]
    planes = document("billing-planes")["planes"]
    curated = document("curated-models")["models"]
    aliases = document("model-aliases")["aliases"]
    served = source(MODULE + "fixtures/served-payloads.json")
    # Record the implementations whose data projection is reproduced below. No Broca
    # source or family mapping participates in generating the expected protocol facts.
    for path in [MODULE + "src/wire_families.rs", MODULE + "src/route.rs",
                 "crates/fusiform-core/src/normalize/mod.rs"]:
        source(path)

    # Fusiform expands the package/shape Cartesian product into exact lookup keys;
    # null shape is an absent shape, not a wildcard (wire_families.rs:58-86,96-100).
    table = {}
    for rule in rules:
        for npm in rule["npm"]:
            for shape in rule["shapes"]:
                assert (npm, shape) not in table, "duplicate producer selector"
                table[npm, shape] = rule["wire_family"]

    def row(provider, model, npm, shape, method=None, family=None):
        return {"model_id": f"{provider}/{model}", "provider": provider,
                "auth_method": method, "wire.npm": npm, "wire.shape": shape,
                "wire.family": family if method else table.get((npm, shape))}

    rows = []
    for provider, card in sorted(seed.items()):
        for model, facts in sorted(card["models"].items()):
            override = facts.get("provider") or {}
            # normalize/mod.rs:518-527: only npm inherits a provider-level default.
            npm = override.get("npm")
            if npm is None:
                npm = card.get("npm")
            rows.append(row(provider, model, npm, override.get("shape")))

    # Auth-plane declarations override package-derived families on current reads
    # (route.rs:1791-1793). Include real seed/curated identities, not padded samples.
    for plane in planes:
        if not plane.get("wire_family"):
            continue
        provider, method = plane["provider_id"], plane["auth_method"]
        candidates = {r["model_id"]: r for r in rows if r["provider"] == provider}
        for model in curated:
            if model["provider_id"] == provider and method in model["auth_methods"]:
                facts = model["facts"]
                value = lambda key: (facts.get(key) or {}).get("value")
                candidate = row(provider, model["model_id"], value("wire.npm"), value("wire.shape"))
                candidates[candidate["model_id"]] = candidate
        for candidate in candidates.values():
            rows.append({**candidate, "auth_method": method, "wire.family": plane["wire_family"]})

    # These public alias ids select Google's Antigravity credential route; their
    # expected family is curated by Fusiform, never inferred from the target SDK.
    for alias in aliases:
        assert alias["provider_id"] == "google" and alias["model_id"].startswith("antigravity-")
        rows.append(row("google", alias["model_id"], None, None,
                        "antigravity", alias["wire_family"]))

    rows.sort(key=lambda r: (r["model_id"], r["auth_method"] or ""))
    keys = [(r["model_id"], r["auth_method"]) for r in rows]
    assert len(set(keys)) == len(keys), "duplicate model/auth route"
    extract = "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows).encode()
    outputs = {"served-payloads.json": served, "wire-family-rows.jsonl": extract}
    metadata = {"repository": "https://github.com/cortexkit/fusiform", "commit": COMMIT,
                "source_sha256": sources, "row_count": len(rows),
                "family_counts": dict(sorted(Counter(r["wire.family"] or "absent" for r in rows).items())),
                "output_sha256": {name: sha256(data).hexdigest() for name, data in outputs.items()}}
    outputs["wire-family-rows.meta.json"] = (json.dumps(metadata, indent=2, sort_keys=True) + "\n").encode()
    for name, content in outputs.items():
        destination = HERE / name
        if args.check:
            assert destination.read_bytes() == content, f"stale pinned fixture: {name}"
        else:
            destination.write_bytes(content)
        print(f"{'checked' if args.check else 'wrote'} {name}: sha256={sha256(content).hexdigest()}")
    print(f"{len(rows)} unique model/auth routes; tokens: {metadata['family_counts']}")


if __name__ == "__main__":
    main()
