"""Read broca's gated WAL frame (framing version 3) the way broca's own reader does.

Layout, from crates/broca-wal/src/framing.rs:

    frame   := [len: u32 LE][ver: u8][seq: u64 LE][fence: u64 LE][digest: 32][payload: len]
    gated   := frame(ver=3, payload={"requires": [feature name, ...], "record": WalRecord})
    digest  := SHA-256 over (ver || seq || fence || payload)

A gated frame carries a record a reader must understand before it may use it. `requires` names
the features the record uses; a reader checks each against the names it knows BEFORE it uses the
record. A frame naming a feature the reader does not know was written by a newer broca build:
broca refuses the whole WAL as "newer-writer". That is not corruption -- the bytes are intact --
so a script must report it by that name, never as damage, and must not skip the record either,
because it may change the meaning of everything after it.
"""
from __future__ import annotations

import json
import pathlib
import re
from typing import Any, Callable, FrozenSet

GATED_FRAMING_VERSION = 3

# broca's own table of feature names, the one every reader in the daemon consults.
FRAMING_RS = pathlib.Path(__file__).resolve().parents[2] / "crates/broca-wal/src/framing.rs"


def broca_known_features(
    source: str, module_source: Callable[[str], str] = lambda module: ""
) -> FrozenSet[str]:
    """The feature names in `KNOWN_FEATURES` in framing.rs, resolved through their constants.

    Read from the source rather than copied, because a copy goes stale: it once stayed empty
    while broca learned five names, so every WAL holding a tool call since then read as
    written by a newer build, and the live-run gate refused every restart as "cannot compare".
    An entry may name a constant in another broca-wal module (`crate::head_proof::NAME`);
    `module_source` returns that module's source text. Raises ValueError when the table or one
    of its names cannot be found, so a renamed or moved constant fails loudly instead of
    quietly shrinking the table.
    """
    table = re.search(r"pub const KNOWN_FEATURES: &\[&str\] = &\[(.*?)\];", source, re.DOTALL)
    if table is None:
        raise ValueError("KNOWN_FEATURES not found in framing.rs")
    names = set()
    for module, constant in re.findall(
        r"(?:crate::([a-z_][a-z0-9_]*)::)?([A-Z][A-Z0-9_]*)", table.group(1)
    ):
        text = module_source(module) if module else source
        value = re.search(rf'pub const {constant}: &str = "([^"]+)";', text)
        if value is None:
            where = f"broca-wal's {module} module" if module else "framing.rs"
            raise ValueError(f"{constant} in KNOWN_FEATURES has no string value in {where}")
        names.add(value.group(1))
    if not names:
        raise ValueError("KNOWN_FEATURES in framing.rs parsed as empty")
    return frozenset(names)


# The feature names these scripts understand: exactly broca's. Every script that reads gated
# records dispatches on the record's `type` and ignores types and fields it does not use, and
# each feature broca knows adds either an optional field or a record type none of them act on,
# so knowing a name broca knows is always safe here. Tests patch this module attribute to
# exercise a gated frame whose features are known.
def _broca_wal_module_source(module: str) -> str:
    """The source of broca-wal's `module`, as `src/<module>.rs` or `src/<module>/mod.rs`."""
    for path in (FRAMING_RS.parent / f"{module}.rs", FRAMING_RS.parent / module / "mod.rs"):
        if path.is_file():
            return path.read_text()
    return ""


KNOWN_FEATURES: FrozenSet[str] = broca_known_features(
    FRAMING_RS.read_text(), _broca_wal_module_source
)


class MalformedGatedFrame(ValueError):
    """A digest-valid version-3 payload that is not a well-formed gated envelope.

    No writer that follows the format produces one, so it is corruption, unlike an unknown
    feature, which is a newer writer.
    """


def open_gated(payload: bytes) -> tuple[list[str], dict[str, Any]]:
    """Split a version-3 payload into (missing features, envelope).

    `missing` holds, sorted and deduplicated, every name in `requires` that is not in
    `KNOWN_FEATURES` at call time. When it is empty the caller may use `envelope["record"]`,
    which is always a JSON object; when it is not, the caller must treat the whole WAL as
    written by a newer build and use none of its records.
    """
    try:
        envelope = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise MalformedGatedFrame(f"gated payload is not valid JSON: {error}") from error
    if not isinstance(envelope, dict):
        raise MalformedGatedFrame("gated payload is not an object")
    requires = envelope.get("requires")
    if not isinstance(requires, list) or not all(isinstance(name, str) for name in requires):
        raise MalformedGatedFrame("gated payload has no list of feature names in 'requires'")
    if "record" not in envelope:
        raise MalformedGatedFrame("gated payload has no 'record'")
    missing = sorted({name for name in requires if name not in KNOWN_FEATURES})
    # Requirements first, as broca checks them: a record that needs an unknown feature may take a
    # shape this reader would reject, and that must still read as a newer writer.
    if not missing and not isinstance(envelope["record"], dict):
        raise MalformedGatedFrame("gated payload's 'record' is not an object")
    return missing, envelope
