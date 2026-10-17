"""Match shutdown readback to the run that was active before a restart."""
from __future__ import annotations

import pathlib

from .archive_read import read_records

SEALED = "sealed"
UNCONFIRMED = "unconfirmed"
CANCELLED = "cancelled"


def _episodes(wal: pathlib.Path) -> list[tuple[str | None, list[dict]]]:
    """Group decoded records by each run_started record in WAL order."""
    episodes: list[tuple[str | None, list[dict]]] = []
    current: tuple[str | None, list[dict]] | None = None
    for frame in read_records(wal):
        record = frame.record
        if record.get("type") == "run_started":
            if current is not None:
                episodes.append(current)
            run_id = record.get("run_id")
            current = (run_id if isinstance(run_id, str) else None, [])
        if current is not None:
            current[1].append(record)
    if current is not None:
        episodes.append(current)
    return episodes


def active_run_id(wal: pathlib.Path) -> str | None:
    """Return the latest run ID that has started but not finished in this WAL."""
    for run_id, records in reversed(_episodes(wal)):
        if run_id is not None and not any(
            record.get("type") == "run_finished" for record in records[1:]
        ):
            return run_id
    return None


def classify_run(wal: pathlib.Path, run_id: str) -> str:
    """Classify one captured run, ignoring any run started later in its session."""
    episode = next(
        (records for candidate, records in reversed(_episodes(wal)) if candidate == run_id),
        None,
    )
    if episode is None:
        return UNCONFIRMED
    terminals = [
        record
        for record in episode[1:]
        if record.get("type") == "run_finished"
        or (record.get("type") == "run_paused" and record.get("reason") == "restart")
    ]
    if any(
        record.get("type") == "run_finished"
        and (record.get("reason") == "cancelled" or record.get("state") == "cancelled")
        for record in terminals
    ):
        return CANCELLED
    return SEALED if terminals else UNCONFIRMED
