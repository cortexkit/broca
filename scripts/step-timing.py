#!/usr/bin/env python3
"""Measure model-step duration and derived content throughput from framed WALs.

The durable files are binary frames, not newline JSON. This probe verifies each
frame digest, decodes its envelope and record, and pairs each
``model_step_finished`` with the preceding ``step_started`` for the same step.
It never substitutes zero for a missing timestamp.

The fixed binary header is length/version/sequence/fence/digest and contains no
timestamp. Its JSON payload can be either a record directly or an envelope of
the form ``{"ts_ms": ..., "record": {...}}``. The latter keeps frame metadata out
of the record body. Direct records remain readable because deployed version-1 WALs
serialize records that way; only record variants that actually carry ``ts_ms``
have a timestamp in that layout. Version-2 lineage metadata frames are verified and skipped
because they do not consume record sequence numbers. Version-3 gated frames (see
scripts/lib/wal_gate.py) are decoded when every feature they require is known; a WAL holding
one that requires an unknown feature was written by a newer broca build, so none of its
records are measured and the report counts it as newer-writer rather than as corrupt.

Python is intentional here: this is an offline corpus probe, and a standalone
script can inspect production and synthetic roots without compiling or linking
the service. It still implements the exact version-1 digest and framing checks,
so it cannot mistake a text match inside a message for a record.

Exit 0 after a usable report, or 2 when the corpus cannot support a timing
measurement (including an empty corpus, no decodable frames, or no positive
step durations).
"""

from __future__ import annotations

import argparse
import collections
import dataclasses
import hashlib
import json
import math
import pathlib
import struct
import sys
from collections.abc import Callable, Iterable
from typing import Any, Optional

from lib import state_root, wal_gate
from lib.archive_read import ArchiveRefusal, logical_wals

HEADER_LEN = 4 + 1 + 8 + 8 + 32
FRAMING_VERSION = 1
LINEAGE_FRAMING_VERSION = 2
GATED_FRAMING_VERSION = wal_gate.GATED_FRAMING_VERSION
LINEAGE_PAYLOAD_LEN = 16
MAX_PAYLOAD_LEN = 64 * 1024 * 1024
DEFAULT_STATE_ROOT = state_root.default_state_root()
FIXTURE_ROOTS = ("/tmp", "/private")


class ProbeRefusal(Exception):
    """The requested conclusion cannot be supported by the observed corpus."""


class FrameDecodeError(Exception):
    """A complete frame is corrupt or uses an unsupported shape."""


class NewerWriterWal(Exception):
    """A WAL holds a gated frame requiring features this script does not know.

    Kept apart from `FrameDecodeError` on purpose: the bytes are intact, a newer broca build
    wrote them. The scan reports the file by that name and carries on with the rest of the
    corpus instead of refusing the whole measurement.
    """

    def __init__(self, path: pathlib.Path, seq: int, missing: list[str]) -> None:
        super().__init__(
            f"{path}: frame {seq} requires features this script does not know: "
            f"{', '.join(missing)} (newer-writer)"
        )
        self.path = path
        self.missing = missing


@dataclasses.dataclass(frozen=True)
class DecodedFrame:
    wal: pathlib.Path
    seq: int
    fence: int
    ts_ms: int | None
    record: dict[str, Any]


@dataclasses.dataclass
class Episode:
    wal: pathlib.Path
    frames: list[DecodedFrame]
    run_id: str
    project_root: str
    provider: str
    family: str
    imported: bool
    fixture_root: bool

    @property
    def excluded(self) -> bool:
        return self.imported or self.fixture_root

    @property
    def outcome(self) -> str:
        for frame in self.frames:
            if frame.record.get("type") == "run_finished":
                return f"terminal:{scalar_name(frame.record.get('reason'), 'unknown')}"
        if any(frame.record.get("type") == "run_paused" for frame in self.frames):
            return "non_terminal:paused"
        return "non_terminal:open"


@dataclasses.dataclass(frozen=True)
class StepMeasurement:
    wal: pathlib.Path
    run_id: str
    step_id: str
    start_seq: int | None
    finish_seq: int
    start_ts_ms: int | None
    finish_ts_ms: int | None
    duration_ms: int | None
    content_bytes: int
    outcome: str
    provider: str
    family: str

    @property
    def rate_bytes_per_second(self) -> float | None:
        if self.duration_ms is None or self.duration_ms <= 0:
            return None
        return self.content_bytes * 1000.0 / self.duration_ms

    @property
    def succeeded(self) -> bool:
        return self.outcome == "terminal:completed"


@dataclasses.dataclass
class ScanResult:
    wal_dir: pathlib.Path
    wal_files: int
    empty_wals: int
    torn_tail_wals: int
    decoded_records: int
    unassigned_records: int
    episodes: list[Episode]
    eligible_episodes: list[Episode]
    excluded_fixture_episodes: int
    excluded_import_episodes: int
    excluded_overlap_episodes: int
    excluded_records: int
    steps: list[StepMeasurement]
    unmatched_starts: list[tuple[Episode, DecodedFrame]]
    finish_without_start: int
    # WALs a newer broca build wrote (a gated frame names a feature this script does not
    # know), with the missing names. None of their records were measured.
    newer_writer_wals: list[tuple[pathlib.Path, list[str]]] = dataclasses.field(
        default_factory=list
    )


TimestampReader = Callable[[dict[str, Any]], Optional[int]]


def scalar_name(value: Any, default: str) -> str:
    """Return a stable lower-case spelling for string or single-key enum JSON."""
    if isinstance(value, str):
        return value.lower()
    if isinstance(value, dict) and len(value) == 1:
        return str(next(iter(value))).lower()
    return default


def envelope_timestamp(envelope: dict[str, Any]) -> int | None:
    """Read frame metadata without manufacturing a value when it is absent."""
    value = envelope.get("ts_ms")
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FrameDecodeError(f"invalid envelope ts_ms: {value!r}")
    return value


def decode_wal(
    path: pathlib.Path,
    timestamp_reader: TimestampReader = envelope_timestamp,
) -> tuple[list[DecodedFrame], bool]:
    """Decode one immutable byte snapshot; return records and whether its tail is torn."""
    try:
        raw = path.read_bytes()
    except OSError as error:
        raise FrameDecodeError(f"cannot read {path}: {error}") from error

    frames: list[DecodedFrame] = []
    offset = 0
    expected_seq = 1
    torn_tail = False
    while offset < len(raw):
        available = len(raw) - offset
        if available < HEADER_LEN:
            torn_tail = True
            break

        payload_len = struct.unpack_from("<I", raw, offset)[0]
        version = raw[offset + 4]
        seq, fence = struct.unpack_from("<QQ", raw, offset + 5)
        digest = raw[offset + 21 : offset + HEADER_LEN]
        if version not in (FRAMING_VERSION, LINEAGE_FRAMING_VERSION, GATED_FRAMING_VERSION):
            raise FrameDecodeError(
                f"{path}: unsupported frame version {version} at byte {offset}"
            )
        if payload_len > MAX_PAYLOAD_LEN:
            raise FrameDecodeError(
                f"{path}: frame {seq} payload {payload_len} exceeds {MAX_PAYLOAD_LEN}"
            )
        total = HEADER_LEN + payload_len
        if available < total:
            torn_tail = True
            break

        payload = raw[offset + HEADER_LEN : offset + total]
        expected_digest = hashlib.sha256(
            bytes((version,))
            + struct.pack("<Q", seq)
            + struct.pack("<Q", fence)
            + payload
        ).digest()
        if digest != expected_digest:
            raise FrameDecodeError(f"{path}: digest mismatch at sequence {seq}")

        if version == LINEAGE_FRAMING_VERSION:
            if payload_len != LINEAGE_PAYLOAD_LEN or seq != 0 or fence != 0:
                raise FrameDecodeError(
                    f"{path}: invalid lineage metadata frame at byte {offset}"
                )
            offset += total
            continue

        if seq != expected_seq:
            raise FrameDecodeError(
                f"{path}: expected sequence {expected_seq}, found {seq}"
            )
        if version == GATED_FRAMING_VERSION:
            # The gated envelope is {"requires": [...], "record": {...}}: once its features are
            # known it reads exactly like the {"ts_ms", "record"} envelope below, minus ts_ms.
            try:
                missing, envelope = wal_gate.open_gated(payload)
            except wal_gate.MalformedGatedFrame as error:
                raise FrameDecodeError(f"{path}: frame {seq}: {error}") from error
            if missing:
                raise NewerWriterWal(path, seq, missing)
        else:
            try:
                envelope = json.loads(payload)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise FrameDecodeError(
                    f"{path}: frame {seq} is not valid JSON: {error}"
                ) from error
        if not isinstance(envelope, dict):
            raise FrameDecodeError(f"{path}: frame {seq} payload is not an object")

        record_value = envelope.get("record", envelope)
        if not isinstance(record_value, dict) or not isinstance(record_value.get("type"), str):
            raise FrameDecodeError(f"{path}: frame {seq} has no tagged WAL record")
        frames.append(
            DecodedFrame(
                wal=path,
                seq=seq,
                fence=fence,
                ts_ms=timestamp_reader(envelope),
                record=record_value,
            )
        )
        expected_seq += 1
        offset += total

    return frames, torn_tail


def is_fixture_root(project_root: str) -> bool:
    normalized = "/" + project_root.lstrip("/")
    return any(
        normalized == prefix or normalized.startswith(prefix + "/")
        for prefix in FIXTURE_ROOTS
    )


def is_import_episode(record: dict[str, Any]) -> bool:
    origin = record.get("origin")
    if not isinstance(origin, dict):
        return False
    if scalar_name(origin.get("kind"), "") != "seeded_from":
        return False
    # Imported seeded episodes predate origin_kind and deserialize to Import by default.
    return scalar_name(origin.get("origin_kind"), "import") == "import"


def config_identity(record: dict[str, Any]) -> tuple[str, str]:
    config = record.get("config")
    if not isinstance(config, dict):
        return "unknown", "unknown"
    model = config.get("model")
    provider = "unknown"
    if isinstance(model, dict):
        value = model.get("provider_module_id", model.get("provider"))
        if isinstance(value, str) and value:
            provider = value
    frozen = config.get("frozen_render_config")
    family = "unknown"
    if isinstance(frozen, dict):
        value = frozen.get("family_id", frozen.get("wire_family", frozen.get("family")))
        if isinstance(value, str) and value:
            family = value
    return provider, family


def episode_from_start(frame: DecodedFrame) -> Episode:
    record = frame.record
    session = record.get("session")
    project_root = ""
    if isinstance(session, dict) and isinstance(session.get("project_root"), str):
        project_root = session["project_root"]
    provider, family = config_identity(record)
    run_id = record.get("run_id")
    return Episode(
        wal=frame.wal,
        frames=[frame],
        run_id=str(run_id) if run_id is not None else "unknown",
        project_root=project_root,
        provider=provider,
        family=family,
        imported=is_import_episode(record),
        fixture_root=is_fixture_root(project_root),
    )


def build_episodes(frames: Iterable[DecodedFrame]) -> tuple[list[Episode], int]:
    episodes: list[Episode] = []
    current: Episode | None = None
    unassigned = 0
    for frame in frames:
        if frame.record.get("type") == "run_started":
            current = episode_from_start(frame)
            episodes.append(current)
        elif current is None:
            unassigned += 1
        else:
            current.frames.append(frame)
    return episodes, unassigned


def content_bytes(record: dict[str, Any]) -> int:
    """Count persisted text, plaintext reasoning, and opaque reasoning payload bytes."""
    assistant = record.get("assistant_message")
    if not isinstance(assistant, dict):
        return 0
    blocks = assistant.get("content")
    if not isinstance(blocks, list):
        return 0
    total = 0
    for block in blocks:
        if not isinstance(block, dict):
            continue
        kind = block.get("type")
        field = "data" if kind == "redacted_reasoning" else "text"
        if kind not in ("text", "reasoning", "redacted_reasoning"):
            continue
        value = block.get(field)
        if isinstance(value, str):
            total += len(value.encode("utf-8"))
    return total


def measure_episode(
    episode: Episode,
) -> tuple[list[StepMeasurement], list[DecodedFrame], int]:
    starts: dict[str, list[DecodedFrame]] = {}
    finished: list[StepMeasurement] = []
    finish_without_start = 0
    for frame in episode.frames:
        kind = frame.record.get("type")
        step_id = str(frame.record.get("step_id", "unknown"))
        if kind == "step_started":
            starts.setdefault(step_id, []).append(frame)
            continue
        if kind != "model_step_finished":
            continue
        candidates = starts.get(step_id, [])
        start = candidates.pop() if candidates else None
        if not candidates:
            starts.pop(step_id, None)
        if start is None:
            finish_without_start += 1
        start_ts = start.ts_ms if start is not None else None
        finish_ts = frame.ts_ms
        duration = None
        if start_ts is not None and finish_ts is not None:
            duration = finish_ts - start_ts
        finished.append(
            StepMeasurement(
                wal=episode.wal,
                run_id=episode.run_id,
                step_id=step_id,
                start_seq=start.seq if start is not None else None,
                finish_seq=frame.seq,
                start_ts_ms=start_ts,
                finish_ts_ms=finish_ts,
                duration_ms=duration,
                content_bytes=content_bytes(frame.record),
                outcome=episode.outcome,
                provider=episode.provider,
                family=episode.family,
            )
        )
    return (
        finished,
        [frame for candidates in starts.values() for frame in candidates],
        finish_without_start,
    )


def scan_corpus(
    state_root: pathlib.Path,
    timestamp_reader: TimestampReader = envelope_timestamp,
) -> ScanResult:
    wal_dir = state_root / "wal" if any((state_root / name).is_dir() for name in ("wal", "wal-ephemeral", "wal-archive")) else state_root
    if not any(root.is_dir() for root in (wal_dir, wal_dir.parent / "wal-ephemeral", wal_dir.parent / "wal-archive")):
        raise ProbeRefusal(f"no WAL directory at {wal_dir}")

    paths = logical_wals(wal_dir)
    if not paths:
        raise ProbeRefusal(f"empty corpus: no .wal files under {wal_dir}")

    all_episodes: list[Episode] = []
    decoded_records = 0
    unassigned_records = 0
    empty_wals = 0
    torn_tail_wals = 0
    newer_writer_wals: list[tuple[pathlib.Path, list[str]]] = []
    for path in paths:
        try:
            frames, torn = decode_wal(path, timestamp_reader)
        except NewerWriterWal as newer:
            newer_writer_wals.append((newer.path, newer.missing))
            continue
        if not frames:
            empty_wals += 1
        if torn:
            torn_tail_wals += 1
        decoded_records += len(frames)
        episodes, unassigned = build_episodes(frames)
        all_episodes.extend(episodes)
        unassigned_records += unassigned

    if decoded_records == 0:
        newer = (
            f" ({len(newer_writer_wals)} of them written by a newer broca build: newer-writer)"
            if newer_writer_wals
            else ""
        )
        raise ProbeRefusal(
            f"no decodable frames in {len(paths)} WAL file(s) under {wal_dir}{newer}"
        )

    eligible = [episode for episode in all_episodes if not episode.excluded]
    fixture_count = sum(episode.fixture_root for episode in all_episodes)
    import_count = sum(episode.imported for episode in all_episodes)
    overlap_count = sum(
        episode.fixture_root and episode.imported for episode in all_episodes
    )
    excluded_records = sum(len(episode.frames) for episode in all_episodes if episode.excluded)

    steps: list[StepMeasurement] = []
    unmatched: list[tuple[Episode, DecodedFrame]] = []
    finish_without_start = 0
    for episode in eligible:
        measured, open_starts, orphan_finishes = measure_episode(episode)
        steps.extend(measured)
        unmatched.extend((episode, start) for start in open_starts)
        finish_without_start += orphan_finishes

    return ScanResult(
        wal_dir=wal_dir,
        wal_files=len(paths),
        empty_wals=empty_wals,
        torn_tail_wals=torn_tail_wals,
        decoded_records=decoded_records,
        unassigned_records=unassigned_records,
        episodes=all_episodes,
        eligible_episodes=eligible,
        excluded_fixture_episodes=fixture_count,
        excluded_import_episodes=import_count,
        excluded_overlap_episodes=overlap_count,
        excluded_records=excluded_records,
        steps=steps,
        unmatched_starts=unmatched,
        finish_without_start=finish_without_start,
        newer_writer_wals=newer_writer_wals,
    )


def percentile(values: list[float], fraction: float) -> float:
    """Nearest-rank percentile, returning an observed value in a small low tail."""
    if not values:
        raise ValueError("percentile of empty population")
    ordered = sorted(values)
    rank = max(1, math.ceil(fraction * len(ordered)))
    return ordered[rank - 1]


def control_suffix(result: ScanResult) -> str:
    return f"unfiltered decoded records={result.decoded_records}"


def filtered_line(result: ScanResult, label: str, value: Any) -> str:
    return f"{label}: {value} ({control_suffix(result)})"


def count_line(
    result: ScanResult,
    label: str,
    value: int,
    nonzero_control_label: str,
    nonzero_control: int,
) -> str:
    suffix = control_suffix(result)
    if value == 0:
        if nonzero_control <= 0:
            raise ProbeRefusal(
                f"cannot pair zero {label!r} with a non-zero control in the same pass"
            )
        return (
            f"{label}: 0 (control {nonzero_control_label}={nonzero_control} > 0; "
            f"{suffix})"
        )
    return f"{label}: {value} ({suffix})"


def format_distribution(
    result: ScanResult,
    label: str,
    measurements: list[StepMeasurement],
) -> list[str]:
    rates = [step.rate_bytes_per_second for step in measurements]
    actual_rates = [rate for rate in rates if rate is not None]
    if not actual_rates:
        return [filtered_line(result, label, "unavailable")]
    durations = [float(step.duration_ms) for step in measurements if step.duration_ms is not None]
    payloads = [float(step.content_bytes) for step in measurements]
    percentiles = (("p1", 0.01), ("p5", 0.05), ("p10", 0.10), ("p25", 0.25), ("p50", 0.50), ("p95", 0.95))

    lines = [filtered_line(result, f"{label} observations", len(measurements))]
    for metric, values, unit in (
        ("duration", durations, "ms"),
        ("content", payloads, "bytes"),
        ("throughput", actual_rates, "bytes/s"),
    ):
        rendered = " ".join(
            f"{name}={percentile(values, fraction):.2f}"
            for name, fraction in percentiles
        )
        lines.append(filtered_line(result, f"{label} {metric} ({unit})", rendered))
    return lines


def render_report(
    result: ScanResult,
    low_tail_count: int,
    sustained_seconds: float,
) -> tuple[str, str | None]:
    """Render diagnostics and return an optional reason the timing result is refused."""
    lines = [
        "decoder: Python binary-frame probe (version/sequence/SHA-256 verified)",
        f"corpus: {result.wal_dir}",
        f"unfiltered decoded records: {result.decoded_records}",
        filtered_line(result, "WAL files examined", result.wal_files),
        filtered_line(result, "empty or undecodable-tail-only WALs", result.empty_wals),
        filtered_line(result, "WALs with ignored torn tails", result.torn_tail_wals),
        filtered_line(
            result,
            "newer-writer WALs (a newer broca wrote them; not measured)",
            len(result.newer_writer_wals),
        ),
        *(
            f"  newer-writer {path.name}: requires {', '.join(missing)}"
            for path, missing in result.newer_writer_wals
        ),
        filtered_line(result, "records before any run_started", result.unassigned_records),
        filtered_line(result, "episodes before exclusions", len(result.episodes)),
        filtered_line(
            result,
            "excluded fixture-root episodes (/tmp or /private)",
            result.excluded_fixture_episodes,
        ),
        filtered_line(result, "excluded import episodes", result.excluded_import_episodes),
        filtered_line(result, "excluded overlap episodes", result.excluded_overlap_episodes),
        filtered_line(
            result,
            "total episodes removed by exclusions",
            len(result.episodes) - len(result.eligible_episodes),
        ),
        filtered_line(result, "records removed by exclusions", result.excluded_records),
        filtered_line(
            result,
            "finished model steps removed by exclusions",
            sum(
                frame.record.get("type") == "model_step_finished"
                for episode in result.episodes
                if episode.excluded
                for frame in episode.frames
            ),
        ),
        filtered_line(result, "eligible episodes", len(result.eligible_episodes)),
    ]

    finished_count = len(result.steps)
    unmatched_count = len(result.unmatched_starts)
    missing = [
        step
        for step in result.steps
        if step.start_ts_ms is None or step.finish_ts_ms is None
    ]
    missing_start = sum(step.start_ts_ms is None for step in result.steps)
    missing_finish = sum(step.finish_ts_ms is None for step in result.steps)
    zero_duration = sum(step.duration_ms == 0 for step in result.steps)
    negative_duration = sum(
        step.duration_ms is not None and step.duration_ms < 0 for step in result.steps
    )
    positive = [step for step in result.steps if step.duration_ms is not None and step.duration_ms > 0]
    positive_content = sum(step.content_bytes > 0 for step in positive)
    zero_content = sum(step.content_bytes == 0 for step in positive)
    count_control_label = "finished model steps" if finished_count else "decoded records"
    count_control = finished_count if finished_count else result.decoded_records
    positive_control_label = (
        "missing-timestamp finished steps" if missing else "decoded records"
    )
    positive_control = len(missing) if missing else result.decoded_records

    lines.extend(
        [
            filtered_line(result, "finished model steps", finished_count),
            filtered_line(result, "started steps with no finished record", unmatched_count),
            filtered_line(result, "finished steps with no preceding start", result.finish_without_start),
            filtered_line(result, "finished steps missing either envelope timestamp", len(missing)),
            filtered_line(result, "finished steps missing start timestamp", missing_start),
            filtered_line(result, "finished steps missing finish timestamp", missing_finish),
            count_line(
                result,
                "zero-duration timestamp pairs",
                zero_duration,
                count_control_label,
                count_control,
            ),
            count_line(
                result,
                "negative-duration timestamp pairs",
                negative_duration,
                count_control_label,
                count_control,
            ),
            count_line(
                result,
                "positive-duration measurements",
                len(positive),
                positive_control_label,
                positive_control,
            ),
        ]
    )

    terminal_counts = collections.Counter(episode.outcome for episode in result.eligible_episodes)
    lines.append("\nepisode outcomes")
    for outcome, count in sorted(terminal_counts.items()):
        lines.append(filtered_line(result, f"  {outcome} episodes", count))

    step_outcomes = collections.Counter(step.outcome for step in result.steps)
    lines.append("\nfinished-step outcomes")
    if step_outcomes:
        for outcome, count in sorted(step_outcomes.items()):
            lines.append(filtered_line(result, f"  {outcome} steps", count))
    else:
        lines.append(
            count_line(
                result,
                "  finished steps",
                0,
                "decoded records",
                result.decoded_records,
            )
        )

    identities = collections.Counter((step.provider, step.family) for step in result.steps)
    lines.append("\nfinished-step provider/family")
    if identities:
        for (provider, family), count in sorted(identities.items()):
            lines.append(filtered_line(result, f"  {provider}/{family} steps", count))
    else:
        lines.append(
            count_line(
                result,
                "  identified steps",
                0,
                "decoded records",
                result.decoded_records,
            )
        )

    unfinished_outcomes = collections.Counter(
        episode.outcome for episode, _ in result.unmatched_starts
    )
    lines.append("\nright-censored started steps")
    if unfinished_outcomes:
        for outcome, count in sorted(unfinished_outcomes.items()):
            lines.append(filtered_line(result, f"  {outcome}", count))
    else:
        lines.append(
            count_line(
                result,
                "  unmatched starts",
                0,
                count_control_label,
                count_control,
            )
        )

    refusal: str | None = None
    if not positive:
        lines.append("\ndistributions (nearest-rank percentiles)")
        lines.append(filtered_line(result, "all eligible", "unavailable"))
        lines.append("\n" + filtered_line(result, "low tail", "unavailable"))
        refusal = (
            "no positive-duration step can be derived: envelope timestamps are absent "
            "or invalid; missing timestamps are not zero durations"
        )
    elif positive_content == 0:
        lines.append("\ndistributions (nearest-rank percentiles)")
        lines.append(filtered_line(result, "all eligible", "unavailable"))
        lines.append("\n" + filtered_line(result, "low tail", "unavailable"))
        refusal = (
            "all positive-duration steps have zero requested content bytes, so a zero-rate "
            "tail has no non-zero content control"
        )
    else:
        lines.append(
            count_line(
                result,
                "positive-duration steps with zero requested content bytes",
                zero_content,
                "positive-content steps",
                positive_content,
            )
        )
        lines.append("\ndistributions (nearest-rank percentiles)")
        lines.extend(format_distribution(result, "all eligible", positive))
        successful = [step for step in positive if step.succeeded]
        lines.extend(format_distribution(result, "successful", successful))
        sustained = [
            step
            for step in successful
            if step.duration_ms is not None
            and step.duration_ms >= sustained_seconds * 1000.0
        ]
        lines.extend(
            format_distribution(
                result,
                f"successful sustained (>={sustained_seconds:g}s)",
                sustained,
            )
        )

        lines.append("\nlow tail: successful sustained steps")
        for step in sorted(
            sustained,
            key=lambda item: item.rate_bytes_per_second
            if item.rate_bytes_per_second is not None
            else math.inf,
        )[:low_tail_count]:
            lines.append(
                filtered_line(
                    result,
                    "  step",
                    (
                        f"rate={step.rate_bytes_per_second:.3f} bytes/s "
                        f"duration_ms={step.duration_ms} content_bytes={step.content_bytes} "
                        f"outcome={step.outcome} provider={step.provider} family={step.family} "
                        f"run={step.run_id} step={step.step_id} wal={step.wal.name}"
                    ),
                )
            )
        if not sustained:
            lines.append(
                count_line(
                    result,
                    "  sustained successful measurements",
                    0,
                    "positive-duration measurements",
                    len(positive),
                )
            )

    interrupted_unfinished = unfinished_outcomes.get("terminal:interrupted", 0)
    lines.append("\ncorpus expressiveness")
    if interrupted_unfinished:
        expressiveness = (
            "CANNOT SEPARATE slow-legitimate from deadline-cut: interrupted episodes contain "
            f"{interrupted_unfinished} started step(s) with no finished record, duration, or "
            "assistant payload. A successful-step low tail is therefore survivor-biased."
        )
    else:
        expressiveness = (
            "CANNOT ESTABLISH rarity from successful steps alone: a caller-cut provider call "
            "has no model_step_finished payload, so this WAL shape cannot record the rate of "
            "the censored final stream."
        )
    lines.append(filtered_line(result, "answer", expressiveness))
    lines.append(
        filtered_line(
            result,
            "default floor",
            "DO NOT SET from this corpus; keep the throughput floor default-off",
        )
    )
    if refusal is not None:
        lines.append(filtered_line(result, "REFUSING timing distribution", refusal))
    return "\n".join(lines), refusal


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state-root",
        type=pathlib.Path,
        default=DEFAULT_STATE_ROOT,
        help="state root containing wal/, or the WAL directory itself",
    )
    parser.add_argument(
        "--low-tail",
        type=int,
        default=20,
        help="number of lowest successful sustained steps to show (default 20)",
    )
    parser.add_argument(
        "--sustained-seconds",
        type=float,
        default=10.0,
        help="minimum whole-step duration called sustained (default 10)",
    )
    args = parser.parse_args(argv)
    if args.low_tail <= 0:
        parser.error("--low-tail must be positive")
    if not math.isfinite(args.sustained_seconds) or args.sustained_seconds <= 0:
        parser.error("--sustained-seconds must be positive and finite")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = scan_corpus(args.state_root)
        report, refusal = render_report(
            result,
            low_tail_count=args.low_tail,
            sustained_seconds=args.sustained_seconds,
        )
    except (ProbeRefusal, FrameDecodeError, ArchiveRefusal) as error:
        print(f"REFUSING: {error}", file=sys.stderr)
        return 2
    print(report)
    return 2 if refusal is not None else 0


if __name__ == "__main__":
    raise SystemExit(main())
