#!/usr/bin/env python3
"""Summarize recorded model-attempt timing and idle-bound outcomes from WALs."""

from __future__ import annotations

import argparse
import collections
import dataclasses
import datetime as dt
import json
import math
import pathlib
import sys
from typing import Any

from lib import state_root
from lib.archive_read import ArchiveRefusal, logical_wals, read_records

DEFAULT_STATE_ROOT = state_root.default_state_root()
FIXTURE_ROOTS = ("/tmp", "/private")
SUCCESS_OUTCOMES = {"complete", "tool_args_trailing_garbage"}
FAILURE_OR_CANCEL_OUTCOMES = {
    "eof_before_completion",
    "chunk_error",
    "idle_timeout",
    "content_timeout",
    "byte_limit",
    "decode_error",
    "rate_floor",
    "cancelled",
    "sealed_in_flight",
    "http_error",
    "provider_error",
    "connect_failed",
    "header_timeout",
    "no_status",
}
IDLE_BOUNDS_MS = (2 * 60_000, 5 * 60_000, 10 * 60_000, 15 * 60_000)


class ProbeRefusal(RuntimeError):
    """The requested report cannot be supported by the selected WAL population."""


@dataclasses.dataclass(frozen=True)
class Attempt:
    session_addr: str
    run_id: str
    step_id: str
    ordinal: str
    provider: str
    model: str
    outcome: str
    sent_at_ms: int | None
    end_at_ms: int | None
    first_content_at_ms: int | None
    last_content_at_ms: int | None
    max_content_gap_ms: int | None
    max_content_gap_ended_at_ms: int | None
    content_time_fields_present: bool
    content_bytes: int | None
    non_content_bytes: int | None
    event_types: Any
    non_content_bytes_after_last_content: int | None
    events_after_last_content: int | None

    @property
    def no_content(self) -> bool:
        return (
            not self.content_time_fields_present
            and self.first_content_at_ms is None
            and self.last_content_at_ms is None
        )

    @property
    def duration_ms(self) -> int | None:
        if self.sent_at_ms is None or self.end_at_ms is None:
            return None
        if self.end_at_ms < self.sent_at_ms:
            return None
        return self.end_at_ms - self.sent_at_ms

    @property
    def time_to_first_content_ms(self) -> int | None:
        first = self.first_content_at_ms
        if first is None or self.sent_at_ms is None:
            return None
        if first < self.sent_at_ms or (self.end_at_ms is not None and first > self.end_at_ms):
            return None
        return first - self.sent_at_ms

    @property
    def trailing_gap_ms(self) -> int | None:
        last = self.last_content_at_ms
        if last is None or self.end_at_ms is None:
            return None
        if last > self.end_at_ms:
            return None
        return self.end_at_ms - last

    @property
    def content_timestamps_valid(self) -> bool:
        first, last = self.first_content_at_ms, self.last_content_at_ms
        if first is None or last is None or last < first:
            return False
        if self.sent_at_ms is not None and first < self.sent_at_ms:
            return False
        if self.end_at_ms is not None and last > self.end_at_ms:
            return False
        return True

    def idle_cut(self, bound_ms: int) -> bool | None:
        """Return whether any measurable no-content interval reaches the bound."""
        if self.no_content:
            duration = self.duration_ms
            return None if duration is None else duration >= bound_ms

        first_gap = self.time_to_first_content_ms
        trailing_gap = self.trailing_gap_ms
        interior_gap = self.max_content_gap_ms
        gaps = [gap for gap in (first_gap, interior_gap, trailing_gap) if gap is not None]
        if any(gap >= bound_ms for gap in gaps):
            return True
        if not self.content_timestamps_valid:
            return None
        if first_gap is None or trailing_gap is None or interior_gap is None:
            return None
        return False


@dataclasses.dataclass
class RunContext:
    run_id: str
    provider: str
    model: str
    excluded: bool


@dataclasses.dataclass
class ScanResult:
    wal_dir: pathlib.Path
    wal_files: int
    attempts: list[Attempt]
    unjoined_attempts: int
    excluded_attempts: int
    attempts_without_end: int


def _integer(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _name(value: Any, default: str = "unknown") -> str:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, dict) and len(value) == 1:
        return str(next(iter(value)))
    return default


def _fixture_root(project_root: str) -> bool:
    normalized = "/" + project_root.lstrip("/")
    return any(normalized == prefix or normalized.startswith(prefix + "/") for prefix in FIXTURE_ROOTS)


def _is_import(record: dict[str, Any]) -> bool:
    origin = record.get("origin")
    return (
        isinstance(origin, dict)
        and _name(origin.get("kind"), "") == "seeded_from"
        and _name(origin.get("origin_kind"), "import") == "import"
    )


def _run_context(record: dict[str, Any]) -> RunContext:
    config = record.get("config")
    model_cfg = config.get("model") if isinstance(config, dict) else None
    model_cfg = model_cfg if isinstance(model_cfg, dict) else {}
    provider = model_cfg.get("provider_module_id", model_cfg.get("provider"))
    model = model_cfg.get("model_id", model_cfg.get("model"))
    session = record.get("session")
    project_root = session.get("project_root", "") if isinstance(session, dict) else ""
    if not isinstance(project_root, str):
        project_root = ""
    return RunContext(
        run_id=_name(record.get("run_id")),
        provider=provider if isinstance(provider, str) and provider else "unknown",
        model=model if isinstance(model, str) and model else "unknown",
        excluded=_is_import(record) or _fixture_root(project_root),
    )


def _attempt(record: dict[str, Any], addr: str, run: RunContext) -> Attempt:
    progress = record
    return Attempt(
        session_addr=addr,
        run_id=run.run_id,
        step_id=_name(record.get("step_id")),
        ordinal=_name(record.get("attempt")),
        provider=run.provider,
        model=run.model,
        outcome=_name(record.get("outcome")),
        sent_at_ms=_integer(record.get("sent_at_ms")),
        end_at_ms=_integer(record.get("ts_ms")),
        first_content_at_ms=_integer(progress.get("first_content_at_ms")),
        last_content_at_ms=_integer(progress.get("last_content_at_ms")),
        max_content_gap_ms=_integer(progress.get("max_content_gap_ms")),
        max_content_gap_ended_at_ms=_integer(progress.get("max_content_gap_ended_at_ms")),
        content_time_fields_present=(
            "first_content_at_ms" in progress or "last_content_at_ms" in progress
        ),
        content_bytes=_integer(progress.get("content_bytes")),
        non_content_bytes=_integer(progress.get("non_content_bytes")),
        event_types=progress.get("event_types"),
        non_content_bytes_after_last_content=_integer(
            progress.get("non_content_bytes_after_last_content")
        ),
        events_after_last_content=_integer(progress.get("events_after_last_content")),
    )


def scan_corpus(
    state_root_path: pathlib.Path,
    since_ms: int,
    until_ms: int,
) -> ScanResult:
    wal_dir = (
        state_root_path / "wal"
        if any((state_root_path / name).is_dir() for name in ("wal", "wal-ephemeral", "wal-archive"))
        else state_root_path
    )
    if not any(
        root.is_dir()
        for root in (wal_dir, wal_dir.parent / "wal-ephemeral", wal_dir.parent / "wal-archive")
    ):
        raise ProbeRefusal(f"no WAL directory at {wal_dir}")

    wals = logical_wals(wal_dir)
    if not wals:
        raise ProbeRefusal(f"empty corpus: no .wal files under {wal_dir}")

    attempts: list[Attempt] = []
    unjoined = excluded = without_end = 0
    for wal in wals:
        addr = getattr(wal, "address", pathlib.Path(wal.name).stem)
        run: RunContext | None = None
        for frame in read_records(wal):
            record = frame.record
            kind = record.get("type")
            if kind == "run_started":
                run = _run_context(record)
                continue
            if kind != "model_attempt_finished":
                continue
            end_at_ms = _integer(record.get("ts_ms"))
            if end_at_ms is None:
                without_end += 1
                continue
            if not since_ms <= end_at_ms <= until_ms:
                continue
            if run is None:
                unjoined += 1
                continue
            if run.excluded:
                excluded += 1
                continue
            attempts.append(_attempt(record, addr, run))

    return ScanResult(wal_dir, len(wals), attempts, unjoined, excluded, without_end)


def percentile(values: list[int], fraction: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def _distribution(attempts: list[Attempt], field: str, *, p99: bool) -> dict[str, int | None]:
    values = [value for attempt in attempts if (value := getattr(attempt, field)) is not None]
    result: dict[str, int | None] = {
        "count": len(values),
        "unknown": len(attempts) - len(values),
        "p50_ms": percentile(values, 0.50),
        "p90_ms": percentile(values, 0.90),
    }
    if p99:
        result["p99_ms"] = percentile(values, 0.99)
    result["max_ms"] = max(values) if values else None
    return result


def _group_report(attempts: list[Attempt]) -> dict[str, Any]:
    outcomes = collections.Counter(attempt.outcome for attempt in attempts)
    content_attempts = [attempt for attempt in attempts if not attempt.no_content]
    suffix = [attempt for attempt in content_attempts if (attempt.events_after_last_content or 0) > 0]
    known_suffix_bytes = [
        attempt.non_content_bytes_after_last_content
        for attempt in suffix
        if attempt.non_content_bytes_after_last_content is not None
    ]
    unknown_suffix_events = sum(
        attempt.events_after_last_content is None for attempt in content_attempts
    )
    unknown_suffix_bytes = len(suffix) - len(known_suffix_bytes)
    bytes_total = (
        None
        if unknown_suffix_events or unknown_suffix_bytes
        else sum(known_suffix_bytes)
    )
    return {
        "attempt_count": len(attempts),
        "outcomes": dict(sorted(outcomes.items())),
        "total_duration": _distribution(attempts, "duration_ms", p99=True),
        "time_to_first_content": _distribution(attempts, "time_to_first_content_ms", p99=False),
        "trailing_gap": _distribution(attempts, "trailing_gap_ms", p99=True),
        "interior_content_gap": _distribution(attempts, "max_content_gap_ms", p99=True),
        "content_attempts": sum(attempt.content_timestamps_valid for attempt in attempts),
        "no_content_attempts": sum(attempt.no_content for attempt in attempts),
        "non_content_suffix": {
            "attempt_count": len(suffix),
            "bytes_total": bytes_total,
            "bytes_measured_total": sum(known_suffix_bytes),
            "bytes_unknown_attempts": unknown_suffix_bytes,
            "events_unknown_attempts": unknown_suffix_events,
        },
    }


def _candidate_cut_summary(attempts: list[Attempt]) -> list[dict[str, int]]:
    summaries = []
    for bound in IDLE_BOUNDS_MS:
        summary = {"bound_ms": bound, "false_cuts": 0, "true_cuts": 0,
                   "unknown_outcome_cuts": 0, "unknown_gap": 0}
        for attempt in attempts:
            cut = attempt.idle_cut(bound)
            if cut is None:
                summary["unknown_gap"] += 1
            elif cut:
                if attempt.outcome in SUCCESS_OUTCOMES:
                    summary["false_cuts"] += 1
                elif attempt.outcome in FAILURE_OR_CANCEL_OUTCOMES:
                    summary["true_cuts"] += 1
                else:
                    summary["unknown_outcome_cuts"] += 1
        summaries.append(summary)
    return summaries


def _gap_candidates(attempt: Attempt) -> list[tuple[int, str]]:
    if attempt.no_content:
        duration = attempt.duration_ms
        return [] if duration is None else [(duration, "no_content_duration")]
    candidates = []
    first = attempt.time_to_first_content_ms
    trailing = attempt.trailing_gap_ms
    if first is not None:
        candidates.append((first, "time_to_first_content"))
    if trailing is not None:
        candidates.append((trailing, "trailing_gap"))
    return candidates


def _report_data(result: ScanResult, since_ms: int, until_ms: int) -> dict[str, Any]:
    groups: dict[tuple[str, str], list[Attempt]] = collections.defaultdict(list)
    for attempt in result.attempts:
        groups[(attempt.provider, attempt.model)].append(attempt)
    provider_models = [
        {"provider": provider, "model": model, **_group_report(attempts)}
        for (provider, model), attempts in sorted(groups.items())
    ]
    ranked = []
    for attempt in result.attempts:
        candidates = _gap_candidates(attempt)
        if not candidates:
            continue
        gap_ms, gap_kind = max(candidates, key=lambda item: (item[0], item[1]))
        ranked.append((gap_ms, gap_kind, attempt))
    ranked.sort(key=lambda item: (-item[0], item[2].session_addr, item[2].run_id,
                                  item[2].step_id, item[2].ordinal))
    longest = [
        {
            "session_addr": attempt.session_addr,
            "run_id": attempt.run_id,
            "step": attempt.step_id,
            "attempt": attempt.ordinal,
            "provider": attempt.provider,
            "model": attempt.model,
            "outcome": attempt.outcome,
            "gap_kind": kind,
            "gap_ms": gap,
        }
        for gap, kind, attempt in ranked[:20]
    ]
    interior_ranked = [
        (attempt.max_content_gap_ms, attempt)
        for attempt in result.attempts
        if attempt.max_content_gap_ms is not None
    ]
    interior_ranked.sort(
        key=lambda item: (-item[0], item[1].session_addr, item[1].run_id,
                          item[1].step_id, item[1].ordinal)
    )
    longest_interior = [
        {
            "session_addr": attempt.session_addr,
            "run_id": attempt.run_id,
            "step": attempt.step_id,
            "attempt": attempt.ordinal,
            "provider": attempt.provider,
            "model": attempt.model,
            "outcome": attempt.outcome,
            "gap_ms": gap,
            "ended_at_ms": attempt.max_content_gap_ended_at_ms,
        }
        for gap, attempt in interior_ranked[:20]
    ]
    return {
        "window": {"since_ms": since_ms, "until_ms": until_ms, "inclusive": True,
                   "filter": "attempt ts_ms end time"},
        "wal_files": result.wal_files,
        "attempts_in_window": len(result.attempts),
        "unjoined_attempts": result.unjoined_attempts,
        "excluded_fixture_or_import_attempts": result.excluded_attempts,
        "attempts_without_end_timestamp": result.attempts_without_end,
        "provider_models": provider_models,
        "idle_bound_sensitivity": _candidate_cut_summary(result.attempts),
        "longest_observed_initial_or_trailing_gaps": longest,
        "longest_observed_interior_gaps": longest_interior,
        "mid_stream_longest_gap": {
            "available": bool(interior_ranked),
            "attempts_with_measurement": len(interior_ranked),
            "unavailable_attempts": len(result.attempts) - len(interior_ranked),
            "reason": (
                None
                if interior_ranked
                else (
                    "unavailable: no selected attempt has a measured interval between content events; "
                    "older records or attempts with fewer than two content events have no value."
                )
            ),
        },
    }


def _iso_utc(timestamp_ms: int) -> str:
    return dt.datetime.fromtimestamp(timestamp_ms / 1000, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _display(value: int | None) -> str:
    return "unknown" if value is None else f"{value} ms"


def _format_distribution(title: str, stats: dict[str, int | None]) -> str:
    names = ["p50_ms", "p90_ms"]
    if "p99_ms" in stats:
        names.append("p99_ms")
    names.append("max_ms")
    values = " ".join(f"{name.removesuffix('_ms')}={_display(stats[name])}" for name in names)
    return f"  {title}: {values} (measured={stats['count']}, unknown={stats['unknown']})"


def render_text(data: dict[str, Any]) -> str:
    window = data["window"]
    if data["mid_stream_longest_gap"]["available"]:
        gap_status = (
            "mid-stream longest gap: measured when at least two content events arrived; "
            "missing measurements remain unavailable for older records or fewer than two events."
        )
    else:
        gap_status = (
            "mid-stream longest gap: unavailable — selected attempts have no recorded interval between content events; "
            "records may predate this field or have fewer than two content events."
        )
    lines = [
        f"window_utc: {_iso_utc(window['since_ms'])} through {_iso_utc(window['until_ms'])} (inclusive; filtered by attempt end ts_ms)",
        f"WAL files examined: {data['wal_files']}",
        f"attempts in window: {data['attempts_in_window']}",
        f"attempts with no run_started join: {data['unjoined_attempts']}",
        f"attempts skipped for fixture/import runs: {data['excluded_fixture_or_import_attempts']}",
        f"attempts missing end timestamp across scanned WALs: {data['attempts_without_end_timestamp']}",
        gap_status,
        "",
        "provider/model distributions",
    ]
    if not data["provider_models"]:
        lines.append("  no attempts in the selected window")
    for group in data["provider_models"]:
        lines.extend([
            f"{group['provider']}/{group['model']}: attempts={group['attempt_count']}",
            "  outcomes: " + (", ".join(f"{key}={count}" for key, count in group["outcomes"].items()) or "none"),
            _format_distribution("total_duration", group["total_duration"]),
            _format_distribution("time_to_first_content", group["time_to_first_content"]),
            _format_distribution("trailing_gap", group["trailing_gap"]),
            _format_distribution("interior_content_gap", group["interior_content_gap"]),
            f"  content attempts={group['content_attempts']} no-content attempts={group['no_content_attempts']}",
            "  non-content traffic after last content: "
            f"attempts={group['non_content_suffix']['attempt_count']} "
            f"bytes_total={_display(group['non_content_suffix']['bytes_total'])} "
            f"bytes_measured_total={group['non_content_suffix']['bytes_measured_total']} "
            f"bytes_unknown_attempts={group['non_content_suffix']['bytes_unknown_attempts']} "
            f"events_unknown_attempts={group['non_content_suffix']['events_unknown_attempts']}",
        ])
    lines.extend([
        "",
        "candidate idle-bound sensitivity (time_to_first_content, interior_content_gap, "
        "and trailing_gap)",
    ])
    for item in data["idle_bound_sensitivity"]:
        mins = item["bound_ms"] // 60_000
        lines.append(
            f"  {mins} min: false cuts={item['false_cuts']} true cuts={item['true_cuts']} "
            f"unknown-outcome cuts={item['unknown_outcome_cuts']} "
            f"unknown-gap attempts={item['unknown_gap']}"
        )
    lines.append("  no-content attempts use their full sent_at_ms-to-ts_ms duration; unknown timestamps are not zero.")
    lines.extend(["", "20 longest measured time_to_first_content, trailing_gap, or no-content durations"])
    if not data["longest_observed_initial_or_trailing_gaps"]:
        lines.append("  no measurable gaps")
    for item in data["longest_observed_initial_or_trailing_gaps"]:
        lines.append(
            f"  session={item['session_addr']} run={item['run_id']} step={item['step']} "
            f"attempt={item['attempt']} provider/model={item['provider']}/{item['model']} "
            f"outcome={item['outcome']} gap={item['gap_kind']}:{item['gap_ms']} ms"
        )
    lines.extend(["", "20 longest measured interior content gaps"])
    if not data["longest_observed_interior_gaps"]:
        lines.append("  unavailable — no attempt has a measured gap between two content events")
    for item in data["longest_observed_interior_gaps"]:
        ended = "unknown" if item["ended_at_ms"] is None else str(item["ended_at_ms"]) + " ms"
        lines.append(
            f"  session={item['session_addr']} run={item['run_id']} step={item['step']} "
            f"attempt={item['attempt']} provider/model={item['provider']}/{item['model']} "
            f"outcome={item['outcome']} gap={item['gap_ms']} ms ended_at={ended}"
        )
    return "\n".join(lines)


def _parse_utc(value: str) -> int:
    try:
        parsed = dt.datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"invalid UTC timestamp {value!r}; use ISO-8601 with Z") from error
    if parsed.tzinfo is None or parsed.utcoffset() != dt.timedelta(0):
        raise argparse.ArgumentTypeError(f"timestamp must be UTC (Z or +00:00): {value!r}")
    return int(parsed.timestamp() * 1000)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", type=pathlib.Path, default=DEFAULT_STATE_ROOT,
                        help="state root containing wal/, or the WAL directory itself")
    parser.add_argument("--since", type=_parse_utc, help="inclusive UTC lower bound (ISO-8601)")
    parser.add_argument("--until", type=_parse_utc, help="inclusive UTC upper bound (ISO-8601)")
    parser.add_argument("--json", action="store_true", help="print machine-readable JSON")
    args = parser.parse_args(argv)
    now_ms = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
    args.until_ms = now_ms if args.until is None else args.until
    args.since_ms = args.until_ms - 24 * 60 * 60 * 1000 if args.since is None else args.since
    if args.since_ms > args.until_ms:
        parser.error("--since must be at or before --until")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = scan_corpus(args.state_root, args.since_ms, args.until_ms)
    except (ProbeRefusal, ArchiveRefusal, OSError, state_root.StateRootError) as error:
        print(f"REFUSING: {error}", file=sys.stderr)
        return 2
    data = _report_data(result, args.since_ms, args.until_ms)
    print(json.dumps(data, indent=2, sort_keys=True) if args.json else render_text(data))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
