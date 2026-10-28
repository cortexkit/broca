"""Read the logical WAL population without extracting or trusting archive payloads.

The byte format and index-recovery rules follow broca-wal/src/archive.rs. Live
paths remain pathlib.Paths; archived members expose the same read_bytes/name API.
"""
from __future__ import annotations

import hashlib
import json
import pathlib
import re
import struct
import sys
import uuid
from dataclasses import dataclass
from typing import Literal, overload

from lib import wal_gate


class ArchiveRefusal(RuntimeError):
    """The corpus cannot be reported on without hiding unavailable evidence."""

LIVE_TREES = ("wal", "wal-ephemeral", "wal-durable")
ARCHIVE_TREES = ("wal-archive", "wal-archive-durable")


def move_exclusions(wal_dir: pathlib.Path, *, refuse_sidecars: bool = False) -> set[str]:
    """Exclude sessions being moved between machines before reading their WAL bytes.

    The move's release and activate operations can leave a partially installed WAL,
    even on a receiver with no previous session or sidecar. Their pending intents
    in agent-data/cuts/ name contract session refs, not WAL filename addresses, so
    refuse the whole scan rather than duplicate the ref hash to filter individual
    sessions. Unreadable cut state cannot prove that installation has finished.
    The .moving/.moved sidecars share the WAL's stem and hide archived copies too.

    Cut-state writers must preserve intent.op for this reader. Refusal messages
    name the ref set from refs, ref_set, or intent.request.refs, in that order.
    """
    if "wal-staging" in wal_dir.resolve().parts:
        raise ArchiveRefusal(f"{wal_dir}: wal-staging is not a live WAL population; "
                             "rerun after the move finishes")
    cut_dir = wal_dir.parent / "agent-data" / "cuts"
    try:
        cuts = sorted(cut_dir.iterdir())
    except FileNotFoundError:
        cuts = []
    except OSError as error:
        raise ArchiveRefusal(f"cannot read cut state at {cut_dir}: {error}; "
                             "rerun after the move finishes") from error
    for path in cuts:
        if path.suffix != ".json":
            continue
        try:
            state = json.loads(path.read_bytes())
            if not isinstance(state, dict):
                raise ValueError("cut state is not an object")
            intent = state.get("intent")
            if intent is not None and (not isinstance(intent, dict)
                                       or not isinstance(intent.get("op"), str)):
                raise ValueError("cut intent has no operation")
        except (OSError, ValueError) as error:
            raise ArchiveRefusal(f"cannot read cut state at {path}: {error}; "
                                 "rerun after the move finishes") from error
        if intent is not None and intent["op"] in ("release", "activate"):
            request = intent.get("request")
            refs = state.get("refs", state.get("ref_set"))
            if refs is None and isinstance(request, dict):
                refs = request.get("refs")
            raise ArchiveRefusal(f"{path}: cut {state.get('cutId', path.stem)} has pending "
                                 f"{intent['op']} intent, refs={json.dumps(refs)}; "
                                 "rerun after the move finishes")
    roots = [wal_dir]
    if wal_dir.name in LIVE_TREES:
        roots = [wal_dir.parent / name for name in LIVE_TREES]
    excluded = set()
    for root in roots:
        try:
            paths = sorted(root.iterdir())
        except FileNotFoundError:
            continue
        except OSError as error:
            raise ArchiveRefusal(f"cannot enumerate move sidecars at {root}: {error}") from error
        for path in paths:
            if path.suffix not in (".moving", ".moved"):
                continue
            message = f"{path}: session {path.stem} has {path.suffix} sidecar"
            if refuse_sidecars:
                raise ArchiveRefusal(f"{message}; rerun after the move finishes")
            excluded.add(path.stem)
            print(f"skipping {message}", file=sys.stderr)
    return excluded


HEADER_LEN = 64
INDEX_ENTRY_LEN = 32
INDEX_V2_EXTENSION_LEN = 64
INDEX_V2_DOMAIN = b"broca-ark-index/v2\0"

# The WAL framing layout from broca-wal/src/framing.rs, not the archive header.
WAL_HEADER_LEN = 53
MAX_PAYLOAD_LEN = 64 * 1024 * 1024
JSON_SPACE = re.compile(r"[ \t\r\n]*")


@dataclass(frozen=True)
class WalFrame:
    seq: int
    fence: int
    record: dict
    payload: bytes
    requires: tuple[str, ...] = ()


def _skip_json_space(text: str, offset: int) -> int:
    match = JSON_SPACE.match(text, offset)
    assert match is not None  # the pattern accepts an empty span
    return match.end()


def _record_payload(payload: bytes) -> bytes:
    """Keep the record's original JSON bytes, excluding the outer envelope.

    Re-encoding would change whitespace, escapes and number spellings, which are
    evidence for a fixed-byte scan. The envelope has already been JSON-validated.
    Walk only its top-level keys so a nested or quoted 'record' cannot select it.
    """
    text = payload.decode("utf-8")
    decoder = json.JSONDecoder()
    offset = _skip_json_space(text, 0) + 1  # opening object brace
    selected = None
    while True:
        offset = _skip_json_space(text, offset)
        if text[offset] == "}":
            break
        key, offset = decoder.raw_decode(text, offset)
        offset = _skip_json_space(text, offset)
        offset += 1  # colon
        offset = _skip_json_space(text, offset)
        start = offset
        _, offset = decoder.raw_decode(text, offset)
        if key == "record":
            selected = text[start:offset].encode("utf-8")
        offset = _skip_json_space(text, offset)
        if text[offset] == "}":
            break
        offset += 1  # comma
    assert selected is not None, "validated envelope must contain a record"
    return selected


def _frame_at(raw: bytes, offset: int, where: str):
    """Validate one frame independently of the direction used to reach its header."""
    if len(raw) - offset < WAL_HEADER_LEN:
        raise ArchiveRefusal(f"{where}: torn tail (incomplete header)")
    length, version, seq, fence = struct.unpack_from("<IBQQ", raw, offset)
    if version not in (1, 2, wal_gate.GATED_FRAMING_VERSION):
        raise ArchiveRefusal(f"{where}: unsupported WAL framing version {version}")
    if length > MAX_PAYLOAD_LEN:
        raise ArchiveRefusal(f"{where}: payload {length} exceeds {MAX_PAYLOAD_LEN}-byte cap")
    end = offset + WAL_HEADER_LEN + length
    if end > len(raw):
        raise ArchiveRefusal(f"{where}: torn tail (incomplete payload)")
    payload = raw[offset + WAL_HEADER_LEN:end]
    digest = raw[offset + 21:offset + WAL_HEADER_LEN]
    expected = hashlib.sha256(raw[offset + 4:offset + 21] + payload).digest()
    if digest != expected:
        raise ArchiveRefusal(f"{where}: digest mismatch at sequence {seq}")
    if version == 2 and (length, seq, fence) != (16, 0, 0):
        raise ArchiveRefusal(f"{where}: invalid lineage frame")
    return version, seq, fence, payload, end


def _verified_frames(wal: pathlib.Path | ArchivedWal):
    """Share framing verification between record scans and lineage discovery."""
    raw = wal.read_bytes()
    offset, expected_seq = 0, 1
    lineage = None
    while offset < len(raw):
        where = f"{wal}: WAL frame at byte {offset}"
        version, seq, fence, payload, end = _frame_at(raw, offset, where)
        if version == 2:
            if lineage is not None and lineage != payload:
                raise ArchiveRefusal(f"{where}: conflicting lineage IDs")
            lineage = payload
        elif seq != expected_seq:
            raise ArchiveRefusal(f"{where}: expected sequence {expected_seq}, found {seq}")
        else:
            expected_seq += 1
        yield where, version, seq, fence, payload
        offset = end


def backward_chunks(handle, size: int, chunk_size: int = 4096):
    """The shared EOF-first chunk walk used by live-run and recent-credential scans."""
    pos = size
    while pos > 0:
        step = min(chunk_size, pos)
        pos -= step
        handle.seek(pos)
        block = handle.read(step)
        if len(block) != step:
            raise ArchiveRefusal(f"{handle.name}: torn tail (file shortened during backward read)")
        yield pos, block


def read_lineage_id(wal: pathlib.Path) -> str | None:
    """Return a verified identity, or None only for a readable legacy/empty WAL.

    Legacy WALs acquire their first lineage frame after existing records, and each
    append repeats it. Walk the entire snapshot so conflicting IDs or unreadable
    metadata cannot turn into a guessed identity or a guessed absence.
    """
    lineage = None
    for _where, version, _seq, _fence, payload in _verified_frames(wal):
        if version == 2:
            lineage = str(uuid.UUID(bytes=payload))
    return lineage


def is_tombstoned(wal: pathlib.Path) -> bool:
    """Check for <wal stem>.expired/<current lineage ID>, not just the directory.

    A session key can be reused after deletion. Tombstones for the previous WAL's
    identity must not suppress a new WAL at the same filename.
    """
    lineage = read_lineage_id(wal)
    if lineage is None:
        return False
    directories = {wal.with_suffix(".expired")}
    if wal.parent.name in LIVE_TREES:
        directories.add(wal.parent.parent / "wal-ephemeral" / f"{wal.stem}.expired")
    for directory in directories:
        try:
            (directory / lineage).stat()
        except FileNotFoundError:
            continue
        return True
    return False


def _decode_record(where, version, seq, fence, payload) -> WalFrame | None:
    if version == 2:
        return None
    try:
        requires = ()
        if version == wal_gate.GATED_FRAMING_VERSION:
            missing, envelope = wal_gate.open_gated(payload)
            if missing:
                raise ArchiveRefusal(f"{where}: newer-writer, missing features: {', '.join(missing)}")
            record = envelope["record"]
            requires = tuple(envelope["requires"])
            payload = _record_payload(payload)
        else:
            record = json.loads(payload)
            # Historical test fixtures wrapped the record in a timestamped object.
            if isinstance(record, dict) and "type" not in record and "record" in record:
                record = record["record"]
                payload = _record_payload(payload)
    except (UnicodeDecodeError, ValueError) as error:
        raise ArchiveRefusal(f"{where}: invalid record JSON: {error}") from error
    if not isinstance(record, dict) or not isinstance(record.get("type"), str):
        raise ArchiveRefusal(f"{where}: no tagged WAL record")
    return WalFrame(seq, fence, record, payload, requires)


def read_records(wal: pathlib.Path | ArchivedWal) -> list[WalFrame]:
    """Decode a live or archived WAL snapshot before exposing any scan results.

    Like Rust's framing decoder, verify the version, size and digest, skip lineage
    metadata, and check every gated requirement before using its record. Feature
    names come from wal_gate's source-backed table, never a second capability list.
    This is a diagnostic JSON reader, not a validator of every WalRecord field.
    Unlike a serving reader, refuse a torn tail: a partial corpus cannot certify
    absence. No repair or extraction writes are made.
    """
    frames = []
    for parts in _verified_frames(wal):
        frame = _decode_record(*parts)
        if frame is not None:
            frames.append(frame)
    return frames


_HEADER_MARKERS = re.compile(rb"[\x00-\x1f]")


def read_records_reverse(wal: pathlib.Path, chunk_size: int = 4096):
    """Yield verified live frames newest first, without decoding the unread prefix.

    Frames have no trailing length. Find headers in backward chunks by their
    leading length ending exactly at the preceding frame boundary, then use the
    same digest, framing and record checks as the full forward reader. Header
    version bytes are control bytes, which cannot occur bare inside JSON strings.
    """
    with wal.open("rb") as handle:
        handle.seek(0, 2)
        end = handle.tell()
        overlap = b""
        newer_seq = None
        lineage = None
        invalid = None
        for pos, block in backward_chunks(handle, end, chunk_size):
            window = block + overlap
            for match in reversed(list(_HEADER_MARKERS.finditer(window))):
                local = match.start() - 4
                start = pos + local
                if local < 0 or start + WAL_HEADER_LEN > end:
                    continue
                length = struct.unpack_from("<I", window, local)[0]
                if start + WAL_HEADER_LEN + length != end:
                    continue
                where = f"{wal}: WAL frame at byte {start}"
                handle.seek(start)
                raw = handle.read(end - start)
                try:
                    version, seq, fence, payload, frame_end = _frame_at(raw, 0, where)
                except ArchiveRefusal as error:
                    # A digest/header byte can resemble a version marker. Only
                    # a validated frame establishes the next backward boundary.
                    invalid = error
                    continue
                if frame_end != len(raw):
                    raise ArchiveRefusal(f"{where}: frame changed during backward read")
                if version == 2:
                    if lineage is not None and lineage != payload:
                        raise ArchiveRefusal(f"{where}: conflicting lineage IDs")
                    lineage = payload
                    if start == 0 and newer_seq not in (None, 1):
                        raise ArchiveRefusal(f"{where}: expected first record sequence 1, found {newer_seq}")
                else:
                    if seq < 1 or (newer_seq is not None and seq != newer_seq - 1) or (start == 0 and seq != 1):
                        raise ArchiveRefusal(f"{where}: unexpected record sequence {seq}")
                    newer_seq = seq
                end = start
                invalid = None
                frame = _decode_record(where, version, seq, fence, payload)
                if frame is not None:
                    yield frame
            overlap = window[:4]
            if end - pos > MAX_PAYLOAD_LEN + WAL_HEADER_LEN:
                raise invalid or ArchiveRefusal(f"{wal}: torn or corrupt frame ending at byte {end}")
        if end:
            raise invalid or ArchiveRefusal(f"{wal}: torn or corrupt frame ending at byte {end}")


def fold_timestamp(path: pathlib.Path) -> int:
    match = re.fullmatch(r"fold-(0|[1-9][0-9]*)\.ark", path.name)
    if match is None or int(match[1]) > 2**64 - 1:
        raise ArchiveRefusal(f"{path}: invalid canonical fold timestamp")
    return int(match[1])


def _exact(source, size: int) -> bytes:
    value = source.read(size)
    if len(value) != size:
        raise ArchiveRefusal("truncated archive")
    return value


def _address(raw: bytes) -> str:
    if re.fullmatch(b"[0-9a-fA-F]{16}", raw) is None:
        raise ArchiveRefusal("invalid archive session address")
    return raw.decode("ascii")


def _header(source, offset: int, end: int):
    if offset + HEADER_LEN > end:
        raise ArchiveRefusal("truncated archive member header")
    source.seek(offset)
    raw = _exact(source, HEADER_LEN)
    if raw[:8] != b"WALM\x01\x00\x00\x00":
        raise ArchiveRefusal("invalid archive member header")
    addr = _address(raw[8:24])
    length = struct.unpack_from("<Q", raw, 24)[0]
    if offset + HEADER_LEN + length > end:
        raise ArchiveRefusal("archive member exceeds container bounds")
    return addr, length, raw[32:64]


@dataclass(frozen=True)
class ArchivedWal:
    container: pathlib.Path
    address: str
    offset: int
    size: int
    digest: bytes
    end: int

    @property
    def name(self) -> str:
        return self.address + ".wal"

    @property
    def activity_upper_bound(self) -> float:
        # Retention is configurable and not stored in the container. Current config
        # cannot reconstruct past policy, so only the fold time bounds activity.
        return fold_timestamp(self.container) / 1000

    def __str__(self) -> str:
        return f"{self.container}::{self.name}"

    def read_bytes(self) -> bytes:
        try:
            with self.container.open("rb") as source:
                end, _ = _trailer(source)
                if end != self.end:
                    raise ArchiveRefusal("archive changed during scan")
                addr, length, digest = _header(source, self.offset, end)
                if (addr, length, digest) != (self.address, self.size, self.digest):
                    raise ArchiveRefusal("archive header changed during scan")
                payload = _exact(source, length)
                if hashlib.sha256(payload).digest() != digest:
                    raise ArchiveRefusal("archive member digest mismatch")
                return payload
        except (OSError, ArchiveRefusal) as error:
            raise ArchiveRefusal(f"{self}: {error}") from error


def _trailer(source):
    source.seek(0, 2)
    size = source.tell()
    if size < 8:
        raise ArchiveRefusal("unsealed archive is not readable")
    source.seek(size - 8)
    trailer = _exact(source, 8)
    if trailer[4:] != b"WALK":
        raise ArchiveRefusal("unsealed archive is not readable")
    return size - 8, struct.unpack_from("<I", trailer)[0]


def _index(source, end: int, length: int):
    if length < 4 or length > end or (length - 4) % INDEX_ENTRY_LEN:
        raise ArchiveRefusal("invalid archive index length")
    start = end - length
    source.seek(start)
    count = struct.unpack("<I", _exact(source, 4))[0]
    entries_length = count * INDEX_ENTRY_LEN + 4
    v2 = entries_length + INDEX_V2_EXTENSION_LEN == length
    if entries_length != length and not v2:
        raise ArchiveRefusal("invalid archive index count")
    hasher = hashlib.sha256()
    if v2:
        hasher.update(INDEX_V2_DOMAIN)
        hasher.update(struct.pack("<QI", fold_timestamp(pathlib.Path(source.name)), length))
        hasher.update(struct.pack("<I", count))
    entries, seen, next_offset = [], set(), 0
    for _ in range(count):
        raw = _exact(source, INDEX_ENTRY_LEN)
        hasher.update(raw)
        addr = _address(raw[:16])
        offset, size = struct.unpack_from("<QQ", raw, 16)
        entries.append((addr, offset, size))
    if v2:
        extension = _exact(source, INDEX_V2_EXTENSION_LEN)
        if extension[32:36] != b"IDX2" or extension[36:] != bytes(28):
            raise ArchiveRefusal("invalid archive index extension")
        hasher.update(extension[32:])
        if hasher.digest() != extension[:32]:
            raise ArchiveRefusal("archive index digest mismatch")
        entries.sort(key=lambda entry: entry[1])
    for addr, offset, size in entries:
        if offset != next_offset or addr in seen:
            raise ArchiveRefusal("invalid archive index coverage")
        next_offset = offset + HEADER_LEN + size
        if next_offset > start:
            raise ArchiveRefusal("index member exceeds bounds")
        seen.add(addr)
    if next_offset != start:
        raise ArchiveRefusal("archive index omits members")
    return entries, start


def _members(path: pathlib.Path) -> list[ArchivedWal]:
    try:
        with path.open("rb") as source:
            end, length = _trailer(source)
            try:
                index, start = _index(source, end, length)
            except ArchiveRefusal:
                # Like the engine, recover a missing/corrupt cache by walking headers.
                index, start = None, end
            members = []
            if index is not None:
                for addr, offset, size in index:
                    actual, actual_size, digest = _header(source, offset, start)
                    if (actual, actual_size) != (addr, size):
                        raise ArchiveRefusal("archive index disagrees with header")
                    members.append(ArchivedWal(path, addr, offset, size, digest, end))
            else:
                offset, seen = 0, set()
                while offset < end:
                    if end - offset < 4:
                        raise ArchiveRefusal("truncated member or index")
                    source.seek(offset)
                    if _exact(source, 4) != b"WALM":
                        # Only a valid index can end the member walk. Otherwise a
                        # damaged header would silently hide the remaining WALs.
                        recovered, index_start = _index(source, end, end - offset)
                        if index_start != offset or recovered != [
                            (member.address, member.offset, member.size) for member in members
                        ]:
                            raise ArchiveRefusal("archive index disagrees with header walk")
                        break
                    addr, size, digest = _header(source, offset, end)
                    if addr in seen:
                        raise ArchiveRefusal("duplicate session in archive headers")
                    seen.add(addr)
                    members.append(ArchivedWal(path, addr, offset, size, digest, end))
                    offset += HEADER_LEN + size
                else:
                    raise ArchiveRefusal("archive member walk has no well-formed index")
            return members
    except (OSError, ArchiveRefusal) as error:
        raise ArchiveRefusal(f"{path}: {error}") from error


# A retained session's log can be deleted by retention between the directory listing and the
# read that verifies it. Rescanning sees the population as it now stands; a log that is gone
# held no live run, because retention never deletes a session that is running.
RESCANS_ON_VANISHED_WAL = 3


@overload
def logical_wals(wal_dir: pathlib.Path, *, include_archives: Literal[False]) -> list[pathlib.Path]: ...


@overload
def logical_wals(wal_dir: pathlib.Path, *, include_archives: Literal[True] = True) -> list[pathlib.Path | ArchivedWal]: ...


@overload
def logical_wals(wal_dir: pathlib.Path, *, include_archives: bool) -> list[pathlib.Path] | list[pathlib.Path | ArchivedWal]: ...


def logical_wals(wal_dir: pathlib.Path, *, include_archives: bool = True):
    """Resolve all live trees, then select the newest archived snapshot. A log that vanishes
    mid-scan triggers a full rescan, up to RESCANS_ON_VANISHED_WAL times, before refusing."""
    for attempt in range(RESCANS_ON_VANISHED_WAL + 1):
        try:
            return _logical_wals_once(wal_dir, include_archives=include_archives)
        except ArchiveRefusal as refusal:
            vanished = isinstance(refusal.__cause__, FileNotFoundError)
            if not vanished or attempt == RESCANS_ON_VANISHED_WAL:
                raise


def _logical_wals_once(wal_dir: pathlib.Path, *, include_archives: bool):
    """Resolve all live trees, then select the newest archived snapshot.

    Only .wal inputs are considered, never .expired/ children or .deleting files.
    Verify each live file's lineage and omit only its own matching tombstone. A
    matching tombstone also blocks archive fallback at that address. Live-only
    instruments opt out of archives without changing the retention filter. Move
    sidecars hide both live and archived copies; pending cut intents refuse a scan.

    For duplicate archived addresses, prefer the greatest numeric timestamp from
    fold-<milliseconds>.ark. Archive writes are serialized, and a resumed incomplete
    archive gets a new timestamp, so this selects the most recent sealed snapshot.
    Discovery validates indexes; payload digests are checked when read, not extracted.

    Unlike the serving engine, diagnostics refuse an unsealed archive even when
    all its members are still live. Serving may safely skip that container because
    folding seals it before unlinking live files. A diagnostic scan must not give
    a clean verdict while a fold is in flight or stopped mid-rewrite: its corpus
    is not stable enough to certify absence.
    """
    excluded = move_exclusions(wal_dir)
    try:
        roots = [wal_dir]
        if wal_dir.name in LIVE_TREES:
            roots = [wal_dir.parent / name for name in LIVE_TREES]
        selected = {}
        occupied = {}
        for root in roots:
            try:
                paths = list(root.iterdir())
            except FileNotFoundError:
                continue
            for path in paths:
                if path.suffix != ".wal" or path.stem in excluded:
                    continue
                if path.stem in occupied:
                    raise ArchiveRefusal(f"dual live WALs at {occupied[path.stem]} and {path}")
                occupied[path.stem] = path
                if not is_tombstoned(path):
                    selected[path.stem] = path
        containers = []
        if include_archives:
            for name in ARCHIVE_TREES:
                directory = wal_dir.parent / name
                if directory.exists():
                    containers.extend(p for p in directory.iterdir() if p.suffix == ".ark")
        for path in sorted(containers, key=fold_timestamp, reverse=True):
            for member in _members(path):
                if member.address not in occupied and member.address not in excluded:
                    selected.setdefault(member.address, member)
        return [selected[addr] for addr in sorted(selected)]
    except OSError as error:
        raise ArchiveRefusal(f"cannot enumerate WAL population: {error}") from error


def in_window(wal, cutoff: float, warned: set[pathlib.Path]) -> bool:
    """Keep uncertain archived ages, but exclude those provably outside the window."""
    if not isinstance(wal, ArchivedWal):
        return wal.stat().st_mtime >= cutoff
    bound = wal.activity_upper_bound
    if cutoff > bound:
        return False
    if wal.container not in warned:
        warned.add(wal.container)
        print(f"archive scope: {wal.container}: last activity <= {bound:.3f} Unix seconds; "
              "source mtime and historical idle retention unavailable, including members of uncertain window membership",
              file=sys.stderr)
    return True


def recency(wal) -> float:
    """Archived ordering is an upper bound, not a reconstructed source mtime."""
    return wal.activity_upper_bound if isinstance(wal, ArchivedWal) else wal.stat().st_mtime
