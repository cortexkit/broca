#!/usr/bin/env python3
"""Separate-process driver for memory-load-server (Python 3.11+, Linux).

Build with --target x86_64-unknown-linux-gnu (so proc macros stay PIC),
RUSTFLAGS='-C force-frame-pointers=yes -C relocation-model=static'
and CARGO_PROFILE_RELEASE_DEBUG=1, then run this script with --server pointing at the
built memory-load-server binary (feature heap-profile).
The loopback peer stands in for subc after authentication; all management requests
still pass through the production serve adapter, registry, actor and core loop.
"""
import argparse
from collections import Counter
import asyncio
import gzip
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import time

HEADER = struct.Struct("<IBBBHIQ")
MODELS = [("openai", "gpt-5.4"), ("anthropic", "claude-opus-4-6"),
          ("google", "gemini-2.5-pro")]


async def until(predicate, label, timeout=600):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise TimeoutError(label)
        await asyncio.sleep(.05)


class Peer:
    def __init__(self, reader, writer):
        self.reader, self.writer = reader, writer
        self.next_corr = 1
        self.pending = {}
        self.events = 0
        self.task = asyncio.create_task(self.read())

    async def read(self):
        try:
            while True:
                size, ver, ty, flags, channel, epoch, corr = HEADER.unpack(
                    await self.reader.readexactly(21))
                body = await self.reader.readexactly(size)
                if ty == 9:
                    self.emit(10, 0, corr, {"negotiated_ver": 2, "subc_ops": [],
                              "subc_capabilities": [], "storage": None, "machine_id": None})
                elif ty in (1, 5) and corr in self.pending:
                    future = self.pending.pop(corr)
                    if ty == 5:
                        future.set_exception(RuntimeError(body.decode()))
                    else:
                        future.set_result(json.loads(body))
                elif ty in (3, 4):
                    self.events += 1
        except (asyncio.IncompleteReadError, ConnectionError) as error:
            for future in self.pending.values():
                future.set_exception(error)

    def emit(self, ty, channel, corr, value, flags=2):
        body = json.dumps(value, separators=(",", ":")).encode()
        self.writer.write(HEADER.pack(len(body), 2, ty, flags, channel,
                                      int(channel != 0), corr) + body)

    async def request(self, channel, value):
        corr = self.next_corr
        self.next_corr += 1
        future = asyncio.get_running_loop().create_future()
        self.pending[corr] = future
        self.emit(0, channel, corr, value)
        await self.writer.drain()
        return await asyncio.wait_for(future, 600)

    async def call(self, channel, method, params):
        reply = await self.request(channel, {"method": method, "params": params})
        if "error" in reply:
            raise RuntimeError(reply)
        return reply.get("result", reply)

    async def bind(self, channel, project, session):
        return await self.request(0, {"op": "route.bind", "route_channel": channel,
            "epoch": 1, "target": {"kind": "management_surface", "module_id": "broca"},
            "identity": {"project_root": str(project), "harness": "memory-load", "session": session}})

    def subscribe(self, channel):
        corr = self.next_corr
        self.next_corr += 1
        self.emit(0, channel, corr, {"method": "session.subscribe", "params": {"from": "live"}}, flags=130)


class Stub:
    def __init__(self):
        self.release = asyncio.Event()
        self.active = 0
        self.requests = []
        self.errors = []

    async def handle(self, reader, writer):
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            path = headers.split(b" ")[1].decode()
            fields = dict(line.lower().split(b":", 1) for line in headers.split(b"\r\n")[1:] if b":" in line)
            body = await reader.readexactly(int(fields[b"content-length"]))
            if b"gzip" in fields.get(b"content-encoding", b""):
                body = gzip.decompress(body)
            request = json.loads(body)
            turns = request.get('messages', request.get('input', request.get('contents', [])))
            latest = json.dumps(turns[-1]) if turns else ''
            short = "ONE_SHOT" in latest or "SEED_SETTLE" in latest
            self.requests.append({"path": path, "bytes": len(body), "short": short})
            del body, request
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n")
            await writer.drain()
            if not short:
                self.active += 1
                await self.release.wait()
                self.active -= 1
            if "messages" in path:
                events = [('message_start', {"type": "message_start", "message": {"id": "msg_load", "type": "message", "role": "assistant", "content": [], "model": "claude", "usage": {"input_tokens": 100, "output_tokens": 0}}}),
                    ('content_block_start', {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}),
                    ('content_block_delta', {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "done"}}),
                    ('content_block_stop', {"type": "content_block_stop", "index": 0}),
                    ('message_delta', {"type": "message_delta", "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 1}}),
                    ('message_stop', {"type": "message_stop"})]
            elif "generatecontent" in path.lower():
                events = [(None, {"candidates": [{"content": {"role": "model", "parts": [{"text": "done"}]}, "finishReason": "STOP"}], "usageMetadata": {"promptTokenCount": 100, "candidatesTokenCount": 1}})]
            elif "responses" in path:
                events = [('response.created', {"type": "response.created", "response": {"id": "resp_load", "status": "in_progress", "output": []}}),
                    ('response.output_text.delta', {"type": "response.output_text.delta", "item_id": "msg_load", "output_index": 0, "content_index": 0, "delta": "done"}),
                    ('response.completed', {"type": "response.completed", "response": {"id": "resp_load", "status": "completed", "output": [{"id": "msg_load", "type": "message", "role": "assistant", "status": "completed", "content": [{"type": "output_text", "text": "done", "annotations": []}]}], "usage": {"input_tokens": 100, "output_tokens": 1, "total_tokens": 101}}})]
            else:
                raise AssertionError(f"unexpected provider path: {path}")
            for event, value in events:
                if event:
                    writer.write(f"event: {event}\n".encode())
                writer.write(b"data: " + json.dumps(value).encode() + b"\n\n")
            await writer.drain()
        except Exception as error:
            self.errors.append(repr(error))
        finally:
            writer.close()
            await writer.wait_closed()


def catalog(path):
    facts = {"existence": "present", "capability.attachment": False,
             "capability.reasoning": False, "capability.tool_call": True,
             "limit.context": 10_000_000, "limit.output": 4096}
    models = {f"{provider}/{model}": facts for provider, model in MODELS}
    for i in range(9997):
        models[f"openai/memory-model-{i:05}"] = facts
    path.write_text(json.dumps({"source": "memory-load", "resolved_at_ms": 1,
                               "catalog_version": 1, "models": models}))


async def snapshot(root, label, pid):
    directory = root / "profiles"
    (directory / f"{label}.request").touch()
    path = directory / f"{label}.json"
    await until(path.exists, f"snapshot {label}")
    profile = json.loads(path.read_text())
    status = Path(f"/proc/{pid}/status").read_text()
    rss = next(int(line.split()[1]) * 1024 for line in status.splitlines() if line.startswith("VmRSS:"))
    sqlite = json.loads((directory / "sqlite-memory.json").read_text())
    profile['rss'] = rss
    profile['sqlite'] = sqlite
    path.write_text(json.dumps(profile))
    print(json.dumps({"phase": label, "live": profile["live_bytes"], "blocks": profile["live_blocks"],
        "peak": profile["exact_peak_bytes"], "sampled_peak": profile["sampled_peak_bytes"],
        "rss": rss, "sqlite": sqlite}), flush=True)


async def run(args):
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    (root / "profiles").mkdir()
    catalog(root / "catalog.json")
    stub = Stub()
    server = await asyncio.start_server(stub.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    log = (root / "server.log").open("w")
    server_args = [str(root), f"http://127.0.0.1:{port}", str(args.sessions)]
    if args.idle_ttl_ms is not None:
        server_args.append(str(args.idle_ttl_ms))
    process = await asyncio.create_subprocess_exec(str(args.server.resolve()), *server_args,
        stdout=log, stderr=log,
        env={**os.environ, "BROCA_HEAP_PROFILE_DIR": str(root / "profiles"), "RUST_BACKTRACE": "1"})
    try:
        await until(lambda: (root / "listen.json").exists() or process.returncode is not None, "seed/listen")
        if process.returncode is not None:
            raise RuntimeError((root / "server.log").read_text())
        info = json.loads((root / "listen.json").read_text())
        host, port = info["address"].split(":")
        peer = Peer(*(await asyncio.open_connection(host, int(port))))
        await snapshot(root, "baseline", process.pid)
        project = root / "project"
        for route in range(1, args.routes + 1):
            await peer.bind(route, project, f"long-{(route - 1) % args.sessions}")
        await snapshot(root, "bound", process.pid)
        # Imports encode their whole history as the episode input. A completed real
        # turn replaces that seed checkpoint with the short input of an ordinary run;
        # otherwise we would profile import-only copies, not a long-lived conversation.
        for i in range(args.sessions):
            provider, model = MODELS[i % 3]
            await peer.call(i + 1, "session.send", {"prompt": "SEED_SETTLE", "model": {"provider": provider, "model": model}, "tools": []})
            await until_idle(peer, i + 1)
        await snapshot(root, "seeded", process.pid)
        for i in range(args.sessions):
            peer.subscribe(i + 1)
        await peer.writer.drain()
        await asyncio.sleep(1)
        await snapshot(root, "subscribed", process.pid)
        await snapshot(root, "reset", process.pid)
        for i in range(args.sessions):
            provider, model = MODELS[i % 3]
            result = await peer.call(i + 1, "session.send", {"prompt": "Continue the investigation.",
                "model": {"provider": provider, "model": model},
                "tools": [{"name": "read_file", "description": "Read source", "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}}}]})
            assert result["state"] == "active", result
        await until(lambda: stub.active == args.sessions or bool(stub.errors), "all HTTP requests held", timeout=120)
        assert not stub.errors, stub.errors
        await snapshot(root, "active", process.pid)
        # Read requests warm the same store/registry page caches as a UI observer.
        for i in range(args.sessions):
            await peer.call(i + 1, "session.read", {"limit": 20})
        shorts_started = time.monotonic()
        for i in range(args.shorts):
            channel = args.routes + i + 1
            await peer.bind(channel, project, f"short-{i}")
            result = await peer.call(channel, "session.send", {"prompt": "ONE_SHOT", "model": {"provider": "anthropic", "model": "claude-opus-4-6"}, "tools": [], "one_shot": True})
            await until_idle(peer, channel)
            assert result["state"] in ("active", "finished"), result
        # Short sessions whose actors outlive this phase are still warm at the steady
        # snapshot. If the phase runs longer than the idle TTL, the earliest ones have closed.
        shorts_seconds = round(time.monotonic() - shorts_started, 1)
        await snapshot(root, "steady", process.pid)
        stub.release.set()
        for i in range(args.sessions):
            await until_idle(peer, i + 1)
        await snapshot(root, "finished", process.pid)
        await asyncio.sleep(args.idle_seconds)
        await snapshot(root, "idle", process.pid)
        assert not stub.errors, stub.errors
        assert len(stub.requests) == 2 * args.sessions + args.shorts, "provider retries or missing requests"
        wal_sizes = [p.stat().st_size for p in (root / "wal").rglob("*") if p.is_file() and p.stat().st_size > 1024 * 1024]
        summary = {"sessions": args.sessions, "shorts": args.shorts, "routes": args.routes,
                   "shorts_seconds": shorts_seconds, "idle_ttl_ms": args.idle_ttl_ms,
                   "requests": stub.requests, "events": peer.events, "wal_sizes": wal_sizes}
        (root / "load.json").write_text(json.dumps(summary, indent=2))
        print(json.dumps({"load": {"sessions": args.sessions, "shorts": args.shorts, "routes": args.routes,
            "shorts_seconds": shorts_seconds, "idle_ttl_ms": args.idle_ttl_ms,
            "request_paths": dict(Counter(item['path'] for item in stub.requests)),
            "request_bytes_total": sum(item['bytes'] for item in stub.requests),
            "request_bytes_min_max": [min(item['bytes'] for item in stub.requests), max(item['bytes'] for item in stub.requests)],
            "wal_count": len(wal_sizes), "wal_bytes_total": sum(wal_sizes),
            "wal_bytes_min_max": [min(wal_sizes), max(wal_sizes)], "events": peer.events}}), flush=True)
        peer.writer.close()
        await peer.writer.wait_closed()
        await asyncio.wait_for(process.wait(), 60)
        assert process.returncode == 0, (root / "server.log").read_text()
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
        server.close()
        await server.wait_closed()
        log.close()


async def until_idle(peer, channel):
    deadline = time.monotonic() + 120
    while True:
        status = await peer.call(channel, "run.status", {})
        if status == "idle" or status.get("state") == "idle":
            return
        if time.monotonic() > deadline:
            raise TimeoutError(f"session on route {channel} did not finish: {status}")
        await asyncio.sleep(.05)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sessions", type=int, default=50)
    parser.add_argument("--shorts", type=int, default=200)
    parser.add_argument("--routes", type=int, default=4397)
    parser.add_argument("--idle-ttl-ms", type=int, default=None,
                        help="actor idle TTL for the server; omitted keeps the production default (30 s)")
    parser.add_argument("--idle-seconds", type=float, default=None,
                        help="wait before the idle snapshot; defaults to the idle TTL plus 5 s")
    args = parser.parse_args()
    if args.idle_seconds is None:
        args.idle_seconds = (args.idle_ttl_ms if args.idle_ttl_ms is not None else 30_000) / 1000 + 5
    assert args.sessions > 0 and args.routes >= args.sessions
    print(f"memory-load driver: Python {sys.version.split()[0]}", flush=True)
    asyncio.run(run(args))
