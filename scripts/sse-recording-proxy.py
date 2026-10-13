#!/usr/bin/env python3
"""Record what an HTTP client really sends to a model provider, and what streams back, by timestamp.

WHAT IT PROVES. When broca and another client (OpenCode, a curl script) disagree about the same
prompt -- one completes, the other goes idle -- the difference is either in the request bytes or
in how the response stream is read. This proxy sits between the other client and the provider and
records both sides without changing either: the request method, path, headers (credential-bearing
header VALUES are never written, only their names) and exact body bytes, then every response chunk
with a monotonic arrival time and size, and the raw response bytes. Point the client's base URL at
it; the proxy forwards to the real upstream over TLS and streams each chunk back as it arrives.
Feed the recording to broca's own stream reader with
`cargo run -p broca-module-serve --example sse-idle-capture -- --wal <wal> --analyze <dir> --out <dir2>`
so both clients' streams are classified and gap-measured by the same code.

RUN
    scripts/sse-recording-proxy.py --upstream https://api.x.ai --port 18555 --out /tmp/rec
    # then e.g. OpenCode, with the provider's baseURL pointed at the proxy:
    OPENCODE_CONFIG_CONTENT='{"provider":{"xai":{"options":{"baseURL":"http://127.0.0.1:18555/v1"}}}}' \
        opencode run -m xai/grok-4.7 "..."

Each proxied request gets a numbered directory under --out: request.json (method, path, redacted
headers, body length and SHA-256), request-body.bin, events.jsonl (headers / chunk / end lines,
times in seconds since the request arrived) and response.raw. Upstream is asked for an
uncompressed body (Accept-Encoding: identity) so the recorded bytes are the SSE text itself; the
client's own Accept-Encoding is still recorded as it sent it. No idle timeout is applied; a hard
cap (default 30 minutes) bounds one request.
"""
from __future__ import annotations

import argparse
import hashlib
import http.client
import http.server
import json
import pathlib
import ssl
import sys
import threading
import time
import urllib.parse

# Header names whose values carry credentials. Their values are never written anywhere.
SECRET_HEADERS = {"authorization", "x-api-key", "api-key", "cookie", "proxy-authorization", "x-goog-api-key"}
# Hop-by-hop or re-framed headers the proxy must not copy verbatim.
HOP_HEADERS = {"connection", "keep-alive", "transfer-encoding", "te", "trailer", "upgrade",
               "proxy-connection", "content-length", "host", "accept-encoding"}


def redacted_headers(items) -> list[list[str]]:
    return [[k, "<redacted>" if k.lower() in SECRET_HEADERS else v] for k, v in items]


class Recorder(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    upstream: urllib.parse.SplitResult
    out: pathlib.Path
    hard_cap: float
    counter = 0
    lock = threading.Lock()

    def log_message(self, fmt, *args):  # keep stderr quiet; paths only, never headers
        sys.stderr.write("proxy: " + (fmt % args) + "\n")

    def do_POST(self):
        self._proxy()

    def do_GET(self):
        self._proxy()

    def _proxy(self):
        t0 = time.monotonic()
        with Recorder.lock:
            Recorder.counter += 1
            n = Recorder.counter
        rec = self.out / f"{n:03d}"
        rec.mkdir(parents=True, exist_ok=True)
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""
        (rec / "request-body.bin").write_bytes(body)
        (rec / "request.json").write_text(json.dumps({
            "method": self.command,
            "path": self.path,
            "headers": redacted_headers(self.headers.items()),
            "body_len": len(body),
            "body_sha256": hashlib.sha256(body).hexdigest(),
            "wall_ms": int(time.time() * 1000),
        }, indent=1))

        events = (rec / "events.jsonl").open("w")
        raw = (rec / "response.raw").open("wb")

        def event(**fields):
            fields["t"] = time.monotonic() - t0
            events.write(json.dumps(fields) + "\n")
            events.flush()

        up = self.upstream
        conn = http.client.HTTPSConnection(up.hostname, up.port or 443, context=ssl.create_default_context(),
                                           timeout=self.hard_cap)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP_HEADERS}
        headers["Host"] = up.netloc
        headers["Accept-Encoding"] = "identity"
        headers["Content-Length"] = str(len(body))
        path = up.path.rstrip("/") + self.path
        reason = "eof"
        try:
            conn.request(self.command, path, body=body, headers=headers)
            resp = conn.getresponse()
            event(ev="headers", status=resp.status,
                  headers=redacted_headers(resp.getheaders()))
            self.send_response(resp.status)
            for k, v in resp.getheaders():
                if k.lower() not in HOP_HEADERS and k.lower() != "content-encoding":
                    self.send_header(k, v)
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            while True:
                if time.monotonic() - t0 > self.hard_cap:
                    reason = "hard_cap"
                    break
                chunk = resp.read1(65536)
                if not chunk:
                    break
                raw.write(chunk)
                raw.flush()
                event(ev="chunk", len=len(chunk))
                try:
                    self.wfile.write(b"%x\r\n%s\r\n" % (len(chunk), chunk))
                    self.wfile.flush()
                except OSError:
                    reason = "client_gone"
                    break
            if reason != "client_gone":
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
        except Exception as error:  # noqa: BLE001 -- record any upstream failure, then stop
            reason = "error"
            event(ev="error", error=type(error).__name__ + ": " + str(error)[:300])
        finally:
            event(ev="end", reason=reason)
            events.close()
            raw.close()
            conn.close()
            self.close_connection = True


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--upstream", required=True, help="e.g. https://api.x.ai")
    ap.add_argument("--port", type=int, default=18555)
    ap.add_argument("--out", required=True, type=pathlib.Path)
    ap.add_argument("--hard-cap-secs", type=float, default=1800.0)
    args = ap.parse_args()
    upstream = urllib.parse.urlsplit(args.upstream)
    if upstream.scheme != "https":
        ap.error("--upstream must be an https URL")
    args.out.mkdir(parents=True, exist_ok=True)
    Recorder.upstream = upstream
    Recorder.out = args.out
    Recorder.hard_cap = args.hard_cap_secs
    server = http.server.ThreadingHTTPServer(("127.0.0.1", args.port), Recorder)
    print(f"recording proxy on http://127.0.0.1:{args.port} -> {args.upstream}, out {args.out}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
