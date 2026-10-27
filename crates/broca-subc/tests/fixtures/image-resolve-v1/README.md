# image.resolve v1 vectors

Byte-for-byte `image.resolve` examples for BROCA, captured at cerebellum's
production `handle_frame_for_test` entry point (which delegates to
`handle_frame`). They are not replies assembled from an expected JSON object.
The success case resolves a handle minted the way a capture reply mints it,
under the scope the query names; the capture is a fixed sixteen-byte fixture
image, so no Screen Recording permission, installed application, window server
or real screenshot is required.

A successful resolve is a binary frame, so its body is pinned in
`success.bin`, byte for byte; `success.json` names that file and its SHA-256
(`reply.body_file`, `reply.body_sha256`) beside the frame's own fields, because
the body is not text and cannot live in a JSON file. Every other case is JSON
in its own file.

Each JSON file contains:

- `contract`: always `image-resolve-v1`;
- `case`: which exchange this is;
- `state`: what the fixture arranged before the request was sent;
- `principal`: always `reserved:broca`, the only admitted caller;
- `handle`: the fixed handle the corpus records, standing in for the random
  one the fixture minted;
- `request`: the management-route request frame;
- `reply`: the request's terminal `Response` frame — `body_utf8` for a JSON
  body, or `body_file`/`body_sha256`/`binary`/`flags` for the binary one.

A handle is 16 random bytes by design, so the corpus cannot pin the one a run
minted. The fixture's handle is replaced, everywhere it appears, by a fixed
handle of the same shape before the bytes are pinned; `body_utf8` and
`wire_hex` are computed from the replaced bodies, so each is still an exact
frame for that handle.

`body_utf8` is the exact body byte string, not reserialized parsed JSON.
`wire_hex` is the entire frame (`header.encode()` followed by body), including
the four-byte little-endian body-length prefix. Request ids, scope refs, and the
resolved bytes are synthetic fixture data. The bodies are scanned for
machine-local paths.

`SHA256SUMS` pins all JSON files and this README. Verify the pins before
consuming a copied corpus. JSON formatting, newlines, and key order are part
of the pinned file bytes, while `body_utf8` and `wire_hex` pin the actual
protocol.

## Check or regenerate

Run on macOS or Linux, with a writable temporary directory. Ordinary runs only
compare captured frames and checksums; they do not rewrite the corpus:

```sh
cargo test -p cerebellum --test it image_resolve_v1
```

To deliberately update after a contract change, run that command once with
`CEREBELLUM_WRITE_IMAGE_RESOLVE_VECTORS=1` in its environment. The capturing
test rewrites its case files and `SHA256SUMS`. Then run the command again
without that variable and review the body-byte diff.

The public behavior and authorization rules are in
[`docs/contracts/image-resolve-v1.md`](../../../../../docs/contracts/image-resolve-v1.md).
