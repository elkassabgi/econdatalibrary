"""HTTP sidecar that serves the blob store to the local origin (docs/ECON_SELF_HOSTING_PLAN.md, change 3).

workerd cannot open files or SQLite, so the worker's SERIES_BUCKET adapter (api/worker/src/localBucket.ts)
reads objects from this service over 127.0.0.1. Protocol, per object key (URL-encoded in the path):

    GET  /o/<key>                  200, the stored bytes; 404 when absent
    GET  /o/<key>  Range: bytes=a-b     206, the slice (x-blob-size stays the FULL size, as R2 does)
    GET  /o/<key>  If-Match: <etag>     412 with no body when the stored etag differs
                                        (the adapter turns it into R2's bodyless object)
    HEAD /o/<key>                  200/404 with the metadata headers only

Metadata travels in x-blob-* headers. The stored encoding is sent as x-blob-content-encoding and NEVER as
Content-Encoding: workerd's fetch would inflate a gzip body and the worker would then serve inflated bytes
labelled gzip (review R1161). Binds 127.0.0.1 only; refuses when the blob store is missing.

Run:  python tools/selfhost/blob_sidecar.py --root <blob store root> --port 8798
"""
from __future__ import annotations

import argparse
import base64
import http.server
import json
import os
import re
import sys
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from blobstore import BlobStore  # noqa: E402

RANGE_RE = re.compile(r"^bytes=(\d+)-(\d*)$")
CHUNK = 1 << 20


def make_handler(store: BlobStore):
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):          # quiet; the origin logs requests
            pass

        def _meta(self):
            if not self.path.startswith("/o/"):
                self.send_error(404)
                return None
            key = urllib.parse.unquote(self.path[3:].split("?", 1)[0])
            meta = store.head(key)
            if meta is None:
                self.send_response(404)
                self.send_header("content-length", "0")
                self.end_headers()
                return None
            return meta

        def _headers(self, meta, status, length):
            self.send_response(status)
            self.send_header("content-type", "application/octet-stream")
            self.send_header("content-length", str(length))
            self.send_header("x-blob-size", str(meta["size"]))
            self.send_header("x-blob-etag", meta["etag"])
            if meta["content_encoding"]:
                self.send_header("x-blob-content-encoding", meta["content_encoding"])
            if meta["content_type"]:
                self.send_header("x-blob-content-type", meta["content_type"])
            cm = json.dumps(meta["custom_metadata"] or {}, sort_keys=True).encode()
            self.send_header("x-blob-custom-metadata", base64.b64encode(cm).decode())
            self.end_headers()

        def do_HEAD(self):
            meta = self._meta()
            if meta is not None:
                self._headers(meta, 200, 0)

        def do_GET(self):
            meta = self._meta()
            if meta is None:
                return
            want = self.headers.get("If-Match")
            if want is not None and want.strip().strip('"') != meta["etag"]:
                self._headers(meta, 412, 0)
                return
            size = meta["size"]
            start, end, status = 0, size - 1, 200
            rng = self.headers.get("Range")
            if rng:
                m = RANGE_RE.match(rng.strip())
                if not m:
                    self.send_response(416)
                    self.send_header("content-length", "0")
                    self.end_headers()
                    return
                start = int(m.group(1))
                end = min(int(m.group(2)) if m.group(2) else size - 1, size - 1)
                if start > end:
                    self.send_response(416)
                    self.send_header("content-length", "0")
                    self.end_headers()
                    return
                status = 206
            length = max(0, end - start + 1) if size else 0
            # OPEN THE FILE BEFORE ANY STATUS LINE (R1168 (d)): a 200 with a content-length followed by a
            # missing file reached the client as a 200 and then a broken read. A missing file is a 500.
            try:
                fh = open(meta["path"], "rb")
            except OSError:
                self.send_response(500)
                self.send_header("content-length", "0")
                self.end_headers()
                return
            # A file whose size is not the indexed size (truncated, damaged) is a 500 too, never a 200 that
            # promises bytes it cannot send (R1171 minor 6).
            if os.fstat(fh.fileno()).st_size != meta["size"]:
                fh.close()
                self.send_response(500)
                self.send_header("content-length", "0")
                self.end_headers()
                return
            with fh:
                self._headers(meta, status, length)
                fh.seek(start)
                left = length
                while left > 0:
                    buf = fh.read(min(CHUNK, left))
                    if not buf:
                        break
                    self.wfile.write(buf)
                    left -= len(buf)

    return Handler


def serve(root: str, port: int):
    store = BlobStore(root)
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", port), make_handler(store))
    return srv


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--port", type=int, default=8798)
    a = ap.parse_args()
    srv = serve(a.root, a.port)
    print(f"blob sidecar on 127.0.0.1:{a.port} over {a.root}", flush=True)
    srv.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
