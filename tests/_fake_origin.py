"""A stand-in for one origin instance in the swap tests (tests/test_selfhost_swap.py) - NOT wrangler.

It serves from the two slot files the swap placed in its persist dir, the way the worker's bindings would:
CATALOG for everything, CATALOG_CLIMATE for noaa ids (the worker's CATALOG_SHARD_FOR). It requires the
origin secret, marks every answer x-econ-origin: 1 and x-econ-instance: <its instance id>, answers metadata
like the real worker (the REQUESTED id echoed, the title read from the database), and has a slow path to
hold a request in flight.

    python tests/_fake_origin.py <port> <persist> <slots json> --instance <id> [--child <pidfile>]
        [--unmarked] [--die] [--garbage] [--gate <source or series id>,...] [--other-instance-on-metadata]
"""
import http.server
import json
import os
import sqlite3
import subprocess
import sys
import time

SLOT_DIR = os.path.join("v3", "d1", "miniflare-D1DatabaseObject")


def main():
    port, persist, slots = int(sys.argv[1]), sys.argv[2], json.loads(sys.argv[3])
    flags = sys.argv[4:]
    if "--die" in flags:
        sys.exit(3)
    if "--child" in flags:                               # a workerd-like child the stop must also end
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
        with open(flags[flags.index("--child") + 1], "w") as fh:
            fh.write(str(child.pid))
    secret = open(os.path.join(os.getcwd(), ".dev.vars")).read().split("ORIGIN_SECRET=", 1)[1].split()[0].strip("\"'")
    instance = flags[flags.index("--instance") + 1] if "--instance" in flags else ""
    gated = set(flags[flags.index("--gate") + 1].split(",")) if "--gate" in flags else set()
    files = {b: os.path.join(persist, SLOT_DIR, f) for b, f in slots.items()}

    def db(binding):
        return sqlite3.connect(f"file:{files[binding]}?mode=ro", uri=True)

    class H(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, status, obj):
            body = b"{not json" if "--garbage" in flags and status == 200 else json.dumps(obj).encode()
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            if "--unmarked" not in flags:
                self.send_header("x-econ-origin", "1")
            if instance:
                other = "--other-instance-on-metadata" in flags and self.path.endswith(".metadata.json")
                self.send_header("x-econ-instance", "someone-else" if other else instance)
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.headers.get("x-econ-origin-secret") != secret:
                return self._send(403, {"error": "secret"})
            if self.path == "/v1/sources":
                c = db("CATALOG")
                rows = [r[0] for r in c.execute("SELECT source_id FROM source") if r[0] not in gated]
                c.close()
                return self._send(200, {"sources": [{"source": r} for r in rows], "port": port})
            if self.path.startswith("/slow"):
                time.sleep(float(self.path.split("=")[1]))
                return self._send(200, {"slow": True, "port": port})
            if self.path.startswith("/v1/series/") and self.path.endswith(".metadata.json"):
                sid = self.path[len("/v1/series/"):-len(".metadata.json")]
                if sid.split(":")[0] in gated or sid in gated:
                    return self._send(451, {"error": "not_redistributable"})
                c = db("CATALOG_CLIMATE" if sid.startswith("noaa:") else "CATALOG")
                row = c.execute("SELECT title FROM series WHERE series_id=?", (sid,)).fetchone()
                c.close()
                # like metadata.ts: the id is the one ASKED for; the title is the database's
                return self._send(200, {"series_id": sid, "title": row[0], "port": port}) if row else self._send(404, {})
            return self._send(404, {"port": port})

    http.server.ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()


if __name__ == "__main__":
    main()
