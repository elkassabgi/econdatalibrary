"""The keyed soak tests of the self-hosted origin (design STEP4_SOAK_DESIGN.md, section E; order step 15).

    python -B -u tools/selfhost/soak_keyed.py --ids-file <file>

THE OWNER RUNS IT, once or twice, with the key of the SOAK ACCOUNT (an account he registered for the soak, not
his own), AT A CONSOLE: the script asks for the key without showing it and refuses when its input is not a
terminal (Python's prompt would then show what is typed). The key lives in memory only: it is sent as the
`X-API-Key` header and to TWO addresses only - the soak worker and, for the byte comparison, production. It is
never printed, never written to the result file, never put in a URL. The origin secret is used for the loopback
control only (soak_keyless.Run).

WHAT THE THREE SHAPES ARE. They are MEASURED on the worker's route handler in
api/worker/test/downloadShapes.test.ts; that test is the source of this list and of the stand-in in
tests/test_soak_keyed.py. (The first version of this script took a large plain object for a passthrough. It is
the pump - review AR-303, ledger R1545.)
  string       a stored object below 256 KiB: the answer has a content-length; citation lines first, or the
               contract header with raw=1; the ETag only for the bare, whole answer.
  passthrough  a stored-GZIP object of 256 KiB or more with no date window: the STORED bytes go out untouched,
               with `content-encoding: gzip`, their exact content-length and `x-econdl-citation-omitted`.
  pump         a PLAIN object of 256 KiB or more of the contract layout, WITH OR WITHOUT a date window: the worker
               reads and writes lines; the answer has NO content-length, carries `x-econdl-bytes-at-rest`, and
               its last line is `# econdl-complete rows=N`. This is the length-less download; the edge counts
               its bytes.
  NOT TESTED HERE: the five wide-layout sources (census, fhfa, hf_equities, treasury, wikidata). Their objects
  go out as stored, with their own header, and refuse a date window. `small` and `large_plain` must NOT be of
  one of them: the ids file is refused when they are.

THE IDS come from a file (JSON): {"small": id, "large_gzip": id, "large_plain": id, "from": "YYYY-MM-DD",
"to": "YYYY-MM-DD"}. The author makes it from the store's own index. The window from..to must hold at least
one row of the small series (a window with no row is a 502 by design). `large_plain` must be MANY times larger
than --abort-after-bytes, or the edge has sent the whole answer before the client closes. No row and no file
shows an id: they read `<the small id>` and so on. Each id is first asked for its metadata WITHOUT a key: an id
that is refused (the licence gate, or any other answer than 200) ends the run before any keyed request.

BEFORE THE RUN, a person notes the highest row id of `econ_download_log` for the soak user (design line 243).
The read-back after the run starts above that id.

WHAT SHOWS THAT THE ORIGIN ANSWERED: for every download through the soak address the number of
`GET ....csv ` lines in the active instance's log rises by exactly one (run with the probe stopped). The run
starts and ends with the loopback control and the path probe of soak_keyless.

A row is PASS, FAIL, SEEN (an observation, or a comparison that was SKIPPED - never a pass) or NOTRUN.
Exit code: 0 = no FAIL and no NOTRUN; 1 = a FAIL; 2 = no FAIL, something NOTRUN.
Result file: <root>/logs/soak_keyed_<UTC>.json.

NOT in this script: the read-back of the download log (the author's own step, by user id, after the run - the
last row says what it must find), and the edge's CPU time (read in the dashboard by a person).
"""
from __future__ import annotations

import argparse
import datetime as dt
import getpass
import hashlib
import http.client
import json
import os
import re
import sys
import time
import urllib.parse
import warnings

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import soak_keyless as K  # noqa: E402

PROD = "https://econdl-api.elkassabgi.workers.dev"
KEY_HEADER = "X-API-Key"
EDGE_WAIT_S = 30.0                       # the edge waits this long for the origin's headers
STREAM_MIN_BYTES = 256 * 1024            # csvStream.ts STREAM_MIN_BYTES
CSV_HEADER = b"series_id,obs_date,value"
GZIP_MAGIC = b"\x1f\x8b"
COMPLETE_RE = re.compile(rb"(?:^|\n)# econdl-complete rows=(\d+)\n\Z")
CSV_LINE = ".csv "                       # in the instance log: `GET /v1/series/<id>.csv <status> ...`
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
# api/worker/src/util.ts NATIVE_ONLY_SOURCES (tests/test_soak_keyed.py compares the two lists)
WIDE_SOURCES = ("census", "fhfa", "hf_equities", "treasury", "wikidata")
ALLOW_PLAIN_HTTP = False                 # the tests set it: their stand-ins are on loopback http


class KeyedRun(K.Run):
    """A soak run that can also send the soak account's key - to the soak worker and to production only."""

    def __init__(self, soak: str, prod: str, root: str, router=K.ROUTER):
        # THE KEY GOES TO TWO FIXED ADDRESSES. A mistyped address would hand the soak account's key to another
        # host, so neither can be chosen on the command line; only the tests (loopback stand-ins) give others.
        if not ALLOW_PLAIN_HTTP and (soak.rstrip("/") != K.SOAK or prod.rstrip("/") != PROD):
            raise SystemExit("refused: the keyed tests talk to the soak worker and to production only")
        super().__init__(soak, root, router)
        self.prod = prod.rstrip("/")
        self._key: str | None = None

    def set_key(self, key: str) -> None:
        key = (key or "").strip()
        if len(key) < 16 or any(c.isspace() for c in key) or not key.isascii() or not key.isprintable():
            raise SystemExit("the key is empty, shorter than 16 characters, not ASCII, or holds a space or a control "
                             "character (in a console, Ctrl+V types a character instead of pasting)")
        self._key = key
        self.hidden.append((key, "<key>"))

    def fetch(self, base: str, target: str, keyed=True, agent=K.UA, accept="identity", stop_after=None, timeout=120):
        """One streamed GET. Returns a dict: status, headers, ttfb, seconds, bytes, sha256, head, tail, stopped,
        body_error. The body is never held: it is counted and hashed. status 0 = no answer (the error's TYPE)."""
        if base not in (self.soak, self.prod):
            raise RuntimeError("refused: this request goes to an address that is neither the soak worker nor production")
        if keyed and self._key is None:
            raise RuntimeError("refused: a keyed request before the key was given")
        for value, what in ((self._key, "the key"), (self._secret, "the origin secret")):
            if value is not None and value in base + target:
                raise RuntimeError(f"refused: {what} in a URL")
        u = urllib.parse.urlsplit(base + target)
        if u.scheme != "https" and not ALLOW_PLAIN_HTTP:
            raise RuntimeError("refused: a request that is not https")
        cls = http.client.HTTPSConnection if u.scheme == "https" else http.client.HTTPConnection
        c = cls(u.netloc, timeout=timeout)
        out = {"status": 0, "headers": {}, "ttfb": 0.0, "seconds": 0.0, "bytes": 0, "sha256": "", "head": b"", "tail": b"",
               "stopped": False, "body_error": ""}
        sha = hashlib.sha256()
        t0 = time.monotonic()
        try:
            c.putrequest("GET", (u.path or "/") + ("?" + u.query if u.query else ""), skip_accept_encoding=True)
            if agent is not None:
                c.putheader("User-Agent", agent)
            c.putheader("Accept-Encoding", accept)
            if keyed:
                c.putheader(KEY_HEADER, self._key)
            c.endheaders()
            r = c.getresponse()
            out["ttfb"] = time.monotonic() - t0
            out["status"] = r.status
            out["headers"] = {k.lower(): v for k, v in r.getheaders()}
            try:
                while True:
                    chunk = r.read(65536)
                    if not chunk:
                        break
                    out["bytes"] += len(chunk)
                    sha.update(chunk)
                    if len(out["head"]) < 64:
                        out["head"] += chunk[:64 - len(out["head"])]
                    out["tail"] = (out["tail"] + chunk)[-256:]
                    if stop_after is not None and out["bytes"] >= stop_after:
                        out["stopped"] = True
                        break
            except (OSError, http.client.HTTPException) as e:
                out["body_error"] = type(e).__name__
        except (OSError, http.client.HTTPException) as e:
            out["body_error"] = "client_" + type(e).__name__
        finally:
            c.close()
        out["seconds"] = time.monotonic() - t0
        out["sha256"] = sha.hexdigest()
        return out


def csv_target(series_id: str, **params) -> str:
    q = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    return K.with_nonce(f"/v1/series/{urllib.parse.quote(series_id, safe='')}.csv" + ("?" + q if q else ""))


def shown(a: dict) -> str:
    h = a["headers"]
    return (f"{a['status']} bytes={a['bytes']} content-length={h.get('content-length')!r} encoding={h.get('content-encoding')!r} "
            f"etag={'yes' if h.get('etag') else 'no'} citation-omitted={h.get('x-econdl-citation-omitted')!r} "
            f"at-rest={h.get('x-econdl-bytes-at-rest')!r} first-byte={a['ttfb']:.1f}s whole={a['seconds']:.1f}s "
            f"body-error={a['body_error']!r} sha256={a['sha256'][:16]}")


def length_ok(a: dict) -> bool:
    cl = a["headers"].get("content-length")
    return cl is not None and cl.isdigit() and int(cl) == a["bytes"]


def through_soak(run: KeyedRun, log_path: str, target: str, **kw):
    """A keyed download through the soak address, with the count of `.csv` lines in the instance log over it.
    Returns (the answer, new log lines)."""
    c0 = K.log_count(log_path, CSV_LINE)
    a = run.fetch(run.soak, target, **kw)
    return a, K.settle(log_path, CSV_LINE, c0 + 1) - c0


def read_ids(path: str) -> dict:
    d = json.load(open(path, encoding="utf-8-sig"))
    want = {"small", "large_gzip", "large_plain", "from", "to"}
    if not isinstance(d, dict) or set(d) != want or not all(isinstance(v, str) and v for v in d.values()):
        raise SystemExit("the ids file must be a JSON object with exactly these text fields: " + ", ".join(sorted(want)))
    if not DATE_RE.fullmatch(d["from"]) or not DATE_RE.fullmatch(d["to"]) or d["from"] > d["to"]:
        raise SystemExit("from and to must be YYYY-MM-DD, from not after to")
    if len({d["small"], d["large_gzip"], d["large_plain"]}) != 3:
        raise SystemExit("the three ids must be three different series")
    for name in ("small", "large_plain"):          # the worker's sourceOf(): the text before the first colon
        if d[name].split(":", 1)[0] in WIDE_SOURCES:
            raise SystemExit(f"the {name} id is of a wide-layout source: its answers have another shape (choose another series)")
    return d


ID_NAMES = ("small", "large_gzip", "large_plain")


def hide_ids(run: KeyedRun, ids: dict) -> None:
    """No row and no file shows an id (review AR-303 C5: only a 451 hid one before, so a refused id of another
    kind was written down). The longest spelling first, so that one id inside another is not cut."""
    spellings = []
    for name in ID_NAMES:
        for form in {ids[name], urllib.parse.quote(ids[name], safe="")}:
            spellings.append((form, f"<the {name} id>"))
    run.hidden.extend(sorted(spellings, key=lambda p: -len(p[0])))


def precheck(run: KeyedRun, ids: dict) -> bool:
    """Each id, WITHOUT a key: its metadata through the soak address. Any answer but a 200 that names the id
    stops the run."""
    ok = True
    for name in ID_NAMES:
        sid = ids[name]
        target = K.with_nonce(f"/v1/series/{urllib.parse.quote(sid, safe='')}.metadata.json")
        st, _h, body, _s = run.edge(target)
        d = K.js(body) if st == 200 else None
        good = st == 200 and isinstance(d, dict) and d.get("series_id") == sid
        run.row(f"the {name} id is served (no key)", f"GET <soak>{target}", f"{st} error={K.err_of(body)!r} names its id={good}",
                "PASS" if good else "FAIL",
                "expected 200 and the id in the answer. 451 = the licence gate refuses this id: choose another one")
        ok = ok and good
    return ok


def ask_key() -> str:
    """The key, typed at a console. getpass falls back to a prompt that SHOWS the input when it cannot switch
    the echo off (stdin is a pipe or a file, or the console is not a real one): both cases are refused."""
    if not sys.stdin.isatty():
        raise SystemExit("refused: the key is typed at a console only (the input is not a terminal, so the prompt could show it)")
    with warnings.catch_warnings():
        warnings.simplefilter("error", getpass.GetPassWarning)
        try:
            return getpass.getpass("The soak account's key (it is not shown): ")
        except getpass.GetPassWarning:
            raise SystemExit("refused: this console cannot hide what is typed") from None


def run_keyed(run: KeyedRun, ids: dict, abort_after: int) -> None:
    started = dt.datetime.now(dt.timezone.utc)
    hide_ids(run, ids)
    ok, _src, _at, log_path = K.control(run, "before:")
    if not ok:
        run.row("the keyed checks", "-", "not run: the loopback control failed", "NOTRUN")
        return
    if not K.edge_status(run, True) or not K.path_probe(run, log_path, "before:"):
        run.row("the keyed checks", "-", "not run: the soak worker or the path to the origin is not as expected", "NOTRUN")
        return
    if not precheck(run, ids):
        run.row("the keyed checks", "-", "not run: an id is not served through the soak address", "NOTRUN")
        return
    one = "exactly one new `.csv` line in the instance log"

    # ---- the string path, bare: bytes against production when both name the same stored object
    t = csv_target(ids["small"], raw="1")
    a, lines = through_soak(run, log_path, t)
    ok_a = (a["status"] == 200 and length_ok(a) and bool(a["headers"].get("etag")) and a["head"].startswith(CSV_HEADER)
            and a["headers"].get("x-econdl-citation-omitted") is None and not a["body_error"] and a["ttfb"] < EDGE_WAIT_S and lines == 1)
    run.row("small CSV, bare (string path)", f"GET <soak>{t} +key", shown(a) + f" log lines={lines}", "PASS" if ok_a else "FAIL",
            f"expected 200, content-length equal to the bytes read, an ETag, the contract header first, NO citation-omitted mark "
            f"(that mark means the object is not a small one), first byte under 30 s, {one}")
    p = run.fetch(run.prod, csv_target(ids["small"], raw="1"))
    same_object = ok_a and p["status"] == 200 and bool(p["headers"].get("etag")) and p["headers"].get("etag") == a["headers"].get("etag")
    if same_object:
        eq = p["sha256"] == a["sha256"] and p["bytes"] == a["bytes"] and not p["body_error"]
        run.row("small CSV, bare: the same bytes as production", "GET <production> the same series +key", shown(p),
                "PASS" if eq else "FAIL", "the two ETags are equal, so the bodies must be equal byte for byte")
    else:
        run.row("small CSV, bare: the same bytes as production", "GET <production> the same series +key", shown(p), "SEEN",
                "SKIPPED, not passed: the two answers do not name the same stored object (ETags differ or one is absent)")

    # ---- the string path with the citation header
    t = csv_target(ids["small"])
    b, lines = through_soak(run, log_path, t)
    ok_b = b["status"] == 200 and length_ok(b) and b["head"].startswith(b"#") and not b["body_error"] and lines == 1
    run.row("small CSV with its citation header", f"GET <soak>{t} +key", shown(b) + f" log lines={lines}", "PASS" if ok_b else "FAIL",
            f"expected 200, content-length equal to the bytes read, a `#` line first, {one}")

    # ---- a date window on the small object
    t = csv_target(ids["small"], raw="1", **{"from": ids["from"], "to": ids["to"]})
    f, lines = through_soak(run, log_path, t)
    ok_f = (f["status"] == 200 and length_ok(f) and not f["headers"].get("etag") and f["head"].startswith(CSV_HEADER)
            and 0 < f["bytes"] <= a["bytes"] and not f["body_error"] and lines == 1)
    run.row("small CSV with a date window", f"GET <soak>{t} +key", shown(f) + f" log lines={lines}", "PASS" if ok_f else "FAIL",
            f"expected 200, content-length equal to the bytes read, NO ETag (it describes the whole object), not more bytes than the whole series, {one}")
    if same_object:
        pf = run.fetch(run.prod, csv_target(ids["small"], raw="1", **{"from": ids["from"], "to": ids["to"]}))
        eq = ok_f and pf["status"] == 200 and pf["sha256"] == f["sha256"] and pf["bytes"] == f["bytes"]
        run.row("date window: the same bytes as production", "GET <production> the same window +key", shown(pf),
                "PASS" if eq else "FAIL", "both sides read the same stored object (equal ETags above), so the window must be equal")
    else:
        run.row("date window: the same bytes as production", "-", "not asked", "SEEN",
                "SKIPPED, not passed: the whole-series answers did not name the same stored object")

    # ---- a client that is a script
    t = csv_target(ids["small"], raw="1")
    s, lines = through_soak(run, log_path, t, agent="python-requests/2.32.3")
    ok_s = s["status"] == 200 and length_ok(s) and s["sha256"] == a["sha256"] and lines == 1
    run.row("small CSV, a script's User-Agent", f"GET <soak>{t} +key, User-Agent python-requests", shown(s) + f" log lines={lines}",
            "PASS" if ok_s else "FAIL", f"expected 200 and the same bytes as the first answer, {one} (a keyed answer is never cached)")

    # ---- the passthrough of a stored-gzip object
    t = csv_target(ids["large_gzip"])
    g, lines = through_soak(run, log_path, t, accept="gzip", timeout=900)
    base_g = (g["status"] == 200 and g["headers"].get("content-encoding") == "gzip" and g["head"].startswith(GZIP_MAGIC)
              and g["headers"].get("x-econdl-citation-omitted") == "large-object" and g["bytes"] >= STREAM_MIN_BYTES
              and not g["body_error"] and g["ttfb"] < EDGE_WAIT_S and lines == 1)
    has_len = g["headers"].get("content-length") is not None
    run.row("large stored-gzip object (passthrough)", f"GET <soak>{t} +key, Accept-Encoding gzip", shown(g) + f" log lines={lines}",
            "PASS" if base_g and (length_ok(g) or not has_len) else "FAIL",
            f"expected 200, content-encoding gzip, gzip bytes first, the citation-omitted mark, 256 KiB or more, first byte under 30 s, {one}; "
            "a content-length, when there is one, equal to the bytes read")
    run.row("large stored-gzip object: the length after the hop", "the content-length header of that answer",
            f"content-length={g['headers'].get('content-length')!r} bytes read={g['bytes']}",
            "PASS" if base_g and length_ok(g) else "SEEN" if base_g and not has_len else "FAIL",
            "PASS = the exact length reached the client. SEEN = the hop dropped it: the edge then counts the bytes itself, "
            "and this answer is one of the three whose edge CPU time is read in the dashboard")

    # ---- a large PLAIN object is the pump, with or without a window (api/worker/test/downloadShapes.test.ts)
    pump = (f"expected 200, NO content-length, the at-rest size header, no citation-omitted mark, no content-encoding, the last "
            f"line `# econdl-complete rows=N` with N above 0, first byte under 30 s, {one}")

    def pump_ok(ans, first, new_lines):
        done = COMPLETE_RE.search(ans["tail"])
        h = ans["headers"]
        return (ans["status"] == 200 and h.get("content-length") is None and (h.get("x-econdl-bytes-at-rest") or "").isdigit()
                and h.get("x-econdl-citation-omitted") is None and not h.get("content-encoding") and ans["head"].startswith(first)
                and ans["bytes"] >= STREAM_MIN_BYTES and done is not None and int(done.group(1)) > 0 and not ans["body_error"]
                and ans["ttfb"] < EDGE_WAIT_S and new_lines == 1), (done.group(1).decode() if done else None)

    # with a date window, bare: the filter runs on every row (one of the answers whose edge CPU time is read)
    t = csv_target(ids["large_plain"], raw="1", **{"from": "1000-01-01", "to": "2999-12-31"})
    wf, lines = through_soak(run, log_path, t, timeout=1800)
    ok_wf, rows_wf = pump_ok(wf, CSV_HEADER, lines)
    run.row("large plain object with a date window (the pump, bare)", f"GET <soak>{t} +key",
            shown(wf) + f" last line rows={rows_wf} log lines={lines}", "PASS" if ok_wf and not wf["headers"].get("etag") else "FAIL",
            pump + "; the contract header first; NO ETag (it describes the whole object)")

    # ---- the length-less pair: the whole object read to the end, then once more closed by the client
    pair_started = dt.datetime.now(dt.timezone.utc)
    t = csv_target(ids["large_plain"])
    w, lines = through_soak(run, log_path, t, timeout=1800)
    ok_w, rows_w = pump_ok(w, b"#", lines)
    run.row("large plain object, whole: a length-less download read to the end (the pump)", f"GET <soak>{t} +key",
            shown(w) + f" last line rows={rows_w} log lines={lines}", "PASS" if ok_w else "FAIL", pump + "; citation lines first")
    t = csv_target(ids["large_plain"])
    x, lines = through_soak(run, log_path, t, stop_after=abort_after, timeout=1800)
    ok_x = (x["status"] == 200 and x["headers"].get("content-length") is None and x["stopped"]
            and abort_after <= x["bytes"] < w["bytes"] and COMPLETE_RE.search(x["tail"]) is None and lines == 1)
    run.row("length-less download, closed by the client", f"GET <soak>{t} +key, closed after {abort_after} bytes",
            shown(x) + f" stopped={x['stopped']} log lines={lines}", "PASS" if ok_x else "FAIL",
            f"expected 200, NO content-length, the client closed it after at least {abort_after} bytes and before the end "
            f"(no completeness line), {one}")

    K.path_probe(run, log_path, "after:")
    K.control(run, "after:")
    ended = dt.datetime.now(dt.timezone.utc)
    run.row("for the read-back of the download log (the author's step, 60 s or more after this run)",
            "econ_download_log rows of the soak user",
            f"the run: {started.strftime('%Y-%m-%dT%H:%M:%SZ')} to {ended.strftime('%Y-%m-%dT%H:%M:%SZ')}; the length-less pair began "
            f"{pair_started.strftime('%Y-%m-%dT%H:%M:%SZ')}: completed bytes={w['bytes']}, closed after bytes>={abort_after} and <{w['bytes']}",
            "SEEN",
            "the read-back goes by ROW-ID ORDER, not by time (the two clocks can differ, and the windowed download before the "
            "pair is length-less too, so its row is written late and its byte count can look like a closed one): above the row "
            "id noted before the run, find the row of the soak user with exactly the completed byte count; the closed download "
            "is the NEXT row of that user above it, with at least the closed count and less than the completed count; no other "
            "row of that user stands above that one. It selects id, bytes and the time only.")


def main(argv=None, key: str | None = None, soak: str = K.SOAK, prod: str = PROD, router=None) -> int:
    """`key`, `soak`, `prod` and `router` are for the tests. From the command line the addresses are fixed and the
    key is asked for without echo. (A test that left `router` out reached the standing origin's real router -
    ledger R1544.)"""
    ap = argparse.ArgumentParser(description="The keyed soak tests of the self-hosted origin.")
    ap.add_argument("--ids-file", required=True, help="JSON: small, large_gzip, large_plain, from, to")
    ap.add_argument("--root", default="F:/econ_origin", help="the standing origin's folder")
    ap.add_argument("--abort-after-bytes", type=int, default=1_000_000, help="the closed download stops after this many bytes")
    a = ap.parse_args(argv)
    if a.abort_after_bytes < 1:
        raise SystemExit("--abort-after-bytes must be 1 or more")
    ids = read_ids(a.ids_file)
    run = KeyedRun(soak, prod, a.root, router=router or K.ROUTER)
    run.set_key(key if key is not None else ask_key())
    run_keyed(run, ids, a.abort_after_bytes)
    return K.finish(run, "keyed")


if __name__ == "__main__":
    raise SystemExit(main())
