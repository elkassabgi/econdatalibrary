"""The keyless soak tests of the self-hosted origin (design STEP4_SOAK_DESIGN.md, section E).

    python -B -u tools/selfhost/soak_keyless.py --mode lock   --origin-url-file <file>
    python -B -u tools/selfhost/soak_keyless.py --mode phase0
    python -B -u tools/selfhost/soak_keyless.py --mode phase1
    python -B -u tools/selfhost/soak_keyless.py --mode suite  [--gated-id <id>]
    python -B -u tools/selfhost/soak_keyless.py --mode probe

Every request to a PUBLIC address goes out as a client's would. None carries a real key, and none can carry the
origin secret (the method that sends them refuses it): two requests carry MADE-UP credentials on purpose (a
random X-API-Key, a service token of zeros) to see them refused. The origin secret is read from the standing
origin's `.dev.vars` for the LOOPBACK requests only (the control, and the answers the floors are compared with);
it is never printed and never written to the result file. The origin's public host name is read from a file
(`lock` mode) and is never printed either: rows say `<origin>`.

WHAT SHOWS THAT THE ORIGIN ANSWERED (ledger R1503: the generation's `instance.log` holds the method, the PATH
without its query, and the status). Two instruments, and not every row has one:
  (a) the path probe - a nonce in a PATH, so exactly one log line names it (start and end of the suite, and
      every probe run);
  (b) the line count - for a fixed path, the number of `GET <path> ` lines rises by the number of requests sent
      (the cache row, the User-Agent rows; and, with ZERO expected, the refusals).
  The content-floor rows of the suite use neither: their proof is the content itself, compared with the
  origin's own loopback answer where the row says so, between the two path probes. They show the hop, not that
  the origin's own answer is right. A count of ZERO proves something only beside a positive control: every
  loopback control checks that its own request added exactly one line to the log that is then counted.
Both instruments need a quiet origin: run a suite with the probe stopped.

A row is PASS, FAIL, SEEN (an observation that proves nothing by itself) or NOTRUN (the check could not be
made; it is never a pass). Exit code: 0 = no FAIL and no NOTRUN; 1 = a FAIL; 2 = no FAIL, something NOTRUN.
Result file: <root>/logs/soak_<mode>_<UTC>.json. `probe` adds ONE line to <root>/logs/soak_probe.log instead.

Modes, in the order of the design's section G:
  lock    step 9: the public origin name WITHOUT a token. PASS needs a refusal that carries a mark of Access
          (a redirect to *.cloudflareaccess.com, or an answer that names Access) and nothing reaching the
          instance. A bare 401/403 is SEEN: it is not told apart from another Cloudflare refusal, and the mode
          then ends NOTRUN (read the Access log by hand). The mode never shows that the name leads to THIS
          origin: phase 2 does.
  phase0  step 10: the soak worker is deployed and holds no origin value: forwarded routes answer 503.
  phase1  step 12: ORIGIN_URL and ORIGIN_SECRET are set, the two Access values are not: 502 origin_bad_answer,
          and the request does not reach the instance. (A tunnel that is down gives the same 502.)
  suite   step 14: everything is set: every forwarded route with a content floor, the cache, the refusals,
          client User-Agents.
  probe   every 10 minutes: /v1/edge-status, /v1/sources with a nonce, and the path probe.
NOT in this script (they need a person at the workstation and get their own rows by hand): the workstation
down, a swap under a download, `swap.py --stop`, an instance without the secret.
"""
from __future__ import annotations

import argparse
import datetime as dt
import http.client
import json
import os
import re
import secrets
import sys
import time
import urllib.parse

SOAK = "https://econdl-api-soak.elkassabgi.workers.dev"
ROUTER = ("127.0.0.1", 8787)
SECRET_HEADER = "x-econ-origin-secret"
MARK_HEADER = "x-econ-origin"
UA = "econdatalibrary-selfhost-soak/1.0"
SRC = "/v1/sources"
ALL_LINES = "[wrangler:inf]"
ORIGIN_RE = re.compile(r"https://[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.econdatalibrary\.com")
CLIENT_AGENTS = (("no User-Agent", None), ("curl", "curl/8.5.0"), ("python-requests", "python-requests/2.32.3"),
                 ("the MCP relay", "elkassabgidata-mcp"),
                 ("a crawler", "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"))
SETTLE_SCALE = 1.0          # the tests set a small value: their stand-ins answer in milliseconds


class Run:
    """One run: its rows, the values that must never be shown, and where the origin's files are."""

    def __init__(self, soak: str, root: str, router=ROUTER):
        self.soak, self.root, self.router = soak.rstrip("/"), root.replace("\\", "/").rstrip("/"), router
        self.rows: list[dict] = []
        self.hidden: list[tuple[str, str]] = []           # (value, what to show instead)
        self._secret: str | None = None

    # ------------------------------------------------------------ what may be shown
    def clean(self, text) -> str:
        s = str(text)
        for value, shown in self.hidden:
            s = s.replace(value, shown)
        return s

    def row(self, check, sent, got, verdict, note=""):
        r = {"check": self.clean(check), "sent": self.clean(sent), "got": self.clean(got)[:500], "verdict": verdict,
             "note": self.clean(note)}
        self.rows.append(r)
        print(f"{verdict:6s} {r['check']}: {r['sent'][:100]} -> {r['got'][:200]}" + (f"   [{r['note']}]" if note else ""), flush=True)

    # ------------------------------------------------------------ the loopback side
    def secret(self) -> str:
        if self._secret is None:
            path = self.root + "/code/api/worker/.dev.vars"
            found = [ln.split("=", 1)[1].strip() for ln in open(path, encoding="ascii").read().splitlines()
                     if ln.startswith("ORIGIN_SECRET=")]
            if len(found) != 1 or not re.fullmatch(r"[0-9a-f]{64}", found[0]):
                raise SystemExit("the origin's .dev.vars does not hold exactly one well-formed ORIGIN_SECRET line")
            self._secret = found[0]
            self.hidden.append((self._secret, "<secret>"))
        return self._secret

    def loop(self, target, with_secret=False):
        """One GET to the router on loopback. A failure is an answer with status 0 and the error's TYPE."""
        value = self.secret() if with_secret else None
        c = http.client.HTTPConnection(*self.router, timeout=120)
        try:
            c.putrequest("GET", target, skip_accept_encoding=True)
            if value is not None:
                c.putheader(SECRET_HEADER, value)
            c.endheaders()
            r = c.getresponse()
            body = r.read()
            return r.status, {k.lower(): v for k, v in r.getheaders()}, body
        except (OSError, http.client.HTTPException) as e:
            return 0, {}, json.dumps({"error": "client_" + type(e).__name__}).encode()
        finally:
            c.close()

    def log_path_of(self, active) -> str | None:
        """The active instance's log, from the router's status word and the swap's own record."""
        try:
            rec = (json.load(open(self.root + "/work/instances.json", encoding="utf-8")) or {}).get(active or "", {})
        except (OSError, ValueError):
            return None
        return rec["gen"].replace("\\", "/") + "/instance.log" if isinstance(rec, dict) and rec.get("gen") else None

    # ------------------------------------------------------------ the public side
    def out(self, base: str, target: str, agent=UA, headers=(), timeout=90):
        """One GET to a public address. The origin secret can never be in it: this method has no way to add it,
        and it refuses when any part of the request holds it. A failure is status 0 and the error's TYPE."""
        u = urllib.parse.urlsplit(base + target)
        if self._secret is not None and any(self._secret in part for part in (base, target, *(v for _k, v in headers))):
            raise RuntimeError("refused: the origin secret in a request to a public address")
        if any(k.lower() == SECRET_HEADER for k, _v in headers):
            raise RuntimeError("refused: the origin's secret header in a request to a public address")
        cls = http.client.HTTPSConnection if u.scheme == "https" else http.client.HTTPConnection
        c = cls(u.netloc, timeout=timeout)
        t0 = time.monotonic()
        try:
            c.putrequest("GET", (u.path or "/") + ("?" + u.query if u.query else ""), skip_accept_encoding=True)
            if agent is not None:
                c.putheader("User-Agent", agent)
            for k, v in headers:
                c.putheader(k, v)
            c.endheaders()
            r = c.getresponse()
            body = r.read()
            return r.status, {k.lower(): v for k, v in r.getheaders()}, body, time.monotonic() - t0
        except (OSError, http.client.HTTPException) as e:
            return 0, {}, json.dumps({"error": "client_" + type(e).__name__}).encode(), time.monotonic() - t0
        finally:
            c.close()

    def edge(self, target, **kw):
        return self.out(self.soak, target, **kw)


def js(body):
    try:
        return json.loads(body)
    except ValueError:
        return None


def err_of(body):
    d = js(body)
    return d.get("error") if isinstance(d, dict) else None


def nonce() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S") + secrets.token_hex(6)


def with_nonce(target: str, n: str | None = None) -> str:
    return target + ("&" if "?" in target else "?") + "_n=" + (n or nonce())


def log_count(log_path: str, fragment: str) -> int:
    lines = open(log_path, encoding="utf-8", errors="replace").read().splitlines()
    return sum(1 for ln in lines if ALL_LINES in ln and fragment in ln)


def settle(log_path: str, fragment: str, want: int, seconds: float = 15.0, extra: float = 2.0) -> int:
    """wrangler writes its line AFTER the answer: wait for the count, then `extra` more (a line too many would
    come now)."""
    end = time.monotonic() + seconds * SETTLE_SCALE
    while log_count(log_path, fragment) < want and time.monotonic() < end:
        time.sleep(0.25 * SETTLE_SCALE)
    time.sleep(extra * SETTLE_SCALE)
    return log_count(log_path, fragment)


def get_line(path: str) -> str:
    return f"] GET {path} "


# ---------------------------------------------------------------------------------------------- shared checks
def control(run: Run, label: str):
    """The loopback control: the router and the origin answer here and now, AND this control's own request is
    seen as exactly one new line of the instance log. So a count taken after it starts from a settled log, and
    the log that is counted is shown to be the live one. Returns (ok, the sources answer, time, the log path)."""
    at = time.monotonic()
    st, _h, body = run.loop("/__router/status")
    d = js(body) or {}
    active = d.get("active") if isinstance(d, dict) else None
    ok1 = st == 200 and active in ("blue", "green")
    run.row(f"{label} loopback: router", "GET 127.0.0.1/__router/status", f"{st} active={active!r} error={err_of(body)!r}", "PASS" if ok1 else "FAIL")
    log_path = run.log_path_of(active) if ok1 else None
    if not log_path or not os.path.isfile(log_path):
        run.row(f"{label} loopback: the instance log", "the active instance's instance.log", "not found", "FAIL",
                "without the log nothing can be counted")
        return False, {}, at, None
    c0 = log_count(log_path, get_line(SRC))
    st, _h, body = run.loop(SRC, with_secret=True)
    s = js(body) or {}
    srcs = s.get("sources") if isinstance(s, dict) else None
    ok2 = st == 200 and isinstance(srcs, list) and len(srcs) >= 100 and s.get("total") == len(srcs)
    run.row(f"{label} loopback: sources with the secret", "GET 127.0.0.1/v1/sources +secret",
            f"{st} sources={len(srcs) if isinstance(srcs, list) else None} error={err_of(body)!r}", "PASS" if ok2 else "FAIL",
            "floor: 200, at least 100 entries, total equal to the list")
    n = settle(log_path, get_line(SRC), c0 + 1)
    ok3 = n - c0 == 1
    run.row(f"{label} loopback: this request is in the instance log", "the count of `GET /v1/sources` lines over the request",
            f"new lines={n - c0}", "PASS" if ok3 else "FAIL", "exactly one: the log that is counted is the live one, and it is settled")
    return ok1 and ok2 and ok3, (s if ok2 else {}), at, log_path


def edge_status(run: Run, want_configured: bool):
    st, _h, body, secs = run.edge("/v1/edge-status")
    d = js(body) if st == 200 else None
    d = d if isinstance(d, dict) else {}
    ok = (st == 200 and re.fullmatch(r"[0-9a-f]{40}", str(d.get("commit") or "")) is not None and d.get("soak") is True
          and d.get("forward") is True and d.get("edge_state") == "users" and d.get("origin_configured") is want_configured)
    run.row("edge-status", "GET <soak>/v1/edge-status",
            f"{st} commit={d.get('commit')!r} soak={d.get('soak')!r} forward={d.get('forward')!r} state={d.get('edge_state')!r} "
            f"origin_configured={d.get('origin_configured')!r} seconds={secs:.1f}", "PASS" if ok else "FAIL",
            f"expected: a 40-hex commit, soak true, forward true, users, origin_configured {str(want_configured).lower()}")
    return ok


def ask_path_probe(run: Run, log_path: str):
    """(ok, status, error word, lines with the nonce, mark shown, the target)."""
    n = secrets.token_hex(8)
    target = f"/v1/series/__soak_{n}.metadata.json"
    st, h, body, _s = run.edge(target)
    lines = settle(log_path, n, 1)
    ok = st == 404 and err_of(body) == "not_found" and lines == 1 and MARK_HEADER not in h
    return ok, st, err_of(body), lines, MARK_HEADER in h, target


def path_probe(run: Run, log_path: str, label: str) -> bool:
    ok, st, err, lines, mark, target = ask_path_probe(run, log_path)
    run.row(f"{label} path probe", f"GET <soak>{target}", f"{st} error={err!r} log lines with the nonce={lines} mark header shown={mark}",
            "PASS" if ok else "FAIL", "expected the origin's 404 not_found, exactly one line in instance.log, and no origin mark in the client's answer")
    return ok


def refusal(st: int, h: dict, body: bytes):
    """What kind of answer the public origin name gave to a request without a valid token: (kind, facts).
    access = a refusal that carries a mark of Access; unknown = a bare 401/403; every other kind is a failure."""
    mark = MARK_HEADER in h
    loc_host = (urllib.parse.urlsplit(h.get("location", "")).hostname or "").lower()
    on_access = loc_host.endswith(".cloudflareaccess.com")
    mitigated = "cf-mitigated" in h
    names_access = b"cloudflare access" in body.lower() or "access" in h.get("www-authenticate", "").lower()
    if mark:
        kind = "the origin answered"
    elif mitigated:
        kind = "a challenge"
    elif st == 302 and on_access:
        kind = "access"
    elif st in (401, 403) and names_access:
        kind = "access"
    elif st in (401, 403):
        kind = "unknown"
    else:
        kind = "not a refusal of Access"
    server = re.sub(r"[^a-z0-9.-]", "", h.get("server", "").lower())[:20]
    ctype = re.sub(r"[^a-z0-9/+.-]", "", h.get("content-type", "").split(";")[0].lower())[:30]
    facts = (f"{st} kind={kind} origin mark={mark} location on cloudflareaccess.com={on_access} cf-mitigated={mitigated} "
             f"server={server or None} cf-ray present={'cf-ray' in h} www-authenticate present={'www-authenticate' in h} "
             f"type={ctype or None} names Access={names_access} bytes={len(body)}")
    return kind, facts


# ---------------------------------------------------------------------------------------------- the modes
def mode_lock(run: Run, origin: str) -> None:
    run.hidden.append((origin, "<origin>"))
    run.hidden.append((urllib.parse.urlsplit(origin).netloc, "<origin host>"))
    ok, _s, _at, log_path = control(run, "before")
    if not ok:
        run.row("lock", "the public origin name without a token", "not asked", "NOTRUN", "no loopback control: a refusal would prove nothing")
        return
    before = log_count(log_path, ALL_LINES)
    kinds = []
    asked = [("lock: no token", t, ()) for t in (SRC, "/__router/status", "/")]
    asked.append(("lock: a token that does not exist", SRC, (("cf-access-client-id", "0" * 32 + ".access"), ("cf-access-client-secret", "0" * 64))))
    for check, target, headers in asked:
        st, h, body, _s = run.out(origin, with_nonce(target), headers=list(headers))
        kind, facts = refusal(st, h, body)
        kinds.append(kind)
        run.row(check, f"GET <origin>{target}?_n=..." + (" +a made-up service token" if headers else ""), facts,
                "PASS" if kind == "access" else "SEEN" if kind == "unknown" else "FAIL",
                "PASS = a refusal with a mark of Access; SEEN = a bare 401/403, not told apart from another Cloudflare refusal")
    ok2, _s, _at, log2 = control(run, "after")
    if not ok2 or log2 != log_path:
        run.row("lock: nothing reached the instance", "the count of instance.log lines over the four requests", "not counted", "NOTRUN",
                "the control after the requests failed, or the active instance changed during the run")
    else:
        after = log_count(log_path, ALL_LINES)
        run.row("lock: nothing reached the instance", "the count of ALL instance.log lines from the control before to the control after",
                f"new lines={after - before}", "PASS" if after - before == 1 else "FAIL",
                "exactly one: the control after the requests (its own line is the positive control of this count)")
    bad = [k for k in kinds if k not in ("access", "unknown")]
    if bad:
        verdict, got = "FAIL", f"{len(bad)} of {len(kinds)} answers are no refusal of Access"
    elif all(k == "access" for k in kinds):
        verdict, got = "PASS", f"{len(kinds)} of {len(kinds)} refusals carry a mark of Access"
    else:
        verdict, got = "NOTRUN", f"{kinds.count('unknown')} of {len(kinds)} refusals carry no mark of Access"
    run.row("lock: the refusals are Access's own", "the four answers above", got, verdict,
            "NOTRUN = read the Access log for these four requests by hand. No verdict here shows that the name leads to THIS origin: phase 2 does")


def mode_phase0(run: Run) -> None:
    edge_status(run, want_configured=False)
    for target in (SRC, "/v1/catalog?q=unemployment", "/v1/series/x.metadata.json", "/"):
        st, _h, body, _s = run.edge(with_nonce(target))
        run.row("phase 0: no origin value yet", f"GET <soak>{target} +nonce", f"{st} error={err_of(body)!r}",
                "PASS" if st == 503 and err_of(body) == "origin_not_configured" else "FAIL", "expected 503 origin_not_configured")


def mode_phase1(run: Run) -> None:
    ok1, _s, at1, log_path = control(run, "before")
    before = log_count(log_path, get_line(SRC)) if ok1 else None
    edge_status(run, want_configured=True)
    st, _h, body, _secs = run.edge(with_nonce(SRC))
    got_502 = st == 502 and err_of(body) == "origin_bad_answer"
    run.row("phase 1: the origin values without the Access token", "GET <soak>/v1/sources +nonce", f"{st} error={err_of(body)!r}",
            "PASS" if got_502 else "FAIL", "expected 502 origin_bad_answer (a tunnel that is down gives the same 502)")
    ok2, _s, at2, log2 = control(run, "after")
    span = at2 - at1
    if ok1 and ok2 and log2 == log_path:
        after = log_count(log_path, get_line(SRC))
        run.row("phase 1: the request did not reach the instance", "the count of `GET /v1/sources` lines from the control before to the control after",
                f"new lines={after - before}", "PASS" if after - before == 1 else "FAIL", "exactly one: the control after the request")
    else:
        run.row("phase 1: the request did not reach the instance", "the count of `GET /v1/sources` lines", "not counted", "NOTRUN",
                "a loopback control failed, or the active instance changed during the run")
    two = ok1 and ok2 and span < 60.0 and got_502
    run.row("phase 1 counts as 'Access refuses this account's Worker without a token' only when all four hold",
            "(1) both loopback controls pass within a minute of each other; (2) the URL has a nonce",
            f"(1) {ok1 and ok2 and span < 60.0} (span {span:.0f} s); (2) True by construction",
            "SEEN" if two else "FAIL",
            "(3) ORIGIN_URL was set once by the helper and not again, and (4) phase 2 then gives 200: NOT shown by this run, so this row is never a PASS")


def mode_suite(run: Run, gated_id: str | None) -> None:
    ok, loop_sources, _at, log_path = control(run, "before")
    if not ok:
        run.row("suite", "every check", "not run", "NOTRUN", "no loopback control: nothing could be compared or counted")
        return
    edge_status(run, want_configured=True)
    path_probe(run, log_path, "start:")

    def floor(check, target, test, shown, note=""):
        st, h, body, secs = run.edge(with_nonce(target))
        d = js(body)
        d = d if isinstance(d, dict) else {}
        good = st == 200 and MARK_HEADER not in h and "x-econ-instance" not in h and bool(test(d))
        run.row(check, f"GET <soak>{target} +nonce", f"{st} {shown(d)} error={err_of(body)!r} seconds={secs:.1f}", "PASS" if good else "FAIL", note)
        return d

    def results(d):
        r = d.get("results") if isinstance(d, dict) else None
        return r if isinstance(r, list) else []

    def ids(d):
        return [r.get("id") or r.get("series_id") for r in results(d) if isinstance(r, dict)]

    for target in ("/", "/v1"):
        floor("root", target, lambda d: isinstance(d.get("endpoints"), list) and d["endpoints"], lambda d: f"endpoints={len(d.get('endpoints') or [])}")
    want_total = loop_sources.get("total")
    floor("sources", SRC, lambda d: d.get("total") == want_total and isinstance(d.get("sources"), list) and len(d["sources"]) == want_total,
          lambda d: f"total={d.get('total')} loopback total={want_total}", "floor: the same count as the loopback answer of this run")
    _st, _h, body = run.loop("/v1/catalog?q=unemployment&limit=5", with_secret=True)
    loop_ids = ids(js(body) or {})
    d = floor("catalogue search", "/v1/catalog?q=unemployment&limit=5", lambda d: bool(loop_ids) and ids(d) == loop_ids,
              lambda d: f"results={len(results(d))} the same ids as on loopback={bool(loop_ids) and ids(d) == loop_ids}",
              "floor: results, and the same ids in the same order as the loopback answer")
    sid = next((i for i in ids(d) if i), None)
    first_source = (loop_sources.get("sources") or [{}])[0].get("source")
    floor("catalogue browse", f"/v1/catalog?source={urllib.parse.quote(str(first_source), safe='')}&limit=5", results, lambda d: f"results={len(results(d))}")
    floor("catalogue page 2", "/v1/catalog?q=unemployment&limit=5&offset=5",
          lambda d: ids(d) and not set(ids(d)) & set(loop_ids), lambda d: f"results={len(results(d))} none of them on page 1={not set(ids(d)) & set(loop_ids)}")
    floor("last-updates", "/v1/last-updates",
          lambda d: any(isinstance(x, dict) and x.get("last_updated") for x in (d.get("datasets") if isinstance(d.get("datasets"), list) else [])),
          lambda d: f"datasets={len(d.get('datasets') or []) if isinstance(d.get('datasets'), list) else None}", "floor: rows, at least one with a time")
    _st, _h, body = run.loop("/v1/stats", with_secret=True)
    loop_entries = (js(body) or {}).get("catalog_entries")
    floor("stats", "/v1/stats", lambda d: isinstance(loop_entries, int) and d.get("catalog_entries") == loop_entries,
          lambda d: f"catalog_entries equal to loopback's={isinstance(loop_entries, int) and d.get('catalog_entries') == loop_entries}",
          "floor: catalog_entries as on loopback")
    floor("guard-heartbeat", "/v1/guard-heartbeat", lambda d: isinstance(d.get("utc"), str) and d["utc"], lambda d: f"has utc={bool(d.get('utc'))}",
          "the beat in the store is a copy of one moment: its AGE is not read here")
    if sid:
        enc = urllib.parse.quote(sid, safe="")
        meta, csv = f"/v1/series/{enc}.metadata.json", f"/v1/series/{enc}.csv"
        floor("metadata", meta, lambda d: d.get("series_id") == sid, lambda d: f"series_id is the id asked for={d.get('series_id') == sid}")
        st, _h, body, _s = run.edge(with_nonce(f"/v1/bundle?ids={enc}"))
        run.row("bundle", "GET <soak>/v1/bundle?ids=<the id found> +nonce", f"{st} bytes={len(body)} error={err_of(body)!r}",
                "PASS" if st == 200 and len(body) > 2 and err_of(body) is None else "FAIL", "floor: 200 and a body that is not empty (its content is not judged)")
    else:
        run.row("metadata and bundle", "an id from the search", "no id found", "NOTRUN")
    st, _h, body, _s = run.edge(with_nonce("/v1/public-stats"))
    run.row("public-stats", "GET <soak>/v1/public-stats +nonce", f"{st} bytes={len(body)} error={err_of(body)!r}",
            "PASS" if 200 <= st < 500 else "FAIL", "an answer, and never a 5xx; the status itself is an observation")
    for target in ("/v1/pv", "/v1/pv/report"):
        st, _h, body, _s = run.edge(with_nonce(target))
        run.row(f"page views are off on the soak worker: {target}", f"GET <soak>{target} +nonce", f"{st}", "PASS" if st == 404 else "FAIL",
                "GET only: a POST that got through would write a row in production")

    # the cache: one URL twice; the origin is asked once. (On the real edge the cache is written AFTER the first
    # answer: the short pause gives it time. Two lines with equal bodies on a healthy system point at the platform.)
    line = get_line(SRC)
    settle(log_path, line, 0, seconds=0.0)                       # the lines of the requests above are in
    before = log_count(log_path, line)
    url = with_nonce(SRC)
    a = run.edge(url)
    time.sleep(2.0 * SETTLE_SCALE)
    b = run.edge(url)
    after = settle(log_path, line, before + 1)
    run.row("the edge cache", "the same GET <soak>/v1/sources +one nonce, twice", f"{a[0]} and {b[0]}, same body={a[2] == b[2]}, new `GET /v1/sources` lines={after - before}",
            "PASS" if a[0] == b[0] == 200 and a[2] == b[2] and after - before == 1 else "FAIL", "expected two equal 200 answers and exactly ONE new line in instance.log")

    # the refusals of the keyed path: none may reach the instance. A zero proves something only beside a positive:
    # one more metadata request for the same id must add exactly one line of that path shape.
    if sid:
        before_meta = log_count(log_path, get_line(meta))
        run.edge(with_nonce(meta))
        after_meta = settle(log_path, get_line(meta), before_meta + 1)
        shape_ok = after_meta - before_meta == 1
        run.row("control: a path with this id is counted in the instance log", "GET <soak>/v1/series/<the id found>.metadata.json +nonce",
                f"new lines of that path={after_meta - before_meta}", "PASS" if shape_ok else "FAIL", "exactly one")
        before = log_count(log_path, get_line(csv))
        st, _h, body, _s = run.edge(with_nonce(csv))
        run.row("a CSV without a key", "GET <soak>/v1/series/<the id found>.csv +nonce", f"{st} error={err_of(body)!r}", "PASS" if st == 401 else "FAIL", "expected 401")
        st, _h, body, _s = run.edge(with_nonce(csv), headers=[("X-API-Key", secrets.token_hex(20))])
        run.row("a CSV with a key that does not exist", "GET <soak>/v1/series/<the id found>.csv +nonce +a random key", f"{st} error={err_of(body)!r}",
                "PASS" if st == 401 and err_of(body) == "invalid_key" else "FAIL", "expected 401 invalid_key")
        after = settle(log_path, get_line(csv), before + 1, seconds=5.0)
        run.row("the two refused CSV requests did not reach the instance", "the count of lines for that CSV path", f"new lines={after - before}",
                "PASS" if after == before and shape_ok else "FAIL", "zero, beside the control row above")
    if gated_id:
        run.hidden.append((gated_id, "<the gated id>"))
        run.hidden.append((urllib.parse.quote(gated_id, safe=""), "<the gated id>"))
        st, _h, body, _s = run.edge(with_nonce(f"/v1/series/{urllib.parse.quote(gated_id, safe='')}.csv"))
        run.row("a gated id", "GET <soak>/v1/series/<the gated id>.csv +nonce, no key", f"{st} error={err_of(body)!r}",
                "PASS" if st == 451 and err_of(body) == "not_redistributable" else "FAIL", "expected 451 before any question about a key")
    else:
        run.row("a gated id", "GET <soak>/v1/series/<a gated id>.csv", "not asked", "NOTRUN", "--gated-id was not given (the id is never written in the repository)")

    # client User-Agents: each one is answered, and each request reaches the origin once
    for name, agent in CLIENT_AGENTS:
        before = log_count(log_path, line)
        st, _h, body, _s = run.edge(with_nonce(SRC), agent=agent)
        d = js(body) if st == 200 else None
        after = settle(log_path, line, before + 1, seconds=10.0)
        total = d.get("total") if isinstance(d, dict) else None
        run.row(f"User-Agent: {name}", "GET <soak>/v1/sources +nonce", f"{st} total={total} error={err_of(body)!r} new lines={after - before}",
                "PASS" if st == 200 and total == want_total and want_total is not None and after - before == 1 else "FAIL",
                "expected 200 with the loopback's count and exactly one new line")
    path_probe(run, log_path, "end:")
    control(run, "after")
    print("by hand at the workstation, NOT part of this script and not in its counts: the workstation down (502/504, a "
          "cached URL still 200); a swap while a large download streams; an instance after its starter is gone and "
          "`swap.py --stop`; an instance without the secret answers 503.", flush=True)


def _word(v) -> str:
    return "true" if v is True else "false" if v is False else "none" if v is None else "other"


def _probe_fields(run: Run):
    """(ok, the fields of the probe's line). Only fixed words and whole numbers go into the line."""
    st, _h, body, s1 = run.edge("/v1/edge-status")
    d = js(body) if st == 200 else None
    d = d if isinstance(d, dict) else {}
    ok1 = st == 200 and d.get("soak") is True and d.get("forward") is True and d.get("origin_configured") is True
    st2, _h, body, s2 = run.edge(with_nonce(SRC))
    s = js(body) if st2 == 200 else None
    s = s if isinstance(s, dict) else {}
    total, srcs = s.get("total"), s.get("sources")
    whole = isinstance(total, int) and not isinstance(total, bool)
    ok2 = st2 == 200 and whole and total >= 100 and isinstance(srcs, list) and len(srcs) == total
    st3, _h, body = run.loop("/__router/status")
    r = js(body) if st3 == 200 else None
    log_path = run.log_path_of(r.get("active")) if isinstance(r, dict) else None
    if log_path and os.path.isfile(log_path):
        ok3, st4, err, lines, _mark, _target = ask_path_probe(run, log_path)
        probe = f"path_probe={int(st4)}/{'not_found' if err == 'not_found' else 'other'}/{int(lines)}"
    else:
        ok3, probe = False, "path_probe=no_router_or_log"
    fields = (f"edge-status={int(st)} soak={_word(d.get('soak'))} forward={_word(d.get('forward'))} configured={_word(d.get('origin_configured'))} "
              f"({s1:.1f}s) sources={int(st2)} total={total if whole else 'other'} ({s2:.1f}s) {probe}")
    return ok1 and ok2 and ok3, fields


def mode_probe(run: Run) -> int:
    """ONE line per run, whatever the answers hold: an exception is a FAIL line with the error's type."""
    t = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        ok, fields = _probe_fields(run)
    except Exception as e:  # noqa: BLE001 - the probe must write its line
        ok, fields = False, "error=" + re.sub(r"[^A-Za-z0-9_]", "", type(e).__name__)
    line = f"{t} {'PASS' if ok else 'FAIL'} {fields}"
    line = re.sub(r"[\x00-\x1f\x7f]", " ", line.encode("ascii", "replace").decode("ascii"))
    os.makedirs(run.root + "/logs", exist_ok=True)
    with open(run.root + "/logs/soak_probe.log", "a", encoding="ascii", newline="\n") as fh:
        fh.write(line + "\n")
    print(line, flush=True)
    return 0 if ok else 1


def read_origin(path: str) -> str:
    lines = [ln.strip() for ln in open(path, encoding="utf-8-sig").read().splitlines() if ln.strip()]
    if len(lines) != 1 or not ORIGIN_RE.fullmatch(lines[0]):
        raise SystemExit("the origin URL file must hold exactly one line: https://<one label>.econdatalibrary.com")
    return lines[0]


def finish(run: Run, mode: str) -> int:
    counts = {v: sum(1 for r in run.rows if r["verdict"] == v) for v in ("PASS", "FAIL", "SEEN", "NOTRUN")}
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    os.makedirs(run.root + "/logs", exist_ok=True)
    path = f"{run.root}/logs/soak_{mode}_{stamp}.json"
    text = json.dumps({"mode": mode, "utc": stamp, "soak": run.soak, "counts": counts, "rows": run.rows}, indent=1)
    for value, _shown in run.hidden:
        assert value not in text, "a hidden value in the result file"
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text + "\n")
    print(f"{len(run.rows)} rows: {counts['PASS']} PASS, {counts['FAIL']} FAIL, {counts['SEEN']} SEEN, {counts['NOTRUN']} NOTRUN -> {path}", flush=True)
    return 1 if counts["FAIL"] else 2 if counts["NOTRUN"] else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="The keyless soak tests of the self-hosted origin.")
    ap.add_argument("--mode", required=True, choices=("lock", "phase0", "phase1", "suite", "probe"))
    ap.add_argument("--soak", default=os.environ.get("SELFHOST_SOAK") or SOAK, help="the soak worker's address")
    ap.add_argument("--root", default="F:/econ_origin", help="the standing origin's folder (its .dev.vars, instances.json, logs)")
    ap.add_argument("--origin-url-file", help="lock mode: a file with one line, the origin's public https address")
    ap.add_argument("--gated-id", help="suite mode: one series id of a source that is not redistributable (never written down in the repository)")
    a = ap.parse_args(argv)
    run = Run(a.soak, a.root)
    if a.mode == "probe":
        return mode_probe(run)
    if a.mode == "lock":
        if not a.origin_url_file:
            raise SystemExit("--mode lock needs --origin-url-file")
        mode_lock(run, read_origin(a.origin_url_file))
    elif a.mode == "phase0":
        mode_phase0(run)
    elif a.mode == "phase1":
        mode_phase1(run)
    else:
        mode_suite(run, a.gated_id)
    return finish(run, a.mode)


if __name__ == "__main__":
    raise SystemExit(main())
