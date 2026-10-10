"""Is there a request target that router.forwardable() lets through and that the ORIGIN's runtime reads as a
path the edge could not send? (Written in review AR-268, which found one class with it: a `#`.)

The oracle is the REAL workerd binary that the pinned miniflare runs - not a model of it. Run this again, and
read its result, before router.MEASURED_WORKERD is changed (tests/test_selfhost_router.py compares that
constant with api/worker/package-lock.json).

    1. start the toy worker (two local ports, 18820 and 18822; nothing else is opened):
         cd tools/selfhost/path_fuzz
         <worker dir>/node_modules/@cloudflare/workerd-windows-64/bin/workerd.exe serve config.capnp
    2. python -B tools/selfhost/path_fuzz/fuzz_paths.py <out.json> [seed]
         TOKENS=hash in the environment uses the alphabet built round `#`, `?` and dot segments
    3. stop workerd

The toy worker reports `new URL(request.url).pathname` at the first hop and after one real HTTP hop made the
way wrangler's ProxyWorker makes it. Targets go out with http.client.putrequest, as router.py's _proxy sends
them; a target http.client refuses is one the router cannot forward either (counted). The leading-`//`
collapse of http.server is applied first, as the router sees a target.

INVARIANT: forwardable(target) and workerd answered  =>  the pathname at BOTH hops is in EDGE_PATHS or starts
with EDGE_PREFIX.

Exit code 0 only when there are 0 violations AND workerd answered every forwardable target that http.client
could send AND it RESOLVED at least one refused target to a path under /cdn-cgi/ (the planted positive: a run
that reached no oracle, or an oracle that echoes the target, proves nothing and fails) AND the oracle shows
the two readings in MARKS (a server that resolves dot segments the WHATWG way alone passed the first check
with no workerd running - review AR-273). The
result file names the router file and the oracle's address; it cannot name the oracle's binary - the person
who starts workerd checks its version against router.MEASURED_WORKERD. What this does NOT cover: miniflare's own
entry worker (the toy stands in for it), and what Cloudflare's edge or cloudflared do to a target before the
router sees it.
"""
import http.client
import itertools
import json
import os
import random
import sys
import time

sys.path.insert(0, os.environ.get("ROUTER_DIR") or os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import router  # noqa: E402

ORACLE = ("127.0.0.1", 18820)
TOK_HASH = ["#", "?", "/", "..", ".", "%2e%2e", "%2E", "a", "\\", "%23", "%3f", ";", "cdn-cgi/mf/scheduled", "/..",
            "#/..", "?/..", "#a/..", "/./", "//", "/../..", "#/../../..", "&", "=", "%"]
TOK = ["/", "//", ".", "..", "%2e", "%2E", ".%2e", "%2E.", "%2e%2E", "\\", "%5c", "%5C", ";", ";x", "?", "#", "%2f",
       "%2F", "%00", "%09", "%0a", "%0d", "%23", "%3f", "%20", "%", "%2", "%252e", "%c0%ae", "%ef%bc%8e", "...", "a",
       "v1", "series", "cdn-cgi", "mf", "scheduled", "catalog", ":", "@", "*", "~", "+", "&", "=", "$", "'", "(", "|",
       "^", "`", "{", "<", '"']
PREFIXES = ["/v1/series/", "/v1/", "/", "/v1/series", "/v1/catalog", "/v1"]
TAILS = ["", "/cdn-cgi/mf/scheduled", "cdn-cgi/mf/scheduled"]
RANDOM_TARGETS = 150000
# THE PLANTED POSITIVES: the two readings forwardable() is written against (review AR-268). The oracle must
# show BOTH, or it is not the runtime this check is for and its "0 violations" proves nothing.
MARKS = {"hash_then_dot_segments (workerd's HTTP layer)": "/v1/series/a#/../../../cdn-cgi/mf/scheduled",
         "percent_2e_as_a_dot (the WHATWG parser)": "/v1/series/%2e%2e/%2E%2e/%2e%2E/cdn-cgi/mf/scheduled"}
MARK_READS_AS = "/cdn-cgi/mf/scheduled"


def allowed(p):
    return p in router.EDGE_PATHS or p.startswith(router.EDGE_PREFIX)


def as_router_sees(target: str) -> str:
    return "/" + target.lstrip("/") if target.startswith("//") else target      # http.server.parse_request


def targets(tokens, seed: int):
    seen = set()
    for pre in PREFIXES:
        for n in (0, 1, 2):
            for combo in itertools.product(tokens, repeat=n):
                for tail in TAILS:
                    t = pre + "".join(combo) + tail
                    if t not in seen:
                        seen.add(t)
                        yield t
    rnd = random.Random(seed)
    made = 0
    while made < RANDOM_TARGETS:
        pre = rnd.choice(PREFIXES)
        t = pre + "".join(rnd.choice(tokens) for _ in range(rnd.randint(3, 9))) + rnd.choice(TAILS)
        if t not in seen:
            seen.add(t)
            made += 1
            yield t


def main() -> int:
    tokens = TOK_HASH if os.environ.get("TOKENS") == "hash" else TOK
    out_path, seed = sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 20261010
    conn = None
    n = fwd = refused = client_refused = answered = non200 = refused_dangerous = resolved = 0
    violations, not_answered, examples_refused_dangerous = [], {}, []
    t0 = time.monotonic()
    marks = {}
    for label, target in MARKS.items():
        assert not router.forwardable(target), target      # the router refuses both
        try:
            c = http.client.HTTPConnection(*ORACLE, timeout=30)
            c.putrequest("GET", target, skip_accept_encoding=True)
            c.endheaders()
            marks[label] = json.loads(c.getresponse().read())["first"]["pathname"]
            c.close()
        except (OSError, ValueError, KeyError, TypeError, http.client.HTTPException) as e:
            marks[label] = f"no reading: {type(e).__name__}"
    for target in targets(tokens, seed):
        n += 1
        seen = as_router_sees(target)
        ok = router.forwardable(seen)
        if ok:
            fwd += 1
        else:
            refused += 1
        # the oracle is asked about EVERY target the router would forward, and about every 7th refused one
        # (to show what the refusals protect against)
        if not ok and n % 7:
            continue
        r = body = None
        for attempt in (0, 1):
            try:
                if conn is None:
                    conn = http.client.HTTPConnection(*ORACLE, timeout=30)
                conn.putrequest("GET", seen, skip_accept_encoding=True)
                conn.endheaders()
                r = conn.getresponse()
                body = r.read()
                break
            except (http.client.InvalidURL, UnicodeEncodeError, ValueError):
                r = None
                conn.close()
                conn = None
                if ok:
                    client_refused += 1
                break
            except (OSError, http.client.HTTPException):
                if conn is not None:
                    conn.close()
                conn = None
                if attempt:
                    raise
        if r is None:
            continue
        if r.will_close:
            conn.close()
            conn = None
        if r.status != 200:
            if ok:
                non200 += 1
                kept = not_answered.setdefault(str(r.status), [])
                if len(kept) < 15:
                    kept.append(seen)
            continue
        j = json.loads(body)
        p1 = j["first"]["pathname"]
        p2 = (j["second"] or {}).get("pathname")
        if ok:
            answered += 1
            if p1 is None or not allowed(p1) or p2 is None or not allowed(p2):
                violations.append({"target": seen, "raw": j["first"]["raw"], "path1": p1, "path2": p2,
                                   "err": j["first"]["err"] or (j["second"] or {}).get("err")})
        elif p1 is not None and p1.startswith("/cdn-cgi/"):
            refused_dangerous += 1
            resolved += not seen.startswith("/cdn-cgi/")       # the oracle RESOLVED it there; an echo could not
            if len(examples_refused_dangerous) < 25:
                examples_refused_dangerous.append({"target": seen, "path1": p1})
        if n % 20000 == 0:
            print(n, fwd, answered, len(violations), f"{time.monotonic() - t0:.0f}s", flush=True)
    res = {"router_file": os.path.abspath(router.__file__), "oracle": "%s:%d" % ORACLE,
           "seed": seed, "tokens": "hash" if tokens is TOK_HASH else "general", "targets": n, "forwardable": fwd,
           "refused_by_router": refused, "forwardable_refused_by_http_client": client_refused,
           "forwardable_answered_200_by_workerd": answered, "forwardable_not_200_at_workerd": non200,
           "not_200_examples": not_answered, "VIOLATIONS": len(violations), "violation_examples": violations[:50],
           "refused_sampled_that_workerd_reads_under_cdn_cgi": refused_dangerous,
           "of_those_resolved_there_by_the_oracle": resolved,
           "planted_positives_read_by_the_oracle_as": marks,
           "refused_dangerous_examples": examples_refused_dangerous, "seconds": round(time.monotonic() - t0, 1)}
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(res, fh, indent=1)
    print(json.dumps({k: v for k, v in res.items() if not k.endswith("examples")}, indent=1))
    whole = fwd > 0 and answered + client_refused == fwd and non200 == 0
    # THE PLANTED POSITIVE: among the targets the router refuses, the oracle must RESOLVE some to a path under
    # /cdn-cgi/ that their text does not start with (workerd resolves their dot segments). An oracle that
    # never does - a stand-in that echoes the target, another runtime - cannot show a violation either, so
    # its "0 violations" proves nothing (review AR-269).
    wrong_marks = sorted(k for k, v in marks.items() if v != MARK_READS_AS)
    if wrong_marks:
        print("FAIL: the oracle does not show the reading(s)", wrong_marks, "- it is not the runtime this check "
              "is for (or a new workerd reads a target in a new way: measure again before the pin changes)")
        return 1
    if violations or not whole or resolved == 0:
        print("FAIL:", "violations" if violations else "the oracle did not answer every forwardable target"
              if not whole else "the oracle resolved NO refused target to a /cdn-cgi/ path: it is not the "
              "runtime this check is for")
        return 1
    print("OK: 0 violations, every forwardable target was answered by the oracle, and the oracle resolved "
          f"{resolved} refused targets to /cdn-cgi/ paths")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
