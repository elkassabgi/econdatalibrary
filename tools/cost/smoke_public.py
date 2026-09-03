"""Post-deploy smoke test of every public route, plus the R675 search regression.

`npx wrangler deploy` is manual and nothing else checks the running system afterwards. tsc and the
unit tests say the code is well-formed; only a request says a route still answers.

TWO THINGS THIS EXISTS TO CATCH, both of which have actually happened:

1. A route quietly stops answering after a deploy. Three deploys went out on 2026-09-03 alone.

2. R675's search bug: `ftsOk` was read from the POST-SLICE page, so paging past the last page of
   a SUCCESSFUL search read as "FTS failed" and re-ran the whole query through a `LIKE` full
   scan - two engines answering one query with different totals. `?source=bls&q=employment`
   returned total=2 at offset 0 and total=4 at offset 99. The check below is that exact case.
   It must stay: the fix is in catalog.ts and a future edit can undo it silently.

The zero-result LIKE fallback is DELIBERATE and is asserted here too - `?source=bls&q=onfarm`
finds `bls:CES0000000001` by infix, which FTS5 cannot do. A change that "fixes" the fallback by
removing it would break that, and this names it so the loss is a failure rather than a surprise.

NOT COVERED, and it cannot be from here: the CSV comment header (the IDB dataset backlink and
the idb caveat). Series downloads are auth-gated - `auth_required` for every source - and no
token exists in an unattended shell. Those live in api/worker/test/seriesHeader.test.ts.

Read-only GETs. No writes and no D1 full scans: `/v1/catalog` with a `q` uses the FTS index.
"""
import json
import sys
import urllib.error
import urllib.request

BASE = "https://econdl-api.elkassabgi.workers.dev"
UA = {"User-Agent": "econdatalibrary/1.0 (+https://econdatalibrary.com)"}


def get(path: str, full: bool = False):
    """(status, body). `full` reads the whole body; otherwise the first 4 KB.

    The cap keeps a large response out of memory when all we want is its size - but a TRUNCATED
    body is not valid JSON, and parsing one made /v1/sources report "(unparsed)" while returning
    a healthy 200. So every caller that parses passes full=True.
    """
    try:
        r = urllib.request.urlopen(urllib.request.Request(BASE + path, headers=UA), timeout=120)
        return r.status, (r.read() if full else r.read(4000))
    except urllib.error.HTTPError as e:
        return e.code, e.read(400)
    except Exception as e:                                            # noqa: BLE001
        return type(e).__name__, b""


def total(path: str):
    code, body = get(path, full=True)
    if code != 200:
        return f"HTTP {code}"
    try:
        return json.loads(body).get("total")
    except Exception:                                                 # noqa: BLE001
        return "unparsed"


ROUTES = [
    ("/v1/sources", 200, "total"),
    ("/v1/stats", 200, None),
    ("/v1/catalog?source=idb&limit=2", 200, "total"),
    ("/v1/catalog?q=inflation&limit=2", 200, "total"),
]


def main() -> int:
    bad = 0
    print("routes")
    for path, want, key in ROUTES:
        code, body = get(path, full=bool(key))
        ok = code == want
        if ok and key:
            try:
                detail = f"{key}={json.loads(body).get(key):,}"
            except Exception:                                         # noqa: BLE001
                detail = "(unparsed)"
        elif ok:
            detail = f"{len(body)} bytes"
        else:
            detail = str(body[:110])
            bad += 1
        print(f"  {'OK ' if ok else 'BAD'} {str(code):>5}  {path:<38} {detail}")

    code, _ = get("/v1/series/imf_cpi%3Ax.csv")
    gated = code == 401
    bad += 0 if gated else 1
    print(f"  {'OK ' if gated else 'BAD'} {str(code):>5}  "
          f"{'/v1/series/<id>.csv  (auth gate)':<38} "
          f"{'gate intact' if gated else 'THE GATE IS OPEN'}")

    print("\nR675 — one query must not get two totals from two engines")
    for q in ("bls&q=employment", "bls&q=onfarm"):
        a = total(f"/v1/catalog?source={q}&limit=5")
        b = total(f"/v1/catalog?source={q}&limit=5&offset=99")
        ok = a == b
        bad += 0 if ok else 1
        print(f"  {'OK ' if ok else 'BAD'} source={q:<18} "
              f"offset=0 -> {a}   offset=99 -> {b}")

    onfarm = total("/v1/catalog?source=bls&q=onfarm&limit=5")
    ok = isinstance(onfarm, int) and onfarm >= 1
    bad += 0 if ok else 1
    print(f"  {'OK ' if ok else 'BAD'} the deliberate zero-result infix fallback still finds "
          f"'onfarm' ({onfarm})")

    print()
    print(f"{bad} failure(s)" if bad else "all public routes answer; no engine divergence")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
