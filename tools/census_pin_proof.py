#!/usr/bin/env python3
"""Read-only checker for the census pin fix's POST-MERGE PROOF (fix/census-pins-retired).

The 2026-10-10 pass (run 2701) refused 29 of 45 census flows with HTTP 400 "unknown predicate
variable" because each sent its own geography (us=1 / world=1) as a pin. The fix is proven only by
a real keyed pass of ALL 29 (review AR-387 correction 1; R236, R1695) - one
`tools/run_local_heavy.ps1 -Only census -Force` pass on the workstation, BEFORE 2026-11-19, with
CENSUS_RETIRED_UPSTREAM_FROM unset - and this tool reads that pass's log and the store it wrote:

  0. GREEN: the summary line reads `ok census/_all` or `no_change census/_all`, and the log carries
     RECEIPT lines at all (a log from before the fix has none).
  1. RECEIPT: each of the 29 flows prints `RECEIPT window>=D returned=R ... stored=T MATCH`, and an
     INDEPENDENT count of the store parquet's rows with obs_date >= D equals both R and T.
  2. REFUSALS: 0 lines containing "HTTP 400", "unknown predicate variable" or a census
     "SUB-UNIT FAILED" line (the fetcher prints every failed sub-unit since this fix; before it, a
     refusal reached only the state note, which is why the 2026-10-10 log shows no "HTTP 400").
  3. PINS: every one of the 45 flows prints a pinning line; for the 16 that merged on 2026-10-10 the
     pin set is IDENTICAL to that night's; for the 29 it is that night's minus us / world / NATION.
  4. NOT RETIRED YET: no flow prints "RETIRED UPSTREAM" (the 8 BETA flows must be tailed).
  5. BETA REFRESH (owner decision 2026-10-26): for each of the 8 BETA tables, its 2025-12 rows equal
     the stable twin's values where both hold the row (joined on the BETA table's dimensions; BETA
     filtered world='1' and DISTRICT='-', stable CTY_CODE='-' and DISTRICT='-'). Required: 0
     differing rows per pair. imports/usdaimport shares no row with its twin (the ag/non-ag split
     exists only in BETA) and is reported as NOT COMPARABLE, not as a pass.

Reads only: the two logs and the store parquets. Writes nothing. Exit 0 only when every check
passes; any check that could not be made is a FAILURE, never a skip.

Usage:
  python -B tools/census_pin_proof.py --log logs/local_heavy_updater_<stamp>.log \
      --old-log logs/local_heavy_updater_20261009-223942.log --store data/clean_full/census
  add --log-only to run checks 1-4 on the log text alone (no store reads; 1 then needs no count).
"""
from __future__ import annotations

import argparse
import ast
import datetime as dt
import os
import re
import sys

EITS = ("advm3 bfs ftd ftdadv hv m3 marts mhs mhs2 mrts mrtsadv mtis mwts mwtsadv qfr qpr qss "
        "qtax resconst ressales vip").split()
BETA = ["exports/enduseexport", "exports/hitechexport", "exports/sitcexport", "exports/usdaexport",
        "imports/enduseimport", "imports/hitechimport", "imports/sitcimport", "imports/usdaimport"]
REFUSED = [f"eits/{x}" for x in EITS] + [f"intltrade/{x}" for x in BETA]
GEO = {"us", "world", "NATION"}
# 2025-12 rows differing BETA vs stable twin, measured on the local mirror 2026-10-26 (census BETA
# review; DuckDB and pandas agreed, AR-387 reproduced): 10,158 in all. Printed beside the new count.
BEFORE = {"exports/enduseexport": 425, "exports/hitechexport": 36, "exports/sitcexport": 6996,
          "exports/usdaexport": 9, "imports/enduseimport": 148, "imports/hitechimport": 12,
          "imports/sitcimport": 2532, "imports/usdaimport": 0}

_PIN = re.compile(r"^\[census\] (\S+): pinning (\{.*\}) - the grain the store holds\s*$")
_RCPT = re.compile(r"^\[census\] (\S+): RECEIPT window>=(\d{4}-\d{2}-\d{2}) returned=(\d+) "
                   r"skipped_new=(\d+) stored=(\S+) (MATCH|DIFFER|UNREAD)\s*$")


def read_lines(path):
    with open(path, encoding="utf-8", errors="replace") as fh:
        return [ln.rstrip("\r\n") for ln in fh]


def pins_of(lines):
    out = {}
    for ln in lines:
        m = _PIN.match(ln.strip())
        if m:
            out[m.group(1)] = ast.literal_eval(m.group(2))
    return out


def receipts_of(lines):
    out = {}
    for ln in lines:
        m = _RCPT.match(ln.strip())
        if m:
            out[m.group(1)] = (dt.date.fromisoformat(m.group(2)), int(m.group(3)), int(m.group(4)),
                               m.group(5), m.group(6))
    return out


def store_rows_since(store, flow, day):
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    od = pq.read_table(os.path.join(store, flow.replace("/", "__") + ".parquet"),
                       columns=["obs_date"]).column("obs_date")
    return int(pc.sum(pc.greater_equal(od, day)).as_py() or 0)


def beta_vs_stable(store):
    """{beta flow: (joined rows at 2025-12, differing rows) or None when the pair shares no row}."""
    import duckdb
    import pyarrow.parquet as pq
    skip = {"series_key", "obs_date", "time", "world", "DISTRICT"}
    con = duckdb.connect()
    out = {}
    for b in BETA:
        fam, leaf = b.split("/")
        base = leaf[:-len("export")] if leaf.endswith("export") else leaf[:-len("import")]
        bp = os.path.join(store, f"intltrade__{fam}__{leaf}.parquet").replace("\\", "/")
        sp = os.path.join(store, f"intltrade__{fam}__{base}.parquet").replace("\\", "/")
        bcols, scols = pq.read_schema(bp).names, pq.read_schema(sp).names
        meas = [c for c in bcols if c.upper().endswith(("_MO", "_YR")) and c in scols]
        dims = [c for c in bcols if c not in skip and not c.upper().endswith(("_MO", "_YR"))]
        on = " AND ".join(f'b."{c}" IS NOT DISTINCT FROM s."{c}"' for c in dims + ["obs_date"])
        anydiff = " OR ".join(f'b."{m}" IS DISTINCT FROM s."{m}"' for m in meas) or "FALSE"
        joined, differ = con.execute(f"""
            WITH b AS (SELECT * FROM read_parquet('{bp}')
                       WHERE world='1' AND DISTRICT='-' AND obs_date = DATE '2025-12-01'),
                 s AS (SELECT * FROM read_parquet('{sp}')
                       WHERE CTY_CODE='-' AND DISTRICT='-' AND obs_date = DATE '2025-12-01')
            SELECT count(*), coalesce(sum(CASE WHEN {anydiff} THEN 1 ELSE 0 END), 0)
            FROM b JOIN s ON {on}""").fetchone()
        out[b] = None if joined == 0 else (int(joined), int(differ))
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True)
    ap.add_argument("--old-log", required=True)
    ap.add_argument("--store")
    ap.add_argument("--log-only", action="store_true")
    a = ap.parse_args(argv)
    if not a.log_only and not a.store:
        ap.error("--store is required unless --log-only")
    lines, old = read_lines(a.log), read_lines(a.old_log)
    fails = []

    # 0. the pass ran census and ended GREEN (ok / no_change), on a fetcher that prints failures
    status = [m.group(1) for ln in lines
              if (m := re.match(r"^\s+(\S+)\s+census/_all\s*$", ln))]
    print(f"census/_all summary status: {status or 'ABSENT'}")
    if status != ["ok"] and status != ["no_change"]:
        fails.append(f"census/_all did not end ok/no_change in this pass: {status or 'no summary line'}"
                     " - read its unit_state note")
    if not any("RECEIPT window>=" in ln for ln in lines):
        fails.append("no RECEIPT line at all: this log predates the fix (or census never ran)")

    # 2. refusals / failures - visible in the log since the fix prints every failed sub-unit
    n400 = sum("HTTP 400" in ln for ln in lines)
    nunk = sum("unknown predicate variable" in ln for ln in lines)
    nfail = sum(ln.startswith("[census] ") and "SUB-UNIT FAILED" in ln for ln in lines)
    print(f"lines with 'HTTP 400': {n400}; 'unknown predicate variable': {nunk}; "
          f"census SUB-UNIT FAILED: {nfail}")
    if n400 or nunk or nfail:
        fails.append(f"failures in the log (HTTP 400 x{n400}, unknown predicate x{nunk}, "
                     f"SUB-UNIT FAILED x{nfail})")

    # 3. pins
    new_p, old_p = pins_of(lines), pins_of(old)
    if len(old_p) != 45:
        fails.append(f"baseline log has {len(old_p)} pinning lines, expected 45 - wrong --old-log?")
    for flow in sorted(old_p):
        want = ({k: v for k, v in old_p[flow].items() if k not in GEO} if flow in REFUSED
                else old_p[flow])
        got = new_p.get(flow)
        ok = got == want
        print(f"pins {'OK  ' if ok else 'BAD '} {flow:34} {got}")
        if not ok:
            fails.append(f"pins of {flow}: got {got}, expected {want}")

    # 4. not retired yet
    nret = sum("RETIRED UPSTREAM" in ln for ln in lines)
    if nret:
        fails.append(f"{nret} RETIRED UPSTREAM line(s): the BETA flows were not all tailed")

    # 1. receipts, per flow, all 29
    rc = receipts_of(lines)
    print(f"\n{'flow':34} {'window':>10} {'returned':>9} {'stored':>9} {'store now':>9} verdict")
    for flow in REFUSED:
        r = rc.get(flow)
        if r is None:
            print(f"{flow:34} {'-':>10} {'-':>9} {'-':>9} {'-':>9} NO RECEIPT")
            fails.append(f"{flow}: no RECEIPT line")
            continue
        day, ret, _skip, stored, verdict = r
        indep = "n/a" if a.log_only else None
        if not a.log_only:
            try:
                indep = store_rows_since(a.store, flow, day)
            except Exception as e:                           # noqa: BLE001
                indep = None
                fails.append(f"{flow}: store could not be read ({type(e).__name__}: {e})")
        good = (verdict == "MATCH" and stored == str(ret)
                and (a.log_only or (indep is not None and indep == ret)))
        print(f"{flow:34} {day.isoformat():>10} {ret:>9,} {stored:>9} {str(indep):>9} "
              f"{verdict}{'' if good else '  <-- FAIL'}")
        if not good:
            fails.append(f"{flow}: receipt {verdict} returned={ret} stored={stored} store-now={indep}")
    print(f"receipts: {sum(1 for f in REFUSED if f in rc)} of {len(REFUSED)} flows")
    # The 16 flows that already merged on 2026-10-10 are the control group. Their receipts are
    # PRINTED, not gated: the fix does not change their requests (check 3 proves that), and a
    # DIFFER there - a series the publisher dropped from a boundary month - is about that flow,
    # not about the pin fix. Read any DIFFER below before activating the exclusion.
    controls = sorted(set(old_p) - set(REFUSED))
    for flow in controls:
        r = rc.get(flow)
        print(f"control {flow:34} " + ("NO RECEIPT" if r is None else
              f"window>={r[0].isoformat()} returned={r[1]:,} stored={r[3]} {r[4]}"))

    # 5. BETA refresh
    if a.log_only:
        print("\nBETA refresh: NOT CHECKED (--log-only)")
    else:
        try:
            res = beta_vs_stable(a.store)
        except Exception as e:                               # noqa: BLE001
            res = None
            fails.append(f"BETA refresh could not be measured ({type(e).__name__}: {e})")
        for b, v in (res or {}).items():
            if v is None:
                print(f"BETA {b:24} 2025-12: shares no row with its stable twin - NOT COMPARABLE")
            else:
                print(f"BETA {b:24} 2025-12: joined {v[0]:,}, differing {v[1]:,} "
                      f"(before the pass, 2026-10-26 local mirror: {BEFORE[b]:,})"
                      f"{'' if v[1] == 0 else '  <-- FAIL'}")
                if v[1]:
                    fails.append(f"BETA {b}: {v[1]:,} of {v[0]:,} 2025-12 rows differ from the twin")
        if res:
            now = sum(v[1] for v in res.values() if v)
            print(f"BETA 2025-12 rows differing from the stable twin: {now:,} now, "
                  f"{sum(BEFORE.values()):,} before the pass")

    print()
    if fails:
        print(f"PROOF FAIL ({len(fails)}):")
        for f in fails:
            print("  - " + f)
        return 1
    print("PROOF PASS" + (" (log checks only)" if a.log_only else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
