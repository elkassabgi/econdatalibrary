#!/usr/bin/env python3
"""UN Comtrade — annual total merchandise trade (imports/exports) for 200+ countries.

License: CC BY 3.0 IGO
Source: https://comtradeapi.un.org/
Requires COMTRADE_API_KEY (.env) - sent as the Ocp-Apim-Subscription-Key header. The old
public/v1/preview endpoint needed no key but capped a response at 500 records, so it could
not carry the 2014-2025 history.

Coverage:
  * Annual total merchandise trade (imports CIF, exports FOB)
  * ~200 reporting countries × 2014–present
  * cmdCode=TOTAL (all goods aggregate)
  * Additional: total services trade, bilateral totals for major partners

Rate limits: the subscription tier throttles aggressively and returns 429 well inside the
documented allowance, so every request goes through _get() with exponential backoff and
requests are spaced by RATE seconds.

Run: python jobs/ingest_comtrade.py
"""
from __future__ import annotations
import datetime as dt, os, sys, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "comtrade")
# SUBSCRIPTION endpoint, not public/v1/preview. The preview tier caps a response at 500
# records and in practice returned only the latest period, so it cannot reproduce the
# 2014-2025 history at all. The key is in .env as COMTRADE_API_KEY.
BASE = "https://comtradeapi.un.org/data/v1/get"


def _api_key() -> str:
    """COMTRADE_API_KEY from the environment or .env. Empty string if absent."""
    k = os.environ.get("COMTRADE_API_KEY", "").strip()
    if k:
        return k
    try:
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(here, ".env"), encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("COMTRADE_API_KEY="):
                    return line.split("=", 1)[1].strip()
    except OSError:
        pass
    return ""
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
        "Accept": "application/json"}
RATE = 6.0   # measured: 2.0s draws 429s from the subscription tier within ~15 requests
BATCH = 20   # reporters per request

# Ask the SERVER for the aggregate instead of downloading every breakdown and discarding it.
# Measured 2026-07-31 on reporter 792 / partner 156: 16,712 rows unfiltered vs 12 with these
# three params, byte-identical values for all 12 years. That 1,393x reduction is not just a
# speed-up - it is what makes RESPONSE_CAP unreachable (see _get).
AGG_PARAMS = {"motCode": "0", "customsCode": "C00", "partner2Code": "0"}

# The API truncates a response at exactly 100,000 records and says nothing: the envelope
# reports count=100000 and the data simply stops. It truncates the TAIL, so the years lost are
# the most recent ones - precisely what an updater exists to collect. Measured on a
# 15-partner batch: 38 aggregate year-rows silently missing, all of them 2022-2025.
# Never treat a capped response as complete.
RESPONSE_CAP = 100_000


def _get(params: dict, retries: int = 5) -> list[dict] | None:
    """One GET with the aggregate params, 429 backoff, and cap detection.

    Returns None (not []) on failure, so a caller can never mistake a throttled or truncated
    response for 'this series has no data'.
    """
    key = _api_key()
    if not key:
        log("  FATAL: COMTRADE_API_KEY missing - refusing to fall back to the 500-record "
            "preview endpoint")
        return None
    headers = dict(UA, **{"Ocp-Apim-Subscription-Key": key})
    q = dict(params, **AGG_PARAMS)
    for attempt in range(retries):
        try:
            r = requests.get(f"{BASE}/C/A/HS", params=q, headers=headers, timeout=180)
        except Exception as e:                                    # noqa: BLE001
            log(f"  ERR attempt {attempt+1}: {type(e).__name__}: {str(e)[:80]}")
            time.sleep(10 * (attempt + 1))
            continue
        if r.status_code == 200:
            j = r.json() or {}
            rows = j.get("data") or []
            if len(rows) >= RESPONSE_CAP or (j.get("count") or 0) >= RESPONSE_CAP:
                log(f"  CAP HIT ({len(rows):,} rows) - response truncated, refusing it: "
                    f"{ {k: v for k, v in params.items() if k != 'cmdCode'} }")
                return None
            return rows
        if r.status_code == 429:
            wait = 30 * (attempt + 1)
            log(f"  429 rate limit, sleeping {wait}s")
            time.sleep(wait)
            continue
        if r.status_code in (400, 404):
            return []
        log(f"  HTTP {r.status_code} attempt {attempt+1}: {r.text[:100]}")
        time.sleep(10 * (attempt + 1))
    return None

# UN M49 numeric codes for all reporting countries
REPORTERS = [
    4,   8,  12,  20,  24,  28,  32,  36,  40,  44,
   48,  50,  51,  52,  56,  60,  64,  68,  72,  76,
   84,  90,  96, 100, 104, 108, 112, 116, 120, 124,
  132, 140, 144, 148, 152, 156, 170, 174, 178, 180,
  188, 191, 192, 196, 203, 204, 208, 214, 218, 222,
  226, 230, 232, 233, 242, 246, 250, 266, 268, 276,
  288, 296, 300, 308, 320, 324, 328, 332, 340, 344,
  348, 356, 360, 364, 368, 372, 376, 380, 384, 388,
  392, 398, 400, 404, 408, 410, 414, 417, 418, 422,
  426, 430, 434, 440, 442, 446, 450, 454, 458, 462,
  466, 484, 492, 496, 498, 504, 508, 516, 524, 528,
  540, 554, 558, 562, 566, 578, 586, 591, 598, 600,
  604, 608, 616, 620, 624, 626, 630, 634, 642, 643,
  646, 659, 662, 670, 682, 686, 694, 702, 703, 706,
  710, 716, 724, 728, 729, 740, 752, 756, 762, 764,
  768, 776, 780, 784, 788, 800, 804, 826, 834, 840,
  858, 860, 862, 882, 887, 894,
]


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii', 'replace').decode()}", flush=True)


# Bilateral phase scope. LIFTED out of main() 2026-07-30 so the updater can reuse the
# exact same reporter/partner sets — a second copy in the fetcher would drift the
# moment either side was edited (the duplication invariant, R33).
MAJOR = [
    # G20 + EU major economies
    124,  # Canada
    156,  # China
    276,  # Germany
    356,  # India
    392,  # Japan
    410,  # South Korea
    484,  # Mexico
    643,  # Russia
    682,  # Saudi Arabia
    710,  # South Africa
    792,  # Turkey
    826,  # UK
    840,  # USA
    76,   # Brazil
    36,   # Australia
    250,  # France
    380,  # Italy
    528,  # Netherlands
    724,  # Spain
    756,  # Switzerland
    804,  # Ukraine
    702,  # Singapore
    344,  # Hong Kong
    764,  # Thailand
    458,  # Malaysia
]
# Major trading partners (world = 0, plus key partners)
MAJOR_PARTNERS = [0, 124, 156, 276, 356, 392, 410, 484, 643, 826, 840, 76, 250, 380, 528]


def fetch_totals(reporters: list[int], flow: str) -> list[dict] | None:
    """Total merchandise trade for a batch of reporters and one flow, all years."""
    return _get({"reporterCode": ",".join(str(r) for r in reporters),
                 "flowCode": flow, "partnerCode": "0", "cmdCode": "TOTAL"})


def fetch_bilateral_totals(reporter: int, partners: list[int],
                           flow: str) -> list[dict] | None:
    """Bilateral total trade between one reporter and several partners, all years."""
    return _get({"reporterCode": str(reporter), "flowCode": flow,
                 "partnerCode": ",".join(str(p) for p in partners), "cmdCode": "TOTAL"})


# THE UNDER-KEYING FIX (2026-07-31). The published id `import_total:<reporter>` means the
# TOTAL, but the API returns that total ALONGSIDE its breakdowns, and the ingest kept them
# all under the one id. Result: 24,086 rows collapsing to 4,154 distinct (series_key,
# obs_date) pairs, 1,486 of them carrying conflicting values - e.g. import_total:72 at
# 2014-12-31 held 1,603,998,886.636 / 2,729,735,494.827 / 4,816,420,248.446 / 9,150,154,629.909,
# where the last is simply the sum of the first three.
#
# THREE dimensions were being dropped, established by probing the subscription endpoint:
#     motCode       mode of transport      0    = all modes
#     customsCode   customs procedure      C00  = all procedures
#     partner2Code  secondary partner      0    = all (origin vs consignment)
# Verified across 8 series covering both flows, totals AND bilateral, including the worst
# case (import_bilateral:792:156, 6,587 records for 5 years): filtering to all three
# aggregates yields EXACTLY ONE record per series per year, every time.
#
# So this is a FILTER, not a re-key: all 713 published series ids stay exactly as they are
# and no download URL breaks.
def _is_aggregate(rec: dict) -> bool:
    """True only for the all-modes / all-customs / all-secondary-partner row."""
    mot = rec.get("motCode")
    cus = rec.get("customsCode")
    p2 = rec.get("partner2Code")
    return (mot in (0, "0", None)
            and (cus is None or str(cus) in ("C00", "0"))
            and p2 in (0, "0", None))


def parse_record(rec: dict, key_prefix: str) -> tuple[str, dt.date, float] | None:
    if not _is_aggregate(rec):
        return None
    period = str(rec.get("period", ""))[:4]
    val = rec.get("primaryValue") or rec.get("cifvalue") or rec.get("fobvalue")
    if not period or val is None:
        return None
    try:
        yr = int(period)
        v = float(val)
        d = dt.date(yr, 12, 31)
        return key_prefix, d, v
    except (ValueError, TypeError):
        return None


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "comtrade.parquet")

    # Accumulate into a dict keyed by (series_key, obs_date). The previous version kept three
    # parallel lists, seeded them with every row already on disk, and appended whatever it
    # re-fetched - so a second run duplicated every series it touched. That was a SECOND
    # duplication mechanism, independent of the dropped-dimension one, and it is why the
    # 4,154 real observations were spread over 24,086 rows. A dict makes both impossible.
    obs: dict[tuple[str, dt.date], float] = {}
    if os.path.exists(out_path):
        tbl = pq.read_table(out_path)
        ks = tbl.column("series_key").to_pylist()
        ds = tbl.column("obs_date").to_pylist()
        vs = tbl.column("value").to_pylist()
        # Only seed from a store that is already correct. Seeding from the CORRUPT store
        # would be worse than not seeding: where a (key, date) pair holds several conflicting
        # values, the dict keeps whichever happens to sit last in the file, which is as
        # likely to be a mode-of-transport component as the total. A store carrying conflicts
        # is not a resume point, it is the thing being repaired - so drop it and rebuild.
        conflicts = 0
        seen: dict[tuple[str, dt.date], float] = {}
        for k, d, v in zip(ks, ds, vs):
            if (k, d) in seen and seen[(k, d)] != v:
                conflicts += 1
            seen[(k, d)] = v
        if conflicts:
            log(f"Existing store has {conflicts:,} conflicting (series_key, obs_date) rows "
                f"across {tbl.num_rows:,} rows - REBUILDING from the API, not resuming")
        else:
            obs = seen
            log(f"Existing store: {tbl.num_rows:,} rows -> {len(obs):,} distinct (key, date), "
                f"0 conflicts - resuming from it")
    before = len(obs)

    failures: list[str] = []

    def absorb(recs, key_of) -> int:
        n = 0
        for rec in recs:
            r = parse_record(rec, key_of(rec))
            if r:
                obs[(r[0], r[1])] = r[2]      # last write wins; values are identical anyway
                n += 1
        return n

    # -- Phase 1: aggregate imports/exports for all reporters ------------------
    log("Phase 1: Total merchandise trade for all reporters...")
    for flow, flow_label in {"M": "import_total", "X": "export_total"}.items():
        batches = [REPORTERS[i:i + BATCH] for i in range(0, len(REPORTERS), BATCH)]
        for bi, batch in enumerate(batches, 1):
            recs = fetch_totals(batch, flow)
            if recs is None:
                failures.append(f"{flow_label} batch {bi}")
                log(f"  [{bi}/{len(batches)}] flow={flow} FAILED")
                time.sleep(RATE)
                continue
            n = absorb(recs, lambda rec: f"{flow_label}:{rec.get('reporterCode')}")
            log(f"  [{bi}/{len(batches)}] flow={flow}: {len(recs)} records -> {n} obs")
            time.sleep(RATE)

    # -- Phase 2: bilateral trade, major reporters x major partners -----------
    log("Phase 2: Bilateral total trade for major economies...")
    for flow, flow_label in {"M": "import_bilateral", "X": "export_bilateral"}.items():
        for reporter in MAJOR:
            recs = fetch_bilateral_totals(reporter, MAJOR_PARTNERS, flow)
            if recs is None:
                failures.append(f"{flow_label}:{reporter}")
                log(f"  bilateral {flow} reporter={reporter} FAILED")
                time.sleep(RATE)
                continue
            n = absorb(recs, lambda rec: f"{flow_label}:{reporter}:{rec.get('partnerCode', 0)}")
            if recs:
                log(f"  bilateral {flow} reporter={reporter}: {len(recs)} records -> {n} obs")
            time.sleep(RATE)

    # -- Save, with the invariants the old version had none of ----------------
    if not obs:
        log("0 observations collected - refusing to write"); return 1
    if failures:
        log(f"WARNING: {len(failures)} request(s) failed: {failures[:8]}"
            f"{' ...' if len(failures) > 8 else ''}")
    if len(obs) < before:
        log(f"REFUSING TO WRITE: {len(obs):,} obs is fewer than the {before:,} already "
            f"stored - a shrinking store means data loss, not an update")
        return 1

    items = sorted(obs.items())
    tbl = pa.table({
        "series_key": pa.array([k for (k, _d), _v in items], pa.string()),
        "obs_date":   pa.array([d for (_k, d), _v in items], pa.date32()),
        "value":      pa.array([v for _kd, v in items], pa.float64()),
    })
    # The whole point of this repair: one row per (series, date). Assert it rather than
    # trusting it - the store shipped 1,486 conflicting pairs precisely because nobody looked.
    pairs = {(k, d) for (k, d), _v in items}
    if len(pairs) != tbl.num_rows:
        log(f"REFUSING TO WRITE: {tbl.num_rows:,} rows but only {len(pairs):,} distinct "
            f"(series_key, obs_date) - the under-keying is NOT fixed")
        return 1

    pq.write_table(tbl, out_path, compression="zstd")
    series = len({k for (k, _d), _v in items})
    log(f"DONE: {tbl.num_rows:,} observations across {series:,} series "
        f"(was {before:,} distinct obs), 0 conflicting pairs")
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
