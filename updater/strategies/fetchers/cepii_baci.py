"""S5 bulk fetcher — CEPII BACI bilateral trade (cepii.fr, no key).

308,561,322 rows across two HS classifications. The source was REGISTERED with a strategy and a
script and had NO fetcher, so the orchestrator could never run it — and the cost of that is now
visible: jobs/ingest_cepii_baci.py hardcodes `V202401b`, while CEPII's download page today
offers **V202601**. The stored data is two vintages behind and nothing in the system noticed,
because nothing was ever asked to look.

THE VINTAGE IS MEASURED, NOT ASSUMED (the R164 rule cepii_gravity's fetcher records). CEPII's
own product page lists every release as `BACI_HS<nn>_V<yyyymm>[letter].zip`:

    https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37
    -> HS02 HS07 HS12 HS17 HS22 HS92 HS96, all V202601, IDENTICAL across two fetches

So the token is a hash over the (classification, version) pairs actually advertised. It moves
exactly when CEPII publishes, which is what a bulk gate needs: BACI has no delta API and a new
release RESTATES all prior years, so the only honest refresh is a whole re-download — and the
only honest way to avoid doing that nightly is a publisher-side version string.

WHICH CLASSIFICATIONS. The existing store holds HS17 and HS96, so those are what this refreshes;
CEPII offers five more. Adding them is a data decision (more classifications means more of the
same trade flows under different product vocabularies, not new trade), so it is left alone
rather than quietly widened by a fetcher rewrite.

SERVING IS A SEPARATE QUESTION and this does not answer it. The store's schema is
(year, exporter, importer, product, value, quantity) — bilateral flows, no series_key, no
obs_date — so it cannot be catalogued in the current model at any grain without a restructure of
the kind usda, istat and census needed. This fetcher keeps the DATA current; making it reachable
is separate work.

SERVING (added 2026-08-04, task #30): answered by a PAIR-GRAIN TIDY PROJECTION built here,
after each vintage lands. One series = one exporter->importer country pair x one measure:

    BACI:tv:<EXP_ISO3>:<IMP_ISO3>   total trade value    (1000 current USD, summed over HS6)
    BACI:tq:<EXP_ISO3>:<IMP_ISO3>   total trade quantity (metric tons,      summed over HS6)

Grain chosen by ARITHMETIC against D1 headroom (~1.65M rows): series grain is 19.3M ids =
1,172% of headroom (impossible, same verdict as eia #45); exporter x product is 45%; the pair
grain is ~74k ids for both measures = ~4.5%. The PRODUCT DIMENSION IS AGGREGATED AWAY — these
are derived totals, stated as such in every title; BACI's native HS6 grain is not being served.

HS96 ONLY. HS17 and HS96 are the SAME flows under different product vocabularies (see above),
so summing both would double-count 2017-2022. HS96 spans 1996-2022 in one vocabulary — the
longest consistent series — so the projection reads baci_hs96.parquet alone.

Country codes are BACI's UN numerics; ISO3 comes from the country_codes_*.csv INSIDE the
publisher's own zip, extracted at ingest into a _country_codes.json sidecar (blob-published,
so CI and workstation read the same mapping). An unmapped code FAILS the build listing the
codes — a silently dropped country is a wrong total, not a smaller one.
"""
from __future__ import annotations

import hashlib
import os
import re
import sys

import requests

from ... import blob, config
from ...errors import DefinitiveError, TransientError
from ..base import Result
from ._common import Tally, finalize

SOURCE = "cepii_baci"
PAGE = "https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37"
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}
# Only what the store already holds — see the docstring. Widening this is a data decision.
WANT_HS = ("HS17", "HS96")
RE_REL = re.compile(r"BACI_HS(\d{2})_V(\d{6}[a-z]?)")


def _releases(sess=None) -> dict:
    """{'HS17': 'V202601', ...} as advertised on CEPII's own product page."""
    sess = sess or requests.Session()
    try:
        r = sess.get(PAGE, headers=UA, timeout=180)
        r.raise_for_status()
    except Exception as e:                                     # noqa: BLE001
        raise TransientError(f"{SOURCE}: CEPII product page unreachable: {e!r}") from e
    out = {}
    for hs, v in RE_REL.findall(r.text):
        out[f"HS{hs}"] = f"V{v}"
    return out


def current_vintage(unit):
    """Hash over the advertised (classification, version) pairs we actually track.

    None when the page yields nothing — undeterminable, so the strategy fetches (cadence-gated)
    rather than freezing on a token that means "we could not look".
    """
    try:
        rel = _releases()
    except TransientError:
        return None
    pairs = sorted((k, v) for k, v in rel.items() if k in WANT_HS)
    if not pairs:
        return None
    h = hashlib.sha256("|".join(f"{k}={v}" for k, v in pairs).encode()).hexdigest()[:16]
    return f"{SOURCE}:{h}"


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    sess = requests.Session()
    rel = _releases(sess)
    todo = {k: rel[k] for k in WANT_HS if k in rel}
    missing = [k for k in WANT_HS if k not in rel]
    if missing:
        # A classification the store holds is no longer advertised: that is a publisher change,
        # not an empty period, and refusing beats quietly serving a stale half.
        raise DefinitiveError(
            f"{SOURCE}: CEPII no longer advertises {', '.join(missing)} — the page lists "
            f"{', '.join(sorted(rel)) or 'nothing'}. Existing data kept; check the product page "
            f"before changing WANT_HS.")

    sys.path.insert(0, config.ROOT if hasattr(config, "ROOT") else os.getcwd())
    from jobs import ingest_cepii_baci as J                    # download() + ingest_zip()

    tally = Tally()
    total = 0
    for hs, ver in sorted(todo.items()):
        url = f"https://www.cepii.fr/DATA_DOWNLOAD/baci/data/BACI_{hs}_{ver}.zip"
        print(f"[{SOURCE}] {hs} {ver} -> {url}", flush=True)
        try:
            zip_path = J.download(hs, url)
        except Exception as e:                                 # noqa: BLE001
            tally.transient_unit(hs)
            print(f"[{SOURCE}] {hs}: download failed: {e!r}", flush=True)
            continue
        try:
            n = J.ingest_zip(hs, zip_path)
        except Exception as e:                                 # noqa: BLE001
            raise TransientError(f"{SOURCE}: {hs} parse failed: {e!r}") from e
        if not n:
            tally.structural_unit(f"{hs} {ver}: zip parsed 0 rows")
            continue
        if hs == "HS96":
            # the pairs projection maps countries with THIS vintage's own crosswalk
            extract_country_codes(zip_path, out_dir)
        tally.added_unit(n, hs)
        total += n
        print(f"[{SOURCE}] {hs} {ver}: {n:,} rows", flush=True)

    # publish whatever the ingest wrote; it uses its own ParquetWriter, so bytes are already
    # correct on disk and only need to reach R2 (the publish_file case).
    published = 0
    for name in sorted(os.listdir(out_dir)):
        if name.endswith(".parquet") and blob.publish_file(os.path.join(out_dir, name)):
            published += 1
    print(f"[{SOURCE}] published {published} object(s)", flush=True)

    # SERVING SURFACE: rebuild the pair-grain projection from the vintage that just landed.
    # A failed projection must NOT sink the vintage publish (the data is good and stored) —
    # but it must not be silent either: transient -> the run reports partial and the projection
    # is retried next tick, which is exactly the "CSVs stale relative to store" semantics the
    # csv-coherence gate exists for.
    try:
        n_pairs = build_pairs_projection(out_dir)
        print(f"[{SOURCE}] pairs projection rebuilt: {n_pairs:,} tidy rows", flush=True)
    except Exception as e:                                     # noqa: BLE001
        tally.transient_unit(f"pairs projection failed — {type(e).__name__} {str(e)[:70]}")
    return finalize(tally, total, since or None, source=SOURCE)


# --------------------------------------------------------------------------------------- #
# Pair-grain tidy projection (the serving surface — see module docstring).
# --------------------------------------------------------------------------------------- #

PAIRS_BASENAME = "cepii_baci_pairs.parquet"
CODES_SIDECAR = "_country_codes.json"
_HS_FOR_PAIRS = "baci_hs96.parquet"      # HS96 ONLY — longest span, one vocabulary, no
                                         # double-count with HS17 (same flows, different codes)


def extract_country_codes(zip_path: str, out_dir: str) -> dict:
    """numeric UN code -> ISO3, from the country_codes_*.csv INSIDE the publisher's zip.

    Written as a blob sidecar so CI and the workstation read the SAME mapping the vintage
    shipped with, instead of each guessing from whatever file happens to be local (R36 class).
    """
    import csv as _csv
    import io as _io
    import json as _json
    import zipfile as _zipfile
    with _zipfile.ZipFile(zip_path) as z:
        names = [n for n in z.namelist() if "country_codes" in n.lower()]
        if not names:
            raise DefinitiveError(f"{SOURCE}: no country_codes csv inside {zip_path}")
        with z.open(names[0]) as f:
            rows = list(_csv.DictReader(_io.TextIOWrapper(f, encoding="utf-8-sig")))
    mapping = {r["country_code"].strip(): r["country_iso3"].strip()
               for r in rows if r.get("country_code") and r.get("country_iso3")}
    if len(mapping) < 200:
        # 238 in V202401b; a mapping that shrank by an order of magnitude is a schema change,
        # not a smaller world.
        raise DefinitiveError(f"{SOURCE}: country_codes csv parsed only {len(mapping)} rows "
                              f"from {names[0]} — schema changed?")
    blob.write_bytes_atomic(os.path.join(out_dir, CODES_SIDECAR),
                            _json.dumps({"from": os.path.basename(names[0]),
                                         "codes": mapping}, indent=1).encode("utf-8"))
    return mapping


def build_pairs_projection(out_dir: str | None = None) -> int:
    """Stream baci_hs96 via blob (R2-routed) into tidy (series_key, obs_date, value).

    Memory is bounded by construction: per-batch pyarrow group_by partials (each at most the
    batch's distinct pairs), re-aggregated once at the end — the accumulator is pair x year x
    measure, ~2M rows worst case, never the 243M input rows. No duckdb: the updater CI pins
    only pandas/pyarrow/numpy, and abs already OOM-killed a runner once (R45).
    """
    import datetime as _dt
    import json as _json

    import pyarrow as pa
    import pyarrow.compute as pc

    out_dir = out_dir or config.source_dir(SOURCE)
    raw_path = os.path.join(out_dir, _HS_FOR_PAIRS)

    raw_codes = blob.read_bytes(os.path.join(out_dir, CODES_SIDECAR))
    if not raw_codes:
        raise DefinitiveError(
            f"{SOURCE}: {CODES_SIDECAR} sidecar missing — run extract_country_codes() from the "
            f"vintage zip first. Refusing to build ids from guessed country codes.")
    codes = _json.loads(raw_codes.decode("utf-8"))["codes"]

    partials = []
    n_in = 0
    for batch in blob.iter_batches(raw_path,
                                   columns=["year", "exporter", "importer",
                                            "value", "quantity"]):
        n_in += batch.num_rows
        t = pa.Table.from_batches([batch])
        g = (t.group_by(["exporter", "importer", "year"])
              .aggregate([("value", "sum"), ("quantity", "sum")]))
        partials.append(g)
    if not partials:
        raise DefinitiveError(f"{SOURCE}: {_HS_FOR_PAIRS} yielded no batches")
    agg = (pa.concat_tables(partials)
             .group_by(["exporter", "importer", "year"])
             .aggregate([("value_sum", "sum"), ("quantity_sum", "sum")]))

    exp = agg.column("exporter").cast(pa.string()).to_pylist()
    imp = agg.column("importer").cast(pa.string()).to_pylist()
    unmapped = sorted({c for c in set(exp) | set(imp) if c not in codes})
    if unmapped:
        # A silently dropped country is a WRONG total, not a smaller one.
        raise DefinitiveError(
            f"{SOURCE}: {len(unmapped)} country code(s) not in the vintage's own "
            f"country_codes csv: {unmapped[:15]}{'...' if len(unmapped) > 15 else ''} — "
            f"re-extract the sidecar from the current vintage zip before building.")

    years = agg.column("year").to_pylist()
    vals = agg.column("value_sum_sum").to_pylist()
    qtys = agg.column("quantity_sum_sum").to_pylist()

    keys, dates, out_vals = [], [], []
    for e, i, y, v, q in zip(exp, imp, years, vals, qtys):
        d = _dt.date(int(y), 12, 31)                 # family convention (cepii_gravity: 12-31)
        pair = f"{codes[e]}:{codes[i]}"
        if v is not None:
            keys.append(f"BACI:tv:{pair}")
            dates.append(d)
            out_vals.append(float(v))
        if q is not None:
            keys.append(f"BACI:tq:{pair}")
            dates.append(d)
            out_vals.append(float(q))

    tidy = pa.table({"series_key": pa.array(keys, pa.string()),
                     "obs_date": pa.array(dates, pa.date32()),
                     "value": pa.array(out_vals, pa.float64())})
    tidy = tidy.sort_by([("series_key", "ascending"), ("obs_date", "ascending")])
    blob.write_table_atomic(os.path.join(out_dir, PAIRS_BASENAME), tidy)
    print(f"[{SOURCE}] pairs projection: {n_in:,} raw rows -> {tidy.num_rows:,} tidy rows, "
          f"{len(set(keys)):,} series", flush=True)
    return tidy.num_rows
