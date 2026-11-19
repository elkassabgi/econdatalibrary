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

THE RELEASE NEVER REACHED THE STORE (fixed 2026-11-19). This module asked for V202601 by URL,
but jobs/ingest_cepii_baci.download() returned any cached <HS>.zip and ingest_zip() returned
early whenever the parquet existed, so the V202401b rows were re-booked as `ok` and nothing new
was ingested. finalize() also stamps new_vintage="date-tail", so the page token was never stored
and the gate never closed. Now every file carries a release sidecar (_baci_<hs>.release.json,
_country_codes.json "release", _cepii_baci_pairs.release.json); a classification is rebuilt
only when its sidecar differs from the advertised release; rebuilds stage OUTSIDE
data/clean_full and replace only after verification; and the stamped vintage is _token() over
the releases actually in place — the same helper current_vintage() uses, so a finished run
closes the gate.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys

import requests

from ... import blob, config
from ...errors import DefinitiveError, TransientError
from ..base import Result
from ._common import Tally, finalize, merge_cursors

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
        # MAX, not last-wins (2026-11-19): the page can list an older release after the current
        # one, and this value now picks the download URL. V-strings sort lexically
        # (V202401 < V202401b < V202601).
        k, v = f"HS{hs}", f"V{v}"
        if k not in out or v > out[k]:
            out[k] = v
    return out


def _token(pairs) -> str:
    """THE vintage token: one helper for the probe and for the stamp, so they cannot drift."""
    pairs = sorted(pairs)
    h = hashlib.sha256("|".join(f"{k}={v}" for k, v in pairs).encode()).hexdigest()[:16]
    return f"{SOURCE}:{h}"


def current_vintage(unit):
    """Hash over the advertised (classification, version) pairs we actually track.

    None when the page yields nothing — undeterminable, so the strategy fetches (cadence-gated)
    rather than freezing on a token that means "we could not look".
    """
    try:
        rel = _releases()
    except TransientError:
        return None
    pairs = [(k, v) for k, v in rel.items() if k in WANT_HS]
    if not pairs:
        return None
    return _token(pairs)


PAIRS_SIDECAR = "_cepii_baci_pairs.release.json"


def staging_dir() -> str:
    """OUTSIDE data/clean_full (R1949), on the same volume as the store so os.replace is atomic."""
    return os.path.join(os.path.dirname(os.path.abspath(config.DATA_ROOT)), "_staging_cepii_baci")


def raw_dir() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(config.DATA_ROOT)), "raw", SOURCE)


def _inside(child: str, parent: str) -> bool:
    c = os.path.normcase(os.path.abspath(child))
    p = os.path.normcase(os.path.abspath(parent))
    try:
        return os.path.commonpath([c, p]) == p
    except ValueError:
        return False


def _codes_release(out_dir: str):
    raw = blob.read_bytes(os.path.join(out_dir, CODES_SIDECAR))
    try:
        return json.loads(raw.decode("utf-8")).get("release") if raw else None
    except (ValueError, UnicodeDecodeError, AttributeError):
        return None


def _pairs_state(out_dir: str):
    """The pairs sidecar, but only while the pairs file's footer still shows its row count."""
    try:
        with open(os.path.join(out_dir, PAIRS_SIDECAR), encoding="utf-8") as fh:
            st = json.load(fh)
        import pyarrow.parquet as _pq
        rows = _pq.read_metadata(os.path.join(out_dir, PAIRS_BASENAME)).num_rows
    except Exception:                                          # noqa: BLE001
        return None
    return st if rows == st.get("rows") else None


def _pairs_release(out_dir: str):
    st = _pairs_state(out_dir)
    return st.get("release") if st else None


def update(unit, since) -> Result:
    """Install the advertised release of each WANT_HS classification, verified, then project.

    Rebuilds ONLY what is not already at the advertised release (per-file release sidecars, see
    jobs/ingest_cepii_baci.py). The stamped vintage is _token() over the releases actually in
    place, and only when every classification is in place; otherwise the run is partial and the
    orchestrator keeps the old vintage. Order: HS96 replaced -> its own country codes ->
    projection; a failed HS96 never touches the codes or the projection.
    """
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    stage, raw = staging_dir(), raw_dir()
    for d in (stage, raw):
        if _inside(d, config.DATA_ROOT):
            raise DefinitiveError(f"{SOURCE}: {d} is inside the store root {config.DATA_ROOT}; "
                                  f"every reader would list it (R1949)")
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
    rebuilt, codes_refreshed = set(), False
    for hs, ver in sorted(todo.items()):
        url = J.release_url(hs, ver)
        codes_ok = hs != "HS96" or _codes_release(out_dir) == ver
        if J.parquet_current(hs, ver, out_dir) and codes_ok:
            n = (J._read_json(J.sidecar_path(hs, out_dir)) or {}).get("rows", 0)
            tally.empty_unit(f"{hs} {ver} already in place")
            total += n
            print(f"[{SOURCE}] {hs} {ver}: already in place ({n:,} rows)", flush=True)
            continue
        print(f"[{SOURCE}] {hs} {ver} -> {url}", flush=True)
        try:
            zip_path = J.download(hs, url, release=ver, raw_dir=raw, sess=sess)
        except Exception as e:                                 # noqa: BLE001
            tally.transient_unit(f"{hs} {ver} download — {type(e).__name__} {str(e)[:80]}")
            continue
        try:
            out = J.ingest_zip(hs, zip_path, release=ver, out_dir=out_dir, stage_dir=stage)
        except J.ReleaseRefused as e:
            tally.structural_unit(f"{hs} {ver} refused — {str(e)[:160]}")
            continue
        except Exception as e:                                 # noqa: BLE001
            # incl. a reader holding the parquet at replace time: old kept, verified part kept
            tally.transient_unit(f"{hs} {ver} ingest — {type(e).__name__} {str(e)[:80]}")
            continue
        if not out["rows"]:
            tally.structural_unit(f"{hs} {ver}: zip parsed 0 rows")
            continue
        if hs == "HS96":
            # the pairs projection maps countries with THIS release's own crosswalk, written only
            # after HS96 itself is in place
            try:
                extract_country_codes(zip_path, out_dir, release=ver)
                codes_refreshed = True
            except DefinitiveError as e:
                tally.structural_unit(f"HS96 {ver} country codes — {str(e)[:160]}")
                continue
        if out["rebuilt"]:
            rebuilt.add(hs)
            tally.added_unit(out["rows"], hs)
        else:
            tally.empty_unit(f"{hs} {ver} already ingested")
        total += out["rows"]
        print(f"[{SOURCE}] {hs} {ver}: {out['rows']:,} rows "
              f"({'rebuilt' if out['rebuilt'] else 'unchanged'})", flush=True)

    # publish every parquet, unconditionally: a run that replaced a file and died before its
    # publish is healed by the next pass, which finds the file current and skips the ingest.
    published = 0
    for name in sorted(os.listdir(out_dir)):
        if name.endswith(".parquet") and blob.publish_file(os.path.join(out_dir, name)):
            published += 1
    print(f"[{SOURCE}] published {published} object(s)", flush=True)

    in_place = {hs: ver for hs, ver in todo.items() if J.parquet_current(hs, ver, out_dir)}
    hs96_ver = todo.get("HS96")
    hs96_ready = in_place.get("HS96") == hs96_ver and _codes_release(out_dir) == hs96_ver

    # SERVING SURFACE: the pair-grain projection, rebuilt when its release differs from HS96's.
    # A failed projection must NOT sink the publish (the data is good and stored), but it must
    # not be silent either: transient -> partial, no vintage advance, retried next tick.
    # NOT when anything was refused this run (AR, 2026-11-19): finalize() then raises, the cursors
    # never reach the orchestrator, and a projection already moved would read current next run.
    cursors, projected = {}, False
    if (hs96_ready and not tally.structural
            and ("HS96" in rebuilt or codes_refreshed or _pairs_release(out_dir) != hs96_ver)):
        try:
            hs96 = J.parquet_path("HS96", out_dir)
            remote = blob.stored_size(hs96)
            if remote is not None and remote != os.path.getsize(hs96):
                raise TransientError(f"published HS96 is {remote:,} B, local {os.path.getsize(hs96):,} B")
            n_pairs = build_pairs_projection(out_dir)
            # rederive_owed: the served CSVs still hold the previous values until the orchestrator
            # books the full re-derive this run signals. A kill before that booking must not lose
            # the signal, so the flag stays and every later run that reaches update() re-signals
            # until the next release replaces this sidecar.
            J._write_json_atomic(os.path.join(out_dir, PAIRS_SIDECAR),
                                 {"release": hs96_ver, "rows": n_pairs, "rederive_owed": True})
            merge_cursors(cursors, os.path.join(out_dir, PAIRS_BASENAME))
            projected = True
            if "HS96" not in rebuilt:
                # served values were rebuilt (codes refreshed / projection behind HS96) with no new
                # raw rows: a revision, so the run is `ok` and the CSV phase runs
                tally.revised_unit("pairs projection rebuilt")
            print(f"[{SOURCE}] pairs projection rebuilt from {hs96_ver}: {n_pairs:,} tidy rows",
                  flush=True)
        except Exception as e:                                 # noqa: BLE001
            tally.transient_unit(f"pairs projection failed — {type(e).__name__} {str(e)[:70]}")
    elif not tally.structural and not tally.transient:
        st = _pairs_state(out_dir)
        if st and st.get("release") == hs96_ver and st.get("rederive_owed"):
            merge_cursors(cursors, os.path.join(out_dir, PAIRS_BASENAME))
            projected = True
            tally.revised_unit(f"served CSV re-derive owed since the {hs96_ver} projection")

    not_in_place = [f"{hs}={ver}" for hs, ver in sorted(todo.items()) if in_place.get(hs) != ver]
    if in_place.get("HS96") == hs96_ver and _codes_release(out_dir) != hs96_ver:
        not_in_place.append(f"country codes={_codes_release(out_dir)}")
    if hs96_ready and _pairs_release(out_dir) != hs96_ver and not tally.transient:
        not_in_place.append(f"pairs projection={_pairs_release(out_dir)}")
    if not_in_place and not (tally.transient or tally.structural):
        # Never let ok/no_change stand over a store that is not at the advertised release: the
        # strategy would fill in the probed token and seal it (bulk_snapshot_if_changed:70).
        tally.transient_unit(f"not at the advertised release: {', '.join(not_in_place)}")

    res = finalize(tally, total, since or None, source=SOURCE, series_cursors=cursors or None)
    if projected:
        # A new release restates EVERY served pair; the cursor set is capped below the ~90,582
        # served ids, so say the evidence is truncated and the orchestrator books a full re-derive.
        res.cursor_cap_hit = True
    if res.status in ("ok", "no_change") and not not_in_place:
        res.new_vintage = _token(todo.items())
    else:
        res.new_vintage = None
    return res


# --------------------------------------------------------------------------------------- #
# Pair-grain tidy projection (the serving surface — see module docstring).
# --------------------------------------------------------------------------------------- #

PAIRS_BASENAME = "cepii_baci_pairs.parquet"
CODES_SIDECAR = "_country_codes.json"
_HS_FOR_PAIRS = "baci_hs96.parquet"      # HS96 ONLY — longest span, one vocabulary, no
                                         # double-count with HS17 (same flows, different codes)


def extract_country_codes(zip_path: str, out_dir: str, release: str | None = None) -> dict:
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
                            _json.dumps({"from": os.path.basename(names[0]), "release": release,
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
