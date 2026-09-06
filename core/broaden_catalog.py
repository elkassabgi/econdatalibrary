"""Catalog the uncataloged uniform-long sources at per-series grain (wave 1).

Self-limiting + crash-proof: streams each source FILE-BY-FILE (never opens hundreds of
files at once -> avoids the Windows handle-exhaustion crash), and DEFERS any source over
a series/file cap (the true giants whose right grain is flow-level, not per-series).
Deferred sources stay generic-resolvable + source-level discoverable; they are LOGGED,
never silently dropped.

Per kept series: series_id=`<source>:<native_key>`, title=native_key (honest — no
fabricated title), start/end = real min/max obs_date, frequency from a freq column if the
source carries one (else null), license from the registry source row. Idempotent per
source (delete+reinsert). Producer-first citations are added afterward by
core/build_series_metadata.py.

  python core/broaden_catalog.py --dry-run     # measure + decide, write nothing
  python core/broaden_catalog.py               # catalog (modifies data/catalog.db)
"""
from __future__ import annotations
import argparse, glob, json, os, sqlite3, time
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
STORE = os.path.join(ROOT, "data", "clean_full")
CATALOG = os.path.join(ROOT, "data", "catalog.db")
OUTDIR = os.path.join(ROOT, "dist", "broaden")
PROTECTED = {"cbs_nl", "gus_dbw", "dbnomics"}

# NEVER catalog a source we are not allowed to host. Sources purged from the catalog on
# 2026-07-22/23 (WTO, cow, polity, sipri, cboe, famafrench, nbp, tcmb, irena, freedomhouse,
# shiller, whr, dbnomics ...) still have their parquet in data/clean_full as an archive, so a
# later `broaden_catalog` run would happily re-catalog them and SILENTLY UNDO the purge.
# Derived from the redistribution gate's own safety floor rather than a second hand-maintained
# list, so the two can never drift.
try:
    from core.gen_denylist import LEGACY_KEEP as _GATE_FLOOR
except ImportError:      # running as a script rather than a module
    import sys as _sys
    _sys.path.insert(0, ROOT)
    from core.gen_denylist import LEGACY_KEEP as _GATE_FLOOR
UNHOSTABLE = set(_GATE_FLOOR)


def _served_ids():
    """The ids the DEPLOYED worker resolves, read from api/worker/src/util.ts.

    Reuses tools/audit_schedule_coverage.supported_sources() instead of re-parsing:
    that function strips line AND block comments before harvesting quoted strings,
    which is the R137 failure shape — util.ts carries retired ids inside comments
    (`// "ksh" RETIRED 2026-07-29`), so a naive quoted-string scan reports them as
    served. It raises SystemExit when the array cannot be located, which is the
    fail-CLOSED behaviour a safety gate needs.
    """
    from tools.audit_schedule_coverage import supported_sources
    return supported_sources()


def _allowed_to_catalog(d, cataloged, served, allow_new):
    """DEFAULT-DENY: may store directory `d` be catalogued at all?

    R819. A fleet-wide `--dry-run` proposed KEEP for 24 uncatalogued store dirs holding
    308,555 series, and every one of them was retired or reserved: the 22 legacy `imf_*`
    ids from the 2026-08-07 retirement wave, `ksh` (withdrawn 2026-08-02 after being
    re-served — R226), and `unctad_cpa` (one of the 38 UNCTAD ids reserved to Ahmed).

    Neither existing gate could see them, and the second reason is the interesting one:
      * `UNHOSTABLE` (= gen_denylist.LEGACY_KEEP) was frozen at the 2026-07-22/23 purge,
        so it lists nothing retired afterwards; and
      * `not_reservable` is computed from a `source ⋈ license` join, but
        `tools/retire_source.py` deletes the `source` row along with the `series` rows —
        so a retired id produces NO row for that join. The more completely a source is
        retired, the blinder that guard becomes.

    A longer id-list cannot fix this: it can only name retirements that already happened,
    and it is blind by construction to a directory like `zillow.pre_zillow_removal.bak`,
    which passed both gates in that same run and was stopped only by SERIES_CAP — a cap
    `--series-cap` overrides, and not a permission gate at all (zillow is the library's
    one recorded live licence breach, R213/R227).

    So the gate is inverted. An uncatalogued store dir is refused unless the deployed
    worker already serves that id, or the operator names it with `--allow-new`. Adding a
    genuinely new source therefore becomes a deliberate act — which is the point, since
    the documented pipeline (Checklist B) catalogues BEFORE util.ts is edited, so a real
    new source is legitimately absent here and must be named.
    """
    return d in cataloged or d in served or d in allow_new


SERIES_CAP = 50_000      # per-source; above this the per-series grain is wrong -> defer
FILE_CAP = 2_000         # ibge (8221 tiny files) etc. -> defer (flow-grain, later wave)
_SKIP = ("__series.parquet",)
_FREQ_COLS = ("freq", "frequency", "FREQ")


def _key_col(cols):
    # 'idbank' is INSEE BDM's native series identifier (insee_bdm parquets).
    for c in ("series_key", "series_id", "idbank"):
        if c in cols:
            return c
    return None


def _scan_source(files, key_col, freq_col):
    """Stream files one at a time; return {key: [min_date, max_date, freq]} or None if
    distinct keys exceed SERIES_CAP (early abort)."""
    agg: dict[str, list] = {}
    cols = [key_col, "obs_date"] + ([freq_col] if freq_col else [])
    for f in files:
        t = pq.read_table(f, columns=cols)
        keys = t.column(key_col).to_pylist()
        dates = t.column("obs_date").to_pylist()
        freqs = t.column(freq_col).to_pylist() if freq_col else [None] * len(keys)
        for k, d, fr in zip(keys, dates, freqs):
            if k is None or d is None:
                continue
            ds_ = d.isoformat() if hasattr(d, "isoformat") else str(d)
            a = agg.get(k)
            if a is None:
                if len(agg) >= SERIES_CAP:
                    return None  # too many -> defer this source
                agg[k] = [ds_, ds_, fr]
            else:
                if ds_ < a[0]: a[0] = ds_
                if ds_ > a[1]: a[1] = ds_
                if a[2] is None and fr is not None: a[2] = fr
    return agg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--source", action="append", help="limit to specific source(s)")
    ap.add_argument("--series-cap", type=int, default=None,
                    help="override SERIES_CAP for this run (e.g. catalog a >50k real source per-series)")
    ap.add_argument("--allow-new", action="append", default=[], metavar="SOURCE_ID",
                    help="catalog a source the deployed worker does not serve yet. REQUIRED for "
                         "a genuinely new source, because the gate is default-deny (R819) — "
                         "repeat the flag per id.")
    a = ap.parse_args()
    if a.series_cap:
        global SERIES_CAP
        SERIES_CAP = a.series_cap
    os.makedirs(OUTDIR, exist_ok=True)

    conn = sqlite3.connect(CATALOG)
    conn.row_factory = sqlite3.Row
    cataloged = {r[0] for r in conn.execute("SELECT DISTINCT source_id FROM series")}
    src_license = {r["source_id"]: r["license_id"] for r in conn.execute("SELECT source_id, license_id FROM source")}

    # Sources whose licence is not verified-redistributable must never be cataloged either.
    # (UNHOSTABLE above covers the PURGED ones, which no longer have a source row at all.)
    not_reservable = {r[0] for r in conn.execute(
        "SELECT s.source_id FROM source s LEFT JOIN license l ON s.license_id = l.license_id "
        "WHERE COALESCE(l.reservable, 0) = 0")}

    # DEFAULT-DENY (R819): what the deployed worker serves is the authoritative local record
    # of what may be catalogued. retire_source.py already removes a retired id from it.
    served = _served_ids()
    allow_new = set(a.allow_new or ())

    todo = []
    skipped_unhostable = []
    refused_unlisted = []
    for d in sorted(os.listdir(STORE)):
        p = os.path.join(STORE, d)
        if not os.path.isdir(p) or d.startswith("_") or d in cataloged or d in PROTECTED:
            continue
        if d in UNHOSTABLE or d in not_reservable:
            skipped_unhostable.append(d)
            continue
        if not _allowed_to_catalog(d, cataloged, served, allow_new):
            refused_unlisted.append(d)
            continue
        if a.source and d not in a.source:
            continue
        files = sorted(f for f in glob.glob(os.path.join(p, "**", "*.parquet"), recursive=True)
                       if not f.endswith(_SKIP))
        if files:
            todo.append((d, files))

    # Resume-safe: load prior progress so a sandbox-killed run continues where it left off.
    spath = os.path.join(OUTDIR, "broaden_summary.json")
    prog = {"kept": [], "deferred": [], "errored": [], "total_new_series": 0}
    if not a.dry_run and os.path.exists(spath):
        try:
            prog = json.load(open(spath))
        except Exception:
            pass
    done = {x["source"] for x in prog["kept"]} | {x["source"] for x in prog["deferred"]} | {x["source"] for x in prog["errored"]}

    def save():
        with open(spath, "w") as f:
            json.dump(prog, f, indent=2)

    for d, files in todo:
        if d in done:
            continue
        t0 = time.time()
        if len(files) > FILE_CAP:
            prog["deferred"].append({"source": d, "why": f"{len(files)} files > {FILE_CAP} (flow-grain, later wave)"})
            print(f"DEFER  {d:20} {len(files)} files", flush=True)
            if not a.dry_run: save()
            continue
        try:
            cols = set(pq.read_schema(files[0]).names)
            kc = _key_col(cols)
            if not kc or "obs_date" not in cols or "value" not in cols:
                prog["errored"].append({"source": d, "why": "not uniform-long"})
                print(f"SKIP   {d:20} not uniform-long", flush=True)
                if not a.dry_run: save()
                continue
            freq_col = next((c for c in _FREQ_COLS if c in cols), None)
            agg = _scan_source(files, kc, freq_col)
            if agg is None:
                prog["deferred"].append({"source": d, "why": f"> {SERIES_CAP:,} series (flow-grain, later wave)"})
                print(f"DEFER  {d:20} >{SERIES_CAP:,} series", flush=True)
                if not a.dry_run: save()
                continue
            if not a.dry_run:
                lic = src_license.get(d)
                conn.execute("DELETE FROM series WHERE source_id=?", (d,))  # idempotent per source
                rows = [(f"{d}:{k}", d, k, (v[2] or None), None, None, None, lic, v[0], v[1], None, "{}")
                        for k, v in agg.items()]
                conn.executemany(
                    "INSERT OR REPLACE INTO series (series_id,source_id,title,frequency,unit,geography,"
                    "category,license_id,start_date,end_date,last_updated,metadata) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    rows)
                conn.commit()  # per-source commit -> resume-safe
            prog["kept"].append({"source": d, "key_col": kc, "n_series": len(agg)})
            prog["total_new_series"] += len(agg)
            print(f"KEEP   {d:20} {len(agg):>7,} series  {round(time.time()-t0,1)}s", flush=True)
            if not a.dry_run: save()
        except Exception as e:
            prog["errored"].append({"source": d, "why": f"{type(e).__name__}: {str(e)[:80]}"})
            print(f"ERROR  {d:20} {type(e).__name__}: {str(e)[:60]}", flush=True)
            if not a.dry_run: save()

    if not a.dry_run:
        # rebuild FTS once all sources are in, so the new series are searchable
        try:
            conn.execute("DELETE FROM series_fts;")
            conn.execute("INSERT INTO series_fts(series_id,title,geography) SELECT series_id,title,geography FROM series;")
            conn.commit()
        except sqlite3.OperationalError:
            pass
        save()
    else:
        with open(spath.replace(".json", "_dryrun.json"), "w") as f:
            json.dump(prog, f, indent=2)
    conn.close()
    nk = len(prog["kept"]); nd = len(prog["deferred"]); ne = len(prog["errored"])
    print(f"\n=== {nk} sources KEPT ({prog['total_new_series']:,} series) | {nd} deferred | {ne} errored ===")
    print("deferred:", ", ".join(x["source"] for x in prog["deferred"]))
    # Never silent: cataloging something we cannot host would undo a deliberate purge.
    if skipped_unhostable:
        print(f"SKIPPED as un-hostable ({len(skipped_unhostable)}): "
              + ", ".join(sorted(skipped_unhostable)))
    # Named, never a bare count: R819's whole failure was reading a refusal total out of a
    # log that had not finished writing it. The ids are the evidence; the number is not.
    if refused_unlisted:
        print(f"REFUSED — not served by the deployed worker and not --allow-new "
              f"({len(refused_unlisted)}): " + ", ".join(sorted(refused_unlisted)))


if __name__ == "__main__":
    main()
