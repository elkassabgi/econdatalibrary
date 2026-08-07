"""Catalog id  ->  native long parquet rows  (the projection, ARCHITECTURE.md §6).

A bundle is a pure projection over the at-rest store: filter(series in ...) and
read the native long parquet. The store is mid-migration (ARCHITECTURE §7), so the
physical schema is NOT yet uniform across all ~299 sources -- the key column and
the key value differ per source. We keep an explicit per-source resolver registry;
each entry is honest about exactly how that source's parquet is laid out today.

Every resolver returns, for one catalog `series_id`: the native parquet file (or
dir) it lives in, the native key column, and a pyarrow predicate selecting its rows.
Read rows are normalised to canonical tidy ``[series_id, source, obs_date, value]``;
the bundle copies the *native* projected parquet verbatim (so the hash pins the real
bytes a researcher gets).

The 26 sources below `bls`/`worldbank_wdi`/`penn_world_table` were generated from the
adversarially-verified resolver-coverage workflow (each resolver was round-trip
tested against the real store with actual row counts). Three cross-cutting concerns
are applied CENTRALLY in resolve() so the per-source bodies stay untouched:
  * dedup_on  -- ecb/bea replicate byte-identical rows across mirror/table files;
                 drop duplicates on (series_key, obs_date) after the filtered read.
  * stamp_id  -- worldbank_esg/worldbank/hf_equities encode identity in the FILENAME
                 (no in-file key column), so we stamp series_id = the catalog id onto
                 every projected row -> self-describing + round-trippable.
  * tidy_ok   -- relational/wide sources (wikidata/fhfa/census/treasury/hf_equities)
                 have no canonical value column; they ship native-verbatim and are
                 excluded from the tidy frame (never silently — see bundle()).
"""
from __future__ import annotations

import glob
import json
import os
import re  # noqa: F401  (used by some generated resolver bodies)
from dataclasses import dataclass
from typing import Callable

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from . import _catalog

_THIS = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_DATA = os.path.normpath(os.path.join(_THIS, "..", "..", "..", "data", "clean_full"))


def default_data_root() -> str:
    """Root of the at-rest store, overridable via $ECONDL_DATA."""
    return os.environ.get("ECONDL_DATA", _DEFAULT_DATA)


class ResolveError(Exception):
    """Raised when a catalog series cannot be located in the at-rest store.

    Never swallowed: bundle()/pull() surface this loudly (goal #4 -- never silently
    skip a series we cannot satisfy).
    """


@dataclass
class Resolution:
    series_id: str            # canonical catalog id
    source: str               # provider segment
    parquet_path: str         # absolute native file OR directory
    key_col: str              # native key column in that file
    predicate: pc.Expression  # selects this series' rows
    tidy_ok: bool = True      # False -> native-verbatim only, excluded from the tidy frame
    dedup_on: tuple | None = None   # drop duplicate rows on these cols after read
    stamp_id: bool = False    # identity is in the FILENAME -> stamp series_id per row
    # Row identity is a COMPOSITE the store does not hold as a column:
    # (BASE_COLUMN, then each further name appended as `|NAME=value`). Needed by TABLE-GRAIN
    # sources, where one catalog id is a whole table and the rows inside it are distinguished
    # by dimensions the stored key omits. usda is the first: its series_key leaves out
    # REFERENCE_PERIOD_DESC, so six forecast vintages of the same measure share an id and a
    # date until it is appended.
    #
    # The base column is IN the tuple rather than taken from key_col, because read_native sets
    # key_col to the synthesised "series_id" once it has built it -- native_to_tidy reads the
    # id from key_col, so leaving it on the raw key silently produced the unqualified id and
    # the byte-parity check failed on exactly that.
    row_id_from: tuple = ()


# --------------------------------------------------------------------------- #
# Reference resolvers (hand-written, pre-date the coverage workflow).
# --------------------------------------------------------------------------- #

def _resolve_bls(series_id: str, root: str) -> Resolution:
    # catalog: bls:CUUR0000SA0   native file: cu.parquet   key_col: series_id
    bls_id = series_id.split(":", 1)[1]
    stem = bls_id[:2].lower()
    path = os.path.join(root, "bls", f"{stem}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected BLS file {path!r} not found")
    return Resolution(series_id, "bls", path, "series_id", pc.equal(ds.field("series_id"), bls_id))


def _resolve_wdi(series_id: str, root: str) -> Resolution:
    # catalog: worldbank_wdi:AG.CON.FERT.ZS -> one grouped file; native key 'WDI:<ind>:<geo>'.
    indicator = series_id.split(":", 1)[1]
    path = os.path.join(root, "worldbank_wdi", "worldbank_wdi.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected WDI file {path!r} not found")
    return Resolution(
        series_id, "worldbank_wdi", path, "series_key",
        pc.starts_with(ds.field("series_key"), f"WDI:{indicator}:"),
    )


def _resolve_pwt(series_id: str, root: str) -> Resolution:
    # catalog: penn_world_table:rgdpe:USA   native file: rgdpe.parquet   key 'rgdpe|USA'
    parts = series_id.split(":")
    if len(parts) != 3:
        raise ResolveError(f"{series_id}: expected penn_world_table:<var>:<geo>")
    _, var, geo = parts
    path = os.path.join(root, "penn_world_table", f"{var}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected PWT file {path!r} not found")
    return Resolution(
        series_id, "penn_world_table", path, "series_key",
        pc.equal(ds.field("series_key"), f"{var}|{geo}"),
    )


def _resolve_defillama(series_id: str, root: str) -> Resolution:
    # DefiLlama crypto cube. Four catalog kinds, each a different on-disk file:
    #   chain_tvl:<Chain>      -> chains_tvl.parquet,         key == '<Chain>'
    #   protocol_tvl:<slug>    -> tvl_protocol_shard*.parquet, key == '<slug>|__total__'
    #   stablecoins:total_usd  -> stablecoins_total.parquet,  key == '__ALL__'
    #   tvl:total              -> chains_tvl.parquet,         key == '__ALL__'
    # The dir mixes _catalog_*.parquet (different schema), so protocol_tvl opens ONLY
    # the shard files (a list dataset), never the whole directory.
    parts = series_id.split(":", 2)
    if len(parts) != 3:
        raise ResolveError(f"{series_id}: expected defillama:<kind>:<key>")
    _, kind, key = parts
    base = os.path.join(root, "defillama")
    if kind == "chain_tvl":
        path, native = os.path.join(base, "chains_tvl.parquet"), key
    elif kind == "protocol_tvl":
        path = sorted(glob.glob(os.path.join(base, "tvl_protocol_shard*.parquet")))
        native = f"{key}|__total__"
        if not path:
            raise ResolveError(f"{series_id}: no defillama protocol shards in {base!r}")
    elif kind == "stablecoins":
        path, native = os.path.join(base, "stablecoins_total.parquet"), "__ALL__"
    elif kind == "tvl":
        # Was an honest store-coverage error; the gap has since been filled. The
        # fetcher's _chains_tvl_aggregate() writes an '__ALL__' row into
        # chains_tvl.parquet, which IS total TVL across chains -- checked against
        # DefiLlama's own v2/historicalChainTvl, the endpoint this error named:
        # 2026-07-25 and 2026-07-26 agree to the printed precision (75.426 and
        # 75.743 B USD), today differing only by an intraday tick. The error text
        # outlived the gap, so a series we could serve kept 404ing.
        path, native = os.path.join(base, "chains_tvl.parquet"), "__ALL__"
    else:
        raise ResolveError(f"{series_id}: unknown defillama kind {kind!r}")
    if isinstance(path, str) and not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected defillama file {path!r} not found")
    return Resolution(
        series_id, "defillama", path, "series_key",
        pc.equal(ds.field("series_key"), native),
    )


# --------------------------------------------------------------------------- #
# Generated resolvers (resolver-coverage workflow, adversarially verified).
# --------------------------------------------------------------------------- #

# --- sec_edgar -------------------------------------------------------------
def _resolve_sec_edgar(series_id: str, root: str) -> Resolution:
    # catalog: sec_edgar:AIR | sec_edgar:CIK0000001961  ->  ONE parquet PER COMPANY.
    # ident = ticker (e.g. AIR) or zero-padded 10-digit CIK (CIK0000001961) when the
    # filer has no ticker. The WHOLE file is that company's series bundle. Columns:
    # metric, obs_date, value, vintage_date -- `metric` is taxonomy:tag:unit
    # (e.g. us-gaap:Revenues:USD), i.e. a per-row label, NOT a per-series key, so we
    # select the whole file (predicate true). jobs/ingest_sec_edgar.py sanitises the
    # filename (ident.replace("/","_").replace(":","_")). The grouped store currently
    # lives under clean_grouped/sec_edgar/ (one company per file); fall back there if
    # the resolver root has no sec_edgar/ subtree yet (mid-migration, ARCHITECTURE §7).
    ident = series_id.split(":", 1)[1]
    safe = ident.replace("/", "_").replace(":", "_")
    candidates = [
        os.path.join(root, "sec_edgar", f"{safe}.parquet"),
        os.path.join(os.path.dirname(root), "clean_grouped", "sec_edgar", f"{safe}.parquet"),
    ]
    path = next((p for p in candidates if os.path.exists(p)), None)
    if path is None:
        raise ResolveError(
            f"{series_id}: expected sec_edgar file for ident {ident!r}; tried {candidates}"
        )
    return Resolution(
        series_id, "sec_edgar", path, "metric",
        pc.is_valid(ds.field("metric")),  # whole file == this company's bundle
    )


# --- eurostat --------------------------------------------------------------
def _resolve_eurostat(series_id: str, root: str) -> Resolution:
    # catalog: eurostat:aact_ali01  native file: AACT_ALI01.parquet  key_col: series_key
    # One catalog id == one whole Eurostat *flow*; the per-flow file holds many native
    # series_key rows (a 'LAST UPDATE=..:freq=A:..:geo=XX' composite of all dimensions).
    # The catalog id names the flow, not a single composite key, so we take the whole
    # file. File name = the flow code upper-cased, with the 'eurostat:' prefix stripped.
    flow = series_id.split(":", 1)[1]
    path = os.path.join(root, "eurostat", f"{flow.upper()}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected eurostat file {path!r} not found")
    return Resolution(
        series_id, "eurostat", path, "series_key",
        pc.is_valid(ds.field("series_key")),  # whole-flow file: select every row
    )


# --- worldbank_esg ---------------------------------------------------------
def _resolve_worldbank_esg(series_id: str, root: str) -> Resolution:
    # catalog: worldbank_esg:<indicator>:<geo>  e.g. worldbank_esg:EN.ATM.CO2E.PC:AFG
    # native layout: one parquet per indicator (73 files, <indicator>.parquet),
    # schema [country, obs_date, value]; one row per (country, year). The indicator
    # code is dotted (never contains ':'), so the id splits cleanly into 3 parts:
    # indicator -> file (like PWT's var -> file), geo -> equality on native 'country'.
    parts = series_id.split(":")
    if len(parts) != 3:
        raise ResolveError(f"{series_id}: expected worldbank_esg:<indicator>:<geo>")
    _, indicator, geo = parts
    path = os.path.join(root, "worldbank_esg", f"{indicator}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected ESG file {path!r} not found")
    return Resolution(
        series_id, "worldbank_esg", path, "country",
        pc.equal(ds.field("country"), geo),
    )


# --- hf_equities -----------------------------------------------------------
def _resolve_hf_equities(series_id: str, root: str) -> Resolution:
    # catalog: hf_equities:AAPL  ->  native file <root>/hf_equities/AAPL.parquet
    # ONE parquet per ticker; the ticker is encoded in the FILENAME, not a column
    # (native schema is datetime/Open/High/Low/Close/Volume/source/... — no key col).
    # Mirrors catalog metadata r2_key "clean/<TICKER>.parquet" (tier clean_1min).
    ticker = series_id.split(":", 1)[1]
    path = os.path.join(root, "hf_equities", f"{ticker}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected hf_equities file {path!r} not found")
    # Whole file == exactly this one series, so there is nothing to filter on:
    # select every row with an always-true predicate; key_col records the
    # filename-as-identity since no in-file key column exists.
    return Resolution(
        series_id, "hf_equities", path, "__file__",
        pc.scalar(True),
    )


# --- worldbank -------------------------------------------------------------
def _resolve_worldbank(series_id: str, root: str) -> Resolution:
    # catalog: worldbank:NY.GDP.MKTP.CD:AFE  (legacy connector source; 3 indicators
    # FP.CPI.TOTL.ZG / NY.GDP.MKTP.CD / SL.UEM.TOTL.ZS x 263 geos = 692 series).
    #
    # CONSOLIDATED STORE FIRST. The fetcher was migrated off the one-file-per-series
    # layout and now writes a single clean_full/worldbank/worldbank.parquet keyed by
    # `series_key` = the catalog id minus the `worldbank:` prefix. This READER was not
    # migrated with it, so on a runner — where the legacy clean/ tier does not exist —
    # every derive failed ("csv_derive failed 684/684 series") in the same run whose
    # fetcher reported +31,445 new rows. It passed locally only because 692 legacy
    # files still sit on this disk, which is what hid it. The store is what the
    # RESOLVER opens, not what the fetcher writes (ledger R241).
    consolidated = os.path.join(root, "worldbank", "worldbank.parquet")
    if os.path.exists(consolidated):
        return Resolution(
            series_id, "worldbank", consolidated, "series_key",
            pc.equal(ds.field("series_key"), series_id.split(":", 1)[1]),
        )
    # LEGACY FALLBACK: ONE parquet PER SERIES, the catalog id encoded in the FILENAME
    # and in no column — core/storage.py wrote
    #   data/clean/<source>/<series_id with ':' -> '__', '/' -> '_'>.parquet
    # with body cols [obs_date, value, version] only. The file IS the series, so the
    # predicate selects every row and the caller stamps the catalog id. Kept because a
    # machine holding only the old tier must still resolve rather than hard-fail.
    safe = series_id.replace(":", "__").replace("/", "_")
    fname = f"{safe}.parquet"
    candidates = [
        os.path.join(root, "worldbank", fname),
        os.path.join(os.path.dirname(root), "clean", "worldbank", fname),    # legacy clean/ sibling
    ]
    path = next((p for p in candidates if os.path.exists(p)), candidates[0])
    if not os.path.exists(path):
        raise ResolveError(
            f"{series_id}: no World Bank data — neither the consolidated store "
            f"{consolidated!r} nor the legacy per-series file {path!r} exists")
    # key_col 'series_id' is virtual (identity is the filename); predicate = select-all.
    return Resolution(series_id, "worldbank", path, "series_id", pc.scalar(True))


# --- wikidata --------------------------------------------------------------
def _resolve_wikidata(series_id: str, root: str) -> Resolution:
    # catalog: wikidata:Q8093  ->  companies.parquet (grouped reference cube)
    # All 250 catalog wikidata ids are COMPANIES (title carries the ticker), so they
    # always live in companies.parquet. native key_col: series_key (== Wikidata QID),
    # exact match, one row per entity. This is relational/wide REFERENCE data
    # (no obs_date/value) -> the bundle ships the native row verbatim (tidy_ok=False);
    # native_to_tidy must NOT be applied to this source.
    qid = series_id.split(":", 1)[1]
    path = os.path.join(root, "wikidata", "companies.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected wikidata file {path!r} not found")
    return Resolution(
        series_id, "wikidata", path, "series_key",
        pc.equal(ds.field("series_key"), qid),
    )


# --- bea -------------------------------------------------------------------
def _resolve_bea(series_id: str, root: str) -> Resolution:
    # catalog: bea:A191RC:Q  ->  native series_key 'A191RC:Q'  (code:frequency); exact match.
    # BEA store = one parquet per BEA table/cube under bea/<dataset>/, every file sharing
    # schema [series_key, obs_date, value]. Stripping the 'bea:' prefix yields the native
    # series_key verbatim. The catalog id does not encode which table holds the series, and
    # the same series_key is replicated (byte-identically) across several tables, so open the
    # WHOLE bea tree as one dataset and exact-match on series_key. A declarative predicate
    # (no eager per-fragment I/O at resolve time) avoids the Windows handle-exhaustion that
    # fragment scanning triggers, and mirrors read_native's ds.dataset(path).to_table(filter=...).
    native = series_id.split(":", 1)[1]
    path = os.path.join(root, "bea")
    if not os.path.isdir(path):
        raise ResolveError(f"{series_id}: expected BEA dir {path!r} not found")
    return Resolution(
        series_id, "bea", path, "series_key",
        pc.equal(ds.field("series_key"), native),
    )


# --- imf -------------------------------------------------------------------
def _resolve_imf(series_id: str, root: str) -> Resolution:
    # catalog: imf:<dataflow>:<rate_code>:<iso3>   (all series monthly, freq M)
    # The native store is ONE grouped parquet per dataflow+frequency, key_col
    # 'series_key' = the joined SDMX dimension values. The catalog id is a
    # FRIENDLY composite that re-joins into the SDMX key per connectors/imf/
    # connector.py (CPI_SPECS transformation values, MFS_IR_SPECS indicators):
    #   CPI    -> file CPI__M__CPI.parquet, key '<iso3>.CPI._T.<trans>.M'
    #             ('all items' is COICOP _T, NOT CP01; IX->IX, INFL_YOY->YOY_PCH_PA_PT)
    #   MFS_IR -> file MFS_IR__M.parquet,   key '<iso3>.<indicator>.M'
    # Exact (one catalog id == one native series_key); read rows are already the
    # tidy [series_key, obs_date, value, freq] long shape.
    _CPI_TRANS = {"IX": "IX", "INFL_YOY": "YOY_PCH_PA_PT"}
    _MFS_IR_IND = {
        "DEPOSIT":  "MFS135_RT_PT_A_PT",
        "LENDING":  "MFS162_RT_PT_A_PT",
        "MONEYMKT": "MMRT_RT_PT_A_PT",
        "TBILL":    "GSTBILY_RT_PT_A_PT",
        "GOVBOND":  "S13BOND_RT_PT_A_PT",
    }
    parts = series_id.split(":")
    if len(parts) != 4:
        raise ResolveError(f"{series_id}: expected imf:<dataflow>:<rate_code>:<iso3>")
    _, dataflow, rate_code, iso3 = parts
    if dataflow == "CPI":
        trans = _CPI_TRANS.get(rate_code)
        if trans is None:
            raise ResolveError(f"{series_id}: unknown imf CPI rate_code {rate_code!r}")
        fname, native_key = "CPI__M__CPI.parquet", f"{iso3}.CPI._T.{trans}.M"
    elif dataflow == "MFS_IR":
        ind = _MFS_IR_IND.get(rate_code)
        if ind is None:
            raise ResolveError(f"{series_id}: unknown imf MFS_IR rate_code {rate_code!r}")
        fname, native_key = "MFS_IR__M.parquet", f"{iso3}.{ind}.M"
    else:
        raise ResolveError(f"{series_id}: unsupported imf dataflow {dataflow!r}")
    path = os.path.join(root, "imf", fname)
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected IMF file {path!r} not found")
    return Resolution(
        series_id, "imf", path, "series_key",
        pc.equal(ds.field("series_key"), native_key),
    )


# --- ilostat ---------------------------------------------------------------
def _resolve_ilostat_indicator(series_id: str, root: str) -> Resolution:
    """INDICATOR GRAIN. catalog: `ilostat:<stem>` or `ilostat:<stem>#<part>`.

    ilostat is 388,161,420 observations averaging 10.0 per series (~38.7M series), so the unit is
    the indicator-frequency file, not the series -- the same call made for istat (9.2), cso, usda
    and the PxWeb family. 192 of the 1,947 files exceed the size bound and are split on one of
    their own DIMENSION COLUMNS (ref_area, sex, classif1, classif2, source), or on a pair of them
    when no single column divides the file; the choice is made at derive time from that file's
    own data and read back from `_split_map.json`.
    """
    rest = series_id.split(":", 1)[1]
    stem, _, part = rest.partition("#")
    src_dir = os.path.join(root, "ilostat")
    path = os.path.join(src_dir, f"{stem}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: no ilostat indicator file at {path!r}")

    # Match the derive's WHERE clause exactly or byte-parity fails and a suppressed cell is
    # served as `nan`.
    pred = ds.field("value").is_valid() & ds.field("obs_date").is_valid()
    if part:
        smap_path = os.path.join(src_dir, "_split_map.json")
        try:
            with open(smap_path, encoding="utf-8") as fh:
                smap = json.load(fh)
        except (OSError, ValueError) as e:
            raise ResolveError(
                f"{series_id}: names a split part but {smap_path!r} is unreadable ({e!r}); "
                f"the split column is chosen at derive time and cannot be inferred") from e
        entry = smap.get(stem)
        if not entry:
            raise ResolveError(
                f"{series_id}: indicator {stem!r} is not in the split map, so it was never "
                f"split — a '#' part cannot be resolved for it")
        dim = entry["dim"]
        if "+" in dim:
            # Matched as two equalities rather than by rebuilding the '~' concatenation: same
            # predicate, no string-join kernel, and the derive has already verified that neither
            # column's values contain '~'.
            d1, d2 = dim.split("+", 1)
            p1, _, p2 = part.partition("~")
            pred = pred & (ds.field(d1) == p1) & (ds.field(d2) == p2)
        else:
            pred = pred & (ds.field(dim) == part)
    return Resolution(series_id, "ilostat", path, "series_key", pred)


def _resolve_ilostat_any(series_id: str, root: str) -> Resolution:
    """Route between the two id shapes by SEGMENT COUNT, which is a rule here rather than a
    coincidence: every one of the 1,947 file stems matches [A-Za-z0-9_]+ (verified), so a stem
    can never introduce a third colon. 4 segments is the legacy per-country form; 2 is the
    indicator grain.
    """
    if len(series_id.split(":")) == 4:
        return _resolve_ilostat(series_id, root)
    return _resolve_ilostat_indicator(series_id, root)


def _resolve_ilostat(series_id: str, root: str) -> Resolution:
    # catalog:  ilostat:<flow>:<classif1>:<geo>
    #   e.g.    ilostat:UNE_DEAP_SEX_AGE_RT:AGE_YTHADULT_YGE15:USA
    # on disk:  one grouped parquet per indicator-FREQUENCY id, named <flow>_<freq>.parquet
    #   key_col series_key = ilostat:<flow>:<ref_area>:<sex>:<classif1>:<classif2>:<source>
    # The catalog id is a PARTIAL spec: it pins flow + classif1 + ref_area, and every
    # catalogued title is "(Total)" => sex=SEX_T (confirmed in catalog.metadata). The
    # classif2/source dims are NOT pinned, so one catalog id selects every (classif2,
    # source) variant of the Total series for that geo -- a composite predicate over
    # (ref_area, sex, classif1). All catalogued ilostat series are annual ('A').
    parts = series_id.split(":", 3)
    if len(parts) != 4 or parts[0] != "ilostat":
        raise ResolveError(f"{series_id}: expected ilostat:<flow>:<classif1>:<geo>")
    _, flow, classif1, geo = parts
    path = os.path.join(root, "ilostat", f"{flow}_A.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected ILOSTAT file {path!r} not found")
    predicate = (
        pc.equal(ds.field("ref_area"), geo)
        & pc.equal(ds.field("sex"), "SEX_T")
        & pc.equal(ds.field("classif1"), classif1)
    )
    return Resolution(series_id, "ilostat", path, "series_key", predicate)


# --- owid ------------------------------------------------------------------
def _resolve_owid(series_id: str, root: str) -> Resolution:
    # catalog: owid:<slug>:<entity>   native file: <slug>.parquet   key_col: series_key
    # native series_key: '<slug>|<metric>|<entity>' (each OWID slug file holds ONE metric).
    # The catalog id drops the metric segment, so we re-join via prefix+suffix: select the
    # file's rows whose key starts with '<slug>|' AND ends with '|<entity>' (unambiguous
    # because there is exactly one metric per slug file -> exactly one matching series_key).
    parts = series_id.split(":", 2)
    if len(parts) != 3 or parts[0] != "owid":
        raise ResolveError(f"{series_id}: expected owid:<slug>:<entity>")
    _, slug, entity = parts
    path = os.path.join(root, "owid", f"{slug}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected OWID file {path!r} not found")
    predicate = (
        pc.starts_with(ds.field("series_key"), f"{slug}|")
        & pc.ends_with(ds.field("series_key"), f"|{entity}")
    )
    return Resolution(series_id, "owid", path, "series_key", predicate)


# --- fhfa ------------------------------------------------------------------
# catalog code -> native token
_FHFA_FLAVOR = {"po": "purchase-only", "at": "all-transactions", "ed": "expanded-data"}
_FHFA_FREQ = {"M": "monthly", "Q": "quarterly", "A": "annual"}


def _resolve_fhfa(series_id: str, root: str) -> Resolution:
    # TWO ID SHAPES, because the store has two layouts and the original id could only
    # address one of them:
    #
    #   fhfa:<dataset>:<series_key>          the general form — one branch per store file
    #   fhfa:<flavor>:<freq>:<place_id>      legacy, hpi_master only, kept so old links live
    #
    # The general form exists because the legacy one cannot NAME most of the source. It
    # hardcodes hpi_type 'traditional' (true of the 61 catalogued ids, not of the data:
    # hpi_master carries traditional, non-metro, distress-free, manufactured and
    # developmental), and it only ever reaches hpi_master.parquet — 1,178 of the source's
    # 89,706 series. The other 88,528 live in nine per-dataset files (annual_cbsa,
    # annual_tract, annual_zip5, hpi_at_3zip_quarterly, ...) that no id could address at all.
    #
    # Disambiguation is by EXISTENCE, not by counting colons: a series_key may contain both
    # ':' and '|' (hpi_master's are 'traditional|purchase-only|monthly|DV_ENC'), so splitting
    # on ':' cannot tell 'fhfa:po:M:DV_ENC' from 'fhfa:<ds>:<key with a colon>'. If the second
    # segment names a real store file, it is the general form; otherwise the legacy form.
    head = series_id.split(":", 2)
    if len(head) == 3:
        cand = os.path.join(root, "fhfa", f"{head[1]}.parquet")
        if os.path.exists(cand):
            return Resolution(series_id, "fhfa", cand, "series_key",
                              pc.equal(ds.field("series_key"), head[2]))

    parts = series_id.split(":")
    if len(parts) != 4:
        raise ResolveError(
            f"{series_id}: expected fhfa:<dataset>:<series_key> "
            f"(or the legacy fhfa:<flavor>:<freq>:<place_id>)")
    _, flavor, freq, place = parts
    if flavor not in _FHFA_FLAVOR or freq not in _FHFA_FREQ:
        raise ResolveError(
            f"{series_id}: unknown FHFA flavor/freq code {flavor!r}/{freq!r}"
        )
    path = os.path.join(root, "fhfa", "hpi_master.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected FHFA cube {path!r} not found")
    native_key = f"traditional|{_FHFA_FLAVOR[flavor]}|{_FHFA_FREQ[freq]}|{place}"
    return Resolution(
        series_id, "fhfa", path, "series_key",
        pc.equal(ds.field("series_key"), native_key),
    )


# --- ember -----------------------------------------------------------------
_EMBER_METRICS = {
    "A": {
        "gen_total_twh":          ("Electricity generation", "Total", "Total Generation", "TWh"),
        "gen_share_clean_pct":    ("Electricity generation", "Aggregate fuel", "Clean", "%"),
        "gen_share_fossil_pct":   ("Electricity generation", "Aggregate fuel", "Fossil", "%"),
        "gen_solar_twh":          ("Electricity generation", "Fuel", "Solar", "TWh"),
        "gen_wind_twh":           ("Electricity generation", "Fuel", "Wind", "TWh"),
        "emissions_total_mtco2":  ("Power sector emissions", "Total", "Total emissions", "mtCO2"),
        "co2_intensity_gco2_kwh": ("Power sector emissions", "CO2 intensity", "CO2 intensity", "gCO2/kWh"),
    },
    "M": {
        "gen_total_twh":          ("Electricity generation", "Total", "Total Generation", "TWh"),
        "gen_share_clean_pct":    ("Electricity generation", "Aggregate fuel", "Clean", "%"),
        "gen_solar_twh":          ("Electricity generation", "Fuel", "Solar", "TWh"),
        "gen_wind_twh":           ("Electricity generation", "Fuel", "Wind", "TWh"),
        "demand_twh":             ("Electricity demand", "Demand", "Demand", "TWh"),
        "co2_intensity_gco2_kwh": ("Power sector emissions", "CO2 intensity", "CO2 intensity", "gCO2/kWh"),
    },
}
# catalog geo token -> native `geography` / series_key prefix. Ember aggregate
# regions keep their short label ("World", "EU"); ISO countries keep the ISO-3 code.
_EMBER_GEO = {
    "WORLD": "World", "EU": "EU",
    "USA": "USA", "CHN": "CHN", "IND": "IND", "DEU": "DEU",
}
_EMBER_FILE = {"A": "yearly_full_release_long_format.parquet",
               "M": "monthly_full_release_long_format.parquet"}


def _resolve_ember(series_id: str, root: str) -> Resolution:
    # catalog: ember:<A|M>:<metric>:<GEO>   e.g. ember:A:gen_total_twh:WORLD
    # One long-format parquet per frequency; a series is the slice fixing
    # (geography, Category, Subcategory, Variable, Unit). The native key column
    # `series_key` joins those dims as '<geo>|<Category>|<Subcategory>|<Variable>|<Unit>',
    # so we re-synthesise that exact value and match it equal (mapping is composite:
    # split the catalog id, re-join into the native pipe-delimited key).
    parts = series_id.split(":")
    if len(parts) != 4:
        raise ResolveError(f"{series_id}: expected ember:<A|M>:<metric>:<geo>")
    _, freq, metric, geo = parts
    metrics = _EMBER_METRICS.get(freq)
    if metrics is None or metric not in metrics or geo not in _EMBER_GEO:
        raise ResolveError(f"{series_id}: unknown ember freq/metric/geo combination")
    cat, sub, var, unit = metrics[metric]
    native_key = f"{_EMBER_GEO[geo]}|{cat}|{sub}|{var}|{unit}"
    path = os.path.join(root, "ember", _EMBER_FILE[freq])
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected Ember file {path!r} not found")
    return Resolution(
        series_id, "ember", path, "series_key",
        pc.equal(ds.field("series_key"), native_key),
    )


# --- zillow ----------------------------------------------------------------
# Headline curated flow per metric (basename of the catalog source_url). Used only
# as a fallback: the same native series_key recurs across MANY flow cuts (bedroom
# counts, price tiers, smoothing, sfr/condo), so we MUST pin the exact flow file
# rather than the whole source directory, or one catalog id would conflate them.
_ZILLOW_HEADLINE = {
    "zhvi": "Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month",
    "zori": "Metro_zori_uc_sfrcondomfr_sm_month",
}


def _resolve_zillow(series_id: str, root: str) -> Resolution:
    # catalog: zillow:zhvi:102001   (zillow:<metric>:<RegionID>; geo_level dropped)
    # native file: <stem>.parquet   key_col: series_key   value: 'zillow:zhvi:Metro:102001'
    # The catalog id splits into metric + region_id and re-joins as the native key
    # with a constant 'Metro' geo_level segment. The exact curated flow file is
    # pinned from the catalog metadata.source_url (its basename == the parquet stem),
    # falling back to the headline flow for the metric.
    parts = series_id.split(":")
    if len(parts) != 3:
        raise ResolveError(f"{series_id}: expected zillow:<metric>:<region_id>")
    _, metric, region_id = parts
    stem = _ZILLOW_HEADLINE.get(metric)
    row = _catalog.get_series(series_id)
    if row and row.get("metadata"):
        try:
            url = json.loads(row["metadata"]).get("source_url")
            if url:
                stem = os.path.splitext(url.rsplit("/", 1)[-1])[0]
        except (ValueError, TypeError):
            pass
    if not stem:
        raise ResolveError(
            f"{series_id}: cannot determine Zillow flow file for metric {metric!r}"
        )
    path = os.path.join(root, "zillow", f"{stem}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected Zillow file {path!r} not found")
    native_key = f"zillow:{metric}:Metro:{region_id}"
    return Resolution(
        series_id, "zillow", path, "series_key",
        pc.equal(ds.field("series_key"), native_key),
    )


# --- bis -------------------------------------------------------------------
def _resolve_bis(series_id: str, root: str) -> Resolution:
    # catalog: bis:WS_CBPOL:CA   native file: WS_CBPOL.parquet   key_col: series_key   value: 'M.CA'
    # BIS catalog id = bis:<flow>:<country>. The store keeps native key '<freq>.<country>'
    # with freq in {D, M}; every catalog entry is titled "Monthly - End of period",
    # so the catalog id maps to the monthly series -> native key 'M.<country>'.
    parts = series_id.split(":")
    if len(parts) != 3:
        raise ResolveError(f"{series_id}: expected bis:<flow>:<country>")
    _, flow, country = parts
    path = os.path.join(root, "bis", f"{flow}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected BIS file {path!r} not found")
    native_key = f"M.{country}"
    return Resolution(
        series_id, "bis", path, "series_key",
        pc.equal(ds.field("series_key"), native_key),
    )


# --- faostat ---------------------------------------------------------------
def _resolve_faostat(series_id: str, root: str) -> Resolution:
    # catalog: faostat:<FLOW>:<area>:<item>:<element>   e.g. faostat:PP:231:15:5532
    # native series_key is pipe-delimited & POSITIONAL (names interleaved with codes):
    #   <FLOW>|<area>|<area_name>|<item>|<item_name>|<element>|<element_name>[|<period>]
    #   e.g. 'PP|231|United States of America|15|Wheat|5532|Producer Price (USD/tonne)|USD|Annual value'
    # FLOW selects the parquet file; (area,item,element) are matched at their pipe
    # positions (fields 1, 3, 5). A naive '|15|' substring is unsafe: the same item
    # code can appear with other element variants (5530/5531/5539), so we anchor all
    # three codes positionally. Each catalog id maps to exactly ONE native series_key.
    import re
    parts = series_id.split(":")
    if len(parts) != 5:
        raise ResolveError(f"{series_id}: expected faostat:<flow>:<area>:<item>:<element>")
    _, flow, area, item, element = parts
    path = os.path.join(root, "faostat", f"{flow}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected FAOSTAT file {path!r} not found")
    esc = re.escape
    pat = rf"^{esc(flow)}\|{esc(area)}\|[^|]*\|{esc(item)}\|[^|]*\|{esc(element)}\|"
    return Resolution(
        series_id, "faostat", path, "series_key",
        pc.match_substring_regex(ds.field("series_key"), pat),
    )


# --- frankfurter -----------------------------------------------------------
def _resolve_frankfurter(series_id: str, root: str) -> Resolution:
    # catalog: frankfurter:EUR:ARS  ->  one whole-source file frankfurter_fx_eur.parquet
    # native series_key joins base+quote with NO separator: 'EURARS'. key_col: series_key.
    parts = series_id.split(":")
    if len(parts) != 3:
        raise ResolveError(f"{series_id}: expected frankfurter:<base>:<quote>")
    native_key = parts[1] + parts[2]
    path = os.path.join(root, "frankfurter", "frankfurter_fx_eur.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected frankfurter file {path!r} not found")
    return Resolution(
        series_id, "frankfurter", path, "series_key",
        pc.equal(ds.field("series_key"), native_key),
    )


# --- ecb -------------------------------------------------------------------
def _resolve_ecb(series_id: str, root: str) -> Resolution:
    # catalog:  ecb:EXR:D.USD.EUR.SP00.A   native series_key: 'EXR.D.USD.EUR.SP00.A'
    # The catalog id is ecb:<FLOW>:<key>; the native key re-joins flow + key with a
    # dot. Files are split per-flow/per-frequency (two naming conventions on disk,
    # 'ECB__<FLOW>__<freq>' and 'ECB.DISS__<FLOW>_PUB__<freq>', plus YC sub-splits),
    # but every file shares one schema [series_key, obs_date, value, freq]. So we open
    # the whole ecb/ DIRECTORY as one dataset and filter by exact series_key -- no
    # need to reconstruct the physical filename.
    parts = series_id.split(":", 2)
    if len(parts) != 3 or parts[0] != "ecb":
        raise ResolveError(f"{series_id}: expected ecb:<flow>:<key>")
    _, flow, key = parts
    src_dir = os.path.join(root, "ecb")
    if not os.path.isdir(src_dir):
        raise ResolveError(f"{series_id}: expected ECB dir {src_dir!r} not found")
    native_key = f"{flow}.{key}"
    return Resolution(
        series_id, "ecb", src_dir, "series_key",
        pc.equal(ds.field("series_key"), native_key),
    )


# --- oecd ------------------------------------------------------------------
def _resolve_oecd(series_id: str, root: str) -> Resolution:
    # catalog: oecd:<INDICATOR>:<geo>  e.g. oecd:GDP_GROWTH_QOQ:USA, oecd:CPI_YOY:GBR
    # On disk: per-dataflow long parquet files (key_col 'series_key' = full SDMX key,
    # e.g. 'Q.Y.USA.S1.S1.B1GQ._Z._Z._Z.PC.L.G1.T0102'). The catalog id is curated
    # (indicator + ref_area only, per the catalog metadata's {dataflow, ref_area}); each
    # indicator pins exactly ONE SDMX dataflow file + ONE series_key template, with {geo}
    # substituted to give an exact-match native key. Composite: split id, re-join into the
    # full dotted SDMX key via the indicator's template.
    _OECD_TEMPLATES = {
        "GDP_GROWTH_QOQ": (
            "OECD.SDD.NAD__DSD_NAMAIN1@DF_QNA_EXPENDITURE_GROWTH_OECD.parquet",
            "Q.Y.{geo}.S1.S1.B1GQ._Z._Z._Z.PC.L.G1.T0102",
        ),
        "GDP_GROWTH_YOY": (
            "OECD.SDD.NAD__DSD_NAMAIN1@DF_QNA_EXPENDITURE_GROWTH_OECD.parquet",
            "Q.Y.{geo}.S1.S1.B1GQ._Z._Z._Z.PC.L.GY.T0102",
        ),
        "CPI_YOY": (
            "OECD.SDD.TPS__DSD_PRICES@DF_PRICES_ALL.parquet",
            "{geo}.M.N.CPI.PA._T.N.GY",
        ),
    }
    parts = series_id.split(":")
    if len(parts) != 3:
        raise ResolveError(f"{series_id}: expected oecd:<indicator>:<geo>")
    _, indicator, geo = parts
    tmpl = _OECD_TEMPLATES.get(indicator)
    if tmpl is None:
        raise ResolveError(
            f"{series_id}: no OECD series-key template for indicator {indicator!r} "
            f"(known: {', '.join(sorted(_OECD_TEMPLATES))})"
        )
    fname, key_tmpl = tmpl
    native_key = key_tmpl.format(geo=geo)
    path = os.path.join(root, "oecd", fname)
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected OECD file {path!r} not found")
    return Resolution(
        series_id, "oecd", path, "series_key",
        pc.equal(ds.field("series_key"), native_key),
    )


# --- worldbank_pink --------------------------------------------------------
def _resolve_worldbank_pink(series_id: str, root: str) -> Resolution:
    # catalog: worldbank_pink:crude_oil_average -> one commodity, disaggregated in the
    # native store into 3 series (annual nominal price, annual real price, monthly
    # nominal price) spread across per-flow files. Native series_key is
    # '<freq>:<measure>:<commodity>' (freq in a|m; measure is price|price_real for
    # every catalogued commodity). Open the source DIRECTORY as one pyarrow dataset so
    # all per-flow parquet files are scanned together, and anchor the match on
    # ':price:'/':price_real:' so a bare commodity suffix never collides with index
    # names whose tail overlaps (e.g. non_energy vs energy, other_food vs food).
    commodity = series_id.split(":", 1)[1]
    path = os.path.join(root, "worldbank_pink")
    if not os.path.isdir(path):
        raise ResolveError(f"{series_id}: expected Pink Sheet dir {path!r} not found")
    key = ds.field("series_key")
    predicate = (
        pc.ends_with(key, f":price:{commodity}")
        | pc.ends_with(key, f":price_real:{commodity}")
    )
    return Resolution(series_id, "worldbank_pink", path, "series_key", predicate)


# --- usda: the slug-id resolver was REMOVED 2026-08-01 ------------------------
# It took a catalog id like `usda:corn_grain_production_measured_in_bu`, looked its verbatim
# SHORT_DESC out of the catalog title, and filtered the `crops` subdirectory. That worked only
# for the 25 hand-curated rows it was written for, and it was wrong for the source: it ignored
# animals_products, demographics, economics and environmental entirely, and it scoped to one
# sector precisely BECAUSE SHORT_DESC collides across them - which the table-grain id fixes by
# carrying SOURCE_DESC and AGG_LEVEL_DESC alongside it.
#
# It also left a DUPLICATE key in _RESOLVERS once the table-grain resolver was registered, and
# Python silently keeps the last one. A dead resolver plus a shadowed dict entry is exactly the
# kind of thing that reads as working code for a year. See _resolve_usda below.

# --- census ----------------------------------------------------------------
def _resolve_census(series_id: str, root: str) -> Resolution:
    # catalog: census:<flow>:<category_code>:<data_type_code>:<sa|nsa>
    #   e.g. census:marts:44X72:SM:sa  ->  file eits__<flow>.parquet
    # Census = Census Economic Indicators Time Series (EITS). Each flow is one
    # per-flow long parquet (eits__marts, eits__vip, eits__m3, ...). A catalog id
    # selects exactly one native series via the composite tuple
    # (category_code, data_type_code, seasonally_adj); native key_col is
    # `series_key`. The catalog's trailing token sa/nsa maps to seasonally_adj
    # yes/no. data_type_code already distinguishes the value series from its
    # parallel error series (E_*), so no error_data filter is needed.
    parts = series_id.split(":")
    if len(parts) != 5:
        raise ResolveError(
            f"{series_id}: expected census:<flow>:<category>:<data_type>:<sa|nsa>"
        )
    _, flow, category, dtype, sa_tok = parts
    path = os.path.join(root, "census", f"eits__{flow}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected census file {path!r} not found")
    sa = "yes" if sa_tok == "sa" else "no"
    predicate = (
        pc.equal(ds.field("category_code"), category)
        & pc.equal(ds.field("data_type_code"), dtype)
        & pc.equal(ds.field("seasonally_adj"), sa)
    )
    return Resolution(series_id, "census", path, "series_key", predicate)


# --- fed_board -------------------------------------------------------------
def _resolve_fed_board(series_id: str, root: str) -> Resolution:
    # catalog: fed_board:<FLOW>:<series_key>   e.g. fed_board:H15:RIFSPFF_N.B
    #      or: fed_board:<series_key>          (legacy, unqualified)
    #
    # fed_board is sharded into per-release flow files (H15.parquet, G17.parquet, H8.parquet,
    # ...), each with cols [dataset, series_key, obs_date, value, obs_status].
    #
    # THE OLD COMMENT HERE ASSERTED series_key IS "globally unique across flows". IT IS NOT,
    # though it is very nearly so. Measured 2026-08-01 across the OBSERVATION files: 52,293
    # distinct keys, of which 29 appear in two flows — the CP/H15 commercial-paper overlap
    # (`RIFSPPFAAD90_N.B`, `RIFSPPNAAD30_N.B`, ...). The loop below took the FIRST file that
    # contained the key, so for those 29 it silently returned whichever flow sorts earliest:
    # a real series, correctly formatted, from the wrong release.
    #
    # Measure against the OBSERVATION files, not the sidecars. My first pass grouped the
    # `__series.parquet` sidecars by their `dataset` column and got 219 — wrong, because for
    # fed_board that column is a PRESENTATION GROUPING: IP.B50001.A is listed under
    # IP_MAJOR_INDUSTRY_GROUPS, IP_MARKET_GROUPS and IP_SPECIAL_AGGREGATES while its
    # observations live only in G17.parquet. Same series, cross-listed 226 times in total.
    #
    # None of the 21 ids catalogued today are among the 29 (checked), so nothing has been
    # mis-served. But cataloguing the other 52,272 would have walked into it, and a
    # wrong-but-plausible series is the failure nobody reports.
    #
    # So: prefer a FLOW-QUALIFIED id, and when given an unqualified one, resolve it only if it
    # is genuinely unambiguous — otherwise refuse and name the flows, rather than guess.
    src_dir = os.path.join(root, "fed_board")
    flow_files = sorted(
        f for f in glob.glob(os.path.join(src_dir, "*.parquet"))
        if not f.endswith("__series.parquet")
    )
    rest = series_id.split(":", 1)[1]
    flow, native_key = (rest.split(":", 1) + [None])[:2]
    if native_key is not None:
        path = os.path.join(src_dir, f"{flow}.parquet")
        if not os.path.exists(path):
            raise ResolveError(f"{series_id}: no fed_board flow file for {flow!r} "
                               f"(expected {path!r})")
        return Resolution(series_id, "fed_board", path, "series_key",
                          pc.equal(ds.field("series_key"), native_key))

    native_key = rest
    hits = [f for f in flow_files
            if pc.any(pc.equal(ds.dataset(f).to_table(columns=["series_key"])
                               .column("series_key"), native_key)).as_py()]
    if not hits:
        raise ResolveError(
            f"{series_id}: series_key {native_key!r} not found in any fed_board flow file")
    if len(hits) > 1:
        names = ", ".join(os.path.splitext(os.path.basename(f))[0] for f in hits)
        raise ResolveError(
            f"{series_id}: series_key {native_key!r} exists in {len(hits)} fed_board flows "
            f"({names}) — this id names more than one series. Use the flow-qualified form, "
            f"e.g. fed_board:{os.path.splitext(os.path.basename(hits[0]))[0]}:{native_key}.")
    return Resolution(series_id, "fed_board", hits[0], "series_key",
                      pc.equal(ds.field("series_key"), native_key))


# --- dbnomics --------------------------------------------------------------
def _resolve_dbnomics(series_id: str, root: str) -> Resolution:
    # catalog: dbnomics:<PROVIDER>/<DATASET>/<series_key>
    #   e.g. dbnomics:AMECO/ZUTN/USA.1.0.0.0.ZUTN  ,  dbnomics:FED/H15/RIFLGFCY10_N.B
    # On disk: one directory per provider, data/clean_full/dbnomics/<PROVIDER>/,
    # a multi-part parquet dataset with columns
    #   provider, dataset, series_key, obs_date, value, license_id.
    # series_key is unique only WITHIN its dataset, so select on (dataset, series_key).
    # DATASET may contain ':' (e.g. 'WEO:latest') but never '/', and series_key never
    # contains '/', so one maxsplit=2 on '/' recovers (provider, dataset, series_key).
    rest = series_id.split(":", 1)[1]            # PROVIDER/DATASET/series_key
    parts = rest.split("/", 2)
    if len(parts) != 3:
        raise ResolveError(f"{series_id}: expected dbnomics:<provider>/<dataset>/<series_key>")
    provider, dataset_code, series_key = parts
    path = os.path.join(root, "dbnomics", provider)
    if not os.path.isdir(path):
        raise ResolveError(f"{series_id}: expected dbnomics provider dir {path!r} not found")
    predicate = pc.equal(ds.field("dataset"), dataset_code) & \
        pc.equal(ds.field("series_key"), series_key)
    return Resolution(series_id, "dbnomics", path, "series_key", predicate)


# --- boe -------------------------------------------------------------------
def _resolve_boe(series_id: str, root: str) -> Resolution:
    # catalog: boe:IUDBEDR  native file: IUD.parquet  key_col: series_key  value: 'IUDBEDR'
    # BoE flow file = first three chars of the series code; series_key == the code verbatim.
    code = series_id.split(":", 1)[1]
    stem = code[:3]
    path = os.path.join(root, "boe", f"{stem}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected BoE file {path!r} not found")
    return Resolution(
        series_id, "boe", path, "series_key",
        pc.equal(ds.field("series_key"), code),
    )


# --- statcan ---------------------------------------------------------------
def _resolve_statcan_table(series_id: str, root: str) -> Resolution:
    """TABLE GRAIN. catalog: `statcan:<productId>` or `statcan:<productId>#<part>`.

    statcan is 56,845,453,642 observations across ~5,258,059,229 series — 10.81 each — so the
    unit is the StatCan Product ID, which is what Statistics Canada itself names, cites and
    versions. One parquet per cube, so the file IS the table.

    Large cubes are split on one of their own dimension columns (uom, geo, coordinate) or, when
    no single column and no pair divides them, on the COORDINATE HIERARCHY: a statcan coordinate
    is a dot-separated dimension tuple like '59.9.37.1.85.3.100.1400', so truncating it to k
    segments walks the publisher's own nesting. 12100152 (427,009,412 rows) has one distinct uom,
    one status, 14 geos and 17,732,442 coordinates — nothing in between — and divides only that
    way, into 6,514 parts at k=4. The choice per cube lives in _split_map.json beside the store.
    """
    rest = series_id.split(":", 1)[1]
    pid, _, part = rest.partition("#")
    src_dir = os.path.join(root, "statcan")
    path = os.path.join(src_dir, f"{pid}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: no statcan cube at {path!r}")

    # Match the derive's WHERE exactly or byte-parity fails and a suppressed cell serves as nan.
    pred = ds.field("value").is_valid() & ds.field("obs_date").is_valid()
    if part:
        smap_path = os.path.join(src_dir, "_split_map.json")
        try:
            with open(smap_path, encoding="utf-8") as fh:
                smap = json.load(fh)
        except (OSError, ValueError) as e:
            raise ResolveError(
                f"{series_id}: names a split part but {smap_path!r} is unreadable ({e!r}); "
                f"the split column is chosen at derive time and cannot be inferred") from e
        entry = smap.get(pid)
        if not entry:
            raise ResolveError(
                f"{series_id}: cube {pid!r} is not in the split map, so it was never split — "
                f"a '#' part cannot be resolved for it")
        dim = entry["dim"]
        if dim.startswith("coordinate:"):
            # The derive groups on the first k dot-segments joined back with '.', so a row
            # belongs to part `p` iff its coordinate either IS p (the coordinate has k segments
            # or fewer) or begins with p + '.' (it has more). Anchoring on the separator is what
            # stops '59.9' from swallowing '59.91'.
            pred = pred & (pc.starts_with(ds.field("coordinate"), part + ".")
                           | (ds.field("coordinate") == part))
        elif "+" in dim:
            d1, d2 = dim.split("+", 1)
            p1, _, p2 = part.partition("~")
            pred = pred & (ds.field(d1) == p1) & (ds.field(d2) == p2)
        else:
            pred = pred & (ds.field(dim) == part)
    return Resolution(series_id, "statcan", path, "series_key", pred)


def _resolve_statcan_any(series_id: str, root: str) -> Resolution:
    """Route between statcan's two id shapes.

    The legacy form is a bare VECTOR — `statcan:V2132579` — and the table form is a Product ID,
    `statcan:10100001` or `statcan:10100001#<part>`. StatCan vectors always begin with 'V' and
    Product IDs are pure digits, so the first character decides it. That is the publisher's own
    convention, not a coincidence of our ids.
    """
    rest = series_id.split(":", 1)[1] if ":" in series_id else ""
    if rest[:1] in ("v", "V"):
        return _resolve_statcan(series_id, root)
    return _resolve_statcan_table(series_id, root)


def _resolve_statcan(series_id: str, root: str) -> Resolution:
    # catalog: statcan:V2132579   native file: <product_id>.parquet   key_col: series_key
    # native value: 'v2132579' (LOWERCASE 'v').  Store = one Parquet per StatCan cube
    # (productId).  The catalog id is a bare VECTOR with no embedded product, so the
    # owning cube is read from the registry metadata (product_id) -- the single source
    # of truth (the same catalog.db this client already reads for provenance).
    import json
    vec = series_id.split(":", 1)[1]                 # 'V2132579'
    row = _catalog.get_series(series_id)
    if row is None or not row.get("metadata"):
        raise ResolveError(f"{series_id}: no catalog metadata to locate its StatCan cube")
    pid = json.loads(row["metadata"]).get("product_id")
    if pid is None:
        raise ResolveError(f"{series_id}: catalog metadata has no product_id")
    path = os.path.join(root, "statcan", f"{pid}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected StatCan cube {path!r} not found")
    native_key = "v" + vec.lstrip("Vv")              # native series_key is lowercase 'v'
    return Resolution(
        series_id, "statcan", path, "series_key",
        pc.equal(ds.field("series_key"), native_key),
    )


# --- abs -------------------------------------------------------------------
def _resolve_abs(series_id: str, root: str) -> Resolution:
    # catalog: abs:<FLOW>:<series_key>  ->  one parquet per ABS dataflow: <FLOW>.parquet
    # key_col: series_key   value: the full remainder after 'abs:<FLOW>:' (verbatim, dots kept)
    # e.g. abs:CPI:1.10001.10.50.Q     -> CPI.parquet,     series_key == '1.10001.10.50.Q'
    #      abs:LF:M13.3.1599.20.AUS.M  -> LF.parquet,      series_key == 'M13.3.1599.20.AUS.M'
    # Composite: split once on ':' twice -> [src, flow, key]; flow names have no ':' so the
    # rest is the native key. Exact match selects exactly that one SDMX series' rows.
    parts = series_id.split(":", 2)
    if len(parts) != 3:
        raise ResolveError(f"{series_id}: expected abs:<flow>:<series_key>")
    _, flow, native_key = parts
    path = os.path.join(root, "abs", f"{flow}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected ABS flow file {path!r} not found")
    return Resolution(
        series_id, "abs", path, "series_key",
        pc.equal(ds.field("series_key"), native_key),
    )


# --- treasury --------------------------------------------------------------
# Fiscal Data flow -> on-disk parquet stem. Treasury files are named
# "<dataset-slug>__<flow>.parquet"; only two flows carry catalog series today.
_TREASURY_FILES = {
    "avg_interest_rates": "average-interest-rates-treasury-securities__avg_interest_rates.parquet",
    "debt_to_penny": "debt-to-the-penny__debt_to_penny.parquet",
}


def _slug(s: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", s.lower())).strip("_")


def _resolve_treasury(series_id: str, root: str) -> Resolution:
    # catalog: treasury:<flow>:<...>  Two relational/wide Fiscal Data flows.
    #   avg_interest_rates -> long table; tail = slug(security_type_desc):slug(security_desc),
    #     value lives in avg_interest_rate_amt. Predicate selects that security's rows.
    #     Native labels are irregularly punctuated ("Treasury Floating Rate Notes (FRN)",
    #     "Non-marketable"), so we slugify the native columns to match instead of
    #     hardcoding strings, then pin the exact native values in the predicate.
    #   debt_to_penny -> WIDE table, one constant series_key; the last id segment is a
    #     measure COLUMN (tot_pub_debt_out_amt / debt_held_public_amt / intragov_hold_amt),
    #     not a row value. No predicate isolates it, so we ship the whole flow (one row
    #     per date) native-verbatim and the consumer projects the column.
    parts = series_id.split(":")
    if len(parts) < 3 or parts[0] != "treasury":
        raise ResolveError(f"{series_id}: expected treasury:<flow>:<...>")
    flow = parts[1]
    fname = _TREASURY_FILES.get(flow)
    if fname is None:
        raise ResolveError(f"{series_id}: treasury flow {flow!r} has no at-rest file mapping")
    path = os.path.join(root, "treasury", fname)
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected treasury file {path!r} not found")

    if flow == "avg_interest_rates":
        if len(parts) != 4:
            raise ResolveError(f"{series_id}: expected treasury:avg_interest_rates:<type>:<desc>")
        type_slug, desc_slug = parts[2], parts[3]
        tbl = ds.dataset(path).to_table(
            columns=["security_type_desc", "security_desc"]).to_pandas()
        mask = (tbl["security_type_desc"].map(_slug) == type_slug) & \
               (tbl["security_desc"].map(_slug) == desc_slug)
        pairs = tbl[mask].drop_duplicates()
        if pairs.empty:
            raise ResolveError(f"{series_id}: no security matches {type_slug!r}:{desc_slug!r}")
        type_desc = pairs["security_type_desc"].iloc[0]
        sec_desc = pairs["security_desc"].iloc[0]
        predicate = (pc.equal(ds.field("security_type_desc"), type_desc) &
                     pc.equal(ds.field("security_desc"), sec_desc))
        return Resolution(series_id, "treasury", path, "security_desc", predicate)

    # debt_to_penny: wide single-series file; select the whole flow verbatim.
    return Resolution(
        series_id, "treasury", path, "series_key",
        pc.is_valid(ds.field("series_key")),
    )


# --- noaa ------------------------------------------------------------------
def _resolve_noaa(series_id: str, root: str) -> Resolution:
    # catalog: noaa:gsom:USW00094728:TAVG
    # store: per-flow, per-station-prefix files  gsom__<2-char station prefix>.parquet
    #   flow = dataset lowercased (GSOM -> gsom, also gsoy); file = <flow>__<station[:2]>.parquet
    # native key_col 'series_key' value = '<flow>:<station>:<element>'
    #
    # THE NATIVE KEY CARRIES THE DATASET, since the 2026-08-01 re-key. It used to be
    # '<station>:<element>', which named BOTH the monthly and the annual series: 1,046,291 of
    # 2,089,582 ids appeared in gsom and gsoy alike, so one id served a monthly series with its
    # own annual aggregates mixed in. There was never a (key, date) collision to reveal it -
    # gsom stamps month-start and gsoy year-end - so it survived every duplicate check.
    #
    # Old uppercase ids keep working on purpose: `flow` is lowercased before it is used to build
    # the key, so a bookmarked noaa:GSOM:USW00094728:TAVG resolves to gsom:USW00094728:TAVG and
    # still returns its series.
    parts = series_id.split(":")
    if len(parts) != 4 or parts[0] != "noaa":
        raise ResolveError(f"{series_id}: expected noaa:<dataset>:<station>:<element>")
    _, dataset, station, element = parts
    flow = dataset.lower()
    path = os.path.join(root, "noaa", f"{flow}__{station[:2]}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected NOAA file {path!r} not found")
    native_key = f"{flow}:{station}:{element}"
    return Resolution(
        series_id, "noaa", path, "series_key",
        pc.equal(ds.field("series_key"), native_key),
    )



# --- usda ------------------------------------------------------------------
def _resolve_usda(series_id: str, root: str) -> Resolution:
    """TABLE GRAIN. catalog: usda:<SOURCE_DESC>|<AGG_LEVEL_DESC>|<SHORT_DESC>

    usda averages 3.7 observations per series across 15,534,339 series, so it is served as
    72,046 tables rather than 15.5 million near-trivial CSVs (the cso / insee_melodi
    precedent). One catalog id therefore selects a SET of rows by three descriptive columns,
    not one row by a key.

    ALL FILES, NOT ONE. 61,644 of the 72,046 tables (86%) have rows in more than one parquet -
    ('CENSUS', 'HOGS - SALES, MEASURED IN $', 'COUNTY') is spread over 11 - so resolving to a
    single file would silently return a fragment of the table.

    row_id_from appends REFERENCE_PERIOD_DESC, which the stored series_key omits even though it
    is a real dimension: Maryland winter wheat area harvested for 2020 carries six values under
    one key (MAY/JUN/JUL/AUG FORECAST, JUN ACREAGE, and the final YEAR estimate). Without it a
    table CSV would hold six numbers on the same id and date with no way to tell a forecast from
    the final figure.

    maxsplit=2 on '|' so a SHORT_DESC containing a pipe still lands whole in the last part.
    """
    rest = series_id.split(":", 1)[1]
    parts = rest.split("|", 2)
    if len(parts) != 3:
        raise ResolveError(
            f"{series_id}: expected usda:<SOURCE_DESC>|<AGG_LEVEL_DESC>|<SHORT_DESC>")
    src_desc, agg, short = parts
    src_dir = os.path.join(root, "usda")
    files = sorted(f for f in glob.glob(os.path.join(src_dir, "**", "*.parquet"),
                                        recursive=True)
                   if not f.endswith(_GENERIC_SKIP))
    if not files:
        raise ResolveError(f"{series_id}: no parquet under {src_dir!r}")
    # `&` on Expressions, NOT pc.and_ — pyarrow has no compute function by that name for
    # dataset expressions and raises ArrowKeyError("No function registered with name: and_")
    # at read time, not at build time, so it looks fine until something actually resolves.
    pred = ((ds.field("SOURCE_DESC") == src_desc)
            & (ds.field("AGG_LEVEL_DESC") == agg)
            & (ds.field("SHORT_DESC") == short)
            # DROP NULL value/date, matching the derive's WHERE clause exactly. usda stores
            # suppressed and not-yet-published cells as NULL, and without this the resolver
            # emitted one extra row per such cell carrying `nan` where the served CSV has
            # nothing — a one-row difference that fails byte-parity and would put "nan" in
            # front of a reader as if it were an observation.
            & ds.field("value").is_valid()
            & ds.field("obs_date").is_valid())
    return Resolution(series_id, "usda", files, "series_key", pred,
                      row_id_from=("series_key", "REFERENCE_PERIOD_DESC"))



# --- istat -----------------------------------------------------------------
def _resolve_istat(series_id: str, root: str) -> Resolution:
    """FLOW GRAIN. catalog: `istat:<flow>` or `istat:<flow>#<part>` for a split flow.

    istat is 398,619,720 observations across 43,564,079 series (9.2 each), so it is served as
    ~1,226 flows rather than 43.5 million CSVs. Flows over the size bound are split on ONE named
    dimension of their own keys, chosen at DERIVE time from that flow's data — so the choice is
    read back from `_split_map.json` in the store. Without that sidecar `istat:101_1015#ART` is
    an id whose meaning died with the process that wrote it.

    '#' is the separator because it cannot occur in an ISTAT flow id or a dimension value, while
    ':' and '|' both appear inside the keys.
    """
    rest = series_id.split(":", 1)[1]
    flow, _, part = rest.partition("#")
    src_dir = os.path.join(root, "istat")
    path = os.path.join(src_dir, f"{flow}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: no istat dataflow file at {path!r}")

    # value/date NULLs are dropped by the derive's WHERE clause; match it or byte-parity fails
    # and a suppressed cell is served as `nan`.
    pred = ds.field("value").is_valid() & ds.field("obs_date").is_valid()
    if part:
        smap_path = os.path.join(src_dir, "_split_map.json")
        try:
            with open(smap_path, encoding="utf-8") as fh:
                smap = json.load(fh)
        except (OSError, ValueError) as e:
            raise ResolveError(
                f"{series_id}: names a split part but {smap_path!r} is unreadable ({e!r}); "
                f"the split dimension is chosen at derive time and cannot be inferred") from e
        entry = smap.get(flow)
        if not entry:
            raise ResolveError(
                f"{series_id}: flow {flow!r} is not in the split map, so it was never split — "
                f"a '#' part cannot be resolved for it")
        dim, trunc = entry["dim"], entry.get("trunc") or 0

        def _dim_value(d: str):
            # extract_regex yields a struct; compare its field.
            return pc.struct_field(
                pc.extract_regex(ds.field("series_key"), pattern=f"{d}=(?P<v>[^:]*)"), "v")

        if "+" in dim:
            # PAIRED SPLIT. Three flows (183_277 at 52.9M rows among them) have no single
            # dimension wide enough to divide them, so the derive crosses two and labels the part
            # `<v1>~<v2>`. Matched as two separate equalities rather than by rebuilding the
            # concatenation: it is the same predicate, needs no string-join kernel, and the
            # derive has already verified that neither dimension's values contain '~'.
            d1, d2 = dim.split("+", 1)
            p1, _, p2 = part.partition("~")
            pred = pred & (_dim_value(d1) == p1) & (_dim_value(d2) == p2)
        else:
            # utf8_slice_codeunits applies the same truncation the derive used, so a truncated
            # part id round-trips exactly.
            got = _dim_value(dim)
            if trunc:
                got = pc.utf8_slice_codeunits(got, 0, trunc)
            pred = pred & (got == part)
    return Resolution(series_id, "istat", path, "series_key", pred)



# --- census (table grain) ---------------------------------------------------
def _resolve_census_table(series_id: str, root: str) -> Resolution:
    """TABLE GRAIN. catalog: `census:<table>` or `census:<table>#<part>`.

    census is 80 WIDE tables (11-238 columns, measures as columns) over 44,242,170 rows. Cell
    grain would be 1,202,396,034 nominal series, so a table is the unit -- and 70 of the 80 are
    genuine PANELS (>3 periods), not the single-period censuses two early samples suggested.

    THIS COEXISTS WITH THE OLDER SERIES-GRAIN `census:<flow>:<cat>:<dtype>:<sa|nsa>` ids, which
    address individual EITS series inside eits__<flow>.parquet. They are told apart the same way
    fhfa's two shapes are: if the second segment names a real store file, this is the table form.
    Counting colons would not work -- the older form has four segments and a table name can
    contain neither, but relying on that is a coincidence rather than a rule.

    Served WIDE, verbatim: no tidy projection, because the measures ARE the columns and melting
    them here would disagree with what the derive writes.
    """
    rest = series_id.split(":", 1)[1]
    table, _, part = rest.partition("#")
    path = os.path.join(root, "census", f"{table}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: no census table file at {path!r}")
    pred = ds.field("obs_date").is_valid()
    if part:
        smap_path = os.path.join(root, "census", "_split_map.json")
        try:
            with open(smap_path, encoding="utf-8") as fh:
                smap = json.load(fh)
        except (OSError, ValueError) as e:
            raise ResolveError(
                f"{series_id}: names a split part but {smap_path!r} is unreadable ({e!r})") from e
        entry = smap.get(table)
        if not entry:
            raise ResolveError(
                f"{series_id}: table {table!r} is not in the split map, so it was never split")
        # dims is a LIST — census tables can need a COMPOSITE split (trade data is naturally
        # commodity x geography, and six tables had no single dimension that divided them below
        # the bound). The part id joins the values with `sep`, so rebuild the same expression.
        dims = entry.get("dims") or ([entry["dim"]] if entry.get("dim") else [])
        if not dims:
            raise ResolveError(f"{series_id}: split map entry for {table!r} names no dimension")
        sep = entry.get("sep", "~")
        got = None
        for d in dims:
            # "NAME" or "NAME:n" — the second form is a TRUNCATION, which is what trade data
            # needs: intltrade__exports__hs splits on E_COMMODITY truncated to 3 characters
            # (275 parts) because HS codes are hierarchical and the full code has 18,511 values.
            name, _, tr = d.partition(":")
            v = pc.struct_field(
                pc.extract_regex(ds.field("series_key"), pattern=f"{name}=(?P<v>[^|]*)"), "v")
            if tr:
                v = pc.utf8_slice_codeunits(v, 0, int(tr))
            got = v if got is None else pc.binary_join_element_wise(got, v, sep)
        pred = pred & (got == part)
    return Resolution(series_id, "census", path, "series_key", pred, tidy_ok=False)


def _resolve_census_any(series_id: str, root: str) -> Resolution:
    """Route between census's two id shapes by whether segment 2 names a store file."""
    rest = series_id.split(":", 1)[1]
    table = rest.split("#", 1)[0].split(":", 1)[0]
    if os.path.exists(os.path.join(root, "census", f"{table}.parquet")):
        return _resolve_census_table(series_id, root)
    return _resolve_census(series_id, root)


# --- eia -------------------------------------------------------------------
def _resolve_eia(series_id: str, root: str) -> Resolution:
    # catalog: eia:PET.RWTC.D  native file: PET.parquet  key_col: series_id  value: 'PET.RWTC.D'
    # The eia store is one parquet per EIA flow; the flow is the first dot-segment of the
    # EIA id (e.g. PET, NG, ELEC). The catalog id is the EIA id prefixed with 'eia:';
    # the native 'series_id' column holds the EIA id verbatim WITHOUT that prefix. Exact match.
    native = series_id.split(":", 1)[1]
    flow = native.split(".", 1)[0]
    path = os.path.join(root, "eia", f"{flow}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: expected EIA file {path!r} not found")
    return Resolution(
        series_id, "eia", path, "series_id",
        pc.equal(ds.field("series_id"), native),
    )


# --------------------------------------------------------------------------- #
# Registry + central cross-cutting policy.
# --------------------------------------------------------------------------- #


def _resolve_cepii_baci(series_id: str, root: str) -> Resolution:
    """cepii_baci: serve ONLY the pair-grain tidy projection, never the raw vintage files.

    The store dir holds the raw BACI vintages (baci_hs17/hs96.parquet — schema
    year/exporter/importer/product/value/quantity) BESIDE the tidy cepii_baci_pairs.parquet
    the fetcher projects from HS96. The generic resolver globs every parquet in the dir and
    would refuse on the raw schema (or worse, scan 243M raw rows per request), so this pins
    the exact serving artifact. Keys: BACI:tv:<EXP>:<IMP> / BACI:tq:<EXP>:<IMP> — product
    dimension aggregated away, stated in every catalogue title.
    """
    src, native = series_id.split(":", 1)
    path = os.path.join(root, "cepii_baci", "cepii_baci_pairs.parquet")
    if not os.path.exists(path):
        raise ResolveError(
            f"{series_id}: pairs projection missing at {path!r} — the fetcher builds it "
            "after each vintage (build_pairs_projection); it must exist before serving.")
    pred = pc.equal(ds.field("series_key"), native)
    return Resolution(series_id, src, path, "series_key", pred)



def _resolve_imf_imts_direct(series_id: str, root: str) -> Resolution:
    """imf_imts_direct: TABLE grain — catalog id names COUNTRY x FREQ x INDICATOR, the store
    keys one row per PARTNER series (5 parts: COUNTRY.COUNTERPART.FREQ.INDICATOR.METHODOLOGY).

    The counterpart sits MID-key, so the PxWeb pure-prefix _FLOW_GRAIN mechanism cannot match;
    the predicate is prefix + suffix instead, exact because IMF codes never contain dots:
        id  imf_imts_direct:IMTS:USA.M.MG_CIF_USD
        key IMTS:USA.<COUNTERPART>.M.MG_CIF_USD.IMTS
    Grain decided by arithmetic 2026-08-05 (#104): 472,234 series vs a D1 at 9.31 GB of 10 —
    series grain would have consumed the library's entire remaining catalogue budget; 2,937
    table rows serve all of them.
    """
    src, native = series_id.split(":", 1)
    country, freq, ind = native.split(":", 1)[1].split(".")
    path = os.path.join(root, src, f"{src}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: store missing at {path!r}")
    flow = native.split(":", 1)[0]
    pred = (pc.starts_with(ds.field("series_key"), f"{flow}:{country}.")
            & pc.ends_with(ds.field("series_key"), f".{freq}.{ind}.{flow}"))
    return Resolution(series_id, src, path, "series_key", pred)



def _resolve_imf_pip_direct(series_id: str, root: str) -> Resolution:
    """imf_pip_direct: TABLE grain with the table dims MID-KEY (positions 4-6 of 7).

    Key: PIP:<ACC>.<CP_COUNTRY>.<CP_SECTOR>.<COUNTRY>.<FREQ>.<INDICATOR>.<SECTOR>.
    Prefix/suffix cannot anchor here - counterpart-country codes share the COUNTRY
    vocabulary, so a dotted-substring match could align at the wrong window. The
    predicate is a POSITION-EXACT regex over the whole key. NOT the World Bank's
    `pip` source - four shared letters, nothing else.
    """
    src, native = series_id.split(":", 1)
    flow, rest = native.split(":", 1)
    country, freq, ind = rest.split(".")
    path = os.path.join(root, src, f"{src}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: store missing at {path!r}")
    rx = ("^" + re.escape(flow) + ":" + r"[^.]+\.[^.]+\.[^.]+\." +
          re.escape(country) + r"\." + re.escape(freq) + r"\." +
          re.escape(ind) + r"\.[^.]+$")
    pred = pc.match_substring_regex(ds.field("series_key"), rx)
    return Resolution(series_id, src, path, "series_key", pred)



def _resolve_imf_dip_direct(series_id: str, root: str) -> Resolution:
    """imf_dip_direct: TABLE grain with the table dims MID-KEY (positions 2/4/5 of 5).

    Key: DIP:<COUNTERPART_COUNTRY>.<COUNTRY>.<DV_TYPE>.<FREQ>.<INDICATOR> - exactly
    5 parts, counterpart FIRST. Same trap as imf_pip_direct: counterpart-country
    codes share the COUNTRY vocabulary, so a dotted-substring match could align
    USA-as-counterpart into the country slot. POSITION-EXACT regex over the whole
    key; note there is NO trailing part after INDICATOR.
    """
    src, native = series_id.split(":", 1)
    flow, rest = native.split(":", 1)
    country, freq, ind = rest.split(".")
    path = os.path.join(root, src, f"{src}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: store missing at {path!r}")
    rx = ("^" + re.escape(flow) + ":" + r"[^.]+\." +
          re.escape(country) + r"\.[^.]+\." + re.escape(freq) + r"\." +
          re.escape(ind) + "$")
    pred = pc.match_substring_regex(ds.field("series_key"), rx)
    return Resolution(series_id, src, path, "series_key", pred)



def _resolve_imf_mfs_tables(series_id: str, root: str) -> Resolution:
    """imf_mfs*_direct family: TABLE grain COUNTRY x FREQ, dims at the key PREFIX.

    Store keys are <FLOW>:<COUNTRY>.<FREQ>.<...tail>; the catalog id carries only
    the first two parts, so a starts_with over "FLOW:C.F." is position-exact - the
    trailing dot stops a short freq code from matching a longer one (A vs AB) and
    the string start anchors COUNTRY. Measured, not assumed: position-to-dim
    mapping was proven against each flow's own codelists before cataloguing.
    """
    src, native = series_id.split(":", 1)
    flow, rest = native.split(":", 1)
    country, freq = rest.split(".")
    path = os.path.join(root, src, f"{src}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: store missing at {path!r}")
    pred = pc.starts_with(ds.field("series_key"), f"{flow}:{country}.{freq}.")
    return Resolution(series_id, src, path, "series_key", pred)



def _resolve_imf_gsli_direct(series_id: str, root: str) -> Resolution:
    """imf_gsli_direct: TABLE grain with COUNTRY.FREQ MID-KEY (positions 3-4 of 11).

    GS_LI's alphabetical dim order puts AGE_GROUP and AGGREGATION_TYPE ahead of
    COUNTRY, so the family's starts_with prefix cannot anchor here - the DIP/PIP
    position-exact class applies instead, with the part count pinned at 11 so a
    key from any other shape cannot alias in.
    """
    src, native = series_id.split(":", 1)
    flow, rest = native.split(":", 1)
    country, freq = rest.split(".")
    path = os.path.join(root, src, f"{src}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: store missing at {path!r}")
    rx = ("^" + re.escape(flow) + ":" + r"[^.]*\.[^.]*\." +
          re.escape(country) + r"\." + re.escape(freq) +
          r"(\.[^.]*){7}$")
    pred = pc.match_substring_regex(ds.field("series_key"), rx)
    return Resolution(series_id, src, path, "series_key", pred)


def _resolve_imf_qgfs_direct(series_id: str, root: str) -> Resolution:
    """imf_qgfs_direct: TABLE grain with COUNTRY.FREQ MID-KEY (positions 2-3 of 7).

    QGFS's alphabetical dim order puts ACCOUNTS ahead of COUNTRY (ACCOUNTS.COUNTRY.
    FREQUENCY.IFS_FLAG.INDICATOR.METHODOLOGY.SECTOR, sidecar-recorded), so the
    family's starts_with prefix cannot anchor - the GS_LI position-exact class
    applies, with the part count pinned at 7 so no other key shape can alias in.
    """
    src, native = series_id.split(":", 1)
    flow, rest = native.split(":", 1)
    country, freq = rest.split(".")
    path = os.path.join(root, src, f"{src}.parquet")
    if not os.path.exists(path):
        raise ResolveError(f"{series_id}: store missing at {path!r}")
    rx = ("^" + re.escape(flow) + ":" + r"[^.]*\." +
          re.escape(country) + r"\." + re.escape(freq) +
          r"(\.[^.]*){4}$")
    pred = pc.match_substring_regex(ds.field("series_key"), rx)
    return Resolution(series_id, src, path, "series_key", pred)


_RESOLVERS: dict[str, Callable[[str, str], Resolution]] = {
    "bls": _resolve_bls,
    "cepii_baci": _resolve_cepii_baci,
    "imf_imts_direct": _resolve_imf_imts_direct,
    "imf_pip_direct": _resolve_imf_pip_direct,
    "imf_dip_direct": _resolve_imf_dip_direct,
    "imf_mfsdc_direct": _resolve_imf_mfs_tables,
    "imf_mfsma_direct": _resolve_imf_mfs_tables,
    "imf_mfsofc_direct": _resolve_imf_mfs_tables,
    "imf_mfsfmp_direct": _resolve_imf_mfs_tables,
    "imf_mfsir_direct": _resolve_imf_mfs_tables,
    "imf_bopagg_direct": _resolve_imf_mfs_tables,
    "imf_psbs_direct": _resolve_imf_mfs_tables,
    "imf_gsli_direct": _resolve_imf_gsli_direct,
    "imf_qgfs_direct": _resolve_imf_qgfs_direct,
    "imf_ctot_direct": _resolve_imf_mfs_tables,
    "imf_er_direct": _resolve_imf_mfs_tables,
    "worldbank_wdi": _resolve_wdi,
    "penn_world_table": _resolve_pwt,
    "defillama": _resolve_defillama,
    "sec_edgar": _resolve_sec_edgar,
    "eurostat": _resolve_eurostat,
    "worldbank_esg": _resolve_worldbank_esg,
    "hf_equities": _resolve_hf_equities,
    "worldbank": _resolve_worldbank,
    "wikidata": _resolve_wikidata,
    "bea": _resolve_bea,
    "imf": _resolve_imf,
    "ilostat": _resolve_ilostat_any,
    "owid": _resolve_owid,
    "fhfa": _resolve_fhfa,
    "ember": _resolve_ember,
    "zillow": _resolve_zillow,
    "bis": _resolve_bis,
    "faostat": _resolve_faostat,
    "frankfurter": _resolve_frankfurter,
    "ecb": _resolve_ecb,
    "oecd": _resolve_oecd,
    "worldbank_pink": _resolve_worldbank_pink,
    "istat": _resolve_istat,
    "usda": _resolve_usda,
    "census": _resolve_census_any,
    "fed_board": _resolve_fed_board,
    "dbnomics": _resolve_dbnomics,
    "boe": _resolve_boe,
    "statcan": _resolve_statcan_any,
    "abs": _resolve_abs,
    "treasury": _resolve_treasury,
    "noaa": _resolve_noaa,
    "eia": _resolve_eia,
}

# Sources whose rows are relational/wide (no canonical value column): ship
# native-verbatim, exclude from the tidy frame (bundle() records them honestly).
_NATIVE_ONLY = {"wikidata", "fhfa", "census", "treasury", "hf_equities"}

# Sources that replicate byte-identical observations across mirror/table files:
# drop duplicates on these columns after the filtered read so neither the native
# copy nor the tidy frame is inflated. (Verified byte-identical by the red-team.)
_DEDUP_ON = {
    "ecb": ("series_key", "obs_date"),
    "bea": ("series_key", "obs_date"),
}

# Sources whose identity lives in the FILENAME (no in-file key column): stamp the
# canonical catalog series_id onto every projected row, so the bundle is
# self-describing and pull() can reconstruct exactly.
_STAMP_ID = {"worldbank_esg", "worldbank", "hf_equities"}

# Identity is (STORE FILE, series_key) and the file part is not in any column, so the tidy
# `series_id` has to be stamped - but with the catalog id MINUS its source prefix, which is
# what these sources' CSVs already contain and what every tidy source emits.
#
# _STAMP_ID stamps the FULL id (`worldbank:NY.GDP.MKTP.CD:AFE`), so it is the wrong tool here:
# fed_board's stored object carries `RIFSPFF_N.B`, not `fed_board:RIFSPFF_N.B`. Two conventions
# already exist in this file; this picks the one these sources are on rather than migrating
# them onto the other and rewriting objects that are correct.
#
# Without it the bulk deriver and the resolver disagree about one column and the byte-exactness
# gate blocks the whole derive - which is exactly what it did, and why this exists.
_STAMP_SHORT_ID = {"fed_board"}
# NOT fhfa. It is served WIDE (tidy_ok False), so its CSV already carries `dataset` and
# `series_key` as columns - together the full identity - and stamping would append a redundant
# third id column that its 61 already-stored objects do not have. A "fix" that changes the
# shape of correct files is not a fix.


_GENERIC_SKIP = ("__series.parquet",)

# FLOW-GRAIN sources: the catalog id names a TABLE (a PxWeb flow), while the store keys
# it one row per SERIES — the flow id followed by one `dim=value` segment per dimension:
#   catalog  stat_latvia:LV:OSP_OD:apsekojumi:arodizgl:1999_2005:ARA30.px
#   store    LV:OSP_OD:apsekojumi:arodizgl:1999_2005:ARA30.px:ContentsCode=…:…=0
# An exact key match therefore finds nothing at all: every one of stat_latvia's 1,952
# catalog ids failed to derive with "zero rows matched in 16 files" (CI run 30148200117),
# and the same holds for the other eight. These sources resolve by PREFIX instead, so a
# flow yields every series in its table — which is the point of cataloguing them at flow
# grain (~40k tables) rather than per series (millions).
# The trailing ":" in the prefix is load-bearing: without it `…ARA3.px` would also match
# `…ARA30.px`. Kept as an explicit set rather than making the generic resolver
# prefix-match everywhere, which would silently change behaviour for ~200 other sources.
# unsdg joins them for the same reason, with the same key shape: catalog id
# `unsdg:AG_LND_DGRD`, store keys `AG_LND_DGRD:AFG`, `AG_LND_DGRD:AFG|Sex=F`, ... One SDG
# series code is the publisher's own unit of publication (713 codes vs 227,955 code:geo|dim
# keys), so flow grain serves every key from 396 catalog rows instead of a quarter-million.
# The trailing ":" matters here too: without it `AG_LND_DGRD` would also match a future
# `AG_LND_DGRD2`.
_FLOW_GRAIN = {"stat_latvia", "stat_estonia", "ssb", "bfs", "dst",
               "statfin", "hagstofa", "stat_slovenia", "scb", "unsdg"}


def _resolve_generic_long(series_id: str, root: str) -> Resolution:
    """Generic resolver for any UNIFORM-LONG source: catalog id is exactly
    ``<source>:<native_key>`` and the store is parquet with a [series_key|series_id,
    obs_date, value] schema. Covers the ~200 sources that follow this shape without a
    hand-written resolver. Raises ResolveError (honest 'needs explicit resolver') for
    relational/wide stores so they are never silently mis-served."""
    import pyarrow.parquet as pq
    src, native = series_id.split(":", 1)
    src_dir = os.path.join(root, src)
    if not os.path.isdir(src_dir):
        raise ResolveError(
            f"{series_id}: source {src!r} has no resolver and no store dir at {src_dir!r}. "
            "Refusing to silently skip it.")
    files = sorted(f for f in glob.glob(os.path.join(src_dir, "**", "*.parquet"), recursive=True)
                   if not f.endswith(_GENERIC_SKIP))
    if not files:
        raise ResolveError(f"{series_id}: no parquet files under {src_dir!r}")
    cols = set(pq.read_schema(files[0]).names)
    # 'idbank' = INSEE BDM native series id (insee_bdm). Detect it as a key column.
    key_col = next((c for c in ("series_key", "series_id", "idbank") if c in cols), None)
    if key_col is None or "obs_date" not in cols or "value" not in cols:
        raise ResolveError(
            f"{series_id}: source {src!r} is not uniform-long (schema {sorted(cols)}); "
            "it needs an explicit resolver, not the generic one.")
    path = files if len(files) > 1 else files[0]
    if src in _FLOW_GRAIN:
        # One flow == every series whose key begins "<flow>:" (see _FLOW_GRAIN).
        pred = pc.starts_with(ds.field(key_col), native + ":")
    else:
        pred = pc.equal(ds.field(key_col), native)
    return Resolution(series_id, src, path, key_col, pred)


def supported_sources() -> list[str]:
    """Explicit resolvers PLUS every source that has catalog series rows (those are
    served by the generic uniform-long resolver). Cheap catalog query; cached."""
    base = set(_RESOLVERS)
    try:
        conn = _catalog.connect()
        try:
            base |= {r[0] for r in conn.execute("SELECT DISTINCT source_id FROM series")}
        finally:
            conn.close()
    except Exception:
        pass
    return sorted(base)


def resolve(series_id: str, root: str | None = None) -> Resolution:
    root = root or default_data_root()
    src = _catalog.source_of(series_id)
    fn = _RESOLVERS.get(src) or _resolve_generic_long
    r = fn(series_id, root)
    # Apply cross-cutting policy centrally (keeps the per-source bodies untouched).
    r.dedup_on = r.dedup_on or _DEDUP_ON.get(src)
    if src in _STAMP_ID or src in _STAMP_SHORT_ID:
        r.stamp_id = True
        r.key_col = "series_id"   # the stamped column carries identity
    if src in _NATIVE_ONLY:
        r.tidy_ok = False
    return r


# --------------------------------------------------------------------------- #
# Reading projected rows.
# --------------------------------------------------------------------------- #

_CANON = ["series_id", "source", "obs_date", "value"]


def read_native(res: Resolution) -> pa.Table:
    """Native projected rows for one series, as a pyarrow Table.

    Applies dedup (cross-file byte-identical dups) and stamp_id (filename-identity
    sources) so the table is correct and self-describing BEFORE the bundle copies it.
    """
    table = ds.dataset(res.parquet_path).to_table(filter=res.predicate)
    if res.dedup_on:
        df = table.to_pandas().drop_duplicates(subset=list(res.dedup_on))
        table = pa.Table.from_pandas(df, preserve_index=False)
    if table.num_rows == 0:
        where = (os.path.basename(res.parquet_path) if isinstance(res.parquet_path, str)
                 else f"{len(res.parquet_path)} files")
        raise ResolveError(
            f"{res.series_id}: zero rows matched in {where} "
            "-- the series id resolves to a file but no observations. Refusing to emit "
            "an empty series silently."
        )
    if res.row_id_from:
        # TABLE GRAIN: the catalog id names a table, and each row needs an id of its own.
        # Build `<key_col>|NAME=value` for each named column, matching byte-for-byte what the
        # source's derive tool writes. If the two ever disagree the byte-exactness gate stops
        # the derive, which is the point of building the id in ONE place conceptually even
        # though two programs construct it.
        import pyarrow.compute as _pc
        base = table.column(res.row_id_from[0]).cast(pa.string())
        for col in res.row_id_from[1:]:
            vals = (table.column(col).cast(pa.string()) if col in table.column_names
                    else pa.array([""] * table.num_rows, pa.string()))
            vals = _pc.fill_null(vals, "")
            base = _pc.binary_join_element_wise(
                _pc.cast(base, pa.string()), f"|{col}=", _pc.cast(vals, pa.string()), "")
        if "series_id" in table.column_names:
            table = table.drop(["series_id"])
        table = table.append_column("series_id", base.cast(pa.string()))
        res.key_col = "series_id"          # native_to_tidy reads the id from key_col
        # COLLAPSE the residual duplicates the same way the source's derive does, or the two
        # disagree on exactly the rows nobody can disambiguate. usda has 2,062 (row_id, date)
        # groups holding more than one value after every geography column is exhausted; the
        # derive keeps the MAXIMUM, so this must too. Sorting by (id, date, value) and keeping
        # the last is the same rule stated in pandas.
        if "obs_date" in table.column_names and "value" in table.column_names:
            _df = table.to_pandas(date_as_object=True)
            _n = len(_df)
            _df = (_df.sort_values(["series_id", "obs_date", "value"], kind="mergesort")
                      .drop_duplicates(subset=["series_id", "obs_date"], keep="last"))
            if len(_df) != _n:
                table = pa.Table.from_pandas(_df, preserve_index=False)

    if res.stamp_id:
        # identity is the filename (or the store file); stamp it onto every row. Sources in
        # _STAMP_SHORT_ID carry the id WITHOUT its source prefix, matching what their stored
        # CSVs already contain and what every tidy source emits.
        stamped = (res.series_id.split(":", 1)[1]
                   if res.source in _STAMP_SHORT_ID and ":" in res.series_id
                   else res.series_id)
        if "series_id" in table.column_names:
            table = table.drop(["series_id"])
        table = table.append_column(
            "series_id", pa.array([stamped] * table.num_rows, pa.string()))
    return table


def native_to_tidy(res: Resolution, table: pa.Table) -> pd.DataFrame:
    """Normalise native rows to canonical tidy [series_id, source, obs_date, value].

    For tidy sources, `series_id` is the native key (or the stamped catalog id),
    matching what pull() reconstructs from a copied resource -> exact reproduction
    is a row-for-row identity.
    """
    return native_table_to_tidy(res.source, res.key_col, table)


def _dates_without_ns_bound(col) -> list:
    """obs_date -> datetime.date, WITHOUT going through pandas datetime64[ns].

    pandas nanosecond timestamps span only ~1677-09-21 .. 2262-04-11, so
    `pd.to_datetime` RAISES OutOfBoundsDatetime on genuinely old observations. That
    is not a hypothetical: Gapminder's `income_per_person_long_series` reaches
    0980-12-31 for chn and 1270-12-31 for gbr, and 98 of its 86,684 series failed to
    derive on exactly this — the deepest historical data we host, lost to a storage
    detail of the conversion layer.

    Parquet already holds these correctly as date32 (years 1..9999); only the pandas
    hop was lossy. Note the alternative fix, `errors="coerce"`, would have been far
    worse: it turns a pre-1677 date into NaT and the series would derive "successfully"
    with its oldest observations silently blanked.
    """
    import datetime as _dt
    out = []
    for v in col:
        if v is None or (isinstance(v, float) and v != v):   # None / NaN
            out.append(None)
        elif isinstance(v, _dt.datetime):
            out.append(v.date())
        elif isinstance(v, _dt.date):
            out.append(v)
        else:
            s = str(v)[:10]
            try:
                out.append(_dt.date(int(s[0:4]), int(s[5:7]), int(s[8:10])))
            except (ValueError, IndexError):
                out.append(None)
    return out


def native_table_to_tidy(source: str, key_col: str, table: pa.Table) -> pd.DataFrame:
    """Shared normaliser used by both bundle() (live) and pull() (from resource)."""
    # date_as_object=True keeps date32 columns as datetime.date instead of letting
    # Arrow convert them to datetime64[ns], which overflows for pre-1677 dates.
    df = table.to_pandas(date_as_object=True)
    out = pd.DataFrame({
        "series_id": df[key_col].astype(str).values,   # native key (or stamped catalog id)
        "source": source,
        "obs_date": _dates_without_ns_bound(df["obs_date"]),
        "value": pd.to_numeric(df["value"], errors="coerce"),
    })
    return out.sort_values(["series_id", "obs_date"]).reset_index(drop=True)
