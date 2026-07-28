"""Shared fetcher for the fao_* sources, DIRECT from FAOSTAT's bulk distribution.

WHY. All 25 fao_* sources (136,754 series) arrive via DBnomics, whose index for the
FAO provider was last refreshed 2022-04-05 — over four years ago. FAOSTAT publishes
a bulk API listing 69 datasets each carrying its own DateUpdate, many refreshed
within the last month. The data is current at the publisher and four years stale in
our copy, and nothing on our side could tell, because the change signal we watched
belonged to the relay (ledger R73).

WHAT MAKES THIS A REPAIR RATHER THAN A RE-KEY. The DBnomics-era keys turn out to BE
FAOSTAT's own codes: `FAO_QCL:5111.1.1016` is element 5111 (Stocks), area 1
(Armenia), item 1016 (Goats) — which is exactly what its title says. So the ids
reconstruct from the bulk CSV's own code columns with no translation table at all.
Measured on QCL: 19,869 of 20,238 published ids reproduced exactly (98.2%), and on
the 988,719 shared (key, year) points the values agree 92.22% — the remainder being
FAO's routine revisions, the same profile the IMF migration showed. Upstream also
carries 78,944 series against our 20,238 and runs two years further.

THE COLUMN ORDER IS DISCOVERED, NOT ASSUMED. Different FAOSTAT datasets expose
different code columns (QCL has Element/Area/Item; AE has Indicator/Cost Category/
Institution/Area), and which order our ids used is a fact about the DBnomics vintage,
not something to guess. tools/prove_faostat_repair.py tries the orderings, scores
each by how many PUBLISHED ids it reproduces, and emits the winner — so a wrong
guess cannot quietly mint a parallel id space.

The runtime self-check is the same one the IMF repairs use, for the same reason: a
wrong template does not raise, it invents ids beside the real ones and reports
success.
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import json
import os
import sqlite3
import urllib.request
import zipfile

import pyarrow as pa

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import Tally, finalize

DEDUP = ("series_key", "obs_date")
CONF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_faostat_maps")
BULK_INDEX = "https://bulks-faostat.fao.org/production/datasets_E.json"
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}
ID_FLOOR = 0.95


def load(source_id: str) -> dict:
    return json.load(io.open(os.path.join(CONF_DIR, f"{source_id}.json"),
                             encoding="utf-8"))


def _get(url, timeout=900):
    return urllib.request.urlopen(urllib.request.Request(url, headers=UA),
                                  timeout=timeout).read()


def _index():
    return json.loads(_get(BULK_INDEX, timeout=180).decode("utf-8-sig"))


def _entry(code: str):
    for x in _index()["Datasets"]["Dataset"]:
        if (x.get("DatasetCode") or "").upper() == code.upper():
            return x
    return None


def vintage(source_id: str):
    """FAOSTAT's own DateUpdate for this dataset — it moves iff FAO republishes.

    Deliberately the PUBLISHER's field and not a relay's hash: the entire reason
    these sources sat four years stale is that the previous signal certified
    DBnomics' index rather than FAO's release.
    """
    try:
        cfg = load(source_id)
        e = _entry(cfg["code"])
        if not e:
            return None
        return f"{cfg['code']}:{e.get('DateUpdate')}:{e.get('FileSize')}"
    except Exception:                                         # noqa: BLE001
        return None


def _catalog_ids(source_id: str) -> set:
    db = os.path.join(config.ROOT, "data", "catalog.db")
    if not os.path.exists(db):
        db = os.path.join("data", "catalog.db")
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        # Strip ONLY the source prefix. A catalog id is
        # `fao_qcl:FAO_QCL:5111.1.1016` and the key this fetcher builds is
        # `FAO_QCL:5111.1.1016`, so the comparison must keep the middle segment.
        # Splitting twice compared `5111.1.1016` against `FAO_QCL:5111.1.1016` and
        # scored 0.0% — the self-check then refused a template the prover had just
        # measured at 98.2%. The guard was right to refuse; the two sides simply
        # were not speaking about the same string.
        return {r[0].split(":", 1)[1] for r in con.execute(
            "SELECT series_id FROM series WHERE source_id=?", (source_id,))
            if ":" in r[0]}
    except Exception:                                         # noqa: BLE001
        return set()


def run(source_id: str) -> Result:
    cfg = load(source_id)
    code, cols = cfg["code"], cfg["key_columns"]
    prefix, conv = cfg["key_prefix"], cfg.get("date_convention", "start")
    tally = Tally()
    out_dir = config.source_dir(source_id)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{source_id}.parquet")
    before = blob.row_count(path) if blob.exists(path) else 0

    try:
        e = _entry(code)
        if not e:
            tally.structural_unit(f"{code}: absent from the FAOSTAT bulk index")
            return finalize(tally, before, None, source=source_id)
        raw = _get(e["FileLocation"])
    except Exception as ex:                                   # noqa: BLE001
        raise TransientError(f"{code}: {ex!r}") from ex

    try:
        z = zipfile.ZipFile(io.BytesIO(raw))
        member = next((n for n in z.namelist()
                       if n.lower().endswith("(normalized).csv")), None)
        if member is None:
            tally.structural_unit(f"{code}: zip has no normalized CSV")
            return finalize(tally, before, None, source=source_id)
        text = z.read(member).decode("utf-8-sig", errors="replace")
    except zipfile.BadZipFile as ex:
        raise TransientError(f"{code}: bad zip ({len(raw):,} B): {ex}") from ex

    rd = csv.DictReader(io.StringIO(text))
    present = set(rd.fieldnames or [])
    missing = [c for c in cols if c not in present]
    if missing:
        # FAO renamed or dropped a code column. Building keys without it would
        # silently collapse distinct series onto one id, so refuse.
        tally.structural_unit(f"{code}: key column(s) gone: {missing}")
        return finalize(tally, before, None, source=source_id)

    keys, dates, vals = [], [], []
    n_bad = 0
    for row in rd:
        v = (row.get("Value") or "").strip()
        y = (row.get("Year Code") or row.get("Year") or "").strip()
        if not v or not y[:4].isdigit():
            continue
        try:
            fv = float(v.replace(",", ""))
        except ValueError:
            n_bad += 1
            continue
        yr = int(y[:4])
        keys.append(f"{prefix}:" + ".".join((row.get(c) or "").strip() for c in cols))
        dates.append(dt.date(yr, 1, 1) if conv == "start" else dt.date(yr, 12, 31))
        vals.append(fv)

    if not keys:
        tally.structural_unit(f"{code}: CSV parsed but no usable observations")
        return finalize(tally, before, None, source=source_id)

    known = _catalog_ids(source_id)
    built = set(keys)
    if known:
        hit = len(built & known) / len(known)
        if hit < ID_FLOOR:
            print(f"[faostat] FAIL {source_id}: rebuilt keys match only "
                  f"{100 * hit:.1f}% of the {len(known):,} published ids "
                  f"(floor {100 * ID_FLOOR:.0f}%). The key template is wrong or "
                  f"FAOSTAT restructured — merging would MINT NEW IDS beside the "
                  f"live ones and leave every published series frozen. Refusing.",
                  flush=True)
            tally.structural_unit(f"{code}: id match {100 * hit:.1f}%")
            return finalize(tally, before, None, source=source_id)
        print(f"[faostat] {source_id}: {len(keys):,} obs / {len(built):,} series, "
              f"{100 * hit:.1f}% of published ids reproduced"
              + (f", {n_bad:,} unreadable values" if n_bad else ""), flush=True)

    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})
    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before), code)
    cursors = {}
    for k, d in zip(tbl.column("series_key").to_pylist(),
                    tbl.column("obs_date").to_pylist()):
        if d is not None and (k not in cursors or d > cursors[k]):
            cursors[k] = d
    return finalize(tally, n, md, source=source_id,
                    series_cursors={k: v.isoformat() for k, v in cursors.items()})
