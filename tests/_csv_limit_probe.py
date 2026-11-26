"""Child process for tests/test_csv_field_size_limit_scoped.py: ONE large-field reader, in a FRESH process.

Not a test file (pytest collects only test_*.py). Usage:

    python tests/_csv_limit_probe.py <case> <work_dir>

It imports ONLY the module that owns the reader (plus the standard library), checks that the import left
csv.field_size_limit() at the default 131072, then feeds the reader one planted field of BIG characters
(> 131072) and checks the reader read it. It prints one JSON line: {"case", "limit_after_import",
"limit_after_read", "ok", "error"}. Exit 0 only when every check passed.

Why a fresh process: csv.field_size_limit() is process state. In a pytest process some other module may
already have raised it, which would hide a reader that does not raise it for itself - the very defect
(import-order dependence) this file is here to catch.

All files it writes go under <work_dir> (the test's tmp_path). It sends nothing over the network.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import os
import sys
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT = 131072
BIG = "x" * 200_000          # one field larger than the default limit


def _abs(work):
    import jobs.ingest_abs_full as m
    lim = csv.field_size_limit()
    lines = ["DATAFLOW,FREQ,REGION,TIME_PERIOD,OBS_VALUE,NOTE",
             f"ABS:CPI(1.0.0),A,AUS,2020,1.5,{BIG}"]

    class _Resp:
        status_code = 200

        def raise_for_status(self):
            return None

        def iter_lines(self, decode_unicode=True, chunk_size=None):
            return iter(lines)

        def close(self):
            return None

    class _Sess:
        def get(self, url, params=None, stream=True, timeout=None):
            return _Resp()

    got = list(m.stream_rows(_Sess(), "CPI"))
    assert len(got) == 2 and got[1][1][-1] == BIG, got[:1]
    return lim


def _bis_full(work):
    import jobs.ingest_bis_full as m
    lim = csv.field_size_limit()
    text = ("STRUCTURE,STRUCTURE_ID,ACTION,FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE,TITLE\n"
            f"dataflow,BIS:WS_CBPOL(1.0),I,A,US,2020,1.5,{BIG}\n")
    rows = list(m.iter_rows(io.StringIO(text)))
    assert len(rows) == 1 and rows[0][0] == "A.US", rows
    return lim


def _ecb(work):
    import jobs.ingest_ecb as m
    lim = csv.field_size_limit()
    p = os.path.join(work, "ecb.csv.gz")
    with gzip.open(p, "wt", encoding="utf-8", newline="") as fh:
        fh.write("KEY,FREQ,TIME_PERIOD,OBS_VALUE,TITLE_COMPL\n")
        fh.write(f"EXR.A.USD.EUR.SP00.A,A,2020,1.5,{BIG}\n")
    bw = m.BatchWriter(os.path.join(work, "ecb.parquet"))
    n = bw.add_from_tmp(p)
    assert n == 1, n
    return lim


def _imf(work):
    import jobs.ingest_imf_full as m
    lim = csv.field_size_limit()
    p = os.path.join(work, "imf.csv.gz")
    with gzip.open(p, "wt", encoding="utf-8", newline="") as fh:
        fh.write("DATAFLOW,COUNTRY,INDICATOR,FREQUENCY,TIME_PERIOD,OBS_VALUE,COMMENT\n")
        fh.write(f"IMF:CPI(1.0),USA,PCPI,A,2020,1.5,{BIG}\n")
    st = m.parse_tmp_to_parquet(p, os.path.join(work, "imf.parquet"))
    assert st.get("status") == "ok" and st.get("n_obs") == 1, st
    return lim


def _statcan(work):
    import jobs.ingest_statcan as m
    lim = csv.field_size_limit()
    zp = os.path.join(work, "12345678-eng.zip")
    with zipfile.ZipFile(zp, "w") as z:
        z.writestr("12345678.csv",
                   "REF_DATE,GEO,DGUID,UOM,VECTOR,COORDINATE,VALUE,STATUS\n"
                   f"2020,{BIG},,Dollars,v1,1.1,1.5,\n")
    st = m.parse_zip_to_parquet(zp, os.path.join(work, "12345678.parquet"))
    assert st.get("n_obs") == 1 and st.get("layout") == "standard", st
    return lim


def _apply_series_names(work):
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    import apply_series_names as m
    lim = csv.field_size_limit()
    os.makedirs(os.path.join(work, "csv"), exist_ok=True)
    with open(os.path.join(work, "csv", "src.csv"), "w", encoding="utf-8", newline="") as fh:
        fh.write("series_id,title,description\n")
        fh.write(f"src/a,A title,{BIG}\n")
    m.SWEEP = work
    rows = list(m.stream_rows("src"))
    assert len(rows) == 1 and rows[0]["description"] == BIG, len(rows)
    return lim


def _bis_check_body(work):
    from updater.strategies.fetchers import bis as m
    lim = csv.field_size_limit()
    body = ("STRUCTURE,STRUCTURE_ID,ACTION,FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE,TITLE\n"
            f"dataflow,BIS:WS_CBPOL(1.0),I,A,US,2020,1.5,{BIG}\n").encode("utf-8")
    rows, err, _n = m._check_body(body, "WS_CBPOL", "A", {"A.US": "2020-12-31"})
    assert err is None and rows and rows[0][0] == "A.US", (err, rows)
    return lim


def _bis_bulk_rows(work):
    from updater.strategies.fetchers import bis as m
    lim = csv.field_size_limit()
    zp = os.path.join(work, "WS_CBS_PUB_csv_flat.zip")
    with zipfile.ZipFile(zp, "w") as z:
        z.writestr("WS_CBS_PUB_csv_flat.csv",
                   "DATAFLOW,FREQ,L_REP_CTY,TIME_PERIOD,OBS_VALUE,TITLE\n"
                   f"BIS:WS_CBS_PUB(1.0),Q,US,2020,1.5,{BIG}\n")
    rows = list(m._bulk_rows("CBS", zp))
    assert len(rows) == 1, rows
    return lim


CASES = {
    "abs": _abs,
    "bis_full": _bis_full,
    "ecb": _ecb,
    "imf": _imf,
    "statcan": _statcan,
    "apply_series_names": _apply_series_names,
    "bis_check_body": _bis_check_body,
    "bis_bulk_rows": _bis_bulk_rows,
}


def main(argv) -> int:
    case, work = argv[1], argv[2]
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    start = csv.field_size_limit()
    out = {"case": case, "limit_at_start": start, "limit_after_import": None,
           "limit_after_read": None, "ok": False, "error": None}
    try:
        out["limit_after_import"] = CASES[case](work)
        out["limit_after_read"] = csv.field_size_limit()
        out["ok"] = (start == DEFAULT and out["limit_after_import"] == DEFAULT
                     and out["limit_after_read"] == DEFAULT)
    except Exception as e:                                         # noqa: BLE001 - reported, then exit 1
        out["error"] = f"{type(e).__name__}: {e}"[:500]
    print(json.dumps(out))
    return 0 if out["ok"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
