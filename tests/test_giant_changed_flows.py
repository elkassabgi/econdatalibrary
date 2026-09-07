"""run_giant's opt-in, merge-measured changed-flow report (ledger R882 rule 5).

Without it a giant source that merges rows returns neither `changed_keys` nor
`series_cursors`, so orchestrate §5.7 books `full_rederive_owed` on EVERY merging tick and
the only exit is a manual desktop campaign (eurostat, 2026-09-07: 400 flows selected, 360
parquets rewritten, ~108 actually changed). Each test below has the direction that would
have caught that defect AND the direction that keeps the old behaviour byte-identical for
callers that do not opt in (oecd, sec_edgar, sdmx_nso, _iep).

Hermetic: a tmp source dir under the LOCAL blob backend; no catalog.db, no store, no R2.
"""
import json
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import merge  # noqa: E402
from updater.strategies.fetchers import _giant  # noqa: E402


class _Unit:
    def __init__(self, d):
        self.out_paths = [d]


def _tbl(rows):
    return pa.table({"series_key": [r[0] for r in rows],
                     "obs_date": [r[1] for r in rows],
                     "value": [float(r[2]) for r in rows]})


CAT = {"aact_ali01": {"vintage": "v1", "filename": "AACT_ALI01.parquet"},
       "tec00115": {"vintage": "v1", "filename": "TEC00115.parquet"}}
BASE_A = [("k1", "2024-01-01", 1.0), ("k1", "2024-02-01", 2.0)]
BASE_B = [("j1", "2024-01-01", 5.0), ("j2", "2024-01-01", 6.0)]


def _bump(cat):
    """A moved vintage token is what makes select_flows re-select an 'ok' flow."""
    return {k: dict(v, vintage="v2") for k, v in cat.items()}


def _run(tmp_path, monkeypatch, catalog, tables, *, report=True, max_flows=400):
    monkeypatch.setattr(_giant.time, "sleep", lambda *_a, **_k: None)
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)

    def fetch_flow(fid, meta, since, session):
        t = tables[fid]
        return (t, "ok") if t is not None else (None, "no_change")

    return _giant.run_giant(_Unit(str(tmp_path)), source="zzgiant",
                            fetch_catalog=lambda: catalog, fetch_flow=fetch_flow,
                            csv_accept="text/csv", rate=0, timeout=1, max_flows=max_flows,
                            report_changed_flows=report)


def _seed(tmp_path, monkeypatch):
    res = _run(tmp_path, monkeypatch, CAT,
               {"aact_ali01": _tbl(BASE_A), "tec00115": _tbl(BASE_B)})
    assert res.status == "ok"
    assert res.changed_keys == {"aact_ali01": "2024-02-01", "tec00115": "2024-01-01"}
    assert os.path.exists(os.path.join(tmp_path, "AACT_ALI01.parquet"))
    return res


def test_first_publish_reports_every_flow_with_its_max_date(tmp_path, monkeypatch):
    _seed(tmp_path, monkeypatch)


def test_idempotent_refetch_reports_an_empty_dict_not_none(tmp_path, monkeypatch):
    _seed(tmp_path, monkeypatch)
    res = _run(tmp_path, monkeypatch, _bump(CAT),
               {"aact_ali01": _tbl(BASE_A), "tec00115": _tbl(BASE_B)})
    # The boundary re-fetch merged nothing new: an honest "{}" — NEVER None, which the
    # orchestrator would read as "un-migrated fetcher" and book the debt.
    assert res.changed_keys == {}
    assert res.changed_keys is not None


def test_new_rows_name_only_that_flow(tmp_path, monkeypatch):
    _seed(tmp_path, monkeypatch)
    res = _run(tmp_path, monkeypatch, _bump(CAT),
               {"aact_ali01": _tbl(BASE_A + [("k1", "2024-03-01", 3.0)]),
                "tec00115": _tbl(BASE_B)})
    assert res.changed_keys == {"aact_ali01": "2024-03-01"}


def test_every_opted_in_tick_prints_the_merge_measured_count(tmp_path, monkeypatch, capsys):
    """Final-diff review condition 3: the rollout evidence compares the derived count against
    the changed-flow count, so a CLEAN tick must print it too (not only the structural path)."""
    _seed(tmp_path, monkeypatch)
    capsys.readouterr()
    res = _run(tmp_path, monkeypatch, _bump(CAT),
               {"aact_ali01": _tbl(BASE_A + [("k1", "2024-03-01", 3.0)]),
                "tec00115": _tbl(BASE_B)})
    out = capsys.readouterr().out
    assert res.changed_keys == {"aact_ali01": "2024-03-01"}
    assert "[zzgiant] changed flows (merge-measured): 1 of 2 merged (0 over the report cap" in out
    # and the opt-out prints nothing of the kind
    capsys.readouterr()
    _run(tmp_path, monkeypatch, _bump(_bump(CAT)),
         {"aact_ali01": _tbl(BASE_A), "tec00115": _tbl(BASE_B)}, report=False)
    assert "changed flows (merge-measured)" not in capsys.readouterr().out


def test_same_period_value_revision_is_a_change(tmp_path, monkeypatch):
    """R549: identical row count, identical max date, one value revised — a footer/shape
    screen cannot see it; the merge can, and the store must serve the revised value."""
    _seed(tmp_path, monkeypatch)
    revised = [("j1", "2024-01-01", 5.0), ("j2", "2024-01-01", 6.5)]
    res = _run(tmp_path, monkeypatch, _bump(CAT),
               {"aact_ali01": _tbl(BASE_A), "tec00115": _tbl(revised)})
    assert res.changed_keys == {"tec00115": "2024-01-01"}
    t = pq.read_table(os.path.join(tmp_path, "TEC00115.parquet")).to_pylist()
    assert t and len(t) == 2
    assert {r["series_key"]: r["value"] for r in t}["j2"] == 6.5


def test_above_the_cap_the_driver_merges_plainly_and_over_reports(tmp_path, monkeypatch):
    """merge_and_write refuses the report BEFORE ANY I/O above CHANGED_KEYS_CAP, so the driver
    pre-checks the row count and never asks (a ValueError escaping the merge block would book
    the whole source transient_fail). The flow is merged plainly and marked changed."""
    _seed(tmp_path, monkeypatch)
    real = merge.merge_and_write
    seen = []

    def wrapped(*a, **k):
        seen.append(dict(k))
        return real(*a, **k)

    monkeypatch.setattr(merge, "merge_and_write", wrapped)
    monkeypatch.setattr(merge, "CHANGED_KEYS_CAP", 1)   # every real tail exceeds it
    res = _run(tmp_path, monkeypatch, _bump(CAT),
               {"aact_ali01": _tbl(BASE_A), "tec00115": None})
    # idempotent in truth, but the report was unavailable -> over-reported, never dropped
    assert res.changed_keys == {"aact_ali01": "2024-02-01"}
    assert seen and all("report_changed_keys" not in k for k in seen)   # never asked
    t = pq.read_table(os.path.join(tmp_path, "AACT_ALI01.parquet"))
    assert t.num_rows == 2                    # the plain merge ran and kept the file whole


def test_report_cap_is_a_module_constant_and_the_keyword_default(tmp_path):
    import inspect
    sig = inspect.signature(merge.merge_and_write)
    assert sig.parameters["changed_keys_cap"].default == merge.CHANGED_KEYS_CAP == 2_000_000


def test_structural_raise_still_returns_the_merged_flows_as_partial(tmp_path, monkeypatch):
    """finalize() raises DefinitiveError on ANY structural sub-unit; the orchestrator maps that
    to `partial` with no derive. With the opt-in, the flows that DID merge must still reach
    the derive: same recorded status (partial, same error text), plus changed_keys."""
    _seed(tmp_path, monkeypatch)
    monkeypatch.setattr(_giant.time, "sleep", lambda *_a, **_k: None)

    def fetch_flow(fid, meta, since, session):
        if fid == "tec00115":
            return None, "structural"          # the SOAP-envelope-instead-of-CSV shape
        return _tbl(BASE_A + [("k1", "2024-03-01", 3.0)]), "ok"

    res = _giant.run_giant(_Unit(str(tmp_path)), source="zzgiant",
                           fetch_catalog=lambda: _bump(CAT), fetch_flow=fetch_flow,
                           csv_accept="text/csv", rate=0, timeout=1,
                           report_changed_flows=True)
    assert res.status == "partial"
    assert "structural" in (res.error or "") and "tec00115" in (res.error or "")
    assert res.changed_keys == {"aact_ali01": "2024-03-01"}
    st = json.load(open(os.path.join(tmp_path, "_giant_state.json"), encoding="utf-8"))
    assert st["tec00115"]["status"] == "definitive_fail"     # reselected next tick, as before


def test_structural_raise_without_merged_flows_still_raises(tmp_path, monkeypatch):
    _seed(tmp_path, monkeypatch)
    monkeypatch.setattr(_giant.time, "sleep", lambda *_a, **_k: None)

    def fetch_flow(fid, meta, since, session):
        return (None, "structural") if fid == "tec00115" else (None, "no_change")

    with pytest.raises(_giant.DefinitiveError):
        _giant.run_giant(_Unit(str(tmp_path)), source="zzgiant",
                         fetch_catalog=lambda: _bump(CAT), fetch_flow=fetch_flow,
                         csv_accept="text/csv", rate=0, timeout=1,
                         report_changed_flows=True)


def test_structural_raise_is_byte_identical_without_the_opt_in(tmp_path, monkeypatch):
    _seed(tmp_path, monkeypatch)
    monkeypatch.setattr(_giant.time, "sleep", lambda *_a, **_k: None)

    def fetch_flow(fid, meta, since, session):
        if fid == "tec00115":
            return None, "structural"
        return _tbl(BASE_A + [("k1", "2024-03-01", 3.0)]), "ok"

    with pytest.raises(_giant.DefinitiveError):
        _giant.run_giant(_Unit(str(tmp_path)), source="zzgiant",
                         fetch_catalog=lambda: _bump(CAT), fetch_flow=fetch_flow,
                         csv_accept="text/csv", rate=0, timeout=1,
                         report_changed_flows=False)


def test_capped_partial_carries_the_fetched_slices_changes(tmp_path, monkeypatch):
    _seed(tmp_path, monkeypatch)
    res = _run(tmp_path, monkeypatch, _bump(CAT),
               {"aact_ali01": _tbl(BASE_A + [("k1", "2024-03-01", 3.0)]),
                "tec00115": _tbl(BASE_B)}, max_flows=1)
    assert res.status == "partial" and "selected>cap" in (res.error or "")
    assert res.changed_keys == {"aact_ali01": "2024-03-01"}


def test_opt_out_is_byte_identical_to_before(tmp_path, monkeypatch):
    real = merge.merge_and_write
    seen = []

    def wrapped(*a, **k):
        seen.append(dict(k))
        return real(*a, **k)

    monkeypatch.setattr(merge, "merge_and_write", wrapped)
    res = _run(tmp_path, monkeypatch, CAT,
               {"aact_ali01": _tbl(BASE_A), "tec00115": _tbl(BASE_B)}, report=False)
    assert res.status == "ok"
    assert res.changed_keys is None and res.series_cursors is None
    assert seen and all("report_changed_keys" not in k for k in seen)


def test_sidecar_state_unchanged_by_the_report(tmp_path, monkeypatch):
    """The report rides on the Result; the per-flow sidecar keeps its shape."""
    _seed(tmp_path, monkeypatch)
    st = json.load(open(os.path.join(tmp_path, "_giant_state.json"), encoding="utf-8"))
    assert set(st) == set(CAT)
    assert st["aact_ali01"]["status"] == "ok" and st["aact_ali01"]["vintage"] == "v1"
    assert "changed" not in st["aact_ali01"]


def test_eurostat_opts_in_and_no_other_giant_caller_does():
    """Pinned so the opt-in cannot silently spread to a source whose catalogue grain has
    not been measured (oecd, sec_edgar, sdmx_nso, _iep)."""
    fdir = os.path.join(ROOT, "updater", "strategies", "fetchers")
    src = {f: open(os.path.join(fdir, f), encoding="utf-8").read()
           for f in os.listdir(fdir) if f.endswith(".py") and f != "_giant.py"}
    optin = sorted(f for f, s in src.items() if "report_changed_flows=True" in s)
    assert optin == ["eurostat.py"], optin
