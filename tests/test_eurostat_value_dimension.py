"""A eurostat flow may carry a real DIMENSION named `value`, and the old rules broke on it.

MEASURED 2026-09-01 on sbs_pen_7b1's live SDMX-CSV header:

    DATAFLOW, LAST UPDATE, freq, value, nace_r1, geo, TIME_PERIOD, OBS_VALUE, OBS_FLAG, ...

Two separate failures, both silent:
  * obs_col was `next(c for c in fields if _norm(c) in ("OBS_VALUE","VALUE"))`, and the
    DIMENSION sits first in DSD order — so the observation was read from a dimension column,
    float() rejected every code, and the flow yielded ZERO rows.
  * dim_cols was a _NON_KEY blacklist, which deleted that dimension from the key, collapsing
    ~5.8 source rows onto one public id (ledger R544).

The replacement is positional: dimensions are the columns BEFORE TIME_PERIOD minus the
structural prefix. The decisive property is that this must be a NO-OP for every flow that does
not carry such a dimension — proven below against the REAL dimension list of all 7,638 raw
flows, not a handful of hand-written headers.
"""
import glob
import gzip
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers.eurostat import _NON_KEY, _STRUCTURAL, _norm  # noqa: E402

RAW = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   "data", "raw", "eurostat")


def _sdmx_csv_header(dims):
    """The SDMX-CSV column layout eurostat actually returns, for a given dimension list."""
    return ["DATAFLOW", "LAST UPDATE"] + list(dims) + ["TIME_PERIOD", "OBS_VALUE",
                                                       "OBS_FLAG", "CONF_STATUS"]


def _old_obs(fields):
    return next((c for c in fields if _norm(c) in ("OBS_VALUE", "VALUE")), None)


def _new_obs(fields):
    return (next((c for c in fields if _norm(c) == "OBS_VALUE"), None)
            or next((c for c in fields if _norm(c) == "VALUE"), None))


def _old_dims(fields):
    return [c for c in fields if _norm(c) not in _NON_KEY]


def _new_dims(fields):
    t = fields.index("TIME_PERIOD")
    return [c for c in fields[:t] if _norm(c) not in _STRUCTURAL]


def test_the_measured_header_is_fixed():
    """The exact header returned by the live API for sbs_pen_7b1."""
    fields = ["DATAFLOW", "LAST UPDATE", "freq", "value", "nace_r1", "geo",
              "TIME_PERIOD", "OBS_VALUE", "OBS_FLAG", "CONF_STATUS"]
    assert _old_obs(fields) == "value", "the old rule's failure is no longer reproduced"
    assert _new_obs(fields) == "OBS_VALUE"
    assert _old_dims(fields) == ["freq", "nace_r1", "geo"]
    assert _new_dims(fields) == ["freq", "value", "nace_r1", "geo"]


def test_a_flow_without_the_collision_is_unchanged():
    fields = ["DATAFLOW", "LAST UPDATE", "freq", "unit", "geo",
              "TIME_PERIOD", "OBS_VALUE", "OBS_FLAG"]
    assert _old_obs(fields) == _new_obs(fields) == "OBS_VALUE"
    assert _old_dims(fields) == _new_dims(fields) == ["freq", "unit", "geo"]


def test_the_new_rule_is_a_no_op_across_every_real_flow_except_the_collisions():
    """THE decisive property, over the REAL dimension lists of all raw flows.

    A rule change to public key grammar is only safe if it cannot move a key that is already
    served. Reading the first line of each raw .tsv.gz gives the true dimension list, from
    which the SDMX-CSV layout follows.
    """
    files = sorted(glob.glob(os.path.join(RAW, "*.tsv.gz")))
    if len(files) < 100:                       # raw mirror absent (CI) -> nothing to assert
        import pytest
        pytest.skip("raw eurostat mirror not present")
    changed, checked = [], 0
    for p in files:
        try:
            with gzip.open(p, "rt", encoding="utf-8") as f:
                head = f.readline()
        except Exception:
            continue
        dims = head.rstrip("\n").split("\t")[0].split("\\")[0].split(",")
        if not dims or not dims[0]:
            continue
        fields = _sdmx_csv_header(dims)
        checked += 1
        if _old_dims(fields) != _new_dims(fields) or _old_obs(fields) != _new_obs(fields):
            changed.append(os.path.basename(p)[: -len(".tsv.gz")])
    assert checked > 7000, f"only {checked} flows read; the mirror looks incomplete"
    assert sorted(changed) == sorted([
        "sbs_cre_esc", "sbs_ins_5d1", "sbs_ins_5d2", "sbs_part_wtsct",
        "sbs_pen_7b1", "sbs_sc_3ctrn_tr", "sbs_sctrn_dt_r2",
    ]), f"the change is not confined to the known collisions: {changed}"
