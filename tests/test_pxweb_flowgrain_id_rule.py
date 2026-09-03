"""`tools/derive_pxweb_flowgrain.py` must not mint different ids than the updater.

THE DIVERGENCE, AND IT IS NOT THE ONE I WAS TOLD. A review said `PREFIX_RE` is the naive "drop
the `=`-bearing segments" rule that `updater/orchestrate.py::_flow_of` replaced after it
corrupted 658 hagstofa keys whose NACE values look like `Atvinnugrein=K: 65`. Measured: FALSE
here. This regex takes the prefix before the first `dim=` segment, and on
`THJ11002.px:Atvinnugrein=K: 65:Ar=2020` both rules return `THJ11002.px`.

They do diverge, on a colon inside the first dimension's NAME:

    FLOW.px:A: B=1:C=2   ->   PREFIX_RE 'FLOW.px:A'   _flow_of 'FLOW.px'

So the guard is warranted, for a shape nobody named. I nearly shipped a comment asserting a
corruption I had not reproduced, and the first version of the test below is what caught it.

The tool has not run since 2026-08-07, which is the only reason a divergent key has not been
published under an id the serving side never looks for.

The tool keeps the vectorised regex - it runs over millions of rows - but now checks every
DISTINCT prefix against `_flow_of` and REFUSES on a disagreement.
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

TOOL = os.path.join(ROOT, "tools", "derive_pxweb_flowgrain.py")


def _load_tool():
    spec = importlib.util.spec_from_file_location("_pxweb_tool", TOOL)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _write(tmp_path, keys):
    t = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array([dt.date(2020, 12, 31)] * len(keys), pa.date32()),
        "value": pa.array([1.0] * len(keys), pa.float64()),
    })
    p = os.path.join(str(tmp_path), "subject.parquet")
    pq.write_table(t, p)
    return p


def test_where_the_two_rules_agree_and_where_they_actually_diverge():
    """Pins BOTH halves: the reported hagstofa divergence does not exist, and a real one does.

    Named for what it checks. It was called "the two rules really do disagree on a colon-bearing
    value", which is the claim I inherited and then disproved - a test whose NAME asserts a
    falsehood is how the falsehood outlives the measurement."""
    from updater.orchestrate import _flow_of
    mod = _load_tool()
    import pyarrow.compute as pc

    hagstofa = "THJ11002.px:Atvinnugrein=K: 65:Ar=2020"
    naive = pc.extract_regex(pa.array([hagstofa]),
                             pattern=mod.PREFIX_RE).field("p").to_pylist()[0]
    assert _flow_of(hagstofa) == "THJ11002.px"
    assert naive == "THJ11002.px", (
        "the hagstofa shape was reported to diverge and does not; if that ever changes it is "
        "this file's docstring that is wrong, not the code")

    # The shape that ACTUALLY diverges: a colon inside the first dimension's NAME.
    key = "FLOW.px:A: B=1:C=2"
    naive = pc.extract_regex(pa.array([key]), pattern=mod.PREFIX_RE).field("p").to_pylist()[0]
    assert _flow_of(key) == "FLOW.px"
    assert naive == "FLOW.px:A", (
        "PREFIX_RE now agrees with _flow_of; the divergence guard in group_subject can be "
        "removed, but remove it deliberately rather than leaving a check that cannot fire")


def test_it_refuses_rather_than_publishing_a_corrupt_id(tmp_path):
    mod = _load_tool()
    path = _write(tmp_path, ["FLOW.px:A: B=1:C=2"])
    with pytest.raises(SystemExit) as e:
        mod.group_subject(path)
    msg = str(e.value)
    assert "ID RULE DIVERGENCE" in msg, msg
    assert "FLOW.px" in msg, msg


def test_it_still_works_on_keys_where_the_rules_agree(tmp_path):
    """The guard must not refuse the ordinary case, or it just disables the tool."""
    mod = _load_tool()
    path = _write(tmp_path, ["SOMEFLOW.px:Dim=Val:Ar=2020", "SOMEFLOW.px:Dim=Other:Ar=2021"])
    out = mod.group_subject(path)
    assert set(out) == {"SOMEFLOW.px"}
    assert len(out["SOMEFLOW.px"]) == 2
