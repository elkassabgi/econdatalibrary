"""tools/repair_insee_spelling.py: the plan, on synthetic tables (no R2). Supersede drops an old row only for an
exact counterpart and keeps the rest; revert is proved equal to the mirror; every precondition refuses."""
from __future__ import annotations

import datetime as dt
import os
import sys

import pyarrow as pa
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools"))
import repair_insee_spelling as R  # noqa: E402

D = dt.date


def tbl(rows):
    return pa.table({"flow": [r[0] for r in rows], "series_key": [r[1] for r in rows],
                     "obs_date": pa.array([r[2] for r in rows], pa.date32()), "value": [r[3] for r in rows]})


OLD = [("F", "A=1:B=x", D(2024, 12, 31), 1.0), ("F", "A=1:B=x", D(2025, 12, 31), 2.0),
       ("F", "A=2:B=y", D(2025, 12, 31), 3.0),                         # old-only series: INSEE dropped it
       ("F", "A=3:B=z", D(2020, 12, 31), 4.0)]                         # a date the new pull lacks
NEW = [("F", "A=1:B=x:GEO=2025-FRANCE-F", D(2024, 12, 31), 1.0),
       ("F", "A=1:B=x:GEO=2025-FRANCE-F", D(2025, 12, 31), 2.5),       # a revised value
       ("F", "A=3:B=z:GEO=2025-FRANCE-F", D(2021, 12, 31), 5.0)]       # same series, another date


def test_supersede_drops_only_exact_counterparts_and_keeps_the_rest():
    pub, rep = R.plan_supersede(tbl(OLD + NEW), tbl(OLD))
    got = sorted((r["series_key"], r["obs_date"], r["value"]) for r in pub.to_pylist())
    assert got == sorted([("A=1:B=x:GEO=2025-FRANCE-F", D(2024, 12, 31), 1.0),
                          ("A=1:B=x:GEO=2025-FRANCE-F", D(2025, 12, 31), 2.5),
                          ("A=3:B=z:GEO=2025-FRANCE-F", D(2021, 12, 31), 5.0),
                          ("A=2:B=y", D(2025, 12, 31), 3.0), ("A=3:B=z", D(2020, 12, 31), 4.0)])
    assert rep["dropped_old_rows"] == 2 and rep["kept_old_rows_residue"] == 2 and rep["kept_old_series"] == 2
    assert rep["dropped_rows_whose_value_the_new_pull_revised"] == 1
    assert rep["rows_before"] == 7 and rep["rows_after"] == 5 and rep["residue"]["rows"] == 2


def test_revert_publishes_the_mirror_and_proves_it():
    pub, rep = R.plan_revert(tbl(OLD + NEW), tbl(OLD))
    assert pub.num_rows == len(OLD) and rep["added_rows_removed"] == len(NEW)


def test_a_mirror_row_missing_on_r2_refuses():
    with pytest.raises(R.Refused, match="not on R2"):
        R.plan_revert(tbl(OLD[1:] + NEW), tbl(OLD))


def test_an_old_key_with_a_geo_refuses():
    old = OLD + [("F", "A=9:GEO=FR", D(2020, 12, 31), 9.0)]
    with pytest.raises(R.Refused, match="not exact"):
        R.plan_supersede(tbl(old + NEW), tbl(old))


def test_an_added_key_without_the_geo_refuses():
    new = NEW + [("F", "A=7:B=q", D(2025, 12, 31), 7.0)]
    with pytest.raises(R.Refused, match="not exact"):
        R.plan_supersede(tbl(OLD + new), tbl(OLD))


def test_two_new_keys_on_one_stripped_key_refuse():
    new = NEW + [("F", "A=1:GEO=2025-FRANCE-F:B=x", D(2023, 12, 31), 0.5)]   # strips to A=1:B=x as well
    with pytest.raises(R.Refused, match="not exact"):
        R.plan_supersede(tbl(OLD + new), tbl(OLD))


def test_a_duplicate_on_the_served_object_refuses():
    with pytest.raises(R.Refused, match="duplicate"):
        R.plan_revert(tbl(OLD + NEW + [NEW[0]]), tbl(OLD))


def test_the_tool_refuses_flows_outside_the_eight_and_a_work_dir_on_D(tmp_path):
    with pytest.raises(R.Refused, match="DS_ANTIPOL is untouched"):
        R.main(["--work", str(tmp_path), "--mirror", str(tmp_path), "--catalog-repo", str(tmp_path),
                "--flows", "DS_ANTIPOL"])
    if os.name == "nt":
        with pytest.raises(R.Refused, match="D:"):
            R.main(["--work", "D:\\x", "--mirror", str(tmp_path), "--catalog-repo", str(tmp_path)])
