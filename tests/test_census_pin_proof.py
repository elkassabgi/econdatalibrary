"""tools/census_pin_proof.py must be able to FAIL (R64, R346): it is the instrument the census pin
fix's post-merge proof is read with. Every check is driven once to PASS and once to FAIL."""
from __future__ import annotations

import datetime as dt
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools import census_pin_proof as P  # noqa: E402
from tests.test_census_pins_retired import RUN_2701  # noqa: E402  the 45 pin sets of run 2701

GEO = {"us", "world", "NATION"}
DAY = "2026-08-01"


def _old_log(tmp_path):
    lines = ["[orchestrator] >>> census/_all (strategy=extend_by_date, cadence=monthly)"]
    lines += [f"[census] {f}: pinning {p} - the grain the store holds" for f, p in RUN_2701.items()]
    lines += ["  partial          census/_all"]
    p = tmp_path / "old.log"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(p)


def _new_lines(returned=3, stored=3, verdict="MATCH", status="ok"):
    out = ["[orchestrator] >>> census/_all (strategy=extend_by_date, cadence=monthly)"]
    for f, p in RUN_2701.items():
        sent = {k: v for k, v in p.items() if k not in GEO} if f in P.REFUSED else p
        out.append(f"[census] {f}: pinning {sent} - the grain the store holds")
        out.append(f"[census] {f}: RECEIPT window>={DAY} returned={returned} skipped_new=0 "
                   f"stored={stored} {verdict}")
    out.append(f"  {status:16} census/_all")
    return out


def _write(tmp_path, lines, name="new.log"):
    p = tmp_path / name
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(p)


def _run(tmp_path, lines, *extra):
    return P.main(["--log", _write(tmp_path, lines), "--old-log", _old_log(tmp_path), *extra])


def test_a_clean_pass_passes_on_the_log(tmp_path, capsys):
    assert _run(tmp_path, _new_lines(), "--log-only") == 0
    assert "PROOF PASS (log checks only)" in capsys.readouterr().out


def test_the_2026_10_10_log_itself_fails(tmp_path):
    old = _old_log(tmp_path)
    assert P.main(["--log", old, "--old-log", old, "--log-only"]) == 1


@pytest.mark.parametrize("mutate", [
    lambda L: [ln for ln in L if "eits/qtax: RECEIPT" not in ln],                     # one missing
    lambda L: [ln.replace("stored=3 MATCH", "stored=4 DIFFER") if "vip:" in ln else ln for ln in L],
    lambda L: L + ["[census] eits/marts: SUB-UNIT FAILED - HTTP 400: error: unknown predicate"],
    lambda L: L + ["[census] SUB-UNIT FAILED - eits/qss time=from 2026 for=us:* - TransientError: "
                   "Read timed out"],                                                # no 'HTTP 400'
    lambda L: [ln.replace("stored=3 MATCH", "stored=3 DIFFER") if "eits/hv:" in ln else ln
               for ln in L],                                                         # verdict alone
    lambda L: [ln.replace("stored=3 MATCH", "stored=3 UNREAD") if "eits/m3:" in ln else ln
               for ln in L],
    lambda L: L + ["census: 1/45 sub-unit(s) REFUSED by the API (HTTP 400 x1)"],
    lambda L: [ln.replace("'time_slot_id'", "'us': '1', 'time_slot_id'") if "eits/mrts:" in ln
               else ln for ln in L],                                                   # geo pin back
    lambda L: [ln.replace("'CTY_CODE': '-'", "'X': '1'") if "exports/hs:" in ln else ln
               for ln in L],                                                           # control changed
    lambda L: L + ["[census] intltrade/exports/sitcexport: RETIRED UPSTREAM - not fetched"],
    lambda L: [ln.replace("  ok ", "  partial ") for ln in L],                          # not green
    lambda L: [ln.replace("  ok ", "  error ") for ln in L],                            # run 3437
    lambda L: [ln for ln in L if "census/_all" not in ln or ">>>" in ln],              # no summary
])
def test_each_check_can_fail(tmp_path, mutate):
    assert _run(tmp_path, mutate(_new_lines()), "--log-only") == 1


def _partial_with(lines, failure):
    out = [ln.replace("  ok ", "  partial ") for ln in lines]
    return out + [f"[census] SUB-UNIT FAILED - {failure}"]


def test_a_partial_whose_only_failure_is_transient_on_a_control_passes(tmp_path, capsys):
    """A census.gov reset on exports/naics (a control: its request is unchanged by the fix) must not
    block the proof - the fix itself turns that reset into an honest named partial."""
    lines = _partial_with(_new_lines(), "intltrade/exports/naics time=2026-09 for=None - "
                          "TransientError: census intltrade/exports/naics: ChunkedEncodingError: "
                          "Connection broken after 3 attempts")
    assert _run(tmp_path, lines, "--log-only") == 0
    assert "ACCEPTED partial" in capsys.readouterr().out


@pytest.mark.parametrize("failure", [
    # transient, but on one of the 29 the proof is about
    "intltrade/exports/sitcexport time=2025-12 for=world:* - TransientError: census x: HTTP 503",
    # structural on a control
    "intltrade/exports/naics: boundary re-fetch returned no rows",
    # merge refused on a control (not a publisher hiccup)
    "intltrade/exports/naics: merge refused",
])
def test_a_partial_with_any_other_failure_fails(tmp_path, failure):
    assert _run(tmp_path, _partial_with(_new_lines(), failure), "--log-only") == 1


def test_a_partial_with_no_named_failure_fails(tmp_path):
    lines = [ln.replace("  ok ", "  partial ") for ln in _new_lines()]
    assert _run(tmp_path, lines, "--log-only") == 1


def _store(tmp_path, beta_val="5", twin_val="5", rows=3):
    d = tmp_path / "store"
    d.mkdir()
    dates = pa.array([dt.date(2026, 8, 1)] * rows + [dt.date(2026, 7, 1)], pa.date32())
    for f in P.REFUSED:
        if f.startswith("eits/"):
            pq.write_table(pa.table({"obs_date": dates}), d / (f.replace("/", "__") + ".parquet"))
    for b in P.BETA:
        fam, leaf = b.split("/")
        base = leaf[:-6]
        n = rows + 1
        pq.write_table(pa.table({
            "series_key": ["k"] * n, "obs_date": dates, "SITC": ["00"] * n, "world": ["1"] * n,
            "DISTRICT": ["-"] * n, "ALL_VAL_MO": [beta_val] * n}),
            d / f"intltrade__{fam}__{leaf}.parquet")
        pq.write_table(pa.table({
            "series_key": ["k"], "obs_date": pa.array([dt.date(2025, 12, 1)], pa.date32()),
            "SITC": ["00"], "CTY_CODE": ["-"], "DISTRICT": ["-"], "ALL_VAL_MO": [twin_val]}),
            d / f"intltrade__{fam}__{base}.parquet")
        # the BETA 2025-12 row
        t = pq.read_table(d / f"intltrade__{fam}__{leaf}.parquet")
        extra = pa.table({"series_key": ["k"], "obs_date": pa.array([dt.date(2025, 12, 1)],
                                                                     pa.date32()),
                          "SITC": ["00"], "world": ["1"], "DISTRICT": ["-"],
                          "ALL_VAL_MO": [beta_val]})
        pq.write_table(pa.concat_tables([t, extra]), d / f"intltrade__{fam}__{leaf}.parquet")
    return str(d)


def test_store_mode_passes_when_the_store_agrees_and_beta_equals_the_twin(tmp_path, capsys):
    store = _store(tmp_path)
    assert _run(tmp_path, _new_lines(), "--store", store) == 0, capsys.readouterr().out


def test_store_mode_fails_when_the_store_disagrees_with_the_receipt(tmp_path):
    store = _store(tmp_path, rows=2)                 # store holds 2 at the window, receipt says 3
    assert _run(tmp_path, _new_lines(), "--store", store) == 1


def test_store_mode_fails_when_a_beta_value_is_still_pre_revision(tmp_path, capsys):
    store = _store(tmp_path, beta_val="5", twin_val="7")
    assert _run(tmp_path, _new_lines(), "--store", store) == 1
    assert "differing 1" in capsys.readouterr().out


def test_a_missing_store_file_is_a_failure_not_a_skip(tmp_path):
    store = _store(tmp_path)
    os.remove(os.path.join(store, "eits__marts.parquet"))
    assert _run(tmp_path, _new_lines(), "--store", store) == 1
