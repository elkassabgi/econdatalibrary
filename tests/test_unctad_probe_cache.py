"""unctad: a resume does not re-ask the size-cap probes an earlier pass already answered (review R1151).

Only LEAF chunks spilled, so a resume replayed the split walk by re-POSTing every capped probe - and a
binary split has about as many capped probes as leaves. The answers now sit next to the spills, under the
same release gate. Hermetic: the Facts API and the code lists are faked; the spill dir is a tmp dir.
"""
import os
import re
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import jobs.ingest_unctad_ds as j  # noqa: E402

YEARS = [2001, 2002, 2003, 2004]
PARTNERS = [f"P{i}" for i in range(8)]
CAP = 4                                                     # cells a request may ask for
META = {"version": 7, "defaults": {"rowAxe": [{"field": "Partner", "name": "Partner"}],
                                   "colAxe": [{"field": "Year", "name": "Year", "isTime": True}]}}
TDIM = {"name": "Year", "field": "Year", "codetype": "number"}


class _Api:
    """Caps any request over CAP cells (years x partners asked), like the real 400 'maximal size'."""

    def __init__(self, die_after=None):
        self.asked, self.die_after = [], die_after

    def __call__(self, ds, select, cid, key, flt=None):
        if self.die_after is not None and len(self.asked) >= self.die_after:
            raise RuntimeError("killed")
        self.asked.append(flt)
        ys = re.search(r"Year in \(([^)]*)\)", flt or "")
        ps = re.search(r"Partner/Code in \(([^)]*)\)", flt or "")
        ny = len(ys.group(1).split(",")) if ys else len(YEARS)
        np_ = len(ps.group(1).split(",")) if ps else len(PARTNERS)
        if ny * np_ > CAP:
            raise j.FactsSizeCap(CAP, ny * np_)
        return f"rows for {flt}\n"


@pytest.fixture
def spill(tmp_path, monkeypatch):
    monkeypatch.setattr(j, "_spill_dir", lambda ds: str(tmp_path / "spill"))
    monkeypatch.setattr(j, "dim_codes", lambda ds, v, name: list(YEARS) if name == "Year" else list(PARTNERS))
    return tmp_path / "spill"


def _pull(monkeypatch, api, meta=META):
    monkeypatch.setattr(j, "facts_csv", api)
    return j.facts_csv_chunked("US.X", "M1", "c", "k", meta, TDIM)


def test_a_resume_asks_nothing_it_already_knows(spill, monkeypatch):
    first = _Api()
    out1 = _pull(monkeypatch, first)
    # top + 4 years capped + 8 leaves (each year: 8 partners > CAP, split into two halves of 4)
    assert len(first.asked) == 13 and len(out1) == 8, first.asked
    second = _Api()
    out2 = _pull(monkeypatch, second)
    assert second.asked == [], "every probe and every leaf was known"
    assert out2 == out1


def test_a_killed_pass_resumes_where_it_stopped(spill, monkeypatch):
    first = _Api(die_after=6)
    with pytest.raises(RuntimeError, match="killed"):
        _pull(monkeypatch, first)
    second = _Api()
    out = _pull(monkeypatch, second)
    assert len(out) == 8
    assert not set(second.asked) & set(first.asked), "nothing answered on pass 1 is asked again"
    assert len(first.asked) + len(second.asked) == 13


def test_negative_control_without_the_cache_a_resume_re_probes(spill, monkeypatch):
    """What the cache saves: with it removed, a fully spilled resume still POSTs the 5 capped probes."""
    _pull(monkeypatch, _Api())
    os.remove(spill / j.CAPPED_FILE)
    again = _Api()
    _pull(monkeypatch, again)
    assert len(again.asked) == 5 and all(a is None or "Partner" not in a for a in again.asked), again.asked


def test_a_new_release_ignores_the_old_answers(spill, monkeypatch):
    _pull(monkeypatch, _Api())
    fresh = _Api()
    _pull(monkeypatch, fresh, meta=dict(META, version=8))
    assert len(fresh.asked) == 13
    lines = (spill / j.CAPPED_FILE).read_text(encoding="utf-8").splitlines()
    assert len(lines) == 5, "the old release's probes were wiped with its spills"


@pytest.mark.parametrize("cut", [10, -2, -3])
def test_a_torn_last_line_is_asked_again(spill, monkeypatch, cut):
    """-2 cuts the end marker, -3 also a digit of the estimate: that one still parsed without the marker
    and would have changed the chunking (review R1152)."""
    _pull(monkeypatch, _Api())
    p = spill / j.CAPPED_FILE
    lines = p.read_text(encoding="utf-8").splitlines()
    p.write_text("\n".join(lines[:-1]) + "\n" + lines[-1][:cut], encoding="utf-8")
    again = _Api()
    _pull(monkeypatch, again)
    assert len(again.asked) == 1, again.asked


def test_a_torn_top_line_does_not_orphan_the_spills(spill, monkeypatch):
    first = _Api()
    _pull(monkeypatch, first)
    p = spill / j.CAPPED_FILE
    lines = p.read_text(encoding="utf-8").splitlines()
    assert first.asked[0] is None and lines[0].split("\t")[2] == "32"   # the top probe is line 1
    p.write_text(lines[0][:-3] + "\n", encoding="utf-8")                 # '...\t3' : estimate torn
    again = _Api()
    out = _pull(monkeypatch, again)
    assert again.asked[0] is None and len(out) == 8
    assert all(a is None or "Partner" not in a for a in again.asked), "no leaf re-fetched"


def test_an_unreachable_request_is_not_cached_and_is_asked_again(spill, monkeypatch):
    """Only a SIZE CAP is a property of the query. An unreachable year probe splits like a cap (the
    ladder is shared) but must be asked again on the next pass - and must not crash the recorder."""
    year_probe = "Year in (2002)"

    class _Flaky(_Api):
        def __init__(self, down):
            super().__init__()
            self.down = down

        def __call__(self, ds, select, cid, key, flt=None):
            if self.down and flt == year_probe:
                self.asked.append(flt)
                raise j.FactsUnreachable("gateway down")
            return super().__call__(ds, select, cid, key, flt)
    first = _Flaky(down=True)
    out1 = _pull(monkeypatch, first)
    assert len(out1) == 8 and year_probe in first.asked
    lines = (spill / j.CAPPED_FILE).read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4, "the top probe and 3 year probes; the unreachable one is not recorded"
    second = _Flaky(down=False)
    out2 = _pull(monkeypatch, second)
    assert second.asked == [year_probe] and out2 == out1


def test_the_spill_merger_reads_only_csv_chunks():
    src = open(os.path.join(ROOT, "tools", "_merge_unctad_spills.py"), encoding="utf-8").read()
    assert 'f.endswith(".csv")' in src and not j.CAPPED_FILE.endswith(".csv")
