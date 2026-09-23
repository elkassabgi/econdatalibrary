"""hagstofa: a stored table that answers HTTP 400/404 on the English site (reviews R1108, R1112).

  1. The Icelandic site may still serve it at the SAME path: SJA04901 answers 400 on pxen but 200 on
     pxis, to 2025, first variable Fisktegund - the stored scheme. It is fetched there, but ONLY when
     the Icelandic codes reproduce a key scheme the table already stores.
  2. Otherwise both WHOLE trees (pxen and pxis) decide: listed at its own path -> structural; found
     elsewhere -> MOVED (structural, named - the path is in the key, so following it is a re-key);
     absent from both, read in full -> WITHDRAWN, kept frozen; any tree unreadable -> structural.
Measured live 2026-09-23 on pxen: FYR02103, FYR02104, FYR03002 moved to fyrirtaeki/skradfyrirtaeki/
9_eldraefni/. The real _get_meta, _listing, _table_tree and _fetch_table run; only HTTP is faked.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import hagstofa as H  # noqa: E402

EN, IS = H.BASE, H.BASE_IS


def _t(i):
    return {"id": i, "type": "t", "text": i, "updated": "2026-09-15T09:00:22"}


def _l(i):
    return {"id": i, "type": "l", "text": i}


def _tree(base, utf=("SJA04902.px", "SJA04903.px"), eldra=("FYR02103.px",)):
    return {
        f"{base}/": [{"dbid": "Atvinnuvegir", "text": "Atvinnuvegir"}],
        f"{base}/Atvinnuvegir/": [_l("sjavarutvegur"), _l("fyrirtaeki")],
        f"{base}/Atvinnuvegir/sjavarutvegur/": [_l("utf")],
        f"{base}/Atvinnuvegir/sjavarutvegur/utf/": [_t(x) for x in utf],
        f"{base}/Atvinnuvegir/fyrirtaeki/": [_l("2_skraningar"), _l("9_eldraefni")],
        f"{base}/Atvinnuvegir/fyrirtaeki/2_skraningar/": [_t("FYR02101.px")],
        f"{base}/Atvinnuvegir/fyrirtaeki/9_eldraefni/": [_t(x) for x in eldra],
    }


GONE = {"sjavarutvegur/utf/SJA04901.px", "fyrirtaeki/2_skraningar/FYR02103.px"}
SJA_IS_META = {"title": "Útflutningur", "variables": [
    {"code": "Fisktegund", "values": ["0"], "valueTexts": ["Alls"]},
    {"code": "Ár", "values": ["2024", "2025"], "valueTexts": ["2024", "2025"], "time": True}]}


class _R:
    def __init__(self, status, body=None, bad_json=False):
        self.status_code, self._body, self._bad = status, body, bad_json

    def json(self):
        if self._bad:
            raise ValueError("not json")
        return self._body


class _Sess:
    """pxen and pxis trees; a table GET answers 400 when GONE on that site, or the given metadata."""
    def __init__(self, en=None, is_=None, override=None, is_meta=None):
        self.tree = {**(en or _tree(EN)), **(is_ or _tree(IS))}
        self.override, self.is_meta, self.asked = dict(override or {}), dict(is_meta or {}), []

    def get(self, url, timeout=None):
        self.asked.append(url)
        if url in self.override:
            o = self.override[url]
            if isinstance(o, list):                          # a sequence: one response per request
                o = o.pop(0) if len(o) > 1 else o[0]
            if isinstance(o, Exception):
                raise o
            return o
        if url in self.tree:
            return _R(200, self.tree[url])
        for base in (EN, IS):
            for p in GONE:
                if url == f"{base}/Atvinnuvegir/{p}/":
                    if base == IS and p in self.is_meta:
                        return _R(200, self.is_meta[p])
                    return _R(400, {"error": "Bad request"})
        raise AssertionError(f"unexpected GET {url}")


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(H.time, "sleep", lambda s: None)


def _fetch(sess, path="sjavarutvegur/utf/SJA04901.px", since=dt.date(2024, 12, 31), schemes=None):
    return H._fetch_table(sess, "Atvinnuvegir", path, "ICE:Atvinnuvegir:" + path.replace("/", ":"), since,
                          stored_schemes=schemes)


# ---- 1. the Icelandic fallback -------------------------------------------------------------
def test_a_table_dropped_from_the_english_site_is_fetched_from_the_icelandic_one(monkeypatch):
    """R1112: SJA04901 answers 400 on pxen, 200 on pxis with the stored scheme - fetch it there."""
    posted = []

    def _post(sess, url, body):
        posted.append(url)
        return {"id": ["Fisktegund", "Ár"], "size": [1, 1], "role": {"time": ["Ár"]},
                "dimension": {"Fisktegund": {"category": {"index": {"0": 0}, "label": {"0": "Alls"}}},
                              "Ár": {"category": {"index": {"2025": 0}, "label": {"2025": "2025"}}}},
                "value": [5.0]}
    monkeypatch.setattr(H, "_post_data", _post)
    sess = _Sess(is_meta={"sjavarutvegur/utf/SJA04901.px": SJA_IS_META})
    rows, outcome = _fetch(sess, schemes={("Fisktegund",)})
    assert outcome == "data" and posted == [f"{IS}/Atvinnuvegir/sjavarutvegur/utf/SJA04901.px/"], posted
    assert rows == [("ICE:Atvinnuvegir:sjavarutvegur:utf:SJA04901.px:Fisktegund=0", dt.date(2025, 12, 31), 5.0)]


def test_an_icelandic_copy_whose_codes_match_no_stored_scheme_is_a_rekey_not_a_merge():
    sess = _Sess(is_meta={"sjavarutvegur/utf/SJA04901.px": SJA_IS_META})
    assert _fetch(sess, schemes={("Species",)}) == ([], "structural")
    assert _fetch(_Sess(is_meta={"sjavarutvegur/utf/SJA04901.px": SJA_IS_META}), schemes=None) == ([], "structural")


def test_an_english_rename_is_fetched_from_the_icelandic_site_that_still_produces_the_stored_keys(monkeypatch):
    """R1117 (c): SJA04903's English site renamed Tegund/... to Species/...; the Icelandic site still
    produces the stored scheme, and value codes are identical - stay on the stored scheme."""
    posted = []
    monkeypatch.setattr(H, "_post_data", lambda sess, url, body: posted.append(url) or None)
    en_meta = {"variables": [{"code": "Species", "values": ["0"], "valueTexts": ["All"]},
                             {"code": "Year", "values": ["2024", "2025"], "valueTexts": ["2024", "2025"],
                              "time": True}]}
    is_meta = {"variables": [{"code": "Tegund", "values": ["0"], "valueTexts": ["Alls"]},
                             {"code": "Ár", "values": ["2024", "2025"], "valueTexts": ["2024", "2025"],
                              "time": True}]}
    url = "sjavarutvegur/utf/SJA04903.px"
    sess = _Sess(override={f"{EN}/Atvinnuvegir/{url}/": _R(200, en_meta),
                           f"{IS}/Atvinnuvegir/{url}/": _R(200, is_meta)})
    _fetch(sess, url, schemes={("Tegund",)})
    assert posted == [f"{IS}/Atvinnuvegir/{url}/"], posted
    posted.clear()
    sess = _Sess(override={f"{EN}/Atvinnuvegir/{url}/": _R(200, en_meta),
                           f"{IS}/Atvinnuvegir/{url}/": _R(200, is_meta)})
    _fetch(sess, url, schemes={("Species",)})
    assert posted == [f"{EN}/Atvinnuvegir/{url}/"], "the English site when it matches - no Icelandic GET"
    assert not any(u.startswith(IS) for u in sess.asked), sess.asked


def test_when_both_sites_renamed_the_english_fetch_stands_and_the_guard_decides(monkeypatch):
    """VIN00002: 'Kyn / aldur' -> 'Kyn/aldur' on BOTH sites. No site produces the stored scheme, so
    the Icelandic copy is not preferred; the key-scheme guard refuses the merge (a re-key)."""
    posted = []
    monkeypatch.setattr(H, "_post_data", lambda sess, url, body: posted.append(url) or None)
    meta = {"variables": [{"code": "Kyn/aldur", "values": ["0"], "valueTexts": ["x"]},
                          {"code": "Mánuður", "values": ["2026M07"], "valueTexts": ["2026M07"], "time": True}]}
    url = "vinnumarkadur/VIN00002.px"
    sess = _Sess(override={f"{EN}/Atvinnuvegir/{url}/": _R(200, meta), f"{IS}/Atvinnuvegir/{url}/": _R(200, meta)})
    _fetch(sess, url, since=dt.date(2026, 6, 1), schemes={("Kyn / aldur",)})
    assert posted == [f"{EN}/Atvinnuvegir/{url}/"], posted


# ---- 2. both trees decide -------------------------------------------------------------------
def test_a_table_absent_from_both_trees_is_withdrawn_and_kept_frozen():
    sess = _Sess()
    sess._hagstofa_withdrawn = {}
    assert _fetch(sess) == ([], "quiet")
    v = sess._hagstofa_withdrawn["Atvinnuvegir/sjavarutvegur/utf/SJA04901.px"]
    assert v == {"verdict": "withdrawn", "date": dt.date.today().isoformat()}
    assert any(u.startswith(IS) for u in sess.asked) and any(u.startswith(EN) for u in sess.asked)


def test_a_table_found_elsewhere_in_either_tree_is_moved_not_withdrawn():
    """Review R1108: 3 of the 4 tables a folder-only rule called withdrawn had MOVED."""
    for en_eldra, is_eldra in ((("FYR02103.px",), ("FYR09999.px",)), (("FYR09999.px",), ("FYR02103.px",))):
        sess = _Sess(en=_tree(EN, eldra=en_eldra), is_=_tree(IS, eldra=is_eldra))
        sess._hagstofa_withdrawn = {}
        assert _fetch(sess, "fyrirtaeki/2_skraningar/FYR02103.px") == ([], "structural")
        v = sess._hagstofa_withdrawn["Atvinnuvegir/fyrirtaeki/2_skraningar/FYR02103.px"]
        assert v["verdict"] == "moved" and v["to"] == ["Atvinnuvegir/fyrirtaeki/9_eldraefni/FYR02103.px"]


def test_a_table_only_the_icelandic_tree_still_lists_at_its_path_is_not_withdrawn():
    """R1112's rule: 'withdrawn' needs EVERY language tree to lack it."""
    sess = _Sess(is_=_tree(IS, utf=("SJA04901.px", "SJA04902.px")))
    sess._hagstofa_withdrawn = {}
    assert _fetch(sess) == ([], "structural")
    assert sess._hagstofa_withdrawn == {}, "not 'moved' to where it already is, and not cached"


@pytest.mark.parametrize("url,bad", [
    (f"{EN}/", _R(500, [{"dbid": "Atvinnuvegir"}])),
    (f"{IS}/", _R(500, [{"dbid": "Atvinnuvegir"}])),
    (f"{EN}/Atvinnuvegir/fyrirtaeki/9_eldraefni/", _R(500, [_t("FYR02103.px")])),
    (f"{IS}/Atvinnuvegir/fyrirtaeki/9_eldraefni/", _R(429, [_t("FYR02103.px")])),
    (f"{EN}/Atvinnuvegir/fyrirtaeki/9_eldraefni/", _R(200, [])),
    (f"{IS}/Atvinnuvegir/fyrirtaeki/9_eldraefni/", _R(200, {"a": 1})),
    (f"{EN}/Atvinnuvegir/fyrirtaeki/9_eldraefni/", _R(200, bad_json=True)),
    (f"{IS}/Atvinnuvegir/fyrirtaeki/9_eldraefni/", H.requests.ConnectionError("dropped")),
])
def test_a_tree_not_read_in_full_is_never_evidence_of_withdrawal(url, bad):
    """One unreadable listing voids the search: a partial tree would call a moved table withdrawn."""
    assert _fetch(_Sess(override={url: bad})) == ([], "structural")


def test_a_listing_refused_once_with_429_is_retried_not_given_up():
    url = f"{EN}/Atvinnuvegir/fyrirtaeki/9_eldraefni/"
    sess = _Sess(override={url: [_R(429), _R(200, _tree(EN)[url])]})
    assert _fetch(sess, "fyrirtaeki/2_skraningar/FYR02103.px") == ([], "structural")
    assert sess.asked.count(url) == 2
    sess = _Sess(override={url: [_R(429), _R(200, _tree(EN)[url])]})
    assert _fetch(sess) == ([], "quiet"), "both trees were read in full after the retry"


def test_each_tree_is_searched_once_per_run():
    sess = _Sess()
    _fetch(sess)
    n = len(sess.asked)
    _fetch(sess, "fyrirtaeki/2_skraningar/FYR02103.px")
    assert len(sess.asked) == n + 2, "only the second table's own two metadata GETs"


def test_a_recent_verdict_is_reused_without_searching_and_an_old_one_is_rechecked():
    today = dt.date.today()
    me = "Atvinnuvegir/sjavarutvegur/utf/SJA04901.px"
    sess = _Sess()
    sess._hagstofa_withdrawn = {me: {"verdict": "withdrawn", "date": today.isoformat()}}
    assert _fetch(sess) == ([], "quiet") and len(sess.asked) == 2, sess.asked   # en + is metadata only
    old = (today - dt.timedelta(days=H.WITHDRAWN_RECHECK_DAYS)).isoformat()
    sess = _Sess()
    sess._hagstofa_withdrawn = {me: {"verdict": "withdrawn", "date": old}}
    assert _fetch(sess) == ([], "quiet") and len(sess.asked) > 2, "a month-old verdict is re-verified"
    moved = "Atvinnuvegir/fyrirtaeki/2_skraningar/FYR02103.px"
    sess = _Sess()
    sess._hagstofa_withdrawn = {moved: {"verdict": "moved", "to": ["x"], "date": today.isoformat()}}
    assert _fetch(sess, "fyrirtaeki/2_skraningar/FYR02103.px") == ([], "structural")
    assert len(sess.asked) == 2


def test_a_never_stored_table_that_400s_is_absent_without_searching():
    sess = _Sess()
    assert _fetch(sess, since=None) == ([], "empty") and len(sess.asked) == 1


def test_update_persists_the_verdict_and_the_next_run_does_not_search(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(H.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(H, "_load_catalog", lambda: [{"db": "Atvinnuvegir", "path": "sjavarutvegur/utf/SJA04901.px",
                                                     "id": "SJA04901.px", "text": "x"}])
    pq.write_table(pa.table({"series_key": ["ICE:Atvinnuvegir:sjavarutvegur:utf:SJA04901.px:Fisktegund=0"],
                             "obs_date": pa.array([dt.date(2024, 12, 31)]), "value": [1.0]}),
                   str(tmp_path / "Atvinnuvegir.parquet"))
    sessions = []

    def _new():
        s = _Sess()
        sessions.append(s)
        return s
    monkeypatch.setattr(H, "_session", _new)
    unit = types.SimpleNamespace(config={}, key="hagstofa/_all")
    H.update(unit, None)
    saved = json.loads((tmp_path / H.WITHDRAWN_FILE).read_text())
    assert saved["Atvinnuvegir/sjavarutvegur/utf/SJA04901.px"]["verdict"] == "withdrawn"
    H.update(unit, None)
    assert len(sessions[1].asked) == 2, sessions[1].asked
