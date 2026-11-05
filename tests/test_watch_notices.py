"""jobs/watch_notices.py and tools/maintenance_queue.py: the publisher-notice watcher.

Every rule is planted with a positive AND a negative case, so a test that cannot see the defect fails
instead of passing (receipts must be able to fail)."""
import datetime as dt
import fnmatch
import json
import os
import re
import sys

import pytest
import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "jobs"))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import watch_notices as wn  # noqa: E402
import maintenance_queue as mq  # noqa: E402

NOW = dt.datetime(2026, 11, 5, 12, 0, tzinfo=dt.timezone.utc)
TERMS = yaml.safe_load(open(wn.TERMS, encoding="utf-8"))

RSS = b"""<?xml version="1.0"?><rss version="2.0"><channel><title>x</title>
<item><title>API v1 will be discontinued on 31 March 2027</title><link>https://pub.example.org/news/1?utm_source=a</link>
<pubDate>Mon, 02 Nov 2026 09:00:00 GMT</pubDate></item>
<item><title>Quarterly GDP release</title><link>https://pub.example.org/news/2</link>
<pubDate>Tue, 03 Nov 2026 09:00:00 GMT</pubDate></item>
<item><title>Old dataset retired</title><link>https://pub.example.org/news/0</link>
<pubDate>Mon, 01 Jan 2024 09:00:00 GMT</pubDate></item>
</channel></rss>"""

ATOM = b"""<?xml version="1.0" encoding="utf-8"?><feed xmlns="http://www.w3.org/2005/Atom"><title>x</title>
<entry><title>Dataflow QNA migrated to the new API</title><link rel="alternate" href="/a/7"/>
<updated>2026-11-01T10:00:00Z</updated><summary>moved</summary></entry></feed>"""

HTML = b"""<html><body><nav><a href="/about">About: service shut down notice</a></nav>
<main class="news"><a href="/n/9">The SDMX endpoint will be removed</a><a href="#top">top</a>
<a href="/n/10">Annual report</a></main></body></html>"""


def reg(feeds, pid="pub", sources=("src_a",)):
    return {"publishers": [{"publisher_id": pid, "sources": list(sources), "feeds": feeds}]}


def fetcher(bodies, calls=None, fail=()):
    def f(url, etag, modified):
        if calls is not None:
            calls.append((url, etag))
        if url in fail:
            raise OSError("connection reset")
        if etag == "E1":
            return 304, None, "E1", None
        return 200, bodies[url], "E1", "Mon, 02 Nov 2026 09:00:00 GMT"
    return f


# ------------------------------------------------------------------------------------------ registry
def test_registry_rules_hold_on_the_committed_file():
    r = yaml.safe_load(open(wn.REGISTRY, encoding="utf-8"))
    ids = [s["source_id"] for s in yaml.safe_load(open(os.path.join(ROOT, "updater", "registry.yaml"),
                                                       encoding="utf-8"))["sources"]]
    pids, urls = set(), set()
    for p in r["publishers"]:
        assert p["publisher_id"] not in pids, f"duplicate publisher {p['publisher_id']}"
        pids.add(p["publisher_id"])
        for g in p["sources"]:
            assert fnmatch.filter(ids, g), f"{p['publisher_id']}: glob {g!r} matches no registry source"
        if not p.get("feeds"):
            assert re.search(r"\d{4}-\d{2}-\d{2}", p.get("notes", "")), \
                f"{p['publisher_id']}: an empty feed list needs a dated note"
        for fd in p.get("feeds") or []:
            assert fd["url"].startswith("https://"), fd["url"]
            assert "nomics" not in fd["url"].lower(), "relay host is banned (R251)"
            assert fd["kind"] in wn.KINDS
            assert int(fd.get("poll_days", 7)) >= 1
            assert fd.get("verified_by"), "a feed goes in only after someone saw it (R381)"
            assert fd["url"] not in urls, f"duplicate feed {fd['url']}"
            urls.add(fd["url"])


# ------------------------------------------------------------------------------------------ parsing
def test_rss_atom_and_html_parse():
    rss = wn.parse_items("rss", RSS, "https://pub.example.org/feed")
    assert [i["title"][:6] for i in rss] == ["API v1", "Quarte", "Old da"]
    atom = wn.parse_items("atom", ATOM, "https://pub.example.org/feed.atom")
    assert atom[0]["link"] == "https://pub.example.org/a/7" and atom[0]["summary"] == "moved"
    page = wn.parse_items("html_page", HTML, "https://pub.example.org/news", "main.news")
    assert [i["title"] for i in page] == ["The SDMX endpoint will be removed", "Annual report"], \
        "links outside the scope (nav) and fragment links are not items"
    whole = wn.parse_items("html_page", HTML, "https://pub.example.org/news")
    assert any("shut down" in i["title"] for i in whole), "no selector = the whole page"


def test_a_body_that_is_not_xml_raises():
    with pytest.raises(ValueError):
        wn.parse_items("rss", b"<html>gone</html", "https://x/")


def test_dates():
    assert wn.parse_date("Mon, 02 Nov 2026 09:00:00 GMT") == dt.datetime(2026, 11, 2, 9, tzinfo=dt.timezone.utc)
    assert wn.parse_date("2026-11-01T10:00:00Z").day == 1
    assert wn.parse_date("not a date") is None and wn.parse_date("") is None


# ------------------------------------------------------------------------------------------ scoring
@pytest.mark.parametrize("title,queued", [
    ("API v1 will be discontinued on 31 March 2027", True),
    ("Dataset X retired", True),
    ("Die Reihe wird eingestellt", True),
    ("Quarterly GDP release", False),
    ("Minor update", False),                   # one medium word is below the threshold
    ("Retirement age statistics published", True),   # retire* is broad on purpose; triage filters
])
def test_score_threshold(title, queued):
    s, _ = wn.score(title, wn.compile_terms(TERMS))
    assert (s >= wn.THRESHOLD) is queued, (title, s)


def test_scope_words_add_weight():
    c = wn.compile_terms(TERMS)
    base, _ = wn.score("planned maintenance", c)
    more, m = wn.score("planned maintenance on api.imf.org", c, ["api.imf.org"])
    assert more == base + wn.SCOPE and "api.imf.org" in m


def test_whole_word_terms_do_not_match_inside_words():
    s, m = wn.score("the shutdowns list", wn.compile_terms(TERMS))
    assert "shutdown" not in m


# ------------------------------------------------------------------------------------------ run
FEED = {"url": "https://pub.example.org/feed", "kind": "rss", "poll_days": 1, "verified_by": "t"}


def test_first_poll_queues_only_recent_strong_items_and_marks_all_seen():
    lines, st, stats = wn.run(reg([FEED]), TERMS, {}, NOW, fetcher({FEED["url"]: RSS}), sleep=lambda s: None)
    assert [l["title"][:6] for l in lines] == ["API v1"], "2024 item is history; GDP item scores low"
    assert lines[0]["sources"] == ["src_a"] and lines[0]["status"] == "new"
    assert len(st["seen"]) == 3 and stats["low"] == 1
    # same feed again later: nothing new, and the stored ETag is sent
    calls = []
    lines2, _, stats2 = wn.run(reg([FEED]), TERMS, st, NOW + dt.timedelta(days=2),
                               fetcher({FEED["url"]: RSS}, calls), sleep=lambda s: None)
    assert lines2 == [] and calls == [(FEED["url"], "E1")] and stats2["not_modified"] == 1


def test_a_feed_is_not_fetched_before_its_poll_days():
    _, st, _ = wn.run(reg([FEED]), TERMS, {}, NOW, fetcher({FEED["url"]: RSS}), sleep=lambda s: None)
    calls = []
    wn.run(reg([FEED]), TERMS, st, NOW + dt.timedelta(hours=3), fetcher({FEED["url"]: RSS}, calls),
           sleep=lambda s: None)
    assert calls == []


def test_a_new_item_after_the_first_poll_is_queued_even_undated():
    _, st, _ = wn.run(reg([FEED]), TERMS, {}, NOW, fetcher({FEED["url"]: RSS}), sleep=lambda s: None)
    st["feeds"][FEED["url"]]["etag"] = None                       # force a full body
    body = RSS.replace(b"</channel>", b"<item><title>Service will cease</title>"
                                      b"<link>https://pub.example.org/news/3</link></item></channel>")
    lines, _, _ = wn.run(reg([FEED]), TERMS, st, NOW + dt.timedelta(days=2),
                         fetcher({FEED["url"]: body}), sleep=lambda s: None)
    assert [l["title"] for l in lines] == ["Service will cease"]


def test_three_failures_raise_one_feed_broken_item():
    st, out = {}, []
    for k in range(5):
        lines, st, _ = wn.run(reg([FEED]), TERMS, st, NOW + dt.timedelta(days=2 * k),
                              fetcher({}, fail={FEED["url"]}), sleep=lambda s: None)
        out += lines
    assert [l["kind"] for l in out] == ["feed_broken"], "exactly one, at the third failure"
    assert st["feeds"][FEED["url"]]["failures"] == 5


def test_two_feeds_on_one_host_are_spaced():
    f2 = dict(FEED, url="https://pub.example.org/feed2")
    waits = []
    wn.run(reg([FEED, f2]), TERMS, {}, NOW, fetcher({FEED["url"]: RSS, f2["url"]: RSS}),
           sleep=waits.append)
    assert len(waits) == 1 and 0 < waits[0] <= wn.HOST_GAP_S


def test_the_request_cap_skips_and_counts(monkeypatch):
    monkeypatch.setattr(wn, "MAX_REQUESTS", 1)
    f2 = dict(FEED, url="https://other.example.org/feed")
    calls = []
    _, _, stats = wn.run(reg([FEED, f2]), TERMS, {}, NOW,
                         fetcher({FEED["url"]: RSS, f2["url"]: RSS}, calls), sleep=lambda s: None)
    assert len(calls) == 1 and stats["skipped_cap"] == 1


def test_dedup_ignores_tracking_query_and_fragment():
    a = wn.item_key("p", {"link": "https://Pub.example.org/news/1/?utm_source=x#frag"})
    b = wn.item_key("p", {"link": "https://pub.example.org/news/1"})
    c = wn.item_key("p", {"link": "https://pub.example.org/news/1?id=5"})
    assert a == b and a != c


def test_run_does_not_mutate_the_callers_state():
    st = {"feeds": {}, "seen": []}
    wn.run(reg([FEED]), TERMS, st, NOW, fetcher({FEED["url"]: RSS}), sleep=lambda s: None)
    assert st == {"feeds": {}, "seen": []}


# ------------------------------------------------------------------------------------------ files + queue
def test_main_dry_run_writes_nothing(tmp_path, monkeypatch):
    r = tmp_path / "n.yaml"
    r.write_text(yaml.safe_dump(reg([FEED])))
    monkeypatch.setattr(wn, "http_get", fetcher({FEED["url"]: RSS}))
    monkeypatch.setattr(wn.time, "sleep", lambda s: None)
    q, s = tmp_path / "q.jsonl", tmp_path / "s.json"
    assert wn.main(["--dry-run", "--registry", str(r), "--queue", str(q), "--state", str(s)]) == 0
    assert not q.exists() and not s.exists()
    assert wn.main(["--registry", str(r), "--queue", str(q), "--state", str(s)]) == 0
    assert len(q.read_text().splitlines()) == 1 and json.loads(s.read_text())["seen"]


def test_queue_folds_status_events_and_repeated_lines(tmp_path):
    q = tmp_path / "q.jsonl"
    item = {"id": "ntc_1", "component": "notices", "kind": "publisher_notice", "status": "new", "title": "t"}
    wn.append_queue(str(q), [item, dict(item, title="re-queued after a crash")])
    mq.set_status("ntc_1", "relevant", "affects imf", str(q), now=NOW)
    items = mq.load(str(q))
    assert list(items) == ["ntc_1"] and items["ntc_1"]["status"] == "relevant"
    assert items["ntc_1"]["title"] == "t" and len(items["ntc_1"]["history"]) == 1
    assert mq.stats(items) == {("notices", "relevant"): 1}


def test_queue_refuses_bad_input(tmp_path):
    q = tmp_path / "q.jsonl"
    q.write_text('{"id": "a", "status": "new"}\nnot json\n')
    with pytest.raises(SystemExit, match="not JSON"):
        mq.load(str(q))
    q.write_text('{"id": "a", "status": "new"}\n')
    with pytest.raises(SystemExit, match="no item"):
        mq.set_status("b", "relevant", "x", str(q))
    with pytest.raises(SystemExit, match="unknown status"):
        mq.set_status("a", "maybe", "x", str(q))


def test_coverage_lists_every_publisher_without_a_feed():
    gaps = mq.coverage({"publishers": reg([])["publishers"] + reg([FEED], pid="has")["publishers"]})
    assert [g[0] for g in gaps] == ["pub"]
