"""Watch publishers' announcement feeds for a dataset, an API or a URL that is ENDING or CHANGING.

WHY. Every past publisher change reached us the same way: a fetch broke, then someone read the
publisher's site (the IMF HPDD->HPD rename, 13 World Bank ESG indicator ids re-coded - runbooks).
This job reads the feeds listed in updater/notices.yaml, scores each NEW item against
jobs/notice_terms.yaml, and appends the ones worth a look to the maintenance queue, so a person
reads the announcement before the fetch breaks. Design: docs/maintenance/SOURCE_NOTICES.md.

WHAT IT NEVER DOES. It publishes nothing: a queue item is private until someone triages it
(tools/maintenance_queue.py) and writes a reviewed entry into catalog/source_changes.yaml. It
reads no D1 and no R2, keeps no article text (title, link, date only - and the title is cut to
160 characters), and never retries a feed in a loop: a failure waits for the next run, and three
in a row raise one `feed_broken` item, because a removed feed is itself news.

Desktop only ("Cloudflare hosts, the desktop computes"). Polite: a feed is fetched only when its
poll_days have passed, with If-None-Match / If-Modified-Since, one request at a time, at least
HOST_GAP_S between two requests to one host, and at most MAX_REQUESTS a run.

    python jobs/watch_notices.py               # fetch due feeds, append to the queue
    python jobs/watch_notices.py --dry-run     # fetch and print; write nothing
"""
from __future__ import annotations

import argparse
import datetime as dt
import email.utils
import hashlib
import html.entities
import html.parser
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core.atomic import atomic_replace  # noqa: E402

REGISTRY = os.path.join(ROOT, "updater", "notices.yaml")
TERMS = os.path.join(ROOT, "jobs", "notice_terms.yaml")
STATE = os.path.join(ROOT, "data", "_notices", "state.json")
QUEUE = os.path.join(ROOT, "data", "_maintenance", "queue.jsonl")

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
KINDS = ("rss", "atom", "html_page")
THRESHOLD = 3            # strong term = 3, so one strong word is enough; medium words alone need 3
STRONG, MEDIUM, SCOPE = 3, 1, 2
HOST_GAP_S = 5.0
MAX_REQUESTS = 200
TIMEOUT_S = 30
MAX_BODY = 20_000_000
BROKEN_AFTER = 3         # consecutive failures before a feed_broken item
FIRST_POLL_DAYS = 90     # on a feed's first poll, only items dated this recently are queued
TITLE_MAX = 160


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso(t: dt.datetime) -> str:
    return t.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_yaml(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ---------------------------------------------------------------------------------------------- parse
def _local(el) -> str:
    return el.tag.rsplit("}", 1)[-1]


def _text(el, *names) -> str:
    """Non-empty child text for the FIRST of `names` present, in the order given - not document
    order, so `published` wins over an `updated` written before it (a revised old entry must not
    pass the first-poll age gate as new; review AR-432 #3). Namespace-agnostic."""
    for name in names:
        for child in el:
            if _local(child) != name:
                continue
            # itertext: an Atom type="xhtml" title holds its words in a <div>/<span>, not in .text
            # (StatCan's Daily: 290 entries read as empty titles - measured 2026-11-05).
            t = " ".join("".join(child.itertext()).split())
            if t:
                return t
    return ""


def _atom_link(entry) -> str:
    """rel=alternate (the default rel); failing that any link that is not an enclosure, a self/edit
    link or a related file (AR-432 #4)."""
    other = ""
    for child in entry:
        if _local(child) != "link" or not child.get("href"):
            continue
        rel = child.get("rel", "alternate")
        if rel == "alternate":
            return child.get("href")
        if rel not in ("enclosure", "self", "edit", "related") and not other:
            other = child.get("href")
    return other


def _rss_link(item) -> str:
    """<link> text; else an atom:link href; else a guid that is a URL (AR-432 #5)."""
    link = _text(item, "link")
    if link:
        return link
    for child in item:
        if _local(child) == "link" and child.get("href"):
            return child.get("href")
    guid = _text(item, "guid")
    return guid if guid.startswith(("http://", "https://")) else ""


def parse_date(s: str) -> "dt.datetime | None":
    s = (s or "").strip()
    if not s:
        return None
    try:
        t = email.utils.parsedate_to_datetime(s)            # RSS: RFC 822
    except (TypeError, ValueError, IndexError):
        t = None
    if t is None:
        m = re.match(r"(\d{4}-\d{2}-\d{2})(?:[T ](\d{2}:\d{2}(?::\d{2})?))?", s)  # Atom / ISO / html
        if not m:
            return None
        try:
            t = dt.datetime.fromisoformat(m.group(1) + ("T" + m.group(2) if m.group(2) else ""))
        except ValueError:
            return None
    return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)


class _Anchors(html.parser.HTMLParser):
    """(text, href) of every <a> inside the scope `tag`, `tag.class` or `.class` (whole page if no
    selector). The scope is tracked by the tag name of the element that opened it, so `.news` on a
    <section> works (AR-432 #8). An <a> left open is closed by the next <a> or the end of the body."""

    def __init__(self, selector: str = ""):
        super().__init__(convert_charrefs=True)
        tag, _, cls = (selector or "").partition(".")
        self.want_tag, self.want_cls = tag.lower(), cls
        self.scoped = bool(tag or cls)
        self.scope_tag, self.depth = None, 0                # depth > 0 = inside the scope
        self.out, self._href, self._buf = [], None, []

    def _inside(self) -> bool:
        return not self.scoped or self.depth > 0

    def _is_scope(self, tag, attrs) -> bool:
        if self.want_tag and tag != self.want_tag:
            return False
        return not self.want_cls or self.want_cls in (dict(attrs).get("class") or "").split()

    def _close_a(self):
        if self._href is not None:
            self.out.append((" ".join("".join(self._buf).split()), self._href))
            self._href = None

    def handle_starttag(self, tag, attrs):
        if self.scoped:
            if self.depth == 0 and self._is_scope(tag, attrs):
                self.scope_tag, self.depth = tag, 1
            elif self.depth and tag == self.scope_tag:
                self.depth += 1
        if tag == "a":
            self._close_a()
            if self._inside() and dict(attrs).get("href"):
                self._href, self._buf = dict(attrs)["href"], []

    def handle_endtag(self, tag):
        if tag == "a":
            self._close_a()
        if self.scoped and self.depth and tag == self.scope_tag:
            self.depth -= 1
            if self.depth == 0:
                self._close_a()

    def handle_data(self, data):
        if self._href is not None:
            self._buf.append(data)

    def close(self):
        super().close()
        self._close_a()


_XML_OWN = {"amp", "lt", "gt", "quot", "apos"}
_ENTITY = re.compile(rb"&([A-Za-z][A-Za-z0-9]{1,31});")


def _xml_entities(body: bytes) -> bytes:
    """HTML named entities (&mdash; &nbsp; ...) as numeric references. XML knows only five names, and a
    feed that uses another one with no DTD is not well-formed: the Fed's Data Download feed failed
    whole on one &mdash; (measured 2026-11-05). An unknown name is left alone, so it still fails loud."""
    def sub(m):
        name = m.group(1).decode("ascii")
        if name in _XML_OWN or name not in html.entities.name2codepoint:
            return m.group(0)
        return f"&#{html.entities.name2codepoint[name]};".encode("ascii")
    return _ENTITY.sub(sub, body)


def parse_items(kind: str, body: bytes, base_url: str, selector: str = "") -> list:
    """[{title, link, date, summary}] from one feed body. Raises ValueError on a body it cannot read."""
    items = []
    if kind in ("rss", "atom"):
        try:
            root = ET.fromstring(_xml_entities(body))
        except ET.ParseError as e:
            raise ValueError(f"not XML: {e}") from None
        for el in root.iter():
            local = el.tag.rsplit("}", 1)[-1]
            if local not in ("item", "entry"):
                continue
            link = _rss_link(el) if local == "item" else _atom_link(el)
            items.append({"title": _text(el, "title"),
                          "link": urllib.parse.urljoin(base_url, link) if link else "",
                          "date": _text(el, "pubDate", "published", "date", "issued", "updated"),
                          "summary": _text(el, "description", "summary")})
    elif kind == "html_page":
        p = _Anchors(selector)
        p.feed(body.decode("utf-8", errors="replace"))
        p.close()
        # ONE item per link, carrying the text of EVERY anchor to it. A "Read more" anchor before the
        # title anchor used to claim the key first, and the real title ("Series X discontinued") was
        # then skipped as seen and never scored (AR-432 #2). Title = the longest text; all texts score.
        by_link: dict = {}
        for text, href in p.out:
            if not text or href.startswith(("#", "mailto:", "javascript:")):
                continue
            by_link.setdefault(urllib.parse.urljoin(base_url, href), []).append(text)
        for link, texts in by_link.items():
            uniq = list(dict.fromkeys(texts))
            items.append({"title": max(uniq, key=len), "link": link, "date": "",
                          "summary": " | ".join(t for t in uniq if t != max(uniq, key=len))})
    else:
        raise ValueError(f"unknown feed kind {kind!r}")
    return items


# ---------------------------------------------------------------------------------------------- score
def _pattern(term: str) -> re.Pattern:
    t = re.escape(term.lower()).replace(r"\*", r"\w*")
    return re.compile(rf"(?<!\w){t}(?!\w)" if not term.endswith("*") else rf"(?<!\w){t}")


def compile_terms(terms: dict) -> dict:
    return {lvl: [(t, _pattern(t)) for words in (terms.get(lvl) or {}).values() for t in words]
            for lvl in ("strong", "medium")}


def score(text: str, compiled: dict, extra=()) -> "tuple[int, list]":
    low = (text or "").lower()
    s, matched = 0, []
    for lvl, w in (("strong", STRONG), ("medium", MEDIUM)):
        for term, pat in compiled[lvl]:
            if pat.search(low):
                s += w
                matched.append(term)
    for word in extra or ():
        if word and word.lower() in low:
            s += SCOPE
            matched.append(word)
    return s, matched


# ---------------------------------------------------------------------------------------------- dedup
# Query keys that only track the visitor. EVERY other key is kept, and so is the fragment: an
# allow-list (id, p, article) folded NewsID=101 and NewsID=102 into one key, and dropping the fragment
# folded /notices#a and /notices#b, so a discontinuation was marked seen without being scored (AR-432 #1).
# Only keys that are never an id anywhere: `source` and `ref` are not here, because World Bank DataBank
# names its database with ?source=N (AR-432 r2). A key kept by mistake costs a duplicate item someone
# dismisses; a key dropped by mistake loses a notice silently - so the list stays short.
TRACKING = re.compile(r"^(utm_\w*|fbclid|gclid|mc_cid|mc_eid|_ga)$", re.IGNORECASE)


def canonical_link(link: str) -> str:
    u = urllib.parse.urlsplit(link)
    keep = sorted((k, v) for k, v in urllib.parse.parse_qsl(u.query, keep_blank_values=True)
                  if not TRACKING.match(k))
    return urllib.parse.urlunsplit((u.scheme.lower(), u.netloc.lower(), u.path.rstrip("/") or "/",
                                    urllib.parse.urlencode(keep), u.fragment))


def item_key(publisher_id: str, it: dict) -> str:
    basis = canonical_link(it["link"]) if it.get("link") else \
        "|".join((" ".join(it.get("title", "").lower().split()), it.get("date", "")))
    return hashlib.sha256(f"{publisher_id}|{basis}".encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------------------------- fetch
def http_get(url: str, etag: "str | None", modified: "str | None"):
    """(status, body, etag, last_modified). 304 -> body None. Raises on network / HTTP >= 400."""
    hdr = dict(UA)
    if etag:
        hdr["If-None-Match"] = etag
    if modified:
        hdr["If-Modified-Since"] = modified
    req = urllib.request.Request(url, headers=hdr)
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as r:   # noqa: S310 - https only (registry test)
            body = r.read(MAX_BODY + 1)
            if len(body) > MAX_BODY:
                # Say so: a cut XML body used to surface as "not XML" and then as feed_broken (AR-432 #9).
                raise ValueError(f"body over {MAX_BODY:,} B - raise MAX_BODY or choose a smaller feed")
            return r.status, body, r.headers.get("ETag"), r.headers.get("Last-Modified")
    except urllib.error.HTTPError as e:
        if e.code == 304:
            return 304, None, etag, modified
        raise


def _due(fs: dict, poll_days: int, now: dt.datetime) -> bool:
    last = parse_date(fs.get("last_checked", ""))
    return last is None or (now - last) >= dt.timedelta(days=poll_days)


def _queue_line(kind: str, pub: dict, now: dt.datetime, **kw) -> dict:
    d = {"id": "ntc_" + kw.pop("key"), "component": "notices", "kind": kind,
         "publisher_id": pub["publisher_id"], "sources": list(pub.get("sources") or []),
         "found_utc": iso(now), "status": "new"}
    d.update(kw)
    return d


def run(registry: dict, terms: dict, state: dict, now: dt.datetime, fetch=None,
        sleep=None) -> "tuple[list, dict, dict]":
    """One pass. Returns (queue_lines, new_state, stats). Pure apart from `fetch` and `sleep`
    (default: http_get and time.sleep, looked up at call time)."""
    fetch = fetch or http_get
    sleep = sleep or time.sleep
    compiled = compile_terms(terms)
    state = json.loads(json.dumps(state or {}))                # never mutate the caller's copy
    feeds_state = state.setdefault("feeds", {})
    seen = set(state.get("seen", []))
    out, stats = [], {"due": 0, "fetched": 0, "not_modified": 0, "failed": 0, "items": 0,
                      "queued": 0, "low": 0, "skipped_cap": 0}
    last_hit: dict = {}
    requests = 0
    for pub in registry.get("publishers") or []:
        for feed in pub.get("feeds") or []:
            url = feed["url"]
            fs = feeds_state.setdefault(url, {})
            if not _due(fs, int(feed.get("poll_days", 7)), now):
                continue
            stats["due"] += 1
            if requests >= MAX_REQUESTS:
                stats["skipped_cap"] += 1
                continue
            host = urllib.parse.urlsplit(url).netloc
            if host in last_hit:
                wait = HOST_GAP_S - (time.monotonic() - last_hit[host])
                if wait > 0:
                    sleep(wait)
            requests += 1
            try:
                status, body, etag, modified = fetch(url, fs.get("etag"), fs.get("last_modified"))
                last_hit[host] = time.monotonic()
                items = [] if status == 304 else parse_items(feed["kind"], body, url,
                                                             feed.get("selector", ""))
            except Exception as e:                             # noqa: BLE001 - recorded, never retried here
                last_hit[host] = time.monotonic()
                stats["failed"] += 1
                fs["failures"] = int(fs.get("failures", 0)) + 1
                fs["last_error"] = f"{type(e).__name__}: {str(e)[:160]}"
                fs["last_checked"] = iso(now)
                if fs["failures"] == BROKEN_AFTER:
                    out.append(_queue_line("feed_broken", pub, now,
                                           key=hashlib.sha256(f"broken|{url}|{iso(now)}".encode()).hexdigest()[:16],
                                           link=url, title=f"feed failed {BROKEN_AFTER} runs in a row",
                                           detail=fs["last_error"]))
                continue
            first = "last_ok" not in fs
            fs.update(failures=0, last_error=None, last_checked=iso(now), last_ok=iso(now),
                      etag=etag, last_modified=modified, last_status=status)
            if status == 304:
                stats["not_modified"] += 1
                continue
            stats["fetched"] += 1
            stats["items"] += len(items)
            for it in items:
                key = item_key(pub["publisher_id"], it)
                if key in seen:
                    continue
                seen.add(key)
                when = parse_date(it.get("date", ""))
                if first and (when is None or now - when > dt.timedelta(days=FIRST_POLL_DAYS)):
                    continue                                   # history at first sight: seen, not news
                s, matched = score(f"{it['title']} {it.get('summary', '')}", compiled,
                                   feed.get("keywords_extra") or ())
                # A NOTICE PAGE lists only announcements (BoE statistical notices, CBS StatLine newsletters),
                # so every new link on it is news even when its title names no strong term - "Statistical
                # Notice 2026/08 - Changes to..." scored 1 and was never queued (review AR-434).
                if s < THRESHOLD and not feed.get("notice_page"):
                    stats["low"] += 1
                    continue
                stats["queued"] += 1
                out.append(_queue_line("publisher_notice", pub, now, key=key,
                                       title=it["title"][:TITLE_MAX], link=it.get("link", ""),
                                       item_date=iso(when) if when else None, score=s, matched=matched))
    state["seen"] = sorted(seen)
    state["last_run"] = iso(now)
    return out, state, stats


def check(registry: dict, fetch=None, sleep=None) -> list:
    """[(publisher_id, url, problem)] for every feed that cannot see what its registry entry says it sees.

    Each feed is fetched once (HOST_GAP_S apart per host) and parsed with its committed kind and selector.
    A feed with no items is a problem; so is an `evidence` text (a title the person who added the feed
    saw there) that no parsed item contains. Review AR-434 found four html_page feeds blind to the very
    item their verified_by cited - a page summary is not what this parser reads."""
    fetch = fetch or http_get
    sleep = sleep or time.sleep
    problems, last_hit = [], {}
    for pub in registry.get("publishers") or []:
        for feed in pub.get("feeds") or []:
            url = feed["url"]
            host = urllib.parse.urlsplit(url).netloc
            if host in last_hit:
                wait = HOST_GAP_S - (time.monotonic() - last_hit[host])
                if wait > 0:
                    sleep(wait)
            try:
                _, body, _, _ = fetch(url, None, None)
                items = parse_items(feed["kind"], body, url, feed.get("selector", ""))
            except Exception as e:                             # noqa: BLE001
                problems.append((pub["publisher_id"], url, f"{type(e).__name__}: {str(e)[:120]}"))
                continue
            finally:
                last_hit[host] = time.monotonic()
            if not items:
                problems.append((pub["publisher_id"], url, "no items"))
                continue
            ev = " ".join(str(feed.get("evidence") or "").lower().split())
            if ev and not any(ev in " ".join(f"{i['title']} {i.get('summary', '')}".lower().split())
                              for i in items):
                problems.append((pub["publisher_id"], url, f"evidence not among {len(items)} items: {ev[:80]}"))
    return problems


# ---------------------------------------------------------------------------------------------- files
def read_state(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def write_state(path: str, state: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=1, sort_keys=True)
    atomic_replace(tmp, path)


def append_queue(path: str, lines: list) -> None:
    if not lines:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8", newline="\n") as f:
        for d in lines:
            f.write(json.dumps(d, sort_keys=True, ensure_ascii=False) + "\n")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--registry", default=REGISTRY)
    ap.add_argument("--state", default=STATE)
    ap.add_argument("--queue", default=QUEUE)
    ap.add_argument("--check", action="store_true",
                    help="fetch every feed once and verify it parses to items holding its evidence; writes nothing")
    a = ap.parse_args(argv)
    if a.check:
        problems = check(load_yaml(a.registry))
        for pid, url, why in problems:
            print(f"  FAIL {pid:<10} {why}  {url[:90]}")
        print(f"[notices] check: {len(problems)} problem(s)")
        return 1 if problems else 0
    now = now_utc()
    lines, state, stats = run(load_yaml(a.registry), load_yaml(TERMS), read_state(a.state), now)
    print(f"[notices] {iso(now)} " + " ".join(f"{k}={v}" for k, v in stats.items()))
    for d in lines:
        print(f"  {d['kind']:<16} {d['publisher_id']:<10} {d.get('score', '-')!s:>3}  {d.get('title', '')[:90]}")
    if a.dry_run:
        print("[notices] --dry-run: nothing written")
        return 0
    # The queue first, then the seen-set: a crash between the two re-queues the same notice ids next run
    # (the queue tool folds lines by id), where the other order would lose them for good. A feed_broken
    # id carries its run time, so that one item can repeat once after such a crash.
    append_queue(a.queue, lines)
    write_state(a.state, state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
