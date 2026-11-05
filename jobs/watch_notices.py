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
import fnmatch
import hashlib
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
def _text(el, *names) -> str:
    """First non-empty child text among `names` (namespace-agnostic: matches the local name)."""
    for child in el:
        local = child.tag.rsplit("}", 1)[-1]
        if local in names and (child.text or "").strip():
            return child.text.strip()
    return ""


def _atom_link(entry) -> str:
    best = ""
    for child in entry:
        if child.tag.rsplit("}", 1)[-1] == "link":
            rel = child.get("rel", "alternate")
            if rel == "alternate" and child.get("href"):
                return child.get("href")
            best = best or child.get("href", "")
    return best


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
    """(text, href) of every <a> inside the scope element `tag` / `tag.class` (whole page if none)."""

    def __init__(self, selector: str = ""):
        super().__init__(convert_charrefs=True)
        tag, _, cls = (selector or "").partition(".")
        self.scope_tag, self.scope_cls = tag.lower(), cls
        self.depth = 0 if tag else 1                        # >0 = inside the scope
        self.out, self._href, self._buf = [], None, []

    def _is_scope(self, tag, attrs) -> bool:
        if tag != self.scope_tag:
            return False
        return not self.scope_cls or self.scope_cls in (dict(attrs).get("class") or "").split()

    def handle_starttag(self, tag, attrs):
        if self.scope_tag and (self.depth or self._is_scope(tag, attrs)) and tag == self.scope_tag:
            self.depth += 1
        if self.depth and tag == "a" and dict(attrs).get("href"):
            self._href, self._buf = dict(attrs)["href"], []

    def handle_endtag(self, tag):
        if tag == "a" and self._href is not None:
            self.out.append((" ".join("".join(self._buf).split()), self._href))
            self._href = None
        if self.scope_tag and tag == self.scope_tag and self.depth:
            self.depth -= 1

    def handle_data(self, data):
        if self._href is not None:
            self._buf.append(data)


def parse_items(kind: str, body: bytes, base_url: str, selector: str = "") -> list:
    """[{title, link, date, summary}] from one feed body. Raises ValueError on a body it cannot read."""
    items = []
    if kind in ("rss", "atom"):
        try:
            root = ET.fromstring(body)
        except ET.ParseError as e:
            raise ValueError(f"not XML: {e}") from None
        for el in root.iter():
            local = el.tag.rsplit("}", 1)[-1]
            if local not in ("item", "entry"):
                continue
            link = _text(el, "link") if local == "item" else _atom_link(el)
            items.append({"title": _text(el, "title"),
                          "link": urllib.parse.urljoin(base_url, link) if link else "",
                          "date": _text(el, "pubDate", "published", "updated", "date"),
                          "summary": _text(el, "description", "summary")})
    elif kind == "html_page":
        p = _Anchors(selector)
        p.feed(body.decode("utf-8", errors="replace"))
        for text, href in p.out:
            if not text or href.startswith(("#", "mailto:", "javascript:")):
                continue
            items.append({"title": text, "link": urllib.parse.urljoin(base_url, href), "date": "",
                          "summary": ""})
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
def canonical_link(link: str) -> str:
    u = urllib.parse.urlsplit(link)
    keep = [(k, v) for k, v in urllib.parse.parse_qsl(u.query) if k.lower() in ("id", "p", "article")]
    return urllib.parse.urlunsplit((u.scheme.lower(), u.netloc.lower(), u.path.rstrip("/"),
                                    urllib.parse.urlencode(keep), ""))


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
            return r.status, r.read(5_000_000), r.headers.get("ETag"), r.headers.get("Last-Modified")
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
                if s < THRESHOLD:
                    stats["low"] += 1
                    continue
                stats["queued"] += 1
                out.append(_queue_line("publisher_notice", pub, now, key=key,
                                       title=it["title"][:TITLE_MAX], link=it.get("link", ""),
                                       item_date=iso(when) if when else None, score=s, matched=matched))
    state["seen"] = sorted(seen)
    state["last_run"] = iso(now)
    return out, state, stats


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
    a = ap.parse_args(argv)
    now = now_utc()
    lines, state, stats = run(load_yaml(a.registry), load_yaml(TERMS), read_state(a.state), now)
    print(f"[notices] {iso(now)} " + " ".join(f"{k}={v}" for k, v in stats.items()))
    for d in lines:
        print(f"  {d['kind']:<16} {d['publisher_id']:<10} {d.get('score', '-')!s:>3}  {d.get('title', '')[:90]}")
    if a.dry_run:
        print("[notices] --dry-run: nothing written")
        return 0
    # The queue first, then the seen-set: a crash between the two re-queues the same ids next run (the
    # queue tool folds lines by id), where the other order would lose them for good.
    append_queue(a.queue, lines)
    write_state(a.state, state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
