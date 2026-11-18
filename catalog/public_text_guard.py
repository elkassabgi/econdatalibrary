"""Refuse to publish internal engineering text on the public site.

WHY THIS EXISTS (2026-11-18). Every dataset page printed the gitignored sidecar's
`description` as its lead and as "Full description". For 91 of the sidecar's 102 sources
that text is an ingester module docstring: istat's page told visitors about
jobs/ingest_istat_sliced.py, "_sliced_done.json" and "lines 697-700", others opened with
"FULL-COVERAGE ingest of ...", and some carried mojibake. The same pages printed
"Storage layout: data/clean_full/eia/<DATASET>.parquet". gen_site.py no longer reads those
fields; this check makes the build FAIL if any such text reaches a page again, whatever
the route.

WHAT IS CHECKED, per page:
  * visible text nodes, outside <script>, <style>, <code> and <pre> (code samples on the API
    and docs pages are meant to show commands, flags and file names);
  * every string value inside each <script type="application/ld+json"> block (schema.org
    Dataset and Croissant), because search engines publish those verbatim;
  * the `content` of <meta name="description"> and every og:/twitter: meta tag;
  * the `title` and `alt` attributes, which browsers and screen readers show.

The <pre> block under "Structured metadata" is skipped as markup but its JSON-LD is checked
through the JSON-LD route, so nothing in it escapes the guard.

The patterns name operational vocabulary, not file formats: "Parquet", "CSV", "API",
"SDMX" and "JSON" are ordinary public words and stay allowed.
"""
from __future__ import annotations

import json
import re
from html.parser import HTMLParser

# (label, compiled pattern). Each one names a class of internal text seen on, or
# plausible for, a page built from engineering notes.
#
# Every pattern is shaped like a CODE or FILE reference, never like an English word, because a
# public subtitle legitimately says "Fixed telephone lines 2020", "International poverty line
# 2.15 USD", "Spare parts/components", "Exports of tools/equipment", "core/periphery spreads" or
# "Border checkpoint crossings" (all pinned as negative controls in the tests). So: a repo path
# needs a file extension, a line reference needs a range or a "x.py:N" shape, and checkpoint and
# parts/ count only as file or directory names.
_EXT = r"(?:py|ps1|sh|cmd|ya?ml|json|jsonl|db|ts|js|toml|txt|parquet|log)"
PATTERNS = [
    ("repo file path", re.compile(
        r"(?<![\w/.])(?:jobs|core|updater|tools|configs|clients|api|catalog|scripts)/[\w./{}<>-]*\.%s\b" % _EXT)),
    ("store path data/clean_full", re.compile(r"data/(?:clean_(?:full|grouped)|raw)/")),
    ("state dir _aqueduct", re.compile(r"_aqueduct")),
    ("python/db file name", re.compile(r"\b\w+\.(?:py|db|ps1)\b")),
    ("source line reference", re.compile(r"\b\w+\.py:\d+")),
    ("line-number range", re.compile(r"\blines \d+\s*[-–]\s*\d+\b")),
    ("CLI flag", re.compile(r"(?<![\w-])--[a-z][\w-]+")),
    ("checkpoint marker .done", re.compile(r"(?<=\S)\.done\b")),
    ("checkpoint file", re.compile(r"\b\w*checkpoint\w*\.\w+\b|_checkpoint\b|\.ckpt\b", re.I)),
    ("internal sidecar _x.json", re.compile(r"(?<![\w/])_\w+\.json\b")),
    ("parts/ directory", re.compile(r"(?:/|\b_)parts/|\bparts/(?:ckpt\b|[\w{}-]*[/_{][\w{}/.-]*\.%s\b)" % _EXT)),
    ("<PLACEHOLDER>", re.compile(r"<[A-Z][A-Z_]+>")),
    # a bare boolean as a whole title segment ("true - IRIIP", "All Items - true"): a parser default
    # that reached a label, never a word a publisher wrote there
    ("boolean title segment", re.compile(r"(?:^|[;:]\s*|\s[-–—]\s)(?:true|false)(?=\s[-–—]\s|\s*[;:]|\s*$)")),
    ("U+FFFD replacement char", re.compile(chr(0xFFFD))),
    # UTF-8 read as cp1252: "Ã" followed by a Latin-1 / cp1252 continuation glyph ("Ã©", "Ã–", "Ã¶").
    # A bare "Ã" is real Portuguese ("SÃO PAULO") and must pass.
    ("mojibake A-tilde", re.compile(r"Ã[\u0080-¿ŒœŠšŸŽžƒ"
                                    r"ˆ˜–—‘-„†-•…‰‹"
                                    r"›€™]")),
    ("mojibake a-circumflex euro", re.compile(chr(0xE2) + chr(0x20AC))),
]

_SKIP_TAGS = {"script", "style", "code", "pre"}
_META_NAMES = {"description"}


class _Extract(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.texts: list[tuple[str, str]] = []      # (where, text)
        self.jsonld: list[str] = []
        self._skip = 0
        self._in_ld = False
        self._ld_buf: list[str] = []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "script" and (a.get("type") or "").lower() == "application/ld+json":
            self._in_ld, self._ld_buf = True, []
        if tag in _SKIP_TAGS:
            self._skip += 1
        if tag == "meta":
            name = (a.get("name") or a.get("property") or "").lower()
            if name in _META_NAMES or name.startswith(("og:", "twitter:")):
                if a.get("content"):
                    self.texts.append((f"meta {name}", a["content"]))
        for attr in ("title", "alt"):
            if a.get(attr):
                self.texts.append((f"{tag}@{attr}", a[attr]))

    def handle_startendtag(self, tag, attrs):
        # <meta .../> and <img .../> must not touch the skip depth
        a = dict(attrs)
        if tag == "meta":
            name = (a.get("name") or a.get("property") or "").lower()
            if name in _META_NAMES or name.startswith(("og:", "twitter:")):
                if a.get("content"):
                    self.texts.append((f"meta {name}", a["content"]))
        for attr in ("title", "alt"):
            if a.get(attr):
                self.texts.append((f"{tag}@{attr}", a[attr]))

    def handle_endtag(self, tag):
        if tag in _SKIP_TAGS and self._skip:
            self._skip -= 1
        if tag == "script" and self._in_ld:
            self.jsonld.append("".join(self._ld_buf))
            self._in_ld = False

    def handle_data(self, data):
        if self._in_ld:
            self._ld_buf.append(data)
            return
        if not self._skip and data.strip():
            self.texts.append(("text", data))


def _json_strings(obj, path="$"):
    if isinstance(obj, str):
        yield path, obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            yield from _json_strings(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from _json_strings(v, f"{path}[{i}]")


def public_strings(html: str):
    """Every (where, text) a visitor or a search engine is shown on this page."""
    p = _Extract()
    p.feed(html)
    p.close()
    out = list(p.texts)
    for i, block in enumerate(p.jsonld):
        try:
            obj = json.loads(block)
        except ValueError:
            out.append((f"jsonld[{i}] (unparseable)", block))
            continue
        out.extend((f"jsonld[{i}] {w}", s) for w, s in _json_strings(obj))
    return out


def scan_text(where: str, text: str):
    """[(label, where, excerpt)] for every internal-token match in one public string."""
    hits = []
    for label, rx in PATTERNS:
        for m in rx.finditer(text):
            s = max(0, m.start() - 40)
            hits.append((label, where, " ".join(text[s:m.end() + 40].split())))
    return hits


def scan_html(html: str):
    hits = []
    for where, text in public_strings(html):
        hits.extend(scan_text(where, text))
    return hits


def scan_json_values(obj, where="json"):
    hits = []
    for w, s in _json_strings(obj, where):
        hits.extend(scan_text(w, s))
    return hits


class InternalTextLeak(SystemExit):
    pass


def assert_public(name: str, html: str):
    """Raise InternalTextLeak (a SystemExit, so the build stops non-zero) naming every hit."""
    hits = scan_html(html)
    if hits:
        lines = [f"  [{lab}] {where}: ...{ex}..." for lab, where, ex in hits[:20]]
        raise InternalTextLeak(
            f"REFUSED: {name} would publish internal engineering text ({len(hits)} hit(s)):\n"
            + "\n".join(lines)
            + "\nFix the field that produced it (gen_site.build_record). If a hit is a FALSE positive - a real"
              " publisher title that happens to look like code - give that source a SOURCE_SUBTITLES override"
              " in gen_site.py; do not loosen or widen these patterns.")
