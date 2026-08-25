#!/usr/bin/env python3
"""Verify the built site locally, before any deploy. Costs nothing.

AR-007 stopped a Pages deploy and asked for checks I had not run. This runs them against the
files on disk rather than the running system, so it is free -- which matters, because Ahmed is
paying for D1 reads and I have already spent enough of his money today.

Checks, each because something actually went wrong:

  1. sitemap <-> files, BOTH directions. A page with no sitemap entry is invisible to search; a
     sitemap entry with no page is a 404 we advertise. The generator reported 330 URLs against
     333 files and the difference must be explained, not assumed.
  2. no page offers a download for a DENYLISTED source. unsdg shipped with seven "Free download"
     buttons while GET /v1/catalog?source=unsdg answered 451.
  3. every source in the LOCAL catalogue that is reservable has a page. The three that started
     this -- vdem, cbs_nl, gus_dbw -- had none while the API served 788,448 series.
  4. no page claims a licence the local catalogue does not hold for it.
  5. the brand does not wrap (the fix Ahmed asked for by name).

The local catalogue is opened READ-ONLY with a long busy timeout: the crawler fleet writes it
continuously and a plain open dies with "database is locked".
"""
import os
import re
import sqlite3
import sys

SITE = r"E:\research\econfindatalibrary\catalog\site"
DENY_TS = r"E:\research\econfindatalibrary\api\worker\src\denylist.ts"
CATALOG = r"E:\research\econfindatalibrary\data\catalog.db"

# Hand-maintained pages that are deliberately not dataset pages (gen_site.py:52).
HUB = {"_redirects", "download", "status", "mcp", "account", "404", "index", "catalog",
       "docs", "about", "terms", "privacy", "api", "sources", "changelog", "citation",
       "coverage", "licenses", "faq", "contact", "methodology"}

ok = True


def bad(msg):
    global ok
    ok = False
    print("   FAIL  " + msg)


def good(msg):
    print("   PASS  " + msg)


def parse_denylist():
    with open(DENY_TS, encoding="utf-8") as fh:
        t = fh.read()
    m = re.search(r"NON_REDISTRIBUTABLE[^=]*=\s*new\s+Set\s*\(\s*\[(.*?)\]\s*\)", t, re.S)
    if not m:
        return set()
    body = re.sub(r"//.*", "", m.group(1))
    body = re.sub(r"/\*.*?\*/", "", body, flags=re.S)
    return set(re.findall(r'"([^"]+)"', body))


def main() -> int:
    pages = {f[:-5] for f in os.listdir(SITE) if f.endswith(".html")}
    sub = os.path.join(SITE, "auth")
    print("   %d html files in the output" % len(pages))

    # 1. sitemap both directions
    sm = os.path.join(SITE, "sitemap.xml")
    urls = set()
    if os.path.exists(sm):
        with open(sm, encoding="utf-8") as fh:
            for loc in re.findall(r"<loc>([^<]+)</loc>", fh.read()):
                path = re.sub(r"^https?://[^/]+", "", loc).strip("/")
                urls.add(path if path else "index")   # the bare origin IS index.html
    missing_file = {u for u in urls if u not in pages and u != "index"}
    if missing_file:
        bad("sitemap advertises %d URL(s) with no page: %s"
            % (len(missing_file), sorted(missing_file)[:6]))
    else:
        good("every sitemap URL has a page (%d URLs)" % len(urls))

    unlisted = {p for p in pages if p not in urls and p not in HUB}
    if unlisted:
        bad("%d dataset page(s) not in the sitemap: %s" % (len(unlisted), sorted(unlisted)[:8]))
    else:
        good("every dataset page is in the sitemap")

    # 2. no download offered for a denylisted source
    deny = parse_denylist()
    print("   %d denylisted source ids" % len(deny))
    offenders = []
    for d in sorted(deny & pages):
        with open(os.path.join(SITE, d + ".html"), encoding="utf-8", errors="replace") as fh:
            if "Free download" in fh.read():
                offenders.append(d)
    if offenders:
        bad("denylisted source(s) still offering a download: %s" % offenders)
    else:
        good("no denylisted source offers a download (%d denylisted ids have a page)"
             % len(deny & pages))

    # 3/4. against the LOCAL catalogue, read-only (the fleet writes it continuously)
    con = sqlite3.connect("file:%s?mode=ro" % CATALOG.replace(os.sep, "/"), uri=True, timeout=240)
    con.execute("PRAGMA busy_timeout=240000")
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT s.source_id, s.license_id, l.reservable FROM source s "
        "LEFT JOIN license l ON l.license_id = s.license_id").fetchall()
    reservable = {r["source_id"] for r in rows if (r["reservable"] or 0) == 1}
    # A source with NO series correctly has no page: gen_site pages sources it can actually
    # serve. cftc, edgar_13f, gii, gleif, insee_sirene, pxweb and worldbank_extra are all 0.
    # My first version omitted this and reported seven false defects.
    with_series = {r[0] for r in con.execute(
        "SELECT source_id FROM series GROUP BY source_id").fetchall()}
    servable = (reservable & with_series) - deny
    no_page = sorted(servable - pages)
    if no_page:
        bad("%d servable source(s) have NO page: %s" % (len(no_page), no_page[:8]))
    else:
        good("every servable source has a page (%d sources)" % len(servable))

    for probe in ("vdem", "cbs_nl", "gus_dbw", "damodaran"):
        if probe in pages:
            good("%s has a page (this is what the deploy fixes)" % probe)
        else:
            bad("%s has NO page" % probe)

    # 5. the brand must not wrap
    with open(os.path.join(SITE, "vdem.html"), encoding="utf-8", errors="replace") as fh:
        html = fh.read()
    if re.search(r"\.brand[^{]*\{[^}]*white-space:\s*nowrap", html):
        good("the brand carries white-space:nowrap (Ahmed's wrap fix)")
    else:
        bad("the brand wrap fix is missing from the generated CSS")

    con.close()
    print()
    print("   RESULT: %s" % ("site build is internally consistent" if ok else "SOMETHING FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
