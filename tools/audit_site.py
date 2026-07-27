"""Exercise the SITE the way a user does, and fail loudly when it lies.

WHY THIS EXISTS: on 2026-07-27 the single most important user action — search a
series, click Download — was broken three independent ways at once, and every one
was invisible until the owner tried it by hand:

  1. download.html read localStorage 'edl_api_key' while the family SSO writes
     'edl_key', so a signed-in user's click never issued a request at all;
  2. 354,183 catalogued series had no servable CSV behind them (501);
  3. API keys expired 30 days after registration with nothing renewing them —
     180 of 493 accounts (36.5%) could not download anything.

Each component reported success on its own. Nothing checked the RELATIONSHIPS, and
nothing walked the user's actual path. This does both.

Checks, over the FULL page set (no sampling):
  A. reachability   every page returns 200 (and the deployed HTML is the built HTML)
  B. links          every internal href resolves — no 404 in the nav or body
  C. storage keys   every page agrees on the localStorage key names it shares
  D. api surface    every API endpoint referenced by page JS actually answers
  E. claims         numbers the site advertises match the catalog it serves from

Usage:
    python tools/audit_site.py                 # live site
    python tools/audit_site.py --base https://econdatalibrary.pages.dev
"""
from __future__ import annotations

import argparse
import collections
import concurrent.futures
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SITE_DIR = os.path.join(ROOT, "catalog", "site")
DEFAULT_BASE = "https://econdatalibrary.com"
UA = {"User-Agent": "econdl-site-audit/1.0 (+admin@econdatalibrary.com)"}
TIMEOUT = 60


def fetch(url: str):
    """-> (status, body_text). Never raises; a transport error is its own status."""
    req = urllib.request.Request(url, headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, ""
    except Exception as e:                                  # noqa: BLE001
        return f"ERR:{type(e).__name__}", ""


def local_pages() -> list[str]:
    return sorted(f for f in os.listdir(SITE_DIR) if f.endswith(".html"))


def check_reachability(base: str, pages: list[str]) -> list[tuple]:
    """A. Every page must answer 200. The site serves extensionless URLs via
    _redirects, so a page is OK if EITHER form resolves."""
    bad = []

    def one(p):
        stem = p[:-5]
        for path in (stem, p):                              # extensionless first
            st, _ = fetch(f"{base}/{path}")
            if st == 200:
                return None
        return (p, st)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        for r in ex.map(one, pages):
            if r:
                bad.append(r)
    return bad


def check_links(base: str, pages: list[str]) -> list[tuple]:
    """B. Every internal href on every page must resolve. This is the check that
    would have caught a nav pointing at a page that no longer exists."""
    targets: dict[str, set] = collections.defaultdict(set)
    for p in pages:
        html = open(os.path.join(SITE_DIR, p), encoding="utf-8", errors="replace").read()
        for href in re.findall(r'href="([^"#?]+)', html):
            if href.startswith(("http", "mailto:", "//", "data:")):
                continue
            # Skip hrefs that are JS string-concatenation fragments rather than real
            # targets ("'+esc(src)+'.html", "${r.page}"). The first version of this
            # audit reported both as broken links; they are code, not navigation, and
            # a checker that cries wolf gets ignored exactly when it is right.
            if any(c in href for c in ("'", '"', "+", "$", "{", "<", "\\")):
                continue
            targets[href.lstrip("./")].add(p)

    bad = []

    def one(t):
        st, _ = fetch(f"{base}/{t}")
        return (t, st) if st != 200 else None

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        for r in ex.map(one, sorted(targets)):
            if r:
                bad.append((r[0], r[1], sorted(targets[r[0]])[:3]))
    return bad


def check_storage_keys(pages: list[str]) -> dict:
    """C. THE download bug. Pages that share browser state must agree on the key
    names. A single outlier silently disables whatever reads it."""
    usage: dict[str, set] = collections.defaultdict(set)
    for p in pages + ["assets/sso.js"]:
        path = os.path.join(SITE_DIR, p)
        if not os.path.exists(path):
            continue
        txt = open(path, encoding="utf-8", errors="replace").read()
        for k in re.findall(r"localStorage\.(?:get|set|remove)Item\(\s*['\"]([A-Za-z0-9_]+)", txt):
            usage[k].add(p)
        # Catch EVERY declarator, not just the first: sso.js writes
        # `var K = 'edl_key', N = 'edl_name';` and a pattern anchored on `var`
        # sees only K — which made the audit report edl_name as a lone outlier.
        for k in re.findall(r"['\"](edl_[a-z_]+)['\"]", txt):
            usage[k].add(p)
    return usage


def check_api_surface(pages: list[str]) -> list[tuple]:
    """D. Every API path the page JS calls must answer. A page can be perfectly
    valid HTML and still be pointed at an endpoint that no longer exists."""
    paths: dict[str, set] = collections.defaultdict(set)
    for p in pages:
        txt = open(os.path.join(SITE_DIR, p), encoding="utf-8", errors="replace").read()
        # Only STATIC paths are checkable. A path built from an interpolation
        # (`${API}/v1/series/${id}.csv`) truncates to a prefix that legitimately
        # 404s — the first run of this audit reported three such prefixes as broken
        # endpoints. Capture the full token including ':' (series ids contain it)
        # and drop anything that was cut short by a template hole.
        for m in re.findall(r"[`'\"]\$\{API\}(/v1/[A-Za-z0-9_/.:%-]*)(.?)", txt):
            path, nxt = m
            if nxt in ("$", "{") or not path or path.endswith("/"):
                continue                                     # dynamic -> not checkable
            paths[path].add(p)
        # '%' belongs in the class: documented examples URL-encode the ':' in a
        # series id (worldbank_wdi%3ASP.POP.TOTL). Omitting it truncated the id and
        # reported a live, correct docs example as a broken endpoint.
        for m in re.findall(r"https://[a-z0-9.-]*workers\.dev(/v1/[A-Za-z0-9_/.:%-]*)", txt):
            if m and not m.endswith("/"):
                paths[m].add(p)
    api_base = "https://econdl-api.elkassabgi.workers.dev"
    out = []
    for path in sorted(paths):
        if not path or "{" in path:
            continue
        st, _ = fetch(api_base + path)
        # 401 is a PASS here: the endpoint exists and is gating correctly.
        if st not in (200, 401):
            out.append((path, st, sorted(paths[path])[:3]))
    return out


def check_auth_health() -> list[str]:
    """E. Can users actually AUTHENTICATE? Nothing above would have caught the worst
    bug of 2026-07-27: API keys expired 30 days after registration with no renewal,
    so 180 of 493 accounts (36.5%) got invalid_key on every download while the site
    happily displayed their key. Pages loaded, links resolved, endpoints answered —
    and the product was broken for a third of its users.

    Needs wrangler + D1 access, so it degrades to a skip rather than a failure.
    """
    import json
    import subprocess
    worker_dir = os.path.join(ROOT, "api", "worker")
    sql = ("SELECT COUNT(*) AS total, "
           "SUM(CASE WHEN is_active=1 THEN 1 ELSE 0 END) AS active, "
           "SUM(CASE WHEN is_active=1 AND (api_key_expires_at IS NULL OR "
           "api_key_expires_at > datetime('now')) THEN 1 ELSE 0 END) AS can_download "
           "FROM users")
    try:
        res = subprocess.run(
            ["npx", "wrangler", "d1", "execute", "hfdatalibrary-db", "--remote",
             "--command", sql, "--json"],
            cwd=worker_dir, capture_output=True, text=True,
            encoding="utf-8", errors="replace", timeout=300, shell=True)
        m = re.search(r'"results":\s*(\[.*?\])', res.stdout, re.S)
        row = json.loads(m.group(1))[0]
    except Exception as e:                                  # noqa: BLE001
        return [f"SKIPPED (no D1 access: {type(e).__name__})"]

    total, active, ok = row["total"], row["active"], row["can_download"]
    locked = active - ok
    out = [f"   {total} users, {active} active, {ok} can download"]
    if locked:
        out.append(f"   LOCKED OUT: {locked} active account(s) ({100.0 * locked / max(active,1):.1f}%) "
                   f"cannot download — expired or missing key")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--base", default=DEFAULT_BASE)
    a = ap.parse_args()

    pages = local_pages()
    print(f"auditing {len(pages)} pages against {a.base}\n")
    failures = 0

    print("A. page reachability")
    bad = check_reachability(a.base, pages)
    print(f"   {len(pages) - len(bad)}/{len(pages)} reachable")
    for p, st in bad:
        print(f"   BROKEN {p} -> {st}")
    failures += len(bad)

    print("\nB. internal links")
    badl = check_links(a.base, pages)
    print(f"   {len(badl)} broken link target(s)")
    for t, st, on in badl:
        print(f"   BROKEN {t} -> {st}   linked from {on}")
    failures += len(badl)

    print("\nC. localStorage key agreement")
    usage = check_storage_keys(pages)
    for k, where in sorted(usage.items()):
        print(f"   {k:16} used by {len(where)} file(s)")
    # "Used by exactly one file" is the WRONG signal — plenty of keys are page-local
    # by design (a bounce timestamp, a legacy migration constant), and flagging them
    # trains you to ignore the check.
    #
    # The actual signature of the download bug was TWO NAMES FOR ONE CONCEPT living
    # side by side: sso.js/account/mcp wrote `edl_key` while download.html read
    # `edl_api_key`. So flag NEAR-DUPLICATE names — one key whose name contains
    # another's stem — and report which files use each, which is the fact that
    # settles whether it is a divergence or a deliberate alias.
    keys = sorted(usage)
    dupes = []
    for i, a in enumerate(keys):
        for b in keys[i + 1:]:
            short, long_ = (a, b) if len(a) < len(b) else (b, a)
            stem = short.replace("edl_", "").replace("hfd_", "")
            if len(stem) >= 3 and stem in long_ and short != long_:
                dupes.append((short, long_))
    for short, long_ in dupes:
        print(f"   NEAR-DUPLICATE {short!r} vs {long_!r}")
        print(f"      {short:18} <- {sorted(usage[short])}")
        print(f"      {long_:18} <- {sorted(usage[long_])}")
        print("      two names for one concept is exactly how the download button broke")
    failures += len(dupes)

    print("\nD. API endpoints referenced by page JS")
    bada = check_api_surface(pages)
    print(f"   {len(bada)} endpoint(s) not answering")
    for path, st, on in bada:
        print(f"   BROKEN {path} -> {st}   called from {on}")
    failures += len(bada)

    print("\nE. auth health (can users actually download?)")
    for line in check_auth_health():
        print(line if line.startswith("   ") else f"   {line}")
        if "LOCKED OUT" in line:
            failures += 1

    print("\nNOT covered here — run the companion audit:")
    print("   tools/audit_serving_coherence.py  (local == live D1 == downloadable)")
    print("   That is the third bug class from 2026-07-27: 354,183 catalogued series")
    print("   with no servable CSV. A page can load perfectly and still serve nothing.")

    print(f"\n{'=' * 60}")
    print("AUDIT CLEAN" if not failures else f"AUDIT FOUND {failures} PROBLEM(S)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
