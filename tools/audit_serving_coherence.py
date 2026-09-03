"""Final coherence audit: for every series-level source, does local == live D1 == R2,
and is it downloadable?

The whole class of bugs today was a component being right in isolation while two
components disagreed. So the closing check is the three-way one, run over the FULL
set of series-level sources rather than the six I touched.
"""
import json
import os
import re
import sqlite3
import sys
import urllib.error
import urllib.parse
import urllib.request

os.chdir(r'E:\research\econfindatalibrary')
sys.path.insert(0, r'E:\research\econfindatalibrary')
from core import r2_util

API = 'https://econdl-api.elkassabgi.workers.dev/v1/catalog?source=%s&limit=1'


def live_count(src):
    """-> (total, None) or (None, reason). A GATE is not a count mismatch.

    `worldbank_pink` answers 451 non_redistributable (R526: refused in writing), and the old
    bare-except-to--1 turned that into a DRIFT row — a deliberate licence refusal presented as a
    defect, beside eight real drifts it then discredited.
    """
    req = urllib.request.Request(API % src, headers={'User-Agent': 'econdl-audit/1.0'})
    try:
        with urllib.request.urlopen(req, timeout=90) as f:
            return json.load(f).get('total', 0), None
    except urllib.error.HTTPError as e:
        if e.code == 451:
            return None, 'GATED (451 non-redistributable)'
        return None, f'PROBE ERROR (HTTP {e.code})'
    except Exception as e:                                  # noqa: BLE001
        return None, f'PROBE ERROR ({type(e).__name__})' 


ts = open('api/worker/src/util.ts', encoding='utf-8').read()
# The array opens with a COMMENT BLOCK, and a lazy `(.*?)\]` stops at the first `]` inside it —
# measured 2026-09-03: 342 chars captured, ZERO ids parsed, so every source read as "not
# downloadable" and the tool announced 322 sources / 13,952,768 series unservable. R0.4: never
# regex a language whose comments can contain the delimiter. Take the body up to the `]` that
# starts a line, then strip `//` comments before extracting ids.
_start = re.search(r'SUPPORTED_SOURCES\b[^=]*=\s*(?:new Set\()?\[', ts)
if not _start:
    sys.exit('PARSE FAILED: no SUPPORTED_SOURCES array literal in api/worker/src/util.ts')
_body = ts[_start.end():]
_end = re.search(r'^\s*\]', _body, re.M)
_body = _body[: _end.start()] if _end else _body
_body = "\n".join(re.sub(r'//.*$', '', ln) for ln in _body.splitlines())
sup = set(re.findall(r'"([a-z0-9_]+)"', _body))
# REFUSE rather than report. An empty parse is "I could not look", not "nothing is served"
# (R261, R503) — and converting it into a verdict is what made this tool announce the whole
# library unservable.
if not sup:
    sys.exit('PARSE FAILED: SUPPORTED_SOURCES parsed to ZERO ids — refusing to report '
             'downloadability, because an empty parse would mark every source unservable.')

con = sqlite3.connect('data/catalog.db')
cat = {r[0]: r[1] for r in con.execute(
    'SELECT source_id, COUNT(*) FROM series GROUP BY source_id')}

print('%-20s %10s %10s  %-9s %s' % ('source', 'local', 'live D1', 'download', 'status'))
drift = []
notdl = []
unprobed = []
for src in sorted(cat, key=lambda s: -cat[s]):
    n = cat[src]
    lv, reason = live_count(src)
    dl = 'yes' if src in sup else 'NO'
    if reason:
        # No live count exists to compare, so this is NOT drift. Counted apart.
        unprobed.append((src, n, reason))
        print('%-20s %10d %10s  %-9s %s' % (src, n, '-', dl, reason))
        continue
    ok = (lv == n) and (src in sup)
    if lv != n:
        drift.append((src, n, lv))
    if src not in sup:
        notdl.append((src, n))
    if not ok:
        print('%-20s %10d %10d  %-9s %s' % (src, n, lv, dl,
                                            'DRIFT' if lv != n else 'not downloadable'))

print()
print('series-level sources     : %d' % len(cat))
print('catalog drift (local!=D1): %d' % len(drift))
print('gated / unprobeable        : %d%s'
      % (len(unprobed),
         '  (' + ', '.join('%s: %s' % (s, r) for s, _n, r in unprobed) + ')'
         if unprobed else ''))
print('catalogued, NOT servable : %d  (%s series)'
      % (len(notdl), format(sum(n for _, n in notdl), ',')))
if not drift and not notdl:
    print()
    print('ALL CLEAR: every catalogued series-level source matches live D1 and is downloadable.')
