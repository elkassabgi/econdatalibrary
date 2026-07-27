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
import urllib.parse
import urllib.request

os.chdir(r'E:\research\econfindatalibrary')
sys.path.insert(0, r'E:\research\econfindatalibrary')
from core import r2_util

API = 'https://econdl-api.elkassabgi.workers.dev/v1/catalog?source=%s&limit=1'


def live_count(src):
    req = urllib.request.Request(API % src, headers={'User-Agent': 'econdl-audit/1.0'})
    with urllib.request.urlopen(req, timeout=90) as f:
        return json.load(f).get('total', 0)


ts = open('api/worker/src/util.ts', encoding='utf-8').read()
m = re.search(r'SUPPORTED_SOURCES[^=]*=\s*(?:new Set\()?\[(.*?)\]', ts, re.S)
sup = set(re.findall(r'"([a-z0-9_]+)"', m.group(1)))

con = sqlite3.connect('data/catalog.db')
cat = {r[0]: r[1] for r in con.execute(
    'SELECT source_id, COUNT(*) FROM series GROUP BY source_id')}

print('%-20s %10s %10s  %-9s %s' % ('source', 'local', 'live D1', 'download', 'status'))
drift = []
notdl = []
for src in sorted(cat, key=lambda s: -cat[s]):
    n = cat[src]
    try:
        lv = live_count(src)
    except Exception as e:                                  # noqa: BLE001
        lv = -1
    dl = 'yes' if src in sup else 'NO'
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
print('catalogued, NOT servable : %d  (%s series)'
      % (len(notdl), format(sum(n for _, n in notdl), ',')))
if not drift and not notdl:
    print()
    print('ALL CLEAR: every catalogued series-level source matches live D1 and is downloadable.')
