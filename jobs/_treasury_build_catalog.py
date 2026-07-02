"""Build data/_treasury_catalog_final.json from already-gathered authoritative data
(no slow live probe). Counts/cols are filled from the earlier master verify where present;
missing ones are left null and discovered at ingest time (ingest paginates by live total-pages).
"""
import json

slug_eps = json.load(open('D:/research/econfindatalibrary/data/_treasury_slug_eps.json'))
master = json.load(open('D:/research/econfindatalibrary/data/_treasury_master.json'))
ok = master['ok']  # endpoint -> {total, ncols, cols}

# Fix the 4 slugs whose HTML didn't embed endpoint paths.
slug_eps['monthly-treasury-disbursements'] = []  # MTD has no public API endpoint (PDF-only dataset)
slug_eps['top-treasury-offset-program'] = ['v1/debt/treasury_offset_program']
slug_eps['treasury-report-on-receivables'] = ['v2/debt/tror']
slug_eps['revenue-collections-management'] = ['v2/revenue/rcm']

DATE_CANDS = ('record_date', 'reporting_date', 'effective_date', 'date',
              'auction_date', 'issue_date', 'index_date')

def detect_date(cols):
    for c in DATE_CANDS:
        if c in cols:
            return c
    for c in cols:
        if c.endswith('_date'):
            return c
    return None

ep_slug = {}
for s, eps in slug_eps.items():
    for e in eps:
        e = e.strip().rstrip('/')
        if e and e not in ep_slug:
            ep_slug[e] = s

catalog = {}
missing_counts = []
for ep, slug in sorted(ep_slug.items()):
    info = ok.get(ep, {})
    cols = info.get('cols') or []
    total = info.get('total')  # may be None
    date_field = detect_date(cols) if cols else None
    catalog[ep] = {'slug': slug, 'total': total, 'ncols': info.get('ncols'),
                   'cols': cols, 'date_field': date_field}
    if total is None:
        missing_counts.append(ep)

known_total = sum((v['total'] or 0) for v in catalog.values())
print(f'catalog endpoints: {len(catalog)}')
print(f'endpoints with known count: {len(catalog) - len(missing_counts)}')
print(f'endpoints needing runtime count ({len(missing_counts)}): these will be counted during ingest')
for e in missing_counts:
    print('   needs-count:', e)
print(f'sum of KNOWN counts: {known_total:,}  (true total is higher; runtime endpoints add more)')
json.dump(catalog, open('D:/research/econfindatalibrary/data/_treasury_catalog_final.json', 'w'), indent=1)
print('wrote data/_treasury_catalog_final.json')
