"""Build the DEFINITIVE FiscalData endpoint catalog, single-threaded, with clean counts.
Writes data/_treasury_catalog_final.json = {endpoint: {dataset, slug, total, date_field, ncols}}.
"""
import requests, json, time
UA = 'Econ-Fin Data Library admin@hfdatalibrary.com'
sess = requests.Session(); sess.headers.update({'User-Agent': UA})
API = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service'

# Authoritative slug->endpoints from the dataset pages (the real catalog), with fixes:
slug_eps = json.load(open('D:/research/econfindatalibrary/data/_treasury_slug_eps.json'))

# Fix the 4 slugs whose HTML didn't embed paths (resolved via probing/announcements):
slug_eps['monthly-treasury-disbursements'] = []  # resolve below by probe
slug_eps['top-treasury-offset-program'] = ['v1/debt/treasury_offset_program']  # NEW combined endpoint
slug_eps['treasury-report-on-receivables'] = ['v2/debt/tror']                   # single wide table
slug_eps['revenue-collections-management'] = ['v2/revenue/rcm']

# Build endpoint->slug map (dedupe; first slug wins for shared endpoints)
ep_slug = {}
for s, eps in slug_eps.items():
    for e in eps:
        e = e.strip().rstrip('/')
        if e and e not in ep_slug:
            ep_slug[e] = s

# Probe candidate names for monthly-treasury-disbursements (MTD)
mtd_candidates = [
    'v1/accounting/od/mtd_outlays_by_organization', 'v1/accounting/od/mtd_outlays',
    'v1/accounting/od/disbursements', 'v1/accounting/od/mtd_disbursements',
    'v1/accounting/od/mtd_appropriations',
]

def probe(p):
    """Single-threaded, generous retry/backoff -> authoritative count + date field."""
    last = None
    for attempt in range(6):
        try:
            r = sess.get(f'{API}/{p}', params={'page[size]': '1', 'sort': '-record_date'}, timeout=90)
            if r.status_code == 400:
                r = sess.get(f'{API}/{p}', params={'page[size]': '1'}, timeout=90)
            if r.status_code == 200:
                j = r.json(); meta = j.get('meta', {})
                row0 = (j.get('data') or [{}])[0]
                cols = list(row0.keys())
                # detect a date field
                date_field = None
                for cand in ('record_date', 'reporting_date', 'date', 'effective_date',
                             'cal_year_month', 'auction_date', 'index_date'):
                    if cand in cols:
                        date_field = cand; break
                if date_field is None:
                    date_field = next((c for c in cols if c.endswith('_date') or c == 'record_date'), None)
                return {'ok': True, 'total': meta.get('total-count'),
                        'ncols': len(cols), 'cols': cols, 'date_field': date_field}
            if r.status_code == 404:
                return {'ok': False, 'status': 404}
            last = f'http{r.status_code}'
        except Exception as e:
            last = str(e)[:50]
        time.sleep(min(1.5 * (attempt + 1), 8))
    return {'ok': False, 'status': last}

# resolve MTD
for c in mtd_candidates:
    info = probe(c)
    if info.get('ok'):
        ep_slug[c] = 'monthly-treasury-disbursements'
        print('MTD resolved ->', c, info['total'], flush=True)
        break
    time.sleep(0.3)

catalog = {}
endpoints = sorted(ep_slug)
print(f'\nProbing {len(endpoints)} endpoints single-threaded...', flush=True)
for i, ep in enumerate(endpoints, 1):
    info = probe(ep)
    if info.get('ok'):
        catalog[ep] = {'slug': ep_slug[ep], 'total': info['total'],
                       'ncols': info['ncols'], 'cols': info['cols'],
                       'date_field': info['date_field']}
        print(f'  [{i:>3}/{len(endpoints)}] {ep:62} total={info["total"]:>12,} date={info["date_field"]}', flush=True)
    else:
        print(f'  [{i:>3}/{len(endpoints)}] {ep:62} BAD {info}', flush=True)
    time.sleep(0.15)

total_rows = sum(v['total'] or 0 for v in catalog.values())
print(f'\nFINAL catalog: {len(catalog)} endpoints, {total_rows:,} total rows (source-published)', flush=True)
json.dump(catalog, open('D:/research/econfindatalibrary/data/_treasury_catalog_final.json', 'w'), indent=1)
print('saved data/_treasury_catalog_final.json', flush=True)
