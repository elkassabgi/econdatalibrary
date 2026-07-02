import requests, re, json, time
from concurrent.futures import ThreadPoolExecutor, as_completed
UA = 'Econ-Fin Data Library admin@hfdatalibrary.com'
sess = requests.Session(); sess.headers.update({'User-Agent': UA})
base = 'https://fiscaldata.treasury.gov'
API = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service'

# 1) start from all endpoints embedded in dataset pages
slug_eps = json.load(open('D:/research/econfindatalibrary/data/_treasury_slug_eps.json'))
master = set()
slug_of = {}
for s, eps in slug_eps.items():
    for e in eps:
        master.add(e); slug_of.setdefault(e, s)

# 2) the 5 slugs whose HTML didn't embed -> fetch their /<slug>/ subpages via known table slugs,
#    plus probe correct names for them.
extra_probe = {
 # monthly-treasury-disbursements
 'v1/accounting/od/mtd_dpts_by_appropriation','v1/accounting/od/disbursements_by_agency',
 # revenue-collections-management
 'v2/revenue/rcm','v1/revenue/rcm','v2/payments/rcm',
 # top-treasury-offset-program (NEW combined)
 'v1/debt/top/top_federal','v1/debt/top/top_state',
 # treasury-report-on-receivables
 'v2/debt/tror','v2/debt/tror/data_act_compliance',
}
master |= extra_probe
# also keep the originally-verified 68 + dts (belt & suspenders)
master |= set(json.load(open('D:/research/econfindatalibrary/data/_treasury_endpoint_verify.json')).keys())

# 3) For the 4 stubborn slugs, mine their page-data AND nested table page-data for endpoints.
for s in ['monthly-treasury-disbursements','revenue-collections-management',
          'top-treasury-offset-program','treasury-report-on-receivables']:
    # the dataset page lists table-slugs in its page-data; grab any v?/.../ path from raw HTML+pd
    for u in [f'{base}/datasets/{s}/', f'{base}/page-data/datasets/{s}/page-data.json']:
        try:
            r = sess.get(u, timeout=40)
            for m in re.findall(r'(v\d/[a-z_]+/[a-z0-9_]+/?[a-z0-9_]*)', r.text):
                mm = m.rstrip('/')
                if mm.count('/') >= 2:
                    master.add(mm)
        except Exception:
            pass

def probe(p):
    for attempt in range(4):
        try:
            r = sess.get(f'{API}/{p}', params={'page[size]': '1', 'sort': 'record_date'}, timeout=40)
            if r.status_code == 400:  # some endpoints lack record_date sort field
                r = sess.get(f'{API}/{p}', params={'page[size]': '1'}, timeout=40)
            if r.status_code == 200:
                j = r.json(); m = j.get('meta', {})
                row0 = (j.get('data') or [{}])[0]
                return p, {'ok': True, 'total': m.get('total-count'), 'ncols': len(row0),
                           'cols': list(row0.keys())}
            if r.status_code == 404:
                return p, {'ok': False, 'status': 404}
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(2**attempt, 8)); continue
            return p, {'ok': False, 'status': r.status_code}
        except Exception as e:
            time.sleep(min(2**attempt, 8)); err = str(e)[:60]
    return p, {'ok': False, 'status': 'err', 'err': err}

res = {}
with ThreadPoolExecutor(max_workers=6) as ex:
    for f in as_completed({ex.submit(probe, p): p for p in master}):
        p, info = f.result(); res[p] = info

ok = {p: i for p, i in res.items() if i.get('ok')}
bad = {p: i for p, i in res.items() if not i.get('ok')}
total = sum((i.get('total') or 0) for i in ok.values())
print(f'MASTER probe: {len(master)} candidates -> {len(ok)} OK, {len(bad)} bad')
print(f'SUM total-count across {len(ok)} OK endpoints = {total:,}\n')
print('=== OK endpoints (sorted) ===')
for p in sorted(ok):
    print(f'  {p:62} total={ok[p]["total"]:>13,} ncols={ok[p]["ncols"]:>3}  [{slug_of.get(p,"?")}]')
print('\n=== still BAD ===')
for p in sorted(bad):
    print('  ', p, bad[p])

json.dump({'ok': ok, 'bad': bad, 'slug_of': slug_of, 'total': total},
          open('D:/research/econfindatalibrary/data/_treasury_master.json','w'), indent=1)
