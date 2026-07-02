import requests, json, time
from concurrent.futures import ThreadPoolExecutor, as_completed

UA = 'Econ-Fin Data Library admin@hfdatalibrary.com'
h = {'User-Agent': UA}
API = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service'

paths = json.load(open('D:/research/econfindatalibrary/data/_treasury_dd_paths.json', encoding='utf-8'))
print('candidates:', len(paths))

sess = requests.Session()
sess.headers.update(h)

def probe(p):
    url = f'{API}/{p}'
    params = {'page[size]': '1', 'page[number]': '1'}
    for attempt in range(4):
        try:
            r = sess.get(url, params=params, timeout=40)
            if r.status_code == 200:
                j = r.json()
                meta = j.get('meta', {})
                cols = list((j.get('data') or [{}])[0].keys())
                return p, {'ok': True, 'total': meta.get('total-count'),
                           'pages_at_10k': None, 'ncols': len(cols), 'cols': cols}
            elif r.status_code == 404:
                return p, {'ok': False, 'status': 404}
            elif r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(2 ** attempt, 8)); continue
            else:
                return p, {'ok': False, 'status': r.status_code}
        except Exception as e:
            time.sleep(min(2 ** attempt, 8))
            last = str(e)
    return p, {'ok': False, 'status': 'err', 'err': last}

results = {}
with ThreadPoolExecutor(max_workers=6) as ex:
    futs = {ex.submit(probe, p): p for p in paths}
    for f in as_completed(futs):
        p, info = f.result()
        results[p] = info

ok = {p: i for p, i in results.items() if i.get('ok')}
bad = {p: i for p, i in results.items() if not i.get('ok')}
print(f'\nOK endpoints: {len(ok)}   BAD/404: {len(bad)}')
print('\n--- BAD ---')
for p, i in sorted(bad.items()):
    print(' ', p, i)
print('\n--- OK (path : total-count : ncols) ---')
total_rows = 0
for p, i in sorted(ok.items()):
    t = i.get('total') or 0
    total_rows += t
    print(f'  {p:55} total={t:>12,} ncols={i["ncols"]}')
print(f'\nSUM of total-count across all OK endpoints: {total_rows:,}')
json.dump(results, open('D:/research/econfindatalibrary/data/_treasury_endpoint_verify.json','w',encoding='utf-8'), indent=1)
