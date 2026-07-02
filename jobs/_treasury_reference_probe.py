import requests, json
UA = 'Econ-Fin Data Library admin@hfdatalibrary.com'
h = {'User-Agent': UA}
API = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service'
sess = requests.Session(); sess.headers.update(h)

cands = [
    'v1/reference/fiscal_data/announcements',
    'v1/reference/fiscal_data/datasets',
    'v1/reference/datasets',
    'v1/reference/apis',
    'v1/reference/fiscal_data/apis',
    'v1/reference/fiscal_data',
    'v1/urls/count',
    'v2/share/stats',
    'v3/w/q',
    'v1/metadata/apis',
]
for p in cands:
    try:
        r = sess.get(f'{API}/{p}', params={'page[size]': '3'}, timeout=30)
        print(r.status_code, p, '->', (r.text[:200] if r.status_code == 200 else ''))
    except Exception as e:
        print('ERR', p, str(e)[:60])

# Also: the dataset metadata the app uses is fetched from a NON-/services path.
# Try fiscaldata.treasury.gov metadata service routes.
base = 'https://fiscaldata.treasury.gov'
for u in [
    f'{base}/api/fiscaldata/api-datasets',
    f'{base}/services/dtg/dataset-search',
    f'{base}/static/data/dataset-metadata.json',
    f'{API}/v1/reference/fiscal_data/announcements?page[size]=1',
]:
    try:
        r = sess.get(u, timeout=30)
        print(r.status_code, u, len(r.content))
    except Exception as e:
        print('ERR', u, str(e)[:60])
