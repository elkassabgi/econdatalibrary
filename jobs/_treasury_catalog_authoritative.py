import requests, re, json
UA = 'Econ-Fin Data Library admin@hfdatalibrary.com'
h = {'User-Agent': UA}
base = 'https://fiscaldata.treasury.gov'
sess = requests.Session(); sess.headers.update(h)

# The /datasets/ index page lists all dataset slugs. Get its page-data, then each
# dataset's page-data contains an "apis"/"config.apis" array w/ endpoint paths.

# Step 1: fetch /datasets/ HTML, find all dataset slugs (/datasets/<slug>/)
r = sess.get(base + '/datasets/', timeout=40)
print('/datasets/ status', r.status_code, 'len', len(r.text))
slugs = sorted(set(re.findall(r'/datasets/([a-z0-9\-]+)/', r.text)))
slugs = [s for s in slugs if s not in ('',)]
print('dataset slugs from HTML:', len(slugs))
for s in slugs:
    print('  ', s)

# Step 2: For each slug, fetch its gatsby page-data.json and extract endpoint paths
all_endpoints = {}
fails = []
for s in slugs:
    pd_url = f'{base}/page-data/datasets/{s}/page-data.json'
    try:
        rr = sess.get(pd_url, timeout=40)
        if rr.status_code != 200:
            fails.append((s, rr.status_code)); continue
        txt = rr.text
        eps = sorted(set(re.findall(r'(?:/)?(v\d/[a-zA-Z0-9_/\-]+)', txt)))
        # keep only real endpoint-looking (>=2 slashes after vN) and not query fragments
        eps = [e for e in eps if e.count('/') >= 2]
        all_endpoints[s] = eps
    except Exception as e:
        fails.append((s, str(e)[:50]))

print('\n=== endpoints per dataset (from authoritative page-data) ===')
flat = set()
for s, eps in all_endpoints.items():
    print(f'\n[{s}]  ({len(eps)} endpoints)')
    for e in eps:
        print('   ', e)
        flat.add(e)
print('\nTOTAL distinct endpoints from FiscalData page-data:', len(flat))
print('page-data fails:', fails)

json.dump({'slugs': slugs, 'per_dataset': all_endpoints, 'flat': sorted(flat), 'fails': fails},
          open('D:/research/econfindatalibrary/data/_treasury_authoritative.json','w',encoding='utf-8'), indent=1)
