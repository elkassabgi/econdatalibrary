import requests, re, json
UA = 'Econ-Fin Data Library admin@hfdatalibrary.com'
h = {'User-Agent': UA}
base = 'https://fiscaldata.treasury.gov'
sess = requests.Session(); sess.headers.update(h)

# 1) Try the datasets index page-data
for u in [f'{base}/page-data/datasets/page-data.json', f'{base}/page-data/index/page-data.json']:
    rr = sess.get(u, timeout=40)
    print(u, rr.status_code, len(rr.content))
    if rr.status_code == 200 and 'datasets/' in rr.text:
        slugs = sorted(set(re.findall(r'datasets/([a-z0-9\-]+)', rr.text)))
        print('  slugs in this page-data:', len(slugs), slugs[:10])

# 2) Mine the big app chunk for dataset slugs / endpoint paths
src = open('D:/research/econfindatalibrary/data/_treasury_catalog_chunk.js', encoding='utf-8').read()
slugs = sorted(set(re.findall(r'datasets/([a-z0-9\-]{3,})', src)))
print('\nslugs in app chunk:', len(slugs))
for s in slugs: print('  ', s)

# 3) The endpoint catalog in FiscalData is the "config.apis" objects with apiId & endpoint.
# Search the chunk for the apiId->endpoint table directly.
eps = sorted(set(re.findall(r'(?:/)?(v\d/[a-z]+/[a-z0-9_]+/[a-z0-9_]+)', src)))
print('\nv? endpoint-shaped strings in app chunk:', len(eps))
for e in eps: print('  ', e)
