import requests, re, json
UA = 'Econ-Fin Data Library admin@hfdatalibrary.com'
h = {'User-Agent': UA}
base = 'https://fiscaldata.treasury.gov'
sess = requests.Session(); sess.headers.update(h)

# Download ALL chunks, search each for the per-dataset config that ties
# a dataset to its endpoint(s). FiscalData config objects have keys like
# "endpoint" alongside "tableName"/"pathName"/"dateField".
r = sess.get(base + '/api-documentation/', timeout=40)
chunks = sorted(set(re.findall(r'/[0-9a-zA-Z_\-]+\.js', r.text)))
chunks = [c for c in chunks if 'Universal' not in c and 'gtm' not in c]

best = None
for c in chunks:
    body = sess.get(base + c, timeout=60).text
    # count occurrences of the catalog-ish keys
    n_tableName = len(re.findall(r'tableName', body))
    n_pathName = len(re.findall(r'pathName', body))
    n_endpoint = len(re.findall(r'endpoint', body))
    n_dateField = len(re.findall(r'dateField', body))
    n_apis = len(re.findall(r'\bapis\b', body))
    n_paths = len(set(re.findall(r'v\d/[a-z]+/[a-z0-9_]+/[a-z0-9_]+', body)))
    print(f'{c:42} tableName={n_tableName:>4} pathName={n_pathName:>4} endpoint={n_endpoint:>4} '
          f'dateField={n_dateField:>4} apis={n_apis:>4} distinctPaths={n_paths:>4} len={len(body)}')
    # save the chunk with the most distinct endpoint paths
    if best is None or n_paths > best[1]:
        best = (c, n_paths, body)

print('\nBEST for catalog:', best[0], 'distinctPaths', best[1])
open('D:/research/econfindatalibrary/data/_treasury_best_config_chunk.js','w',encoding='utf-8').write(best[2])

# Dump every distinct endpoint path from the best chunk
paths = sorted(set(re.findall(r'v\d/[a-z]+/[a-z0-9_]+(?:/[a-z0-9_]+)?', best[2])))
print('distinct endpoint paths in best chunk:', len(paths))
for p in paths: print('  ', p)
