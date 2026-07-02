import re, json
src = open('D:/research/econfindatalibrary/data/_treasury_catalog_chunk.js', encoding='utf-8').read()

# Extract every endpoint:"..." literal (the authoritative per-dataset config values)
raw = re.findall(r'endpoint:"([^"]+)"', src)
print('raw endpoint: literals found:', len(raw))

# Normalize: strip leading slash, drop query string, keep base path
norm = set()
for e in raw:
    e = e.strip()
    if not e:
        continue
    e = e.lstrip('/')
    base = e.split('?')[0].rstrip('/')
    if re.match(r'^v\d/', base) and base.count('/') >= 1:
        norm.add(base)

norm = sorted(norm)
print('normalized distinct endpoint base paths:', len(norm))
for p in norm:
    print('  ', p)

json.dump({'raw': sorted(set(raw)), 'normalized': norm},
          open('D:/research/econfindatalibrary/data/_treasury_literal_endpoints.json','w',encoding='utf-8'), indent=1)
