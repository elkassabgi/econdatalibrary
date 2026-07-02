import re, json
src = open('D:/research/econfindatalibrary/data/_treasury_catalog_chunk.js', encoding='utf-8').read()
print('chunk len', len(src))

# FiscalData endpoint paths look like: v1/accounting/od/...  or  /v1/accounting/od/...
# In the bundle they appear as the value of "endpoint":"v1/accounting/..."
# Extract all quoted strings that look like API endpoint paths (vN/.../...).
paths = set()
for m in re.findall(r'["\']((?:/)?v\d/[a-zA-Z0-9_/\-]+)["\']', src):
    p = m.lstrip('/')
    # endpoints have at least 2 segments after vN
    if p.count('/') >= 2:
        paths.add(p)

paths = sorted(paths)
print('candidate endpoint paths:', len(paths))
for p in paths:
    print(' ', p)

# Also try to recover the "endpoint": "..." associations and any "pathName"/"tableName"
print('\n--- "endpoint": associations ---')
ep = sorted(set(re.findall(r'endpoint["\']?\s*:\s*["\']([^"\']+)["\']', src)))
print('endpoint: count', len(ep))
for e in ep:
    print('  EP', e)

open('D:/research/econfindatalibrary/data/_treasury_endpoint_paths.json','w',encoding='utf-8').write(json.dumps(paths,indent=1))
