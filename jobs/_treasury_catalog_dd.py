import requests, re, json
UA = 'Econ-Fin Data Library admin@hfdatalibrary.com'
h = {'User-Agent': UA}

# DataDistillr docs page lists all endpoints in a table.
urls = [
    'https://docs.datadistillr.com/connecting-data/connecting-to-apis-and-external-data/fiscaldata-api/',
]
allpaths = set()
for u in urls:
    try:
        r = requests.get(u, headers=h, timeout=40)
        print(u, r.status_code, len(r.text))
        t = r.text
        for m in re.findall(r'(/?v\d/[a-zA-Z0-9_/\-]+)', t):
            p = m.lstrip('/')
            if p.count('/') >= 2 and not p.endswith('/'):
                allpaths.add(p)
    except Exception as e:
        print('ERR', u, e)

allpaths = sorted(allpaths)
print('paths from datadistillr:', len(allpaths))
for p in allpaths:
    print(' ', p)
open('D:/research/econfindatalibrary/data/_treasury_dd_paths.json', 'w', encoding='utf-8').write(json.dumps(allpaths, indent=1))
