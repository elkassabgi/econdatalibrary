import requests, re, json
UA = 'Econ-Fin Data Library admin@hfdatalibrary.com'
h = {'User-Agent': UA}
base = 'https://fiscaldata.treasury.gov'

# Pull the api-documentation HTML again, collect ALL js chunk names
r = requests.get(base + '/api-documentation/', headers=h, timeout=40)
t = r.text
js = sorted(set(re.findall(r'/[0-9a-zA-Z_\-]+\.js', t)))
print('js chunks found:', len(js))

# Download each chunk, search for endpoint-catalog signatures
# FiscalData endpoint objects contain keys like "pathName","endpoint","apiId","dataset"
hits = {}
for j in js:
    url = base + j
    try:
        rr = requests.get(url, headers=h, timeout=40)
    except Exception as e:
        print('ERR', j, e); continue
    body = rr.text
    score = body.count('pathName') + body.count('"endpoint"') + body.count('apiId')
    hits[j] = (len(body), score, body.count('/v1/accounting'), body.count('/v2/accounting'))
    print(f'{j:45} len={len(body):>8} pathName/endpoint/apiId={score:>5} v1acct={body.count("/v1/accounting")} v2acct={body.count("/v2/accounting")}')

# pick the chunk with most endpoint signatures
best = max(hits, key=lambda k: hits[k][1])
print('BEST chunk:', best, hits[best])
rr = requests.get(base + best, headers=h, timeout=60)
open('D:/research/econfindatalibrary/data/_treasury_catalog_chunk.js', 'w', encoding='utf-8').write(rr.text)
print('saved best chunk')
