import requests, re, json
UA = 'Econ-Fin Data Library admin@hfdatalibrary.com'
h = {'User-Agent': UA}
r = requests.get('https://fiscaldata.treasury.gov/api-documentation/', headers=h, timeout=40)
t = r.text
print('doc len', len(t))

pd = sorted(set(re.findall(r'/page-data/[^"\'\\ ]+', t)))
print('--- page-data refs ---')
for m in pd[:30]:
    print(m)

app = sorted(set(re.findall(r'/[^"\'\\ ]*app-data\.json', t)))
print('--- app-data ---')
for m in app:
    print(m)

js = sorted(set(re.findall(r'/[^"\'\\ ]+\.js', t)))
print('--- js bundles (first 15) ---')
for m in js[:15]:
    print(m)

# Direct guesses for gatsby page-data
print('--- direct page-data probes ---')
for u in [
    'https://fiscaldata.treasury.gov/page-data/api-documentation/page-data.json',
    'https://fiscaldata.treasury.gov/page-data/app-data.json',
]:
    rr = requests.get(u, headers=h, timeout=30)
    print(rr.status_code, len(rr.content), u)
    if rr.status_code == 200:
        open('D:/research/econfindatalibrary/data/_treasury_pagedata.json','wb').write(rr.content)
        print('  saved')
