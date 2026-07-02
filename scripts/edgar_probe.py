import zipfile, re, sys, time, json

ZIP = 'D:/research/econfindatalibrary/data/raw/sec_edgar/submissions.zip'

t0 = time.time()
print('opening zip...', flush=True)
z = zipfile.ZipFile(ZIP)
infos = z.infolist()
print(f'infolist done: {len(infos)} entries in {time.time()-t0:.1f}s', flush=True)

cik_main = []
cik_sub = []
other = []
main_re = re.compile(r'^CIK\d{10}\.json$')
sub_re = re.compile(r'^CIK\d{10}-submissions-\d+\.json$')
total_uncomp = 0
for info in infos:
    n = info.filename
    total_uncomp += info.file_size
    if main_re.match(n):
        cik_main.append(n)
    elif sub_re.match(n):
        cik_sub.append(n)
    else:
        other.append(n)

print(f'CIK main json:   {len(cik_main)}', flush=True)
print(f'CIK sub json:    {len(cik_sub)}', flush=True)
print(f'other entries:   {len(other)}', flush=True)
print(f'sample other:    {other[:15]}', flush=True)
print(f'total uncompressed: {total_uncomp/1e9:.2f} GB', flush=True)
print(f'sample main: {cik_main[:3]}', flush=True)
print(f'sample sub:  {cik_sub[:3]}', flush=True)

# Inspect one main file structure
if cik_main:
    sample = cik_main[0]
    data = json.loads(z.read(sample))
    print(f'--- structure of {sample} ---', flush=True)
    print('top keys:', list(data.keys()), flush=True)
    if 'filings' in data:
        print('filings keys:', list(data['filings'].keys()), flush=True)
        rec = data['filings'].get('recent', {})
        print('recent keys:', list(rec.keys()), flush=True)
        forms = rec.get('form', [])
        print('recent count:', len(forms), flush=True)
        files = data['filings'].get('files', [])
        print('files[] entries:', len(files), flush=True)
        if files:
            print('files[0]:', files[0], flush=True)
    print('cik field:', data.get('cik'), flush=True)
    print('tickers:', data.get('tickers'), flush=True)

# Find a filer WITH overflow files[] to inspect a sub-json
print('--- searching for a filer with files[] overflow ---', flush=True)
found = 0
for n in cik_main[:5000]:
    d = json.loads(z.read(n))
    files = d.get('filings', {}).get('files', [])
    if files:
        print(f'{n} has {len(files)} overflow files; first={files[0]["name"]}', flush=True)
        subname = files[0]['name']
        sd = json.loads(z.read(subname))
        print(f'  sub-json {subname} top keys: {list(sd.keys())}', flush=True)
        print(f'  sub-json form count: {len(sd.get("form", []))}', flush=True)
        print(f'  sub-json sample form/date/acc/doc:', flush=True)
        print(f'    form={sd.get("form",[])[:2]} date={sd.get("filingDate",[])[:2]} acc={sd.get("accessionNumber",[])[:2]} doc={sd.get("primaryDocument",[])[:2]}', flush=True)
        found += 1
        if found >= 2:
            break
if not found:
    print('no overflow files[] in first 5000 (unusual)', flush=True)

print(f'TOTAL PROBE TIME {time.time()-t0:.1f}s', flush=True)
