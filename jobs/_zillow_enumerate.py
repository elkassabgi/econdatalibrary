#!/usr/bin/env python3
"""Enumerate the FULL Zillow Research catalog from the embedded `var data` object."""
import json
import os
from collections import Counter


# Repo root derived from this file, never a drive letter: the store moved D: -> E: in the
# workstation cutover, and a verify script pointed at an absent tree reports "0 files,
# nothing wrong" instead of failing. R330.
def _RD(*parts):
    _r = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(_r, *parts) if parts else _r

PAGE = _RD('data', '_zillow_page.html')
def extract_data_object(t: str):
    start = t.find('var data')
    brace = t.find('{', start)
    i = brace
    depth = 0
    instr = False
    esc = False
    while i < len(t):
        c = t[i]
        if instr:
            if esc:
                esc = False
            elif c == '\\':
                esc = True
            elif c == '"':
                instr = False
        else:
            if c == '"':
                instr = True
            elif c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    end = i + 1
                    return t[brace:end]
        i += 1
    raise RuntimeError('no balanced object found')


def main():
    t = open(PAGE, encoding='utf-8', errors='replace').read()
    raw = extract_data_object(t)
    data = json.loads(raw)

    print('SETS:', len(data))
    total_urls = 0
    uniq_urls = set()
    for setname, types in data.items():
        n_types = len(types)
        set_urls = 0
        for typelabel, geos in types.items():
            for geolabel, url in geos.items():
                uniq_urls.add(url.split('?')[0])
                set_urls += 1
                total_urls += 1
        print(f'  {setname:35} types={n_types:>3}  url-options={set_urls:>4}')

    print()
    print('TOTAL url-options (type x geo):', total_urls)
    print('UNIQUE csv files:', len(uniq_urls))

    # Flat unique file list with metadata (dedup by url, keep first set/type/geo)
    seen = {}
    for setname, types in data.items():
        for typelabel, geos in types.items():
            for geolabel, url in geos.items():
                u = url.split('?')[0]
                if u not in seen:
                    seen[u] = {'set': setname, 'type': typelabel, 'geo': geolabel, 'url': u}

    with open(r'D:/research/econfindatalibrary/data/_zillow_files.json', 'w', encoding='utf-8') as f:
        json.dump(list(seen.values()), f, indent=1)
    print('unique file records written:', len(seen))

    fold = Counter(u.split('/public_csvs/')[1].split('/')[0] for u in uniq_urls)
    print('\nFOLDER (datatype) breakdown:')
    for k, v in sorted(fold.items()):
        print(f'  {k:32} {v}')

    geo = Counter(seen[u]['url'].split('/public_csvs/')[1].split('/')[1].split('_')[0] for u in seen)
    print('\nGEOGRAPHY-prefix breakdown:')
    for k, v in sorted(geo.items()):
        print(f'  {k:18} {v}')


if __name__ == '__main__':
    main()
