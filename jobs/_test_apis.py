#!/usr/bin/env python3
"""Quick API endpoint probe — run once and delete."""
import requests
hdrs = {'User-Agent': 'EconFinLib admin@hfdatalibrary.com', 'Accept':'application/json'}

tests = [
    ('BoC series list', 'GET', 'https://www.bankofcanada.ca/valet/series/json', None),
    ('BoC obs FXCADUSD', 'GET', 'https://www.bankofcanada.ca/valet/observations/FXCADUSD/json', None),
    ('SNB v2 series', 'GET', 'https://data.snb.ch/api/v2/series', None),
    ('SNB query root', 'GET', 'https://data.snb.ch/query/', None),
    ('Riksbank cross', 'GET', 'https://api.riksbank.se/swea/v1/CrossSectionalSeries', None),
    ('Riksbank obs test', 'GET', 'https://api.riksbank.se/swea/v1/Observations/SEKUSDPMI', None),
    ('CBS TypedDataSet', 'GET', 'https://opendata.cbs.nl/OData/83582NED/TypedDataSet?$top=2', None),
    ('SCB POST', 'POST', 'https://api.scb.se/OV0104/v1/doris/sv/ssd/BE/BE0101/BE0101N1',
     {'query':[],'response':{'format':'json-stat2'}}),
    ('SSB table subdir', 'GET', 'https://data.ssb.no/api/v0/en/table/', None),
]
for name, method, url, body in tests:
    try:
        if method == 'POST':
            r = requests.post(url, json=body, headers=hdrs, timeout=20)
        else:
            r = requests.get(url, headers=hdrs, timeout=20, allow_redirects=True)
        ct = r.headers.get('Content-Type','?')[:40]
        txt = r.text[:150].replace('\n',' ').replace('\r','')
        print(f"{name}: HTTP {r.status_code}  ct={ct}  body={txt}")
    except Exception as e:
        print(f"{name}: ERR {e}")
