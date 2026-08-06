"""Probe every expirable credential the update system depends on — read-only, 1 request each.

WHY (2026-08-06, Ahmed: "be sure to consider api expiration and other such issues"): an
expired key surfaces as a per-source fetch failure and gets misread as a fetcher bug —
the insee 201/201 "transient" class. This audits the credential layer directly so auth
rot is named as auth rot. R37: a standing obligation is automated, never left to memory.

Covers the .env/.env.local keys (workstation route) via _common.api_key. NOT covered here
(verify elsewhere): gh OAuth (`gh auth status`), wrangler OAuth (`npx wrangler whoami`),
R2 keys (every catalog refresh exercises them), CLOUDFLARE_API_TOKEN (every D1 sync),
RESEND (every digest email). Dashboard-only: whether the CF/R2 tokens carry a TTL.

Exit 0 = all present keys valid; 1 = any invalid/errored. A missing key is reported but
does not fail the audit (some are optional).
"""
from __future__ import annotations

import os
import sys
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers._common import api_key  # noqa: E402

UA = {"User-Agent": "econdl-credential-audit"}


def _get(url: str, headers: dict | None = None) -> tuple[int, str]:
    req = urllib.request.Request(url, headers={**UA, **(headers or {})})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.status, r.read(300).decode(errors="replace")


# name -> (url_builder, extra_headers_builder, validity predicate on (status, body))
PROBES = {
    "FRED_API_KEY": (
        lambda k: f"https://api.stlouisfed.org/fred/series?series_id=GNPCA&api_key={k}&file_type=json",
        None, lambda s, b: s == 200 and "seriess" in b),
    "BLS_API_KEY": (
        lambda k: f"https://api.bls.gov/publicAPI/v2/timeseries/data/CUUR0000SA0?registrationkey={k}",
        None, lambda s, b: s == 200 and "REQUEST_SUCCEEDED" in b),
    "EIA_API_KEY": (
        lambda k: f"https://api.eia.gov/v2/petroleum/pri/spt/data/?api_key={k}&frequency=daily&data[0]=value&length=1",
        None, lambda s, b: s == 200 and "response" in b),
    "CENSUS_API_KEY": (
        lambda k: ("https://api.census.gov/data/timeseries/eits/marts"
                   f"?get=cell_value,data_type_code,seasonally_adj,category_code&time=2026-05&key={k}"),
        None, lambda s, b: s == 200 and b.lstrip().startswith("[[")),
    "BEA_API_KEY": (
        lambda k: f"https://apps.bea.gov/api/data/?UserID={k}&method=GetDataSetList&ResultFormat=JSON",
        None, lambda s, b: s == 200 and "Dataset" in b),
    "NOAA_API_KEY": (
        lambda k: "https://www.ncei.noaa.gov/cdo-web/api/v2/datasets?limit=1",
        lambda k: {"token": k}, lambda s, b: s == 200 and ("results" in b or "metadata" in b)),
    "COMTRADE_API_KEY": (
        lambda k: ("https://comtradeapi.un.org/data/v1/get/C/A/HS?reporterCode=36&period=2023"
                   "&partnerCode=0&flowCode=X&cmdCode=TOTAL&maxRecords=1"),
        lambda k: {"Ocp-Apim-Subscription-Key": k},
        lambda s, b: s == 200 and ("data" in b or "count" in b)),
}


def main() -> int:
    bad = 0
    for name, (url_fn, hdr_fn, ok_fn) in PROBES.items():
        k = api_key(name)
        if not k:
            print(f"{name:20s} NOT SET (optional or lives elsewhere)")
            continue
        try:
            s, b = _get(url_fn(k), hdr_fn(k) if hdr_fn else None)
            if ok_fn(s, b):
                print(f"{name:20s} VALID (HTTP {s})")
            else:
                bad += 1
                print(f"{name:20s} INVALID: HTTP {s}, body starts {b[:80]!r}")
        except urllib.error.HTTPError as e:
            body = e.read(150).decode(errors="replace")
            bad += 1
            print(f"{name:20s} INVALID: HTTP {e.code}, body starts {body[:80]!r}")
        except Exception as e:                               # noqa: BLE001
            bad += 1
            print(f"{name:20s} ERROR: {type(e).__name__}: {str(e)[:90]}")
    print(f"\ncredential audit: {'ALL PRESENT KEYS VALID' if not bad else f'{bad} PROBLEM(S)'}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
