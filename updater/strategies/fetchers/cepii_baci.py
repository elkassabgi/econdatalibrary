"""S5 bulk fetcher — CEPII BACI bilateral trade (cepii.fr, no key).

308,561,322 rows across two HS classifications. The source was REGISTERED with a strategy and a
script and had NO fetcher, so the orchestrator could never run it — and the cost of that is now
visible: jobs/ingest_cepii_baci.py hardcodes `V202401b`, while CEPII's download page today
offers **V202601**. The stored data is two vintages behind and nothing in the system noticed,
because nothing was ever asked to look.

THE VINTAGE IS MEASURED, NOT ASSUMED (the R164 rule cepii_gravity's fetcher records). CEPII's
own product page lists every release as `BACI_HS<nn>_V<yyyymm>[letter].zip`:

    https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37
    -> HS02 HS07 HS12 HS17 HS22 HS92 HS96, all V202601, IDENTICAL across two fetches

So the token is a hash over the (classification, version) pairs actually advertised. It moves
exactly when CEPII publishes, which is what a bulk gate needs: BACI has no delta API and a new
release RESTATES all prior years, so the only honest refresh is a whole re-download — and the
only honest way to avoid doing that nightly is a publisher-side version string.

WHICH CLASSIFICATIONS. The existing store holds HS17 and HS96, so those are what this refreshes;
CEPII offers five more. Adding them is a data decision (more classifications means more of the
same trade flows under different product vocabularies, not new trade), so it is left alone
rather than quietly widened by a fetcher rewrite.

SERVING IS A SEPARATE QUESTION and this does not answer it. The store's schema is
(year, exporter, importer, product, value, quantity) — bilateral flows, no series_key, no
obs_date — so it cannot be catalogued in the current model at any grain without a restructure of
the kind usda, istat and census needed. This fetcher keeps the DATA current; making it reachable
is separate work.
"""
from __future__ import annotations

import hashlib
import os
import re
import sys

import requests

from ... import blob, config
from ...errors import DefinitiveError, TransientError
from ..base import Result
from ._common import Tally, finalize

SOURCE = "cepii_baci"
PAGE = "https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37"
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}
# Only what the store already holds — see the docstring. Widening this is a data decision.
WANT_HS = ("HS17", "HS96")
RE_REL = re.compile(r"BACI_HS(\d{2})_V(\d{6}[a-z]?)")


def _releases(sess=None) -> dict:
    """{'HS17': 'V202601', ...} as advertised on CEPII's own product page."""
    sess = sess or requests.Session()
    try:
        r = sess.get(PAGE, headers=UA, timeout=180)
        r.raise_for_status()
    except Exception as e:                                     # noqa: BLE001
        raise TransientError(f"{SOURCE}: CEPII product page unreachable: {e!r}") from e
    out = {}
    for hs, v in RE_REL.findall(r.text):
        out[f"HS{hs}"] = f"V{v}"
    return out


def current_vintage(unit):
    """Hash over the advertised (classification, version) pairs we actually track.

    None when the page yields nothing — undeterminable, so the strategy fetches (cadence-gated)
    rather than freezing on a token that means "we could not look".
    """
    try:
        rel = _releases()
    except TransientError:
        return None
    pairs = sorted((k, v) for k, v in rel.items() if k in WANT_HS)
    if not pairs:
        return None
    h = hashlib.sha256("|".join(f"{k}={v}" for k, v in pairs).encode()).hexdigest()[:16]
    return f"{SOURCE}:{h}"


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    sess = requests.Session()
    rel = _releases(sess)
    todo = {k: rel[k] for k in WANT_HS if k in rel}
    missing = [k for k in WANT_HS if k not in rel]
    if missing:
        # A classification the store holds is no longer advertised: that is a publisher change,
        # not an empty period, and refusing beats quietly serving a stale half.
        raise DefinitiveError(
            f"{SOURCE}: CEPII no longer advertises {', '.join(missing)} — the page lists "
            f"{', '.join(sorted(rel)) or 'nothing'}. Existing data kept; check the product page "
            f"before changing WANT_HS.")

    sys.path.insert(0, config.ROOT if hasattr(config, "ROOT") else os.getcwd())
    from jobs import ingest_cepii_baci as J                    # download() + ingest_zip()

    tally = Tally()
    total = 0
    for hs, ver in sorted(todo.items()):
        url = f"https://www.cepii.fr/DATA_DOWNLOAD/baci/data/BACI_{hs}_{ver}.zip"
        print(f"[{SOURCE}] {hs} {ver} -> {url}", flush=True)
        try:
            zip_path = J.download(hs, url)
        except Exception as e:                                 # noqa: BLE001
            tally.transient_unit(hs)
            print(f"[{SOURCE}] {hs}: download failed: {e!r}", flush=True)
            continue
        try:
            n = J.ingest_zip(hs, zip_path)
        except Exception as e:                                 # noqa: BLE001
            raise TransientError(f"{SOURCE}: {hs} parse failed: {e!r}") from e
        if not n:
            tally.structural_unit(f"{hs} {ver}: zip parsed 0 rows")
            continue
        tally.added_unit(n, hs)
        total += n
        print(f"[{SOURCE}] {hs} {ver}: {n:,} rows", flush=True)

    # publish whatever the ingest wrote; it uses its own ParquetWriter, so bytes are already
    # correct on disk and only need to reach R2 (the publish_file case).
    published = 0
    for name in sorted(os.listdir(out_dir)):
        if name.endswith(".parquet") and blob.publish_file(os.path.join(out_dir, name)):
            published += 1
    print(f"[{SOURCE}] published {published} object(s)", flush=True)
    return finalize(tally, total, since or None, source=SOURCE)
