#!/usr/bin/env python3
"""INSEE SIRENE company registry full ingest.

Downloads all active French legal units (siren) and establishments (siret).
~12M active legal units, ~30M establishments.

License: Licence Ouverte / Open Licence 2.0 — commercial redistribution OK.
GDPR carve-out: skip records where statutDiffusion = 'N' (natural person opt-out).
Attribution: Source: INSEE SIRENE (Licence Ouverte 2.0) www.insee.fr

API: https://api.insee.fr/api-sirene/3.11
Auth: X-INSEE-Api-Key-Integration header
Pagination: curseurSuivant cursor, 10k records/page

Run: python jobs/ingest_insee_sirene.py [siren|siret|both]
"""
from __future__ import annotations
import os, sys, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
sys.path.insert(0, ROOT)
from core.config import load_env; load_env()
import os as _os

OUT = os.path.join(ROOT, "data", "clean_full", "insee_sirene")
KEY = _os.environ.get("INSEE_SIRENE_KEY", "")
BASE = "https://api.insee.fr/api-sirene/3.11"
UA  = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
       "X-INSEE-Api-Key-Integration": KEY}
PAGE = 1_000  # SIRENE API hard max per request

# Fields confirmed from live API response (top-level + in periodesUniteLegale[0])
SIREN_FIELDS_TOP = [
    "siren", "statutDiffusionUniteLegale", "dateCreationUniteLegale",
    "sigleUniteLegale", "trancheEffectifsUniteLegale", "anneeEffectifsUniteLegale",
    "categorieEntreprise", "anneeCategorieEntreprise",
]
SIREN_FIELDS_PERIOD = [
    "denominationUniteLegale", "denominationUsuelle1UniteLegale",
    "categorieJuridiqueUniteLegale", "activitePrincipaleUniteLegale",
    "etatAdministratifUniteLegale", "economieSocialeSolidaireUniteLegale",
    "societeMissionUniteLegale", "caractereEmployeurUniteLegale",
]

SIRET_FIELDS_TOP = [
    "siret", "siren", "nic", "statutDiffusionEtablissement",
    "dateCreationEtablissement", "trancheEffectifsEtablissement",
    "anneeEffectifsEtablissement", "etablissementSiege", "nombrePeriodesEtablissement",
]
SIRET_FIELDS_PERIOD = [
    "etatAdministratifEtablissement", "activitePrincipaleEtablissement",
    "nomenclatureActivitePrincipaleEtablissement", "caractereEmployeurEtablissement",
]
SIRET_FIELDS_ADDR = [
    "codePostalEtablissement", "libelleCommuneEtablissement",
    "codeCommuneEtablissement",
    "libelleCommuneEtrangerEtablissement", "codePaysEtrangerEtablissement",
]

ALL_SIREN = SIREN_FIELDS_TOP + SIREN_FIELDS_PERIOD
ALL_SIRET = SIRET_FIELDS_TOP + SIRET_FIELDS_PERIOD + SIRET_FIELDS_ADDR


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def flatten(unit, top_fields, period_fields, addr_fields=None, period_key="periodesUniteLegale"):
    rec = {}
    for f in top_fields:
        rec[f] = str(unit.get(f, "") or "")
    periods = unit.get(period_key, [])
    latest = periods[0] if periods else {}
    for f in period_fields:
        rec[f] = str(latest.get(f, "") or "")
    if addr_fields:
        addr = unit.get("adresseEtablissement", {}) or {}
        for f in addr_fields:
            rec[f] = str(addr.get(f, "") or "")
    return rec


def fetch(endpoint, items_key, top_fields, period_fields, addr_fields,
          period_key, all_fields, out_path, label):
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"{label}: already {n:,} rows"); return n

    schema = pa.schema([(f, pa.string()) for f in all_fields])
    writer = pq.ParquetWriter(out_path + ".tmp", schema, compression="zstd")
    bufs = {f: [] for f in all_fields}
    offset = 0; total = 0; page = 0; BATCH = 50_000
    # SIRENE uses offset-based pagination (`debut`), max 1000 per request

    while True:
        params = {"q": "*", "nombre": PAGE, "debut": offset,
                  "champs": ",".join(all_fields)}
        for attempt in range(4):
            try:
                r = requests.get(f"{BASE}/{endpoint}", headers=UA, params=params, timeout=120)
                break
            except Exception as e:
                log(f"  {label}: ERR {e} (attempt {attempt+1})"); time.sleep(10*(attempt+1))
        else:
            log(f"  {label}: giving up at offset {offset}"); break

        if r.status_code == 429:
            log(f"  {label}: rate limit, sleep 60s"); time.sleep(60); continue
        if r.status_code != 200:
            log(f"  {label}: HTTP {r.status_code} at offset {offset}: {r.text[:100]}"); break

        d = r.json()
        items = d.get(items_key, [])
        if not items:
            break  # exhausted

        for item in items:
            # GDPR: skip opt-out natural persons (statutDiffusion = 'N')
            sd = item.get("statutDiffusionUniteLegale") or item.get("statutDiffusionEtablissement")
            if sd == "N":
                continue
            rec = flatten(item, top_fields, period_fields, addr_fields, period_key)
            for f in all_fields:
                bufs[f].append(rec.get(f, ""))
            total += 1
            if total % BATCH == 0:
                batch = pa.record_batch({f: pa.array(bufs[f], pa.string()) for f in all_fields}, schema=schema)
                writer.write_batch(batch)
                for f in all_fields: bufs[f].clear()

        offset += len(items)
        page += 1
        if page % 500 == 0:
            log(f"  {label}: {total:,} records stored / {offset:,} fetched (page {page})")
        if len(items) < PAGE:
            break  # last page
        time.sleep(0.2)

    if any(bufs[f] for f in all_fields):
        batch = pa.record_batch({f: pa.array(bufs[f], pa.string()) for f in all_fields}, schema=schema)
        writer.write_batch(batch)
    writer.close()
    os.replace(out_path + ".tmp", out_path)
    n = pq.read_metadata(out_path).num_rows
    log(f"{label}: DONE {n:,} records"); return n


def main():
    if not KEY:
        raise SystemExit("INSEE_SIRENE_KEY not in .env")
    os.makedirs(OUT, exist_ok=True)
    ds = sys.argv[1] if len(sys.argv) > 1 else "both"
    total = 0
    if ds in ("siren", "both"):
        total += fetch("siren", "unitesLegales",
                       SIREN_FIELDS_TOP, SIREN_FIELDS_PERIOD, None, "periodesUniteLegale",
                       ALL_SIREN, os.path.join(OUT, "siren.parquet"), "SIRENE legal units (siren)")
    if ds in ("siret", "both"):
        total += fetch("siret", "etablissements",
                       SIRET_FIELDS_TOP, SIRET_FIELDS_PERIOD, SIRET_FIELDS_ADDR,
                       "periodesEtablissement", ALL_SIRET,
                       os.path.join(OUT, "siret.parquet"), "SIRENE establishments (siret)")
    log(f"GRAND TOTAL: {total:,} SIRENE records")


if __name__ == "__main__":
    main()
