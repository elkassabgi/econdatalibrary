#!/usr/bin/env python3
"""Classify each DBnomics provider as DUPLICATE (we already host it at origin via a
direct connector) or UNIQUE (only reachable through DBnomics), and attach a
best-effort license_id per provider for the passthrough gate.

Run AFTER _dbnomics_catalog.py. Reads _catalog_summary.json + _catalog_providers.json,
prints duplicate-vs-unique series fractions, and writes _classification.json which the
data-pull step uses to decide which providers to actually download.
"""
import json
import os

ROOT = r"D:/research/econfindatalibrary"
OUT = os.path.join(ROOT, "data", "raw", "dbnomics")

# DBnomics provider_code -> our direct source id (these are DUPLICATES; we already
# ingest them at origin, so we do NOT re-download their observations via DBnomics).
DUPLICATE_OF = {
    "BLS": "bls",
    "BEA": "bea",
    "EIA": "eia",
    "FHFA": "fhfa",
    "FED": "fed_board",          # Federal Reserve Board (H.15 etc.)
    "IMF": "imf",
    "OECD": "oecd",
    "Eurostat": "eurostat",
    "ILO": "ilostat",
    "FAO": "faostat",
    "STATCAN": "statcan",
    "WB": "worldbank",
    "ECB": "ecb",
    "BOE": "boe",
    "BIS": "bis",
    "GGDC": "penn_world_table",  # Groningen GGDC hosts the Penn World Table
    # Note: Census/Treasury/USDA/NOAA have no DBnomics provider, so nothing to dedupe.
}

# Best-effort license_id per provider for the passthrough note (the enforced source
# license stays dbnomics-passthrough; the live terms_of_use URL is also stored).
# Public bodies -> typically open/public-domain or CC-BY; defaulted to provider-terms
# when unknown so the publish gate treats it conservatively.
PROVIDER_LICENSE = {
    # EU / European Commission family
    "AMECO": "cc-by-4.0", "EC": "cc-by-4.0", "Eurostat": "cc-by-4.0", "JRC": "cc-by-4.0",
    # intl orgs
    "IMF": "imf-terms", "WB": "cc-by-4.0", "OECD": "cc-by-4.0", "BIS": "bis-attrib-nc",
    "ILO": "cc-by-4.0", "FAO": "cc-by-4.0", "WTO": "provider-terms", "WHO": "provider-terms",
    "UNCTAD": "provider-terms", "UNDATA": "provider-terms", "UNDP": "provider-terms",
    "UNESCO": "provider-terms", "UNIDO": "provider-terms", "AFDB": "provider-terms",
    "AIH": "provider-terms", "BCEAO": "provider-terms", "Franc-zone": "provider-terms",
    "SAIS-CARI": "provider-terms", "ND_GAIN": "provider-terms", "FH": "provider-terms",
    # US federal (public domain)
    "BLS": "us-public-domain", "BEA": "us-public-domain", "EIA": "us-public-domain",
    "FED": "us-public-domain", "FHFA": "us-public-domain", "CBO": "us-public-domain",
    # central banks / NSOs (open-gov, attribution; provider-terms as conservative default)
    "ECB": "ecb-attrib-nomodify", "BOE": "ogl-uk-3.0", "ONS": "ogl-uk-3.0", "OBR": "ogl-uk-3.0",
    "BUBA": "provider-terms", "DESTATIS": "provider-terms", "BDF": "provider-terms",
    "BOC": "provider-terms", "STATCAN": "statcan-open", "BOJ": "provider-terms",
    "STATJP": "provider-terms", "METI": "provider-terms", "ESRI": "provider-terms",
    "JILPT": "provider-terms", "ISTAT": "provider-terms", "ELSTAT": "provider-terms",
    "INE-SPAIN": "provider-terms", "INEPT": "provider-terms", "NBB": "provider-terms",
    "SECO": "provider-terms", "INSEE": "etalab-2.0", "ACOSS": "provider-terms",
    "DARES": "etalab-2.0", "DREES": "etalab-2.0", "DGDDI": "etalab-2.0", "ENEDIS": "etalab-2.0",
    "RTE": "etalab-2.0", "IPP": "provider-terms", "CEPII": "provider-terms",
    "CEPREMAP": "provider-terms", "pole-emploi": "etalab-2.0", "meteofrance": "etalab-2.0",
    "NBS": "provider-terms", "ROSSTAT": "provider-terms", "INDEC": "provider-terms",
    "INEGI": "provider-terms", "MOSPI": "provider-terms", "BCB": "provider-terms",
    "BI": "provider-terms", "RBA": "provider-terms", "SARB": "provider-terms",
    "SAMA": "provider-terms", "TCMB": "provider-terms", "SCB": "provider-terms",
    "STATPOL": "provider-terms", "CSO": "provider-terms", "NAR": "provider-terms",
    "ISM": "provider-terms", "SCSMICH": "provider-terms", "GGDC": "cc-by-4.0",
    # mobility / alt-data (mostly COVID-era, often CC-BY or provider-specific)
    "Apple": "provider-terms", "Google": "provider-terms", "OpenTable": "provider-terms",
    "citymapper": "provider-terms", "OAG": "provider-terms", "JHU": "cc-by-4.0",
    "AQICN": "provider-terms", "ENTSOE": "provider-terms", "ICE": "provider-terms",
    "CWD": "provider-terms", "oppins": "provider-terms", "openfisca-tunisia": "provider-terms",
    "SAFE": "provider-terms",
}


def main():
    summ = json.load(open(os.path.join(OUT, "_catalog_summary.json"), encoding="utf-8"))
    provs = json.load(open(os.path.join(OUT, "_catalog_providers.json"), encoding="utf-8"))
    tou = {p["code"]: p.get("terms_of_use") for p in provs["providers"]}
    name = {p["code"]: p.get("name") for p in provs["providers"]}

    byp = summ["by_provider"]
    rows = []
    dup_series = dup_ds = uniq_series = uniq_ds = 0
    for code, d in byp.items():
        ns, nd = d["series"], d["datasets"]
        is_dup = code in DUPLICATE_OF
        rows.append({
            "provider": code,
            "name": name.get(code),
            "datasets": nd,
            "series": ns,
            "duplicate": is_dup,
            "duplicate_of": DUPLICATE_OF.get(code),
            "license_id": PROVIDER_LICENSE.get(code, "provider-terms"),
            "terms_of_use": tou.get(code),
        })
        if is_dup:
            dup_series += ns
            dup_ds += nd
        else:
            uniq_series += ns
            uniq_ds += nd

    rows.sort(key=lambda r: -r["series"])
    total_series = dup_series + uniq_series
    total_ds = dup_ds + uniq_ds

    print("=== DUPLICATE vs UNIQUE (enumerated catalog) ===")
    print(f"  DUPLICATE providers: {sum(1 for r in rows if r['duplicate'])}")
    print(f"  UNIQUE    providers: {sum(1 for r in rows if not r['duplicate'])}")
    print(f"  DUPLICATE series:    {dup_series:>15,}  ({100*dup_series/total_series:.1f}%)")
    print(f"  UNIQUE    series:    {uniq_series:>15,}  ({100*uniq_series/total_series:.1f}%)")
    print(f"  DUPLICATE datasets:  {dup_ds:>15,}")
    print(f"  UNIQUE    datasets:  {uniq_ds:>15,}")
    print()
    print("  UNIQUE providers by series (top 30):")
    for r in [r for r in rows if not r["duplicate"]][:30]:
        print(f"    {r['provider']:14} ds={r['datasets']:>5,} series={r['series']:>14,} lic={r['license_id']}")

    json.dump({"rows": rows, "dup_series": dup_series, "uniq_series": uniq_series,
               "dup_datasets": dup_ds, "uniq_datasets": uniq_ds,
               "total_series": total_series, "total_datasets": total_ds},
              open(os.path.join(OUT, "_classification.json"), "w", encoding="utf-8"), indent=2)
    print("\nWROTE _classification.json")


if __name__ == "__main__":
    main()
