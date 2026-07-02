"""License gate -- the code that enforces the free-only / re-serveable rule.

Only license classes in RESERVABLE may be cached on our servers and re-served to
the public. The ingest pipeline calls assert_reservable() at both the source and
per-series level, so restricted data can never be published by accident.

Decisions locked 2026-06-01 (see configs/sources.yaml):
  - HOST: all GREEN + kept YELLOW (IMF, ECB, BIS*, DBnomics*)
  - DROP: CoinGecko, Alternative.me
  - CARVE-OUT: Eurostat (non-EU/some trade), OWID (upstream 3rd-party), FRED (Copyright series)
  * BIS = non-commercial only; DBnomics = per-series license passthrough.
"""

# Re-serveable license classes (green + the conditional ones we accepted).
RESERVABLE = {
    "us-public-domain",      # SEC EDGAR, BLS, BEA, Census, Treasury, Fed, EIA, USDA, NOAA, FHFA
    "cc-by-4.0",             # World Bank, OECD, Eurostat(EU), IMF, ILOSTAT, FAOSTAT, PWT, ABS, OWID(own)
    "cc0",                   # Wikidata
    "ogl-uk-3.0",            # Bank of England
    "etalab-2.0",            # INSEE
    "statcan-open",          # Statistics Canada
    "ecb-attrib-nomodify",   # ECB / Frankfurter (cache RAW, do not modify the values)
    "imf-terms",             # IMF (must disclose the data is free)
    "bis-attrib-nc",         # BIS (NON-COMMERCIAL redistribution only)
    "zillow-research",       # Zillow ("Data Provided by Zillow Group")
    "defillama-open",        # DeFiLlama
    "dbnomics-passthrough",  # per-series: each series carries its provider's license
}

# Explicitly NOT re-serveable (dropped or red). Listed for clarity / tests.
EXCLUDED = {
    "coingecko-display-only",   # dropped per storage critic
    "altme-attrib",             # Alternative.me -- dropped with CoinGecko
    "vendor-no-redist",         # Alpha Vantage, Finnhub, Tiingo, Polygon, Alpaca, ...
    "cc-by-nc",                 # WHO GHO, Yale EPI
    "fred-copyright",           # FRED "Copyright"-flagged series (e.g. S&P/Case-Shiller)
    "proprietary",              # LBMA/ICE metals, Baltic Dry, FINRA TRACE, MSRB EMMA
}


class LicenseError(PermissionError):
    pass


def assert_reservable(license_id: str, *, context: str = "") -> None:
    """Raise unless this license may be cached & re-served publicly."""
    if license_id not in RESERVABLE:
        where = f" [{context}]" if context else ""
        raise LicenseError(
            f"License {license_id!r} is not re-serveable{where} -- refusing to publish."
        )


def is_non_commercial(license_id: str) -> bool:
    """BIS is re-serveable but non-commercial: gate it out of any future paid tier."""
    return license_id in {"bis-attrib-nc", "cc-by-nc"}
