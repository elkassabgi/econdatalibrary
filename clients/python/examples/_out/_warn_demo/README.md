# mystudy

Econ Data Library bundle. Snapshot pinned: **2026-06-26**.

This is a re-runnable lockfile. Rebuild the exact data with:

```python
import econdl
df = econdl.pull('datapackage.json')        # exact snapshot (verifies sha256)
df = econdl.pull('datapackage.json', latest=True)  # opt in to refreshed data
```

## Sources, licenses & citations

### Bureau of Labor Statistics  (`bls`)
- License: us-public-domain
- Attribution: Source: U.S. BLS (public domain)
- Citation: Bureau of Labor Statistics (2026). Accessed via Econ Data Library, snapshot 2026-06-26.
- Resource: `data/bls.parquet`  (27014 bytes, sha256:ce33d0fddea8879adff32e3be6fda0f88005413ea446ae4c66e92d092b58aa41)

### Penn World Table 11.0  (`penn_world_table`)
- License: cc-by-4.0
- Attribution: Source: Penn World Table 11.0 (CC BY 4.0)
- Citation: Penn World Table 11.0 (2026). Accessed via Econ Data Library, snapshot 2026-06-26.
- Resource: `data/penn_world_table.parquet`  (1972 bytes, sha256:321b656e71cd62db42058161b88e4574f513144c78422d77552b0f8040cbffa5)

### The World Bank — World Development Indicators (WDI)  (`worldbank_wdi`)
- License: cc-by-4.0
- Attribution: Source: The World Bank, World Development Indicators. Licence: CC BY 4.0.
- Citation: The World Bank — World Development Indicators (WDI) (2026). Accessed via Econ Data Library, snapshot 2026-06-26. https://datatopics.worldbank.org/world-development-indicators/
- Resource: `data/worldbank_wdi.parquet`  (283339 bytes, sha256:cbb0e0a180edf93e470e2dd294eda410e7ae98eab0ff03b1689000fe565c5008)
