# Econ Data Library — A Citable Catalog of Economic & Financial Time Series

**Author:** Ahmed Elkassabgi
**Affiliation:** University of Central Arkansas
**ORCID:** 0000-0002-5926-7493
**Version:** 1.0
**Release date:** 2026-07-16
**License (this compilation):** Creative Commons Attribution 4.0 International (CC BY 4.0)
**DOI:** (assigned by Zenodo on publication)

---

## About this dataset

The Econ Data Library is a free, non-commercial academic data library that gives
students and researchers one clean, well-documented, citable place to find
economic and financial statistics. It is part of the ElkassabgiData family of
research libraries (one free account works across every library).

**Scale (measured on the complete data store, census of 2026-07-02):**
- ~7.73 billion individual time series held across all sources
  (global distinct series keys per source, HyperLogLog estimate, ~1% error —
  a conservative floor)
- ~79.8 billion observations (exact Parquet row counts)
- 1,234,073 curated, searchable catalog entries (as of 2026-07-16)
- 182 sources served (as of 2026-07-16), spanning international organizations
  (World Bank, IMF, UN, OECD, Eurostat, ECB, BIS, ILO, FAO), national
  statistical offices and central banks (U.S. federal agencies, Statistics
  Canada, ABS, INSEE, Destatis-Bundesbank, CBS Netherlands, Statistics Poland,
  and many more), and curated research datasets hosted with written permission.

Every series is served with full source attribution: the CSV download header
and the per-series `metadata.json` carry the original producer, its license,
a ready-made citation, and a link back to the authoritative source.

---

## Accessing the data

**This Zenodo record is the citable reference for the library.** The data
itself is hosted at:

**https://econdatalibrary.com**

Access channels:
- **Browser**: searchable catalog, per-series pages, CSV downloads
- **REST API**: free, documented at https://econdatalibrary.com/api.html
- **AI tools**: an MCP server (Model Context Protocol) so AI assistants can
  query the catalog directly — https://econdatalibrary.com/mcp.html
- **Clients**: Python and R client libraries

Registration is free (email, ORCID, or Google); catalog search and metadata
require no account.

---

## Licensing — read this before redistributing

This deposit (the catalog compilation, its documentation, and this record) is
CC BY 4.0. **The underlying data series are NOT relicensed by this deposit.**
Each series remains governed by its original producer's terms, which the
library surfaces per-series (CSV citation header + metadata):

- Most sources are open licenses (public domain, CC BY 4.0, national
  open-government licenses).
- Some sources are non-commercial or attribution-with-no-modification terms —
  the per-series license flag says so explicitly.
- Sources whose terms do not permit redistribution are not served (the library
  gates them and directs users to the original provider).
- A small number of collections are hosted under written permission from their
  producers, recorded and honored per their stated conditions.

When you use a series in research, cite the **original producer first** (the
ready-made citation shipped with every download does this for you), and the
library as the access point.

---

## How to cite

If you use the library in your research, please cite it as:

> Elkassabgi, Ahmed. 2026. *Econ Data Library: A Citable Catalog of Economic
> & Financial Time Series* (version 1.0) [Data set]. Zenodo.
> https://doi.org/(DOI assigned on publication)

### BibTeX

```bibtex
@dataset{elkassabgi2026econdatalibrary,
  author    = {Elkassabgi, Ahmed},
  title     = {{Econ Data Library: A Citable Catalog of Economic
               \& Financial Time Series}},
  year      = {2026},
  version   = {1.0},
  publisher = {Zenodo},
  doi       = {(assigned on publication)},
  url       = {https://econdatalibrary.com}
}
```

---

## Contact

**Email:** admin@hfdatalibrary.com (ElkassabgiData family)
**Website:** https://econdatalibrary.com
**Family portal:** https://elkassabgidata.com
**ORCID:** https://orcid.org/0000-0002-5926-7493
