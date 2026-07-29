# un_wpp titles: the country name is already parsed, then discarded

*Found 2026-07-29, after shipping un_wpp with 334,236 key-as-title rows. NOT YET COMMITTED —
written while the Bash tool was unavailable.*

## The gap

`un_wpp` went live with every title equal to its own series key:

    un_wpp:WPP:Births1519:AcceleratedABRdecline:ABW   ->   "WPP:Births1519:AcceleratedABRdecline:ABW"

That is the honest `broaden_catalog` fallback rather than an invented title, and it is why
search works only on the key: `Births1519` finds 8,784 series, but "adolescent birth rate" or
"Aruba" find nothing. Key grain is `WPP:{Indicator}:{Variant}:{ISO3}`.

## What is recoverable WITHOUT any new source

`jobs/ingest_un_wpp.py` already reads the human location name and then throws it away:

```python
loc_col = next((h for h in headers if h.lower() == "location"), None)   # line 100
...
loc = iso3 or (row.get(loc_col) or "UNKNOWN").strip()                   # line 128
```

`Location` is parsed into `loc_col`, but used only as a FALLBACK for a missing ISO3 — which
essentially never happens, so "Aruba" is discarded on every row and only `ABW` survives into
the key. The WPP2024 CSVs carry `ISO3_code` and `Location` side by side, so an authoritative
ISO3 -> name mapping is available from the publisher's own file. Nothing needs inventing.

Same for the variant: `AcceleratedABRdecline` is already a readable name, needing only
spacing.

**So a title like `Births1519 — Accelerated ABR decline — Aruba` is derivable entirely from
data UN already ships and this job already reads.**

## What is NOT recoverable from these files

The INDICATOR long name. In these CSVs the indicator IS the column header (`val_cols`,
line 107) — `Births1519`, `TFR`, and so on — and WPP publishes no long-form label inside the
same file. Expanding `Births1519` to "Births to women aged 15-19" would be me guessing at a
publisher's definition, which is the fabrication trap that deferred the per-ticker page
generator. If a WPP indicator-metadata file exists it must be fetched and quoted; until then
the code stays as-is.

## Suggested change (not made)

1. Keep the key EXACTLY as it is — it is published and downloadable, and re-keying a live
   source is a reserved decision, not a titling convenience.
2. Build `{ISO3 -> Location}` from the WPP CSVs themselves and set catalog `title` (and
   `geography`) from it. Titles change; ids do not.
3. Leave the indicator code in the title verbatim until a sourced indicator-name mapping
   exists.

Result: 334,236 series become searchable by country name, with no id churn and nothing
invented.
