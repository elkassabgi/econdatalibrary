# Served receipt

`.github/workflows/served-receipt.yml` runs `tools/served_receipt.py`. It is the receipt for any claim
that a fix is *served*: it downloads each series the way a user does (public worker, real API key,
`?raw=1`) and compares the body with the R2 object the worker serves it from.

Per id it prints the served newest `obs_date`, the R2 newest `obs_date`, the data-row counts and
`equal` or `DIFFERENT`. The job fails on any difference, on any id it cannot fetch from either side, and
when the secret is missing. It never passes on a skipped id.

## One-time owner step

Create an API key for the receipt on the site's account page and add it to this repository as the
secret `ECON_RECEIPT_API_KEY`. Until then each run fails with
`secret ECON_RECEIPT_API_KEY not set - owner step`.

## How to run

Actions -> served-receipt -> Run workflow. Leave `ids` empty for the default sample, or give ids
separated by spaces or commas (at most 25).

The weekly schedule in the workflow is commented out. Turn it on after the secret exists and the
expected-pending ids below are equal.

## Default sample and its expected state

| id | expected |
|---|---|
| `imf_imts_direct:IMTS:USA.M.XG_FOB_USD` | equal |
| `fhfa:hpi_at_3zip_quarterly:010` | equal |
| `fhfa:hpi_at_3zip_quarterly:100` | equal |
| `noaa:gsom:CA004047080:TAVG` | **expected-pending**: DIFFERENT until the noaa re-derive runs |
| `bis:WS_CBPOL:US` | equal |

A DIFFERENT on an expected-pending id is the known state, not a new defect. Remove the mark here when
the re-derive has run and the receipt shows it equal.

## What it does not prove

- **Cache lag.** CSV answers carry `cache-control: public, max-age=300`. The worker does not cache
  them in its own edge cache and documents no cache-busting parameter, so the tool adds none and sends
  `Cache-Control: no-cache`. An object replaced in the last five minutes can still show DIFFERENT once
  through a shared cache. Re-run before acting on it.
- **The origin.** The comparison is with R2 only while the worker serves `.csv` from R2. The tool reads
  `/v1/edge-status` first; if it reports `forward: true` the data comes from the self-hosted origin and
  the tool refuses (exit 2) instead of comparing with the wrong backend.
- **The citation header.** `?raw=1` omits it, so the receipt covers the data rows, not the header text.
