"""Write tests/fixtures/business_calendar/ofr_weekday_closures.json from OFR API dumps - the publisher's own closures.

WHAT IT MEASURES. On which weekdays does the Office of Financial Research publish NO daily value? The answer is
read from the data alone: no holiday rule is applied, and nothing from updater/calendars.py is imported (the
fixture is the evidence that rule is tested against, so it must not be derived from it).
  * ofr: the union of every series in the fnyr and repo datasets, from the first fnyr date (2016-03-01) to the
    newest date in the dumps. ofr's newest observation is the max over these series.
  * nyfed_equivalent: the fnyr headline series FNYR-SOFR-A, FNYR-OBFR-A and FNYR-TGCR-A (the New York Fed rates
    the nyfed fetcher reads from FRED), from 2018-04-03 (SOFR's first publication).
For each it records every weekday with no non-null value, and, for each Friday before a Saturday federal holiday,
the number of series that DO carry a value that Friday (the days a federal-employee Saturday->Friday rule would
close in error).

INPUT DUMPS (as measured on 2026-11-27). The daily-maintenance run of 2026-11-27 03:24 CST fetched
    data.financialresearch.gov/v1/series/dataset?dataset=fnyr   ->  F:/work/maintenance/2026-11-27_ofr/fnyr.json
    data.financialresearch.gov/v1/series/dataset?dataset=repo   ->  F:/work/maintenance/2026-11-27_ofr/repo.json
at about 09:33Z (gzipped JSON: {"<mnemonic>": {"timeseries": {"aggregation": [[date, value], ...]}}}). Those files
are not in the repo. To re-measure, fetch the two datasets again into a NEW empty directory and point this tool
at it; a downloaded file is untrusted data, so run the tool with `python -I` and keep it outside that directory.

    python -I tools/measure_ofr_weekday_closures.py <dump dir> tests/fixtures/business_calendar/ofr_weekday_closures.json <UTC stamp>

The stamp is when you measured (`date -u +%Y-%m-%dT%H:%M:%SZ`). Then run tests/test_health_business_calendar.py:
if the publisher's closures moved away from updater/calendars.py, that test fails and names the dates.
"""
from __future__ import annotations

import gzip
import json
import os
import sys
from datetime import date, timedelta

NYFED_SERIES = ("FNYR-OBFR-A", "FNYR-SOFR-A", "FNYR-TGCR-A")
NYFED_FROM = date(2018, 4, 3)


def load(path: str) -> dict:
    """{mnemonic: {iso date: value}} of the non-null values in one OFR dump (gzipped or plain JSON)."""
    raw = open(path, "rb").read()
    try:
        raw = gzip.decompress(raw)
    except OSError:
        pass
    doc = json.loads(raw)
    out = {}
    for sid, obj in doc.get("timeseries", doc).items():
        agg = None
        if isinstance(obj, dict):
            agg = (obj.get("timeseries") or {}).get("aggregation")
            if agg is None:
                for v in obj.values():
                    if isinstance(v, list):
                        agg = v
        out[sid] = {str(row[0])[:10]: row[1] for row in agg or [] if row and row[1] is not None}
    return out


def _nth_weekday(y: int, m: int, wd: int, n: int) -> date:
    d = date(y, m, 1)
    d += timedelta(days=(wd - d.weekday()) % 7)
    return d + timedelta(weeks=n - 1)


def saturday_holidays(y: int) -> list:
    """The ACTUAL dates of the 5 U.S.C. 6103(a) holidays of year y that fall on a Saturday (Juneteenth from 2021).
    Used only to choose which Fridays to report a count for; the closures themselves come from the data."""
    may31 = date(y, 5, 31)
    days = [date(y, 1, 1), _nth_weekday(y, 1, 0, 3), _nth_weekday(y, 2, 0, 3),
            may31 - timedelta(days=may31.weekday()), date(y, 7, 4), _nth_weekday(y, 9, 0, 1),
            _nth_weekday(y, 10, 0, 2), date(y, 11, 11), _nth_weekday(y, 11, 3, 4), date(y, 12, 25)]
    if y >= 2021:
        days.append(date(y, 6, 19))
    return [d for d in days if d.weekday() == 5]


def weekdays_without(present: set, start: date, end: date) -> list:
    out, d = [], start
    while d <= end:
        if d.weekday() < 5 and d.isoformat() not in present:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def saturday_fridays(by_date: dict, start: date, end: date) -> dict:
    out = {}
    for y in range(start.year, end.year + 1):
        for d in saturday_holidays(y):
            f = d - timedelta(days=1)
            if start <= f <= end:
                out[f.isoformat()] = len(by_date.get(f.isoformat(), ()))
    return dict(sorted(out.items()))


def measure(fnyr: dict, repo: dict, stamp: str) -> dict:
    union: dict = {}
    for ds, series in (("fnyr", fnyr), ("repo", repo)):
        for sid, days in series.items():
            for x in days:
                union.setdefault(x, set()).add(f"{ds}:{sid}")
    last = date.fromisoformat(max(union))
    o_start = date.fromisoformat(min(x for s in fnyr.values() for x in s))
    missing = [s for s in NYFED_SERIES if s not in fnyr]
    if missing:
        raise SystemExit(f"fnyr dump lacks {missing}: not the dataset this tool measures")
    sub: dict = {}
    for s in NYFED_SERIES:
        for x in fnyr[s]:
            sub.setdefault(x, set()).add(s)
    return {
        "what": ("Weekdays on which the publisher's daily data carry NO value, read from the data alone "
                 "(no holiday rule applied). Written by tools/measure_ofr_weekday_closures.py from the Office "
                 "of Financial Research API dumps; tests/test_health_business_calendar.py pins "
                 "updater/calendars.py to it."),
        "source": ("data.financialresearch.gov/v1/series/dataset?dataset=fnyr and dataset=repo, fetched "
                   "2026-11-27 ~09:33Z (daily-maintenance run, files fnyr.json and repo.json)"),
        "measured_utc": stamp,
        "ofr": {
            "population": f"union of every series in fnyr ({len(fnyr)}) and repo ({len(repo)})",
            "window": [o_start.isoformat(), last.isoformat()],
            "weekdays_without_any_value": weekdays_without(set(union), o_start, last),
            "saturday_holiday_fridays_series_with_a_value": saturday_fridays(union, o_start, last),
        },
        "nyfed_equivalent": {
            "population": "fnyr series: " + ", ".join(NYFED_SERIES),
            "window": [NYFED_FROM.isoformat(), last.isoformat()],
            "weekdays_without_any_value": weekdays_without(set(sub), NYFED_FROM, last),
            "saturday_holiday_fridays_series_with_a_value": saturday_fridays(sub, NYFED_FROM, last),
        },
    }


def main(argv) -> int:
    if len(argv) != 4:
        print(__doc__)
        return 2
    root, out_path, stamp = argv[1], argv[2], argv[3]
    doc = measure(load(os.path.join(root, "fnyr.json")), load(os.path.join(root, "repo.json")), stamp)
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(doc, f, indent=1)
        f.write("\n")
    for k in ("ofr", "nyfed_equivalent"):
        b = doc[k]
        print(f"{k}: window {b['window'][0]}..{b['window'][1]}, {len(b['weekdays_without_any_value'])} weekdays "
              f"without a value, Saturday-holiday Fridays {b['saturday_holiday_fridays_series_with_a_value']}")
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
