"""Unflagged POSITIONAL time axes (hagstofa's 33 false structural breaks, 2026-08-31).

The publisher ships live tables whose time axis (`Ár`/`Year`/`Mánuður`) carries NO
`time: true` flag, positional codes '0','1','2'… and the period only in valueTexts.
resolve_time_dim's step-3 name-match judged CODES alone and refused them, so
_fetch_table classified 20+ live tables (deaths to 2025, elections 2024) as
schema/structural breaks on every run — finalize() then held hagstofa `partial`
forever (a partial never sets last_success_utc, R231).

The rescue must be SAME-AXIS-ONLY (the scb Region door, R331, stays shut) and
opt-in via dim_labels (23 label-less call sites keep byte-identical behaviour).
"""
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.pxweb import resolve_time_dim  # noqa: E402


def _parse(s):
    """Minimal year grammar for the tests: 'YYYY' only, sane handled by caller."""
    s = str(s).strip()
    if len(s) == 4 and s.isdigit():
        return dt.date(int(s), 12, 31)
    return None


# The MAN05401 shape, verbatim from live metadata 2026-08-31: four dims, none
# flagged, time axis 'Ár' positional with year-window + year labels.
MAN_IDS = ["Kyn", "Aldur", "Skipting", "Ár"]
MAN_CODES = [["0", "1", "2"],
             [str(i) for i in range(96)],
             ["0", "1"],
             [str(i) for i in range(29)]]
MAN_LABELS = [["Males", "Females", "Difference"],
              ["At birth", "1 year", "2 years"] + [f"{i} years" for i in range(3, 96)],
              ["Average life expectancy", "Survivors of 1,000 born"],
              # windows (unparseable) then single years (parseable) — the real mix
              ["1971-1975", "1976-1980", "1981-1985"] + [str(y) for y in range(2000, 2026)]]


def test_unflagged_positional_axis_rescued_by_labels():
    idx = resolve_time_dim(MAN_IDS, MAN_CODES, meta_time_code=None,
                           parse_fn=_parse, dim_labels=MAN_LABELS)
    assert idx == 3, f"expected the Ár axis (3), got {idx}"


def test_without_dim_labels_old_behaviour_is_byte_identical():
    """The 23 label-less call sites must see NO change: refused, as before."""
    idx = resolve_time_dim(MAN_IDS, MAN_CODES, meta_time_code=None, parse_fn=_parse)
    assert idx is None


def test_garbage_labels_do_not_rescue():
    labels = list(MAN_LABELS)
    labels[3] = ["Week 1", "Week 2", "Week 3"]      # vecka's shape: named, nothing parses
    idx = resolve_time_dim(MAN_IDS, MAN_CODES, meta_time_code=None,
                           parse_fn=_parse, dim_labels=labels)
    assert idx is None, "a name-matched axis with unparseable labels must stay refused"


def test_non_named_axis_with_date_labels_is_never_promoted():
    """The scb Region door (R331): date-like labels on a NON-time-named axis must not
    make it the time dimension — the rescue is same-axis-only by construction."""
    ids = ["Region", "Item"]
    codes = [["0114", "0115"], ["0", "1"]]
    labels = [["2011", "2012"], ["a", "b"]]         # date-parsing labels on Region
    idx = resolve_time_dim(ids, codes, meta_time_code=None,
                           parse_fn=_parse, dim_labels=labels)
    assert idx is None


def test_code_parsing_axis_still_wins_without_labels_consulted():
    """A genuine dated axis resolves at step 2 exactly as before."""
    ids = ["Sex", "Year"]
    codes = [["0", "1"], ["2020", "2021", "2022"]]
    idx = resolve_time_dim(ids, codes, meta_time_code=None, parse_fn=_parse,
                           dim_labels=[["Males", "Females"], ["x", "y", "z"]])
    assert idx == 1


def test_fetcher_time_var_passes_labels_through():
    """The reviewer's required test (a): the ENTIRE recovery is delivered by the
    `dim_labels=` argument in hagstofa._time_var — reverting that one line leaves the
    core rescue inert and every other test green (mutation-proven). Pin it at the
    fetcher's own function with the MAN05401 shape, under the REAL parse_date."""
    from updater.strategies.fetchers import hagstofa as H

    variables = [
        {"code": "Kyn", "values": ["0", "1", "2"],
         "valueTexts": ["Males", "Females", "Difference between males and females"]},
        {"code": "Aldur", "values": [str(i) for i in range(96)],
         "valueTexts": ["At birth", "1 year"] + [f"{i} years" for i in range(2, 96)]},
        {"code": "Skipting", "values": ["0", "1"],
         "valueTexts": ["Average life expectancy", "Survivors of 1,000 born"]},
        {"code": "Ár", "values": [str(i) for i in range(29)],
         "valueTexts": ["1971-1975", "1976-1980", "1981-1985"]
                       + [str(y) for y in range(2000, 2026)]},
    ]
    tvar = H._time_var(variables)
    assert tvar is not None and tvar["code"] == "Ár", (
        "the unflagged positional Ár axis must resolve via its labels — if this fails, "
        "the dim_labels= argument in _time_var was dropped and the 33-table false-"
        "structural class is back")


def test_ingester_parse_resolves_positional_time_without_flag_or_role():
    """The reviewer's required test (b): jobs/ingest_hagstofa.py's parse_jsonstat2
    must pass its own collected dim_labels into the resolver — reverting that line
    parses 0 rows from a perfect body while every other test stays green (the
    authoritative-branch fixtures all carry role.time and mask it). No flag, no
    role.time, positional codes, periods only in labels."""
    from updater.strategies.fetchers.hagstofa import parse_jsonstat2

    body = {
        "id": ["Sex", "Year"],
        "size": [2, 3],
        "dimension": {
            "Sex": {"category": {"index": {"0": 0, "1": 1},
                                 "label": {"0": "Males", "1": "Females"}}},
            "Year": {"category": {"index": {"0": 0, "1": 1, "2": 2},
                                  "label": {"0": "2020", "1": "2021", "2": "2022"}}},
        },
        "value": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
    }
    rows = parse_jsonstat2(body, "PFX", None)      # time_code=None, no role.time
    assert len(rows) == 6, f"expected 6 rows, got {len(rows)}"
    dates = sorted({d.isoformat() for _, d, _ in rows})
    assert dates == ["2020-12-31", "2021-12-31", "2022-12-31"], dates
    assert all(k.startswith("PFX:Sex=") for k, _, _ in rows)


def test_single_event_table_year_axis():
    """KOS06001's shape: 'Year' with ONE positional value labelled '2024'."""
    ids = ["Unit", "constituency", "Year", "Sex", "Origin"]
    codes = [["0", "1"], [str(i) for i in range(7)], ["0"],
             ["0", "1", "2"], [str(i) for i in range(7)]]
    labels = [["Number", "Ratio, %"], ["Total"] * 7, ["2024"],
              ["Total", "Males", "Females"], ["Total"] * 7]
    idx = resolve_time_dim(ids, codes, meta_time_code=None,
                           parse_fn=_parse, dim_labels=labels)
    assert idx == 2
