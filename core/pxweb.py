"""core/pxweb.py — the canonical PxWeb time-dimension resolver + period parser.

ONE shared implementation of "which axis of this JSON-stat2 cube is the date axis?"
so every PxWeb source (SCB, SSB, StatFin, Hagstofa, BFS, Statistics Estonia /
Latvia / Slovenia, DST, CSO, IRENA, …) selects it the same, robust way instead of
each carrying a copy that can drift.

WHY THIS EXISTS — the 2026-07-21 hagstofa / statfin freeze
---------------------------------------------------------
The per-source parsers historically chose the obs_date axis by NAME first ("is this
dim called tid / year / ar / month / kuukausi / …?") and only value-parsed when NO
name matched. That is wrong for any cube carrying BOTH a month axis and a year axis
(extremely common: Ár+Mánuður, Vuosi+Kuukausi): the month dimension is coded as
bare indices ('0'..'11') that parse to NO date, yet it *name*-matches, and when it
precedes the year axis it is picked — so every observation's date is unparseable,
the table drops to 0 rows, merge_and_write's never-shrink guard freezes the
previously-published data, and the daily run reports a false "structural break".
Hagstofa (37 tables) and StatFin (5) froze in exactly this way; even the most
evolved copy in the tree (jobs/ingest_pxweb.py) still had the name-first defect.

THE ROBUST RULE (value-first, used by every PxWeb parser now)
-------------------------------------------------------------
  1. AUTHORITATIVE  — the PxWeb metadata `time: true` flag (meta_time_code) or the
                      JSON-stat2 `role.time`. When present it is always correct.
  2. VALUE-DRIVEN   — else pick the dimension whose category CODES actually parse to
                      sane dates (highest parse-rate). A month axis coded 0..11
                      scores 0 and can never beat a real year axis again.
  3. NAME (last)    — only if NO dimension's values parse as dates do we fall back to
                      a literal name match (a genuinely date-less table stays 0).

Because step 2 reads the DATA, not the labels, it is robust to new tables, upstream
re-orderings, and localisation — the failure mode that froze hagstofa cannot recur.

DESIGN NOTE — byte-identical migration
--------------------------------------
`resolve_time_dim` takes the CALLER's own `parse_fn`, so a source keeps its exact
date grammar; only the *selection* is unified. On every table that currently parses
(one time axis, or the correct axis already chosen) this returns the same dimension
the old code did, so migrating a source is a no-op on its working data — proven by
tools/pxweb_regression.py, which replays every cached table old-vs-new and asserts
identical (series_key, obs_date, value) on all non-empty tables before any source is
migrated. Only currently-0-row (frozen / never-landed) tables change.
"""
from __future__ import annotations

import datetime as _dt
import re as _re

# Codes that literally name a PxWeb time dimension, across the Nordic/Baltic/DACH
# NSOs we ingest. Used ONLY as the step-3 last resort and as a tie-breaker among
# equally date-parseable candidates — never to override a value-parse decision.
TIME_CODES = frozenset({
    # generic
    "tid", "time", "aika", "date", "datum", "period", "periode",
    "datums", "periods",                                    # lv
    # year
    "year", "ar", "år", "ár", "aar", "aasta", "vuosi", "leto", "jahr", "gads",
    "année", "año",
    "gadi", "gadu",                                         # lv (plural/genitive)
    # month
    "month", "manad", "månad", "maaned", "måned", "maned", "manudur", "mánuður",
    "kuukausi", "kk", "mesec", "kuu", "monat", "kuupaev", "kuupäev", "año_mes",
    "menesis", "menesi", "manesis",                         # lv
    # quarter / half
    "quarter", "kvartal", "kvartaal", "quartal", "neljannes", "neljännes",
    "kausi", "half", "arsfjordungur", "ársfjórðungur", "ceturksnis",
    "cetrtletje", "četrtletje",
    "kvartals", "kvartalls",                                # lv
    # week
    "week", "uke", "vecka", "vika", "woche", "uge", "nadal", "nädal", "viikko",
    "nedela",                                               # lv
    # other-language variants
    "tími", "timi", "periood",
    # StatFin explicit period-role codes
    "timeperiod_y", "timeperiod_m", "timeperiod_q",
})
# INVARIANT: this MUST stay a strict superset of every per-source list in the tree
# (jobs/ingest_{hagstofa,pxweb,scb,ssb,statfin,dst,bfs,stat_estonia,stat_slovenia}.py
# + fetcher copies). A name dropped here is exactly what froze hagstofa MAN00000
# (missing Icelandic "ár"). tools/pxweb_regression.py asserts set-difference == empty.


def parse_period(s: str) -> "_dt.date | None":
    """Parse a PxWeb period CODE to a date. Returns None for anything that is not a
    real, in-range time code, so non-time numeric category codes (municipality ids,
    8-digit ContentsCode values, positional indices 0..11, sentinels like 9999/2584)
    do NOT parse — which is exactly what lets the value-driven resolver reject them.

    Grammar (union of every source's copy): YYYY, YYYYMmm, YYYY-MM, YYYYQq / YYYYKk,
    YYYYHh, YYYYWww, YYYY-MM-DD, and 6-digit YYYYMM (month 01..12 only)."""
    s = (s or "").strip()
    try:
        if _re.match(r"^\d{4}$", s):
            return _dt.date(int(s), 12, 31)
        m = _re.match(r"^(\d{4})M(\d{2})$", s, _re.IGNORECASE)
        if m:
            mo = int(m.group(2))
            if 1 <= mo <= 12:
                return _dt.date(int(m.group(1)), mo, 1)
            return None
        m = _re.match(r"^(\d{4})-(\d{2})$", s)
        if m:
            mo = int(m.group(2))
            if 1 <= mo <= 12:
                return _dt.date(int(m.group(1)), mo, 1)
            return None
        m = _re.match(r"^(\d{4})[QK](\d)$", s, _re.IGNORECASE)
        if m:
            q = int(m.group(2))
            if 1 <= q <= 4:
                return _dt.date(int(m.group(1)), (q - 1) * 3 + 1, 1)
            return None
        m = _re.match(r"^(\d{4})H(\d)$", s, _re.IGNORECASE)
        if m:
            h = m.group(2)
            if h in ("1", "2"):
                return _dt.date(int(m.group(1)), 1 if h == "1" else 7, 1)
            return None
        m = _re.match(r"^(\d{4})W(\d{2})$", s, _re.IGNORECASE)
        if m:
            wk = int(m.group(2))
            if 1 <= wk <= 53:
                return _dt.date.fromisocalendar(int(m.group(1)), wk, 1)
            return None
        if _re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return _dt.date.fromisoformat(s)
        m = _re.match(r"^(\d{4})(\d{2})$", s)   # bare YYYYMM
        if m and 1 <= int(m.group(2)) <= 12:
            return _dt.date(int(m.group(1)), int(m.group(2)), 1)
    except (ValueError, TypeError):
        pass
    return None


def date_parse_rate(codes, parse_fn=parse_period, *, sane_lo=1500, sane_hi=2100):
    """Fraction of `codes` that parse to a sane-range date via `parse_fn`.
    A month axis ('0'..'11') scores 0.0; a year axis ('1949','1950',…) scores 1.0.
    sane_lo=1500 so genuine historical statistical axes (Statistics Iceland's
    population series runs to 1703) score as dates; municipality/category codes that
    happen to be 4 digits are too sparse inside [1500,2100] to reach min_rate (verified
    adversarially: SCB Region codes scored 0.08). The regression harness re-confirms
    that lowering this floor changes NO currently-parsing table's chosen axis."""
    clean = [c for c in codes if c not in (None, "")]
    if not clean:
        return 0.0
    good = 0
    for c in clean:
        d = parse_fn(str(c))
        if d is not None and sane_lo <= d.year <= sane_hi:
            good += 1
    return good / len(clean)


def resolve_time_dim(dim_ids, dim_codes, *, meta_time_code=None, role_time=None,
                     parse_fn=parse_period, sane_lo=1500, sane_hi=2100,
                     min_rate=0.6):
    """Return the INDEX (into dim_ids) of the time dimension, or None if the cube has
    no date axis at all.

    dim_ids        : ordered dimension ids (JSON-stat2 `id`).
    dim_codes      : dim_codes[i] = the ordered category CODES for dim_ids[i].
    meta_time_code : the PxWeb metadata `time: true` variable code, if known (authoritative).
    role_time      : JSON-stat2 `role.time` list, if present (authoritative).
    parse_fn       : the caller's period parser — keeps each source's exact grammar so a
                     migration is byte-identical on working tables.

    Precedence: authoritative flag -> highest date-parse-rate (>= min_rate) with a
    name-match tie-break -> literal name match as a last resort. A name match on an
    index-coded axis can NEVER outrank an axis whose values are real dates."""
    # 1. AUTHORITATIVE — the upstream told us which axis is time.
    auth = meta_time_code
    if auth is None and role_time:
        auth = role_time[0] if isinstance(role_time, (list, tuple)) else role_time
    if auth is not None:
        for i, did in enumerate(dim_ids):
            if did == auth:
                return i

    # 2. VALUE-DRIVEN — the axis whose codes actually parse as dates wins.
    best_i, best_rate, best_named = None, 0.0, False
    for i, did in enumerate(dim_ids):
        codes = dim_codes[i] if i < len(dim_codes) else []
        rate = date_parse_rate(codes, parse_fn, sane_lo=sane_lo, sane_hi=sane_hi)
        if rate < min_rate:
            continue
        named = str(did).strip().lower() in TIME_CODES
        # strictly higher rate wins; on a tie a named axis wins; else keep the earlier dim
        if rate > best_rate or (rate == best_rate and named and not best_named):
            best_i, best_rate, best_named = i, rate, named
    if best_i is not None:
        return best_i

    # 3. LAST RESORT — no axis parses as dates; fall back to a literal name match. Such
    #    a table is genuinely date-less or index-coded and will legitimately yield 0 rows,
    #    which the caller must classify as EMPTY (never-landed), not a structural break.
    for i, did in enumerate(dim_ids):
        if str(did).strip().lower() in TIME_CODES:
            return i
    return None


def role_time_of(jsonstat: dict):
    """The JSON-stat2 `role.time` list from a response body (authoritative when present)."""
    try:
        rt = jsonstat.get("role", {}).get("time")
        if isinstance(rt, (list, tuple)) and rt:
            return list(rt)
    except AttributeError:
        pass
    return None
