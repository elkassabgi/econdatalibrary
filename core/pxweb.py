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
        # DAILY: "2010M01D01" (PxWeb TLIST(D1)). Measured on CSO 2026-08-03: MTD05
        # ("Precipitation Amount") and MTH05 each publish ~6,025 of these; the endpoint returns
        # HTTP 200 with 557,685 bytes of real data and EVERY row was dropped, because the
        # grammar had no daily case. The source then reported it as a network failure.
        m = _re.match(r"^(\d{4})M(\d{2})D(\d{2})$", s, _re.IGNORECASE)
        if m:
            mo, dy = int(m.group(2)), int(m.group(3))
            if 1 <= mo <= 12 and 1 <= dy <= 31:
                return _dt.date(int(m.group(1)), mo, dy)   # ValueError -> None for 02-30 etc.
            return None
        # SPLIT / ACADEMIC YEAR: "2003-2004" (CSO EDA21, "Average Class Size in Mainstream
        # National Schools"). Dated to the year the period BEGINS, consistent with bare YYYY
        # mapping to that year's 31 Dec.
        #
        # The second year MUST be the first + 1. That is what an academic year is, and the
        # constraint is load-bearing rather than cosmetic: parse_period doubles as the DETECTOR
        # in date_parse_rate(), so a loose "^\d{4}-\d{4}$" would let an ordinary range label
        # ("1990-2000", a cohort or a footnoted span) parse as a date and could hand the
        # value-driven resolver a classification axis as the time axis — the exact defect the
        # two-pass resolver exists to prevent.
        m = _re.match(r"^(\d{4})-(\d{4})$", s)
        if m:
            y1, y2 = int(m.group(1)), int(m.group(2))
            if y2 == y1 + 1:
                return _dt.date(y1, 12, 31)
            return None
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
                     min_rate=0.6, dim_labels=None):
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
    # 1. AUTHORITATIVE — the upstream told us WHICH axis is time.
    #
    #    A publisher CAN mis-flag it. SURS marks `time: true` on the AGE axis of 05L1027S
    #    ("Deaths by COMPLETED YEAR / YEAR OF BIRTH"), whose codes are '1000' (labelled "Deaths -
    #    TOTAL"), '000' ("Age 0"), '001' ("Age 0, year of birth 2025")... '1000' parses to year
    #    1000 and three served rows were dated to it.
    #
    #    But refusing a flagged axis on its CODES alone is worse in two distinct ways, and both
    #    are load-bearing:
    #
    #      (a) POSITIONAL TIME CODES. Some tables index time as '0','1','2' and carry the period
    #          only in category.label (Hagstofa SJA01101: codes ['0','1',..] vs valueTexts
    #          ['2010','2011',..]). The parsers already fall back to labels — that fallback is
    #          what fixed hagstofa's 26 false "structural breaks", and tests/test_pxweb_time_
    #          labels.py gates it with a labels-stripped negative control. A codes-only check
    #          makes those axes look unreadable and kills it.
    #      (b) FALLING THROUGH to the value scan when the flagged axis will not parse hands the
    #          table to another dimension. scb's `Tid` held '2011-2012' and '2025V01', unreadable
    #          until R331, and the fall-through picked `Region` — municipality codes 0114..2584
    #          became years 114..2026 across 87,358 rows. The publisher was RIGHT about which
    #          axis was time; we were wrong about how to read it.
    #
    #    So: judge the flagged axis on CODES **or LABELS**, and when neither yields a sane date
    #    return None — never a different dimension.
    #
    #    BACKWARD COMPATIBLE BY CONSTRUCTION: a caller that does not pass dim_labels cannot be
    #    judged on labels, so it keeps the old unconditional behaviour exactly. Only callers that
    #    supply labels (and can therefore be judged fairly) get the stricter rule. 23 call sites
    #    exist; migrating them is opt-in, one at a time, each provable.
    auth = meta_time_code
    if auth is None and role_time:
        auth = role_time[0] if isinstance(role_time, (list, tuple)) else role_time
    if auth is not None:
        for i, did in enumerate(dim_ids):
            if did == auth:
                if dim_labels is None:
                    return i
                codes = dim_codes[i] if i < len(dim_codes) else []
                labels = dim_labels[i] if i < len(dim_labels) else []
                if (date_parse_rate(codes, parse_fn, sane_lo=sane_lo, sane_hi=sane_hi) > 0
                        or date_parse_rate(labels, parse_fn,
                                           sane_lo=sane_lo, sane_hi=sane_hi) > 0):
                    return i
                return None

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

    # 3. LAST RESORT — no axis parses as SANE dates; fall back to a literal name match.
    #
    # THIS BRANCH FABRICATED ~637,000 SERVED OBSERVATIONS, and the comment above used to claim
    # it could not. It said such a table "will legitimately yield 0 rows" — true only if
    # parse_fn REJECTS the codes. It does not: every source's parse_date turns ANY 4-digit
    # token into a year, so a zero-padded counter becomes a calendar.
    #
    #     parse_date('0001') -> 0001-12-31    parse_date('0114') -> 0114-12-31
    #     parse_date('9999') -> 9999-12-31
    #
    # And the names matched here are genuinely time-ish while their VALUES are not years:
    # `vecka` is Swedish for week, `manudur` Icelandic for month, `leto` Slovenian for year on
    # an axis that turned out to be index-coded. Measured on the live stores 2026-08-04:
    #
    #     stat_slovenia 05W   506,605 rows — one key holding years 1,2,3...6152, all at 12-31
    #     scb BE / HE          71,368 rows below year 1500 (DodaVeckaRegionCKM = deaths by WEEK)
    #     statfin tyonv        32,013 rows at 9999-12-31
    #     hagstofa Umhverfi     1,120 rows at 3005-12-31
    #
    # Step 2 already refuses every one of these — date_parse_rate on a padded counter is 0.0
    # inside [sane_lo, sane_hi] — and then this branch handed back the same axis on its NAME.
    # The sanity check existed and step 3 walked around it.
    #
    # So a name match must now also produce at least ONE sane date. The threshold is
    # deliberately "any", not min_rate: a real axis that mixes sane years with odd codes should
    # still resolve, and only an axis with NOTHING parseable is rejected. When nothing
    # qualifies we return None — which is exactly what the original comment promised, the
    # caller yields 0 rows and classifies the table EMPTY (never-landed), not a structural
    # break.
    for i, did in enumerate(dim_ids):
        if str(did).strip().lower() in TIME_CODES:
            codes = dim_codes[i] if i < len(dim_codes) else []
            if date_parse_rate(codes, parse_fn, sane_lo=sane_lo, sane_hi=sane_hi) > 0:
                return i
            # SAME-AXIS LABEL RESCUE (2026-08-31, hagstofa's 33 false structural breaks).
            # Unflagged POSITIONAL time axes exist: `Ár`/`Year`/`Mánuður` with codes
            # '0','1','2'… and the period only in valueTexts ('1971-1975', '2024') — the
            # publisher never sets `time: true` on them, so the authoritative branch's
            # label fallback (case (a) above) can never apply, and this branch refused
            # them on codes alone. Measured 2026-08-31: 20 of 20 recoverable tables from
            # hagstofa's standing "33/1170 structural" note are exactly this shape,
            # labels parsing at 0.65-1.00 — live tables (deaths to 2025, elections 2024)
            # booked as schema breaks on every run. The rescue mirrors the flagged
            # branch's rule and its safety shape: judge the NAME-MATCHED axis on codes
            # OR labels, and when neither parses return None — NEVER a different
            # dimension (the scb Region door, R331, stays shut: `Region` is not in
            # TIME_CODES and a non-named axis can never enter this loop). Gated on the
            # caller SUPPLYING dim_labels. MEASURED at ship (the reviewer's count, not
            # mine): 22 non-test callers, 19 label-less — those keep the old behaviour
            # byte-identically. TWO callers pass labels: hagstofa (the migration this
            # rescue exists for) and jobs/ingest_stat_slovenia.py:213, which therefore
            # INHERITS this rescue — reviewed as safe (SURS is code-coded; its FETCHER's
            # own parser is label-blind, so production slovenia is unchanged), but any
            # future dim_labels caller adopts this branch and should say so.
            if dim_labels is not None:
                labels = dim_labels[i] if i < len(dim_labels) else []
                if date_parse_rate(labels, parse_fn,
                                   sane_lo=sane_lo, sane_hi=sane_hi) > 0:
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
