"""Strategy contract + Unit/Result types shared by all adapters.

A Strategy answers three questions about a refresh Unit:
  is_due()        -> has enough time elapsed (cadence) to bother checking?
  detect_change() -> did upstream actually move? (returns a new vintage token or None)
  run()           -> fetch the delta/refresh, publish atomically, return a Result.

The orchestrator only ever runs a unit when is_due() AND (detect_change() or forced).
A unit is marked `ok` only after run() reports a successful atomic publish.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

# A NON-FAILURE tail a fetcher may join onto its result error ("; not hosted note: ..."): tables
# the publisher lists but serves no data for, named for the reader. health._deferral_only strips it
# like a 'csv coverage note:' tail, so it never turns a pure deferral pass into ATTENTION (R1121).
NOT_HOSTED_NOTE = "not hosted note:"
# The same kind of tail for where a rotation stands (tables owed / never fetched) - ksh_stadat, R1123.
ROTATION_NOTE = "rotation note:"
# The same kind of tail for what a pass's merge did, counted from a whole-row compare: changed / added /
# held rows per table (stat_estonia, R1830's shape). A refusal or a defect is NEVER only in this note:
# the fetcher also books it transient, which keeps the source in ATTENTION.
MERGE_NOTE = "merge note:"
NON_FAILURE_NOTES = ("csv coverage note:", NOT_HOSTED_NOTE, ROTATION_NOTE, MERGE_NOTE,
                     "probe note:")      # = PROBE_NOTE (defined with the probe stamp, below)

# nominal days between upstream updates per cadence (0.9 slack applied in cadence_due)
CADENCE_DAYS = {
    "daily": 1, "weekly": 7, "monthly": 28, "quarterly": 90,
    "annual": 365, "irregular": 7, "static": 10 ** 6,
}

# How long a unit that ended PARTIAL waits before the whole pass is attempted again.
# See Strategy.is_due for why a partial has to count for scheduling at all. Capped rather
# than using the unit's own cadence so a source whose sub-units failed TRANSIENTLY is not
# stranded for up to a month, while a source whose failures are permanent still stops
# re-running its full cost every single day.
PARTIAL_RETRY_DAYS = 7

# finalize() stamps this placeholder as new_vintage on every Result (_common.finalize), so it is what
# a unit stores when its fetcher names no release token. It is NOT a release token and must never be
# one: a probe that returns it compares EQUAL to the stored placeholder, detect_change() answers
# "unchanged", and the orchestrator books an earned no_change with no fetch - advancing last_success
# for ever while nothing is fetched. who_hwf/who_rs/who_sdg sealed themselves this way from 2026-08
# (their probe returned the literal). probe_token() turns it into None ("cannot tell") so the probe
# strategies fetch instead.
DATE_TAIL = "date-tail"


def probe_token(cur):
    """A probed vintage as detect_change() may compare it: the date-tail placeholder reads as None."""
    return None if cur == DATE_TAIL else cur


# ---- the pre-run probe stamp: OPT-IN per fetcher (2026-11-19) ------------------------------------------
#
# finalize() stamps DATE_TAIL on every Result, so a probe-gated unit whose fetcher names no token stores the
# placeholder and is fetched at every due tick. Stamping the pre-run probe instead would let an unchanged
# release be skipped - but only where the probe provably moves on every change the fetch would take AND the
# fetch provably takes what the probe saw. The review of a default-on stamp found probes that see a newer
# release than the fetch ingests (a cached zip), probes that cover 1 of 21 or 1 of 23 fetched files, and
# probes of a different URL than the one fetched; for those the placeholder is the only backstop. So:
#
#   STAMP_PROBE = True     a fetcher module sets this to have its pre-run probe stored on a clean pass.
#                          Absent or False (the default): exactly the old behaviour. NO unit sets it yet;
#                          a fetcher is admitted only on shadow evidence (the probe note below): every run
#                          that added or revised rows coincided with a moved probe token (R1154).
#   STAMP_MAX_AGE_DAYS     for an admitted fetcher, a stamp older than this forces a real fetch although the
#                          probe has not moved (the bis.py / fhfa.py MAX_AGE_DAYS backstop). Default below.
#
# An admitted stamp is stored as "<token>#fetched=<YYYY-MM-DD>": the date of the fetch that took it, which an
# earned no_change does not move (it never writes upstream_vintage), so the age is measured from the last real
# fetch. orchestrate.run_once compares the token part (stamp_token).
STAMP_MAX_AGE_DAYS = 30
STAMP_SEP = "#fetched="

# A fetcher's update() returns this as new_vintage to say "store NO vintage for this pass, not even the
# probe": its pass returned ok/no_change while work is still owed (ons_uk's capped rotation). It never
# reaches the state: orchestrate.run_once stores None for it, so the next due tick fetches. A plain None
# could not say it: the probe strategies fill a None with the probe token.
NO_STAMP = "no-stamp"

# The statuses after which a stored vintage may gate a later due pass. After any other one (partial,
# transient_fail, ...) the next due tick fetches whatever the probe says: run_once keeps the OLD vintage on a
# not-clean pass, and a probe that returns to that value (a replica's ETag, a gh:/http: token kind switch on
# a GitHub 403) would otherwise book an earned no_change over the unfinished pass.
CLEAN_STATUSES = ("ok", "no_change")

# The prefix of the shadow note run_once appends to the runs row of every full pass of a probe strategy:
# "probe note: <the pre-run token>" (or "none"). health strips it before reading a note (NON_FAILURE_NOTES,
# health.read_runs), so it changes no gate, alarm or digest line.
PROBE_NOTE = "probe note:"


def stamps_probe(fetcher) -> bool:
    return getattr(fetcher, "STAMP_PROBE", False) is True


def release_token(cur):
    """The probed vintage as a value an ADMITTED fetcher may store, or None: no probe, the DATE_TAIL
    placeholder, NO_STAMP, or a bare integer (http_vintage's Content-Length fallback - a byte count does not
    move on a same-size revision)."""
    tok = probe_token(cur)
    if tok is None or tok == NO_STAMP or str(tok).strip().isdigit():
        return None
    return tok


def split_stamp(stored):
    """(token, fetched date or None) of a stored vintage; a vintage without the suffix has no date."""
    s = "" if stored is None else str(stored)
    if STAMP_SEP in s:
        tok, _, day = s.rpartition(STAMP_SEP)
        try:
            return tok, date.fromisoformat(day)
        except ValueError:
            return s, None
    return stored, None


def stamp_token(stored):
    """The token part of a stored vintage (itself when it carries no fetch date)."""
    return split_stamp(stored)[0]


def _today() -> date:
    return datetime.now(timezone.utc).date()


@dataclass
class Unit:
    """An atomic refresh chunk: a flow, cube, dataset, endpoint, survey, or whole source."""
    source_id: str
    unit_id: str
    strategy: str
    cadence: str = "monthly"
    out_paths: list[str] = field(default_factory=list)   # parquet path(s) this unit owns
    config: dict = field(default_factory=dict)           # strategy-specific: url, since_param, rate, key_env...

    @property
    def key(self) -> str:
        return f"{self.source_id}/{self.unit_id}"


@dataclass
class Result:
    status: str                       # ok | partial | transient_fail | definitive_fail | no_change
    obs: int = 0
    new_vintage: str | None = None
    last_obs_date: str | None = None
    error: str | None = None
    # optional per-series {series_key: last_obs_date} so the orchestrator can persist
    # per-series freshness (a frozen series can't hide behind a unit-level max).
    series_cursors: dict | None = None
    # OPTIONAL merge-measured changed set: {series_key: max changed obs_date or None}
    # from merge_and_write(report_changed_keys=True). When present (not None), §5.7
    # derives CSVs from THIS instead of series_cursors.keys() — the two answer
    # different questions and the audit of 2026-08-31 found series_cursors serving
    # three contradictory contracts at once (health frontier / changed set / cursor
    # state): the seeding that health NEEDS (every on-disk flow, every run) makes the
    # changed-set reading over-report (ecb changed==attempted 25/25), while max-date
    # cursors under-report same-period revisions. The merge is the only place that
    # knows what actually changed. None (the default) = the fetcher has not migrated;
    # behaviour is exactly as before. An EMPTY dict is a real statement ("nothing
    # changed") and is honoured. Keys are STORE grain; for a source whose catalogue is
    # series-grain they map exactly (norgesbank, the pilot).
    changed_keys: dict | None = None
    # TRUNCATED-EVIDENCE FLAG (WU-2b): True when the fetcher KNOWS its reported
    # cursor set hit CURSOR_CAP, i.e. the changed-set evidence is incomplete by
    # construction. The orchestrator answers by booking full_rederive_owed — the
    # catalogued ids beyond the cap would otherwise starve SILENTLY (fhfa: a
    # rebuild changes all 89,706 catalogued series against a 50,000 cap, leaving
    # 39,706 CSVs that never re-derive via §5.7, with no note and no demotion).
    # False (default) = evidence complete or fetcher un-migrated; nothing changes.
    cursor_cap_hit: bool = False
    # ROWS ACTUALLY MERGED THIS RUN, and ONLY when the fetcher can prove it. None (the default)
    # means "not reported" and behaves exactly as before.
    #
    # WHY THIS IS NOT `obs`. `finalize`'s second parameter is named `total_rows` and about 120
    # of ~123 call sites pass the STORE'S TOTAL - `blob.row_count(path)` - so `obs` says how big
    # the store is, not what a run did. Thirteen fetchers additionally substitute the whole-store
    # total when a run merged nothing, which is how idb was reported as "merged 15,066,444 obs"
    # on runs that merged zero, three times, with the same number each time.
    #
    # WHY NOT `tally.added` EITHER. It carries two conventions in production - net new rows
    # (`added_unit(max(0, n - before))`) in ~40 fetchers, rows PARSED in _giant, noaa, usda, abs,
    # ecb, bcb, insee_melodi and istat, several deliberately so they do not trip the all-empty
    # structural floor. And under the first convention a SAME-PERIOD REVISION collapses old and
    # new into one row carrying the new value (merge.py:186-190, "new wins"), so the count is
    # unchanged while the served value changed. Inferring "nothing merged" from it would silence
    # a genuine coherence violation.
    #
    # So this is an AFFIRMATIVE CLAIM, not an inference: a fetcher sets it only where it knows.
    merged_rows: int | None = None
    # WINDOW STOP (2026-11-02, admission for window-stoppable units). Set ONLY by a fetcher whose registry
    # entry declares `window_stop: true`, on a pass that its own run-ceiling check ended:
    #   "progress" - the pass made durable progress before it stopped (a request completed, a country was
    #                stamped, a slice kept, a chunk spilled, a merge landed): recorded as today.
    #   "none"     - it stopped before ANY durable progress. orchestrate.run_once then records NO unit
    #                state for it (no status, no last_attempt), so the unit does not land on the partial
    #                clock; it logs a `window_stopped` runs row instead (which moves its ORDER key back,
    #                orchestrate.declared_last_turn_utc, and no cost estimate reads). Cursors,
    #                changed keys or merged rows on the Result override "none" (fail toward recording).
    # None (default) = not a window stop, or a fetcher that does not declare one: nothing changes.
    window_stop: str | None = None
    # BUILD RECEIPT (review AR-477 F4). Set only by a fetcher whose store records a build that the STATE must
    # confirm (fhfa: the build id in its sidecar). run_once writes it to state.db's build_receipt AFTER the
    # pass's CSV bookings, on an ok or partial pass, so a later run sees it only if those bookings were kept.
    # It is not upstream_vintage: a partial does not advance that, although its bookings are in the state.
    # None (default) = nothing is written.
    build_receipt: str | None = None


def cadence_due(cadence: str, last_success_utc: str | None, now: datetime | None = None) -> bool:
    if not last_success_utc:
        return True
    now = now or datetime.now(timezone.utc)
    try:
        last = datetime.fromisoformat(last_success_utc)
    except Exception:
        return True
    days = CADENCE_DAYS.get(cadence, 7)
    return (now - last).total_seconds() >= days * 86400 * 0.9


def _due_after(days: float, stamp: str | None, now: datetime | None = None) -> bool:
    """Has `days` elapsed since `stamp`? Same 0.9 slack as cadence_due.

    Split out so the partial-retry path can use an explicit number of days rather than a
    cadence name; an unparseable or missing stamp means due, never silently never-due.
    """
    if not stamp:
        return True
    now = now or datetime.now(timezone.utc)
    try:
        last = datetime.fromisoformat(stamp)
    except Exception:                                        # noqa: BLE001
        return True
    return (now - last).total_seconds() >= days * 86400 * 0.9


class Strategy(ABC):
    name: str = "base"

    def is_due(self, unit: Unit, unit_state: dict | None, now: datetime | None = None) -> bool:
        """Has enough time elapsed to bother checking this unit again?

        A PARTIAL PASS STILL COUNTS AS A PASS, FOR SCHEDULING ONLY (2026-07-31).

        This used to key solely on last_success_utc, and a `partial` deliberately never sets
        that — correctly, because a partial is not a success and the SLA/health gate must
        keep saying so. But scheduling drew the wrong conclusion from it: a source whose
        partial is PERMANENT (upstream retired some tables, a subset can never parse) has
        last_success NULL forever, so cadence_due returns True forever, so it re-runs its
        ENTIRE cost on every single run no matter what cadence it declares.

        Measured on production state 2026-07-31: 32 of 103 units had never recorded a
        success, and their typical durations summed to 1,303 minutes against a 240-minute
        budget — unsdg 351 min, ssb 231, statfin 138, insee_bdm 105, stat_estonia 103,
        hagstofa 64. The run therefore spent its whole budget on whoever sorted first and
        never reached the rest, every day. hagstofa declares cadence `monthly` and was being
        re-crawled — 1,906 tables, ~55 min — on every run it was offered.

        So: when a unit has never succeeded but its last attempt was a PARTIAL, the cadence
        is measured from that attempt. A partial means the pass RAN and some sub-units
        failed; repeating it tomorrow cannot help if those failures are permanent.

        `transient_fail` is deliberately NOT included: it means the pass did not finish - it
        could not run at all (5xx, network), or the hard per-unit limit ended it - so it must
        stay immediately retryable. Since PR #151 a transient_fail can also have merged rows and
        derived their CSVs: _giant.run_giant returns transient_fail WITH its changed set when
        the limit ends a sweep that had merged changed flows (orchestrate._should_derive_csvs),
        and for a capped pass (more flows selected than its cap) when BOTH changed or new flows
        remain beyond the cap AND the pass merged at least one changed flow itself; a capped
        pass that merged nothing (all its flows failed) stays partial. That pass is still
        unfinished, and it must stay due at the next run like any other transient_fail. Do not
        move it to the partial clock.

        Nothing here changes what is REPORTED. last_success stays unset, the source still
        reads as stale to the health gate and to /v1/last-updates, and `--force` still
        overrides. This is only about how often we pay the cost of trying again.
        """
        st = unit_state or {}
        ls = st.get("last_success_utc")
        if ls:
            return cadence_due(unit.cadence, ls, now)
        if st.get("status") == "partial" and st.get("last_attempt_utc"):
            # CAPPED AT PARTIAL_RETRY_DAYS, not the full cadence. A partial can be caused by
            # PERMANENT sub-unit failures (upstream retired a table -> re-running sooner
            # cannot help) or by TRANSIENT ones (insee_bdm's last pass had 201/201 sub-units
            # transient-fail -> re-running sooner is exactly what should happen). The state
            # does not distinguish them structurally, so waiting a source's full cadence
            # would strand a recoverable source for up to a month, while waiting a day is
            # what caused the pathology. The cap splits the difference: far cheaper than
            # daily, far more responsive than monthly.
            return _due_after(min(CADENCE_DAYS.get(unit.cadence, 7), PARTIAL_RETRY_DAYS),
                              st.get("last_attempt_utc"), now)
        return True

    def _probe_matches(self, fetcher, cur, unit_state) -> bool:
        """May the probe strategies skip this unit as unchanged? (They also need cadence: is_due.)

        * a probe and a stored vintage, and the probe equals it - for an ADMITTED fetcher (STAMP_PROBE) the
          token part of its stamp, and only while the stamp is younger than its max age;
        * and the unit's last recorded pass was clean (CLEAN_STATUSES). A unit with no recorded status
          (a fresh row) is not refused on that ground.
        """
        us = unit_state or {}
        stored = us.get("upstream_vintage")
        if cur is None or stored is None:
            return False
        st = us.get("status")
        if st is not None and st not in CLEAN_STATUSES:
            return False
        if stamps_probe(fetcher):
            tok, fetched = split_stamp(stored)
            if fetched is None or tok != cur:
                return False
            max_age = getattr(fetcher, "STAMP_MAX_AGE_DAYS", STAMP_MAX_AGE_DAYS)
            return (_today() - fetched).days < max_age
        return cur == stored

    def _carry_probe(self, r: Result, fetcher=None) -> Result:
        """Hand the PRE-RUN probe (self._cur, read by detect_change before the pull) to the orchestrator.

        Never re-probe here: a release that lands during the pull must still read as changed at a later
        due pass (R1154, R1283 M2). Whether the value is STORED stays the orchestrator's decision: only a clean
        pass advances upstream_vintage.
          * NO_STAMP passes through untouched (it is neither None nor DATE_TAIL, so no branch below fills it);
          * a token the fetcher set itself wins;
          * an ADMITTED fetcher (STAMP_PROBE) whose Result carries None or finalize's DATE_TAIL gets
            "<probe>#fetched=<today>", when the probe is a usable token (release_token);
          * any other fetcher: exactly the old fill - a None is filled with the probe, DATE_TAIL is left.
        """
        if r is None:
            return r
        cur = getattr(self, "_cur", None)
        if fetcher is not None and stamps_probe(fetcher):
            if r.new_vintage in (None, DATE_TAIL):
                tok = release_token(cur)
                if tok is not None:
                    r.new_vintage = f"{tok}{STAMP_SEP}{_today().isoformat()}"
            return r
        if r.new_vintage is None and cur:
            r.new_vintage = cur
        return r

    @abstractmethod
    def detect_change(self, unit: Unit, unit_state: dict | None) -> str | None:
        """Return a new upstream vintage token if changed (or an always-fetch sentinel
        for cheap date-tail sources), else None to skip."""

    @abstractmethod
    def run(self, unit: Unit, since: str | None) -> Result:
        """Fetch the delta/refresh, publish atomically via merge.merge_and_write,
        and return a Result. Must obey the Transient/Definitive failure contract."""
