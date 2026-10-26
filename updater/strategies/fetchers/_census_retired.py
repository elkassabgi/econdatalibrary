"""Census datasets the PUBLISHER retires, as ONE dated list that every census code path reads.

WHY A LIST, AND WHY HERE. The Census Bureau removes its 18 BETA International Trade time-series
datasets from api.census.gov on 2026-11-19. We store 8 of them (the end-use, hi-tech, SITC and
USDA world totals) and serve 144 catalogue ids from them. Two code paths can call those endpoints:

  * updater/strategies/fetchers/census.py   `_flows()` tails every stored intltrade parquet;
  * jobs/_census_intltrade_full.py          the full re-ingest; its default path takes all 36
                                            intltrade datasets, `--beta` takes the 8 in BETA_OK.

After the removal date a request to a removed endpoint is a non-200 answer. In the fetcher that
is a DefinitiveError for the WHOLE census source every run (finalize raises, the orchestrator
records `partial` with no Result: no cursors, no CSV derive for the 37 flows that still work).
So both paths read this one module, and a flow listed here is skipped LOUDLY, by name.

WHAT IT DOES NOT DO. It deletes nothing and changes no id. The parquets, the 144 catalogue ids and
the served objects stay as they are; the owner's decision (2026-10-26) is to keep the 144 series
served and frozen, after one refresh pass while the endpoints still answer.

WHEN IT TAKES EFFECT - the switch. An entry is excluded from the EARLIER of two dates:

  1. its removal date (2026-11-19). The safety net: on that day the endpoint is gone, so a run
     that still asked would fail the whole source. Nothing has to be remembered for this.
  2. the OPERATOR's activation date, set AFTER the one refresh pass has been proven (see the PR's
     post-merge proof). Either a one-line reviewed commit setting OPERATOR_ACTIVATED_ON below
     (durable, the preferred way), or the environment variable CENSUS_RETIRED_UPSTREAM_FROM=YYYY-MM-DD
     on the workstation (same day, no deploy). The environment wins when both are set.

The order matters and is the reason for the second date. The owner wants the 8 BETA flows tailed
by ONE green census pass first, so that their stored 2025-12 values (10,158 rows that differ from
the stable twins' revised values, measured on the local mirror 2026-10-26) are the revised ones
before they freeze. A switch that bit at merge time would freeze the pre-revision values for ever;
a switch that waited only for the removal date would keep fetching flows the operator has already
finished with. A malformed environment value is reported loudly and IGNORED, so the removal date
still applies: it can never move the exclusion later than the day the endpoints disappear.

"Today" is the UTC date.
"""
from __future__ import annotations

import datetime as dt
import os

ENV_ACTIVATE = "CENSUS_RETIRED_UPSTREAM_FROM"

# Set by a reviewed one-line commit once the refresh pass is proven. None = not activated: the
# entries then take effect on their removal date only.
OPERATOR_ACTIVATED_ON: dt.date | None = None

_BETA_REMOVAL = dt.date(2026, 11, 19)
_BETA_NOTICE = ("Census Bureau removes the 18 BETA International Trade time-series datasets "
                "from api.census.gov on 2026-11-19 (date recorded 2026-10-26; data.json lists "
                "them with a 'BETA' title and carries no removal date)")

# flow path (no 'timeseries/' prefix) -> (removal date, notice). All 18 BETA datasets, not only
# the 8 we store: the full re-ingest job can reach any of them.
RETIRED_UPSTREAM: dict[str, tuple[dt.date, str]] = {
    f"intltrade/{p}": (_BETA_REMOVAL, _BETA_NOTICE) for p in (
        "exports/enduseexport", "exports/hitechexport", "exports/hsexport",
        "exports/naicsexport", "exports/porthsexport", "exports/sitcexport",
        "exports/statehsexport", "exports/statenaicsexport", "exports/usdaexport",
        "imports/enduseimport", "imports/hitechimport", "imports/hsimport",
        "imports/naicsimport", "imports/porthsimport", "imports/sitcimport",
        "imports/statehsimport", "imports/statenaicsimport", "imports/usdaimport",
    )
}


def flow_id(path: str) -> str:
    """'timeseries/intltrade/exports/hs' or 'intltrade/exports/hs' -> 'intltrade/exports/hs'."""
    p = path.strip().strip("/")
    return p[len("timeseries/"):] if p.startswith("timeseries/") else p


def today_utc() -> dt.date:
    return dt.datetime.now(dt.timezone.utc).date()


def operator_date(environ=None) -> dt.date | None:
    """The operator's activation date: the environment variable, else OPERATOR_ACTIVATED_ON."""
    env = os.environ if environ is None else environ
    raw = (env.get(ENV_ACTIVATE) or "").strip()
    if raw:
        try:
            return dt.date.fromisoformat(raw)
        except ValueError:
            print(f"[census] WARNING: {ENV_ACTIVATE}={raw!r} is not an ISO date (YYYY-MM-DD) - "
                  f"IGNORED; retired-upstream flows take effect on their removal date only",
                  flush=True)
    return OPERATOR_ACTIVATED_ON


def _effective(path: str, op: dt.date | None) -> dt.date | None:
    entry = RETIRED_UPSTREAM.get(flow_id(path))
    if entry is None:
        return None
    removal = entry[0]
    return min(removal, op) if op is not None else removal


def effective_from(path: str, environ=None) -> dt.date | None:
    """The first day `path` is excluded, or None when it is not listed."""
    return _effective(path, operator_date(environ))


def split_retired(paths, today: dt.date | None = None, environ=None):
    """(kept, excluded) for a list of flow paths. Order of `kept` is preserved.

    excluded is a list of (path, effective date, notice). Paths keep the caller's own spelling, so
    the job's 'timeseries/...' paths come back as given. The operator date is read ONCE per call,
    so a malformed value is reported once, not once per flow.
    """
    day = today or today_utc()
    op = operator_date(environ)
    kept, excluded = [], []
    for p in paths:
        eff = _effective(p, op)
        if eff is not None and day >= eff:
            excluded.append((p, eff, RETIRED_UPSTREAM[flow_id(p)][1]))
        else:
            kept.append(p)
    return kept, excluded


def exclusion_line(prefix: str, path: str, eff: dt.date, notice: str) -> str:
    """The loud per-flow line both callers print."""
    return (f"{prefix} {flow_id(path)}: RETIRED UPSTREAM - not fetched (excluded from "
            f"{eff.isoformat()}). {notice}. Stored rows, catalogue ids and served objects are "
            f"kept unchanged.")
