"""Daily operator digest for the Aqueduct updater — the econ twin of the
hfdatalibrary morning email.

Runs as the LAST step of updater-daily.yml (even when earlier steps failed:
`if: always()`), reads the run's state.db off the runner, and emails a short
honest summary via Resend: per-source status, data frontiers, and anything
needing attention. Sender reuses the Resend-verified hfdatalibrary.com domain.

Honesty rules: the digest reports what the ledger says — counts and dates come
from unit_state rows written only after verified publishes; a red overall run
status is stated in the subject line, never softened. If RESEND_API_KEY is
absent the step prints a loud notice and exits 0 (email is an add-on, not a
gate — the health gate step is what turns runs red).
"""
from __future__ import annotations

import datetime as dt
import json
import datetime as _dt
import re as _re
import os
import sqlite3
import time as _time
import sys
import urllib.request

FROM = "Econ Data Library <noreply@hfdatalibrary.com>"
# Recipient comes from the environment (DIGEST_TO secret in CI) — never hardcode a
# personal address in a public repo. `or` (not a dict default) because an unset repo
# secret expands to an EMPTY STRING, which would otherwise send the digest to "".
TO = os.environ.get("DIGEST_TO") or "admin@hfdatalibrary.com"
STATE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                     "data", "_aqueduct", "state.db")



# An age means nothing without the cadence it is measured against. Measured 2026-09-03 across all
# 229 live sources: 6 are late by this rule, while the four OLDEST ages in the render (38.2 days —
# pwt, oxcgrt, barro_lee, gppd) are cadence `static` and perfectly fine. Before this, the digest's
# `tried=` column read as alarming exactly where nothing was wrong, and read as unremarkable for
# eia at 11.6 days, which is DAILY and four cycles late.
#
# Limits are deliberately generous: a source is late only after missing several of its own cycles,
# so a LATE marker is unarguable rather than borderline.
CADENCE_LIMIT_DAYS = {"daily": 3, "weekly": 14, "monthly": 45, "quarterly": 120, "annual": 400}




def late_label(cadence: str, run_location: str, ts, now) -> str:
    """The marker a digest row carries: '', ' LATE', or ' LATE(<route>)'.

    The route is named because it is the diagnosis more often than the source is. On 2026-09-03
    all 6 late sources in the fleet were on the local route — whose full sweep is ~17.7 days
    (R683) — and none were in the cloud. `eia` is declared DAILY and was 11.6 days stale. Reading
    "eia LATE" sends someone to eia; "eia LATE(local)" sends them to what is actually wrong.

    An absent `run_location` means "any" (the reading orchestrate.py uses), and "any" is not
    printed: labelling every row with the default would be noise, not signal.
    """
    if not is_late(cadence, ts, now):
        return ""
    where = run_location or "any"
    return " LATE" if where == "any" else f" LATE({where})"

def live_source_ids(entries) -> set:
    """The live tier, read EXACTLY as `registry.to_units()` reads it.

    `registry.load()["sources"]` hands back raw yaml entries and 15 of the 282 have no `live` key
    at all. `to_units` normalises with `bool(entry.get("live", False))`, so an absent flag means
    NOT live; a reader testing `e.get("live") is False` would classify those 15 as live and
    silently restore the noise this removes. One expression, one place — two interpretations of a
    single flag is how R676 and R685 happened.
    """
    return {e["source_id"] for e in entries if bool(e.get("live", False))}


# The first-pass crawlers (orchestrate.py FIRSTPASS_DIRS) are excluded from the normal run and
# have no unit_state row, so nothing reported them at all until 2026-09-03 — the day gus_dbw sat
# stalled for two hours and was found only by accident. Six hours is deliberately loose: cbs_nl
# was 0.01 h old and gus_dbw 1.99 h when this was written, so it alarms on a dead crawler without
# firing on a slow one.
FIRSTPASS_STALE_HOURS = 6.0


def firstpass_ages(store_root: str, names, now: float):
    """[(name, age_hours, newest_filename)] for those that exist, oldest first.

    The age is of the newest file of ANY type, not the newest parquet. gus_dbw writes a checkpoint
    continuously and parquet about every ten days; cbs_nl writes both constantly. A parquet-only
    signal reports gus_dbw as ten days dead while it is working normally — precisely the false
    alarm that trains a reader to skip the section.

    A directory that does not exist is OMITTED, not alarmed: it means no first pass is in flight
    (dbnomics has none — the domain is banned, R251).
    """
    out = []
    for name in sorted(names):
        d = os.path.join(store_root, name)
        if not os.path.isdir(d):
            continue
        newest, newest_name = 0.0, ""
        try:
            with os.scandir(d) as it:
                for e in it:
                    try:
                        if not e.is_file():
                            continue
                        m = e.stat().st_mtime
                    except OSError:
                        continue
                    if m > newest:
                        newest, newest_name = m, e.name
        except OSError:
            continue
        if newest:
            out.append((name, (now - newest) / 3600.0, newest_name))
    return sorted(out, key=lambda r: -r[1])

def is_late(cadence: str, ts, now) -> bool:
    """True when `ts` is older than the source's cadence allows.

    `irregular` and `static` declare NO expectation and are never late — marking them would
    recreate the false alarm this exists to remove. An absent or unparseable timestamp is also
    never late: not knowing when something last ran is a different condition from knowing it ran
    too long ago, and reporting it as lateness would be a guess wearing a measurement's clothes.
    """
    lim = CADENCE_LIMIT_DAYS.get(cadence or "")
    if not lim or not ts:
        return False
    try:
        t = _dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return False
    if t.tzinfo is None:
        t = t.replace(tzinfo=_dt.timezone.utc)
    return (now - t).total_seconds() / 86400.0 > lim

def main() -> None:
    key = os.environ.get("RESEND_API_KEY", "").strip()
    run_status = os.environ.get("RUN_STATUS", "unknown")   # ${{ job.status }} from the yml
    run_id = os.environ.get("GITHUB_RUN_ID", "local")

    con = sqlite3.connect(f"file:{STATE}?mode=ro", uri=True)
    rows = con.execute(
        "SELECT source_id, status, last_obs_date, last_success_utc, last_error, "
        "last_attempt_utc FROM unit_state ORDER BY source_id").fetchall()
    con.close()

    # unit_state accumulates a row for every source that has EVER run, including ones
    # since removed from the registry. Those leftovers kept their last status forever, so
    # the digest reported de-registered sources as failing every single day — norgesbank
    # and unsdg were both counted as `partial` while being entirely unmanaged (no registry
    # entry, hence never re-run and never able to recover). Scope the pass/fail verdict to
    # what we actually manage, but list the orphans explicitly rather than dropping them:
    # a silent filter would be the same mistake in the other direction.
    # ABSOLUTE FIRST, RELATIVE SECOND. The workflow runs this as `python updater/send_digest.py`
    # (updater-daily.yml), i.e. as a SCRIPT - so `__package__` is empty and `from . import
    # registry` raises ImportError, is swallowed by the except below, and `managed` becomes None.
    # The orphan filter has therefore never run on a scheduled digest, which is why
    # `fred_releases` - de-registered in July, unschedulable, last attempted 71 days ago - was
    # still being listed as needing attention every morning.
    #
    # The None fallback stays: a genuinely unreadable registry must report everything rather
    # than silently hide rows. It just must not be reached by an invocation style.
    managed = None
    try:
        import os as _os                                              # noqa: PLC0415
        import sys as _sys                                            # noqa: PLC0415
        _root = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
        if _root not in _sys.path:
            _sys.path.insert(0, _root)
        from updater import registry                                  # noqa: PLC0415
        _entries = registry.load().get("sources", [])
    except Exception:                                                 # noqa: BLE001
        try:
            from . import registry                                    # noqa: PLC0415
            _entries = registry.load().get("sources", [])
        except Exception:                                             # noqa: BLE001
            _entries = None     # registry genuinely unreadable -> report everything
    managed = {e["source_id"] for e in _entries} if _entries is not None else None
    # cadence comes from the SAME load, so a row can say whether its age is actually late
    cadence = {e["source_id"]: (e.get("cadence") or "") for e in (_entries or [])}
    # WHERE a late source runs is the first thing a reader needs: on 2026-09-03 all
    # 6 late sources were on the local route and no cloud source was late at all.
    route = {e["source_id"]: (e.get("run_location") or "any") for e in (_entries or [])}
    orphans = [r for r in rows if managed is not None and r[0] not in managed]
    if managed is not None:
        rows = [r for r in rows if r[0] in managed]

    # COMPUTED ONCE, rendered in BOTH bodies. The text body is a fallback almost nobody sees —
    # every HTML-capable client renders html_doc — so a section that exists only in `lines` has
    # not shipped. Computing it twice would be a second definition of one measurement (R676).
    try:
        from updater.orchestrate import FIRSTPASS_DIRS                # noqa: PLC0415
    except Exception:                                                 # noqa: BLE001
        FIRSTPASS_DIRS = None    # skip the section rather than report a remembered list
    fp = []
    if FIRSTPASS_DIRS:
        fp = firstpass_ages(
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "data", "clean_full"),
            FIRSTPASS_DIRS, _time.time())

    ok = [r for r in rows if r[1] in ("ok", "no_change")]
    warn = [r for r in rows if r[1] in ("partial", "transient_fail")]
    bad = [r for r in rows if r[1] not in ("ok", "no_change", "partial", "transient_fail")]

    # NOT IN THE LIVE TIER -> CANNOT IMPROVE. updater-daily.yml sets AQUEDUCT_LIVE_ONLY=1 and
    # orchestrate.py:1536 honours it, so a non-live source is never executed by the daily run and
    # its status is frozen. Measured 2026-09-03: 7 of 36 attention rows were such sources (bls,
    # census, imf_imts_direct, istat, oecd, owid, sipri_polity) — 19% of a list whose whole
    # purpose is to say what needs doing. Same shape as the unmanaged leftovers above, one layer
    # in: those had no registry entry, these have one and are simply not live.
    #
    # `bool(e.get("live", False))` is the EXACT expression registry.py:79 and health.py:507 use,
    # so an absent flag means not-live here too. Four of the seven have no `live` key at all, and
    # a second interpretation of the same field is how R676 and R685 happened.
    dormant = []
    if _entries is not None:
        _live_ids = live_source_ids(_entries)
        dormant = [r for r in warn + bad if r[0] not in _live_ids]
        warn = [r for r in warn if r[0] in _live_ids]
        bad = [r for r in bad if r[0] in _live_ids]

    healthy = run_status.lower() == "success" and not bad
    subject = (f"econdatalibrary daily: OK — {len(ok)} sources current"
               if healthy else
               f"econdatalibrary daily: ATTENTION — run={run_status}, "
               f"{len(bad)} failed, {len(warn)} retrying")

    feed = "https://econdl-api.elkassabgi.workers.dev/v1/last-updates"
    runlog = f"https://github.com/elkassabgi/econdatalibrary/actions/runs/{run_id}"

    def late_mark(source_id, ts) -> str:
        """' LATE', with the route when it is not the default, else ''.

        The route is the diagnosis more often than the source is: every late source measured on
        2026-09-03 was on the local route, whose full sweep is ~17.7 days (R683), and `eia` is
        declared DAILY. "eia LATE" alone sends the reader to eia; "eia LATE(local)" sends them
        to the thing that is actually wrong.
        """
        return late_label(cadence.get(source_id, ""), route.get(source_id, "any"), ts,
                          _dt.datetime.now(_dt.timezone.utc))

    def tried_age(ts) -> str:
        """How long ago we last ATTEMPTED this source, as a short human string.

        Without it a 16-day-old error reads exactly like this morning's. eurostat's row on
        2026-09-03 described a re-key migration that had completed two days earlier - a fossil,
        indistinguishable in the email from a live failure.
        """
        if not ts:
            return "never"
        try:
            t = _dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            if t.tzinfo is None:
                t = t.replace(tzinfo=_dt.timezone.utc)
        except Exception:                                             # noqa: BLE001
            return "?"
        h = (_dt.datetime.now(_dt.timezone.utc) - t).total_seconds() / 3600.0
        if h < 0:
            return "?"
        if h < 36:
            return f"{h:.0f}h ago"
        return f"{h / 24:.0f}d ago"

    # Ordering, not filtering. Measured 2026-09-03: 20 of 37 attention rows were permanent by
    # construction — a subset declaration or a budget window away from clearable — and a list
    # that is always that long cannot distinguish a new problem from the standing one. Rows are
    # sorted and given a reason; NONE is dropped, because a misclassification would otherwise
    # bury the row it misjudged, and this is a regex over error text, not an oracle.
    _KINDS = (
        ("SCHEMA", 0, _re.compile(r"returned 200 but parsed 0 rows", _re.I)),
        ("SHRINK", 1, _re.compile(r"refusing shrink", _re.I)),
        ("MIGRATE", 2, _re.compile(r"migration has not completed", _re.I)),
        ("RETRY", 3, _re.compile(r"transient-failed|UNEXPECTED:|UnitTimeout", _re.I)),
        ("SUBSET", 8, _re.compile(r"csv coherence unmet|csv coverage note", _re.I)),
        ("BUDGET", 9, _re.compile(r"deferred by budget", _re.I)),
    )

    def kind_of(err) -> tuple:
        """(sort rank, short label). Unmatched rows sort with the actionable ones, never last."""
        text = str(err or "")
        for name, rank, pat in _KINDS:
            if pat.search(text):
                return rank, name
        return 4, "OTHER"

    # ---- plain-text fallback (some clients / previews) ----
    lines = [f"Run {run_id}: {run_status}",
             f"{len(ok)} ok/no_change · {len(warn)} partial/transient · {len(bad)} failed", ""]
    for r in sorted(warn + bad, key=lambda x: (kind_of(x[4])[0], x[0])):
        lines.append(f"  !! {r[0]:20} {kind_of(r[4])[1]:8} {r[1]:15} tried={tried_age(r[5]):>8}{late_mark(r[0], r[5]):<13}  err={str(r[4] or '')[:72]}")
    if warn or bad:
        lines.append("")
    if orphans:
        lines.append(f"  ({len(orphans)} unmanaged leftover state row(s), excluded from the "
                     f"counts above — no registry entry, so they never re-run: "
                     f"{', '.join(sorted(r[0] for r in orphans))})")
        lines.append("")
    if dormant:
        lines.append(f"  ({len(dormant)} source(s) not in the live tier, excluded from the "
                     f"counts above — the daily run does not execute them "
                     f"(AQUEDUCT_LIVE_ONLY), so their status cannot change: "
                     f"{', '.join(sorted(r[0] for r in dormant))})")
        lines.append("")

    # FIRST-PASS CRAWLERS. These have no unit_state row and appeared NOWHERE in this email until
    # 2026-09-03, when gus_dbw sat stalled for two hours on upstream connection resets and was
    # found only by accident. They are not small: gus_dbw's completed backfill is 1,237,900,173
    # observations. FIRSTPASS_DIRS is imported, never copied — a second copy of one list is
    # R676's shape and drifts the first time a crawler is added.
    if fp:
        if True:
            lines.append("  first-pass crawlers (no unit_state row — invisible to the gate):")
            for name, age_h, newest in fp:
                mark = "  STALE" if age_h > FIRSTPASS_STALE_HOURS else ""
                lines.append(f"     {name:12} last wrote {age_h:5.1f}h ago "
                             f"({newest[:34]}){mark}")
            lines.append("")
    for r in ok:
        # A "data through" frontier far in the future is a legitimate projection
        # horizon (e.g. fred_releases carries CBO potential-GDP / WEO forecasts that
        # extend ~10y out), NOT a data bug. Flag it so a projection is never mistaken
        # for a stale/garbled date in the digest.
        _proj = ""
        try:
            _d = dt.date.fromisoformat(str(r[2])[:10]) if r[2] else None
            if _d and (_d - dt.date.today()).days > 366:
                _proj = " (incl. forecast/projection horizon)"
        except (ValueError, TypeError):
            _proj = ""
        lines.append(f"     {r[0]:20} {r[1]:15} data through {r[2] or '—'}{_proj}")
    lines += ["", f"Live freshness feed: {feed}", f"Run log: {runlog}"]
    body = chr(10).join(lines)

    # ---- HTML (the real email) ----
    import html as H
    def esc(x): return H.escape(str(x if x is not None else "—"))
    ink, gold = "#0b1220", "#c9a961"
    green, amber, red, grey = "#15803d", "#b45309", "#b91c1c", "#6b7280"

    def chip(n, label, color):
        return (f'<td style="padding:10px 16px;background:#fff;border:1px solid #e5e7eb;'
                f'border-radius:10px;text-align:center;">'
                f'<div style="font-size:22px;font-weight:700;color:{color};'
                f'font-family:Georgia,serif;">{n}</div>'
                f'<div style="font-size:11px;color:{grey};text-transform:uppercase;'
                f'letter-spacing:.05em;">{label}</div></td>')

    # THE SAME THREE SECTIONS AS THE TEXT BODY. They were text-only until 2026-09-03, which
    # meant the reader of the actual email never saw them.
    def _note(text):
        return (f'<div style="margin:8px 0;padding:8px 11px;background:#f9fafb;'
                f'border-left:3px solid {grey};border-radius:4px;font-size:12px;'
                f'color:{grey};line-height:1.5;">{text}</div>')

    notes_html = ""
    if orphans:
        notes_html += _note(
            f"<b>{len(orphans)}</b> unmanaged leftover state row(s), excluded from the counts "
            f"above — no registry entry, so they never re-run: "
            f"{esc(', '.join(sorted(r[0] for r in orphans)))}")
    if dormant:
        notes_html += _note(
            f"<b>{len(dormant)}</b> source(s) not in the live tier, excluded from the counts "
            f"above — the daily run does not execute them (AQUEDUCT_LIVE_ONLY), so their status "
            f"cannot change: {esc(', '.join(sorted(r[0] for r in dormant)))}")
    if fp:
        _rows = ""
        for _name, _age, _newest in fp:
            _stale = _age > FIRSTPASS_STALE_HOURS
            _rows += (f'<tr><td style="padding:3px 10px 3px 0;">{esc(_name)}</td>'
                      f'<td style="padding:3px 10px 3px 0;color:{red if _stale else grey};'
                      f'font-weight:{"700" if _stale else "400"};">{_age:.1f}h ago'
                      f'{" — STALE" if _stale else ""}</td>'
                      f'<td style="padding:3px 0;color:{grey};">{esc(_newest)}</td></tr>')
        notes_html += _note(
            "<b>first-pass crawlers</b> — no unit_state row, so they are invisible to the health "
            f"gate:<table role='presentation' style='margin-top:5px;font-size:12px;'>{_rows}"
            "</table>")

    attention_rows = ""
    for r in sorted(warn + bad, key=lambda x: (kind_of(x[4])[0], x[0])):
        color = red if r in bad else amber
        attention_rows += (
            f'<tr><td style="padding:7px 10px;border-top:1px solid #e5e7eb;'
            f'font-family:Consolas,monospace;font-size:13px;"><b>{esc(r[0])}</b></td>'
            f'<td style="padding:7px 10px;border-top:1px solid #e5e7eb;color:{color};'
            f'font-weight:600;font-size:13px;">{esc(r[1])}</td>'
            f'<td style="padding:7px 10px;border-top:1px solid #e5e7eb;'
            f'font-family:Consolas,monospace;font-size:13px;">{esc(r[2])}</td>'
            f'<td style="padding:7px 10px;border-top:1px solid #e5e7eb;color:{grey};'
            f'font-family:Consolas,monospace;font-size:13px;">{esc(tried_age(r[5]) + late_mark(r[0], r[5]))}</td>'
            f'<td style="padding:7px 10px;border-top:1px solid #e5e7eb;color:{grey};'
            f'font-size:12px;">{esc(str(r[4] or "")[:110])}</td></tr>')

    current_rows = ""
    for r in ok:
        st_color = green if r[1] == "ok" else grey
        current_rows += (
            f'<tr><td style="padding:5px 10px;border-top:1px solid #f3f4f6;'
            f'font-family:Consolas,monospace;font-size:12.5px;">{esc(r[0])}</td>'
            f'<td style="padding:5px 10px;border-top:1px solid #f3f4f6;color:{st_color};'
            f'font-size:12.5px;">{esc(r[1])}</td>'
            f'<td style="padding:5px 10px;border-top:1px solid #f3f4f6;'
            f'font-family:Consolas,monospace;font-size:12.5px;">{esc(r[2])}</td></tr>')

    banner_color = green if healthy else red
    banner_word = "All systems normal" if healthy else "Needs attention"
    html_doc = f"""<!doctype html><html><body style="margin:0;padding:0;background:#f4f5f7;">
<div style="max-width:680px;margin:0 auto;padding:18px;font-family:-apple-system,'Segoe UI',Roboto,Arial,sans-serif;color:#111827;">
  <div style="background:{ink};border-radius:12px 12px 0 0;padding:18px 22px;">
    <div style="color:{gold};font-size:11px;text-transform:uppercase;letter-spacing:.14em;">ElkassabgiData</div>
    <div style="color:#fff;font-family:Georgia,serif;font-size:20px;margin-top:2px;">Econ Data Library — daily report</div>
  </div>
  <div style="background:{banner_color};color:#fff;padding:8px 22px;font-size:13px;font-weight:600;">
    {banner_word} · run {esc(run_id)} · {esc(run_status)}
  </div>
  <div style="background:#fff;border:1px solid #e5e7eb;border-top:none;border-radius:0 0 12px 12px;padding:20px 22px;">
    <table role="presentation" cellspacing="8" style="border-collapse:separate;margin:0 auto 14px;"><tr>
      {chip(len(ok), "current", green)}{chip(len(warn), "retrying", amber)}{chip(len(bad), "failed", red)}
    </tr></table>
    {"<h3 style='font-family:Georgia,serif;font-size:15px;margin:14px 0 6px;color:" + red + ";'>Needs attention</h3><table role='presentation' width='100%' style='border-collapse:collapse;background:#fffbeb;border:1px solid #fde68a;border-radius:8px;'><tr><th align='left' style='padding:7px 10px;font-size:11px;color:" + grey + ";text-transform:uppercase;'>Source</th><th align='left' style='padding:7px 10px;font-size:11px;color:" + grey + ";text-transform:uppercase;'>Status</th><th align='left' style='padding:7px 10px;font-size:11px;color:" + grey + ";text-transform:uppercase;'>Data through</th><th align='left' style='padding:7px 10px;font-size:11px;color:" + grey + ";text-transform:uppercase;'>Last tried</th><th align='left' style='padding:7px 10px;font-size:11px;color:" + grey + ";text-transform:uppercase;'>Detail</th></tr>" + attention_rows + "</table>" if (warn or bad) else ""}
    {notes_html}
    <h3 style="font-family:Georgia,serif;font-size:15px;margin:18px 0 6px;">Current sources ({len(ok)})</h3>
    <table role="presentation" width="100%" style="border-collapse:collapse;">
      <tr><th align="left" style="padding:5px 10px;font-size:11px;color:{grey};text-transform:uppercase;">Source</th>
          <th align="left" style="padding:5px 10px;font-size:11px;color:{grey};text-transform:uppercase;">Status</th>
          <th align="left" style="padding:5px 10px;font-size:11px;color:{grey};text-transform:uppercase;">Data through</th></tr>
      {current_rows}
    </table>
    <p style="margin:18px 0 0;font-size:12.5px;color:{grey};line-height:1.6;">
      <a href="{feed}" style="color:#1d4ed8;">Live freshness feed</a> ·
      <a href="{runlog}" style="color:#1d4ed8;">Run log</a><br>
      Dates only advance when observations were actually fetched — far-future dates are
      legitimate forecast datasets (e.g. WEO projections). Failures show here until fixed, never hidden.
    </p>
  </div>
</div>
</body></html>"""

    # INSPECT THE REAL EMAIL WITHOUT SENDING IT. On 2026-09-03 three sections shipped into the
    # text body alone — the orphan list, the non-live list and the first-pass crawlers — and no
    # one would have seen any of them, because the text body is the fallback almost no client
    # renders. There was no way to look: without an API key this script prints the TEXT and drops
    # the HTML. This runs before the send decision, so it works with no key present.
    _html_out = os.environ.get("AQUEDUCT_DIGEST_HTML_OUT")
    if _html_out:
        with open(_html_out, "w", encoding="utf-8") as _fh:
            _fh.write(html_doc)
        print(f"[digest] html written to {_html_out} ({len(html_doc):,} bytes)", flush=True)


    print(f"[digest] {subject}\n{body}\n", flush=True)
    if not key:
        print("[digest] RESEND_API_KEY not set — email SKIPPED (add the GitHub secret "
              "to enable the morning email; the run's red/green state still notifies "
              "via GitHub Actions).", flush=True)
        return

    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=json.dumps({"from": FROM, "to": [TO], "subject": subject,
                         "text": body, "html": html_doc}).encode(),
        headers={"Authorization": f"Bearer {key}",
                 "Content-Type": "application/json",
                 # api.resend.com sits behind Cloudflare bot protection, which
                 # 1010-blocks urllib's default signature — identify honestly.
                 "User-Agent": "econdatalibrary-digest/1.0"},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            print(f"[digest] sent: HTTP {resp.status}", flush=True)
    except urllib.error.HTTPError as e:
        # Print Resend's exact error body (safe: their error JSON carries no
        # secrets) so a misconfigured key/domain is diagnosable from the log.
        try:
            detail = e.read().decode()[:300]
        except Exception:
            detail = "(no body)"
        print(f"[digest] SEND FAILED: HTTP {e.code} — {detail} (run status is "
              f"still governed by the health gate, not the email)", flush=True)
    except Exception as e:  # noqa: BLE001 — email failure must not flip a green run red
        print(f"[digest] SEND FAILED: {e!r} (run status is still governed by the "
              f"health gate, not the email)", flush=True)


if __name__ == "__main__":
    sys.exit(main())
