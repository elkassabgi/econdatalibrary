"""Generate the per-source RUNBOOK — one file per database, from GROUND TRUTH only.

WHY THIS EXISTS. When a source stops updating, the next person (or the next Claude session)
needs to know, without re-deriving it: what this source is, where its data lives, how it fetches,
what its current state actually says, which failure modes it has ALREADY had, and the exact
commands to diagnose and repair it. That knowledge existed in three places that do not survive —
a session transcript, an in-session task list, and one engineer's head.

EVERY FACT HERE IS READ FROM THE SYSTEM, NEVER TYPED. That is the whole design constraint:

    identity / strategy / cadence / adapter notes  <- updater/registry.yaml
    live status, last error, last success          <- data/_aqueduct/state.db
    catalogued series                              <- data/catalog.db
    served?                                        <- api/worker/src/util.ts
    how it works                                   <- the fetcher module's own docstring
    licence                                        <- DATABASE_LICENSES_VERBATIM.md
    known failures + repairs                       <- .claude/MISTAKES.md (by source mention)

A hand-written runbook rots the moment someone changes a fetcher. A generated one is re-runnable:
`python tools/gen_runbook.py` and it is current again. Where a fact is genuinely unknown the file
says NOT ESTABLISHED rather than guessing — a runbook that invents a repair is worse than none.

    python tools/gen_runbook.py                 # write docs/runbook/*.md + index
    python tools/gen_runbook.py --source cso    # one source, to stdout
    python tools/gen_runbook.py --with-store    # also count store rows (SLOW: a GET per file)
"""
from __future__ import annotations
import argparse
import datetime as dt
import json
import os
import re
import sqlite3
import sys
import textwrap

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import yaml                                                    # noqa: E402
from updater import config                                     # noqa: E402

OUT_DIR = os.path.join(ROOT, "docs", "runbook")
LEDGER = r"D:\research\hfdatalibrary\.claude\MISTAKES.md"
UTIL_TS = os.path.join(ROOT, "api", "worker", "src", "util.ts")
LICENCES = os.path.join(ROOT, "DATABASE_LICENSES_VERBATIM.md")


# ---------------------------------------------------------------- ground-truth readers
def load_registry():
    d = yaml.safe_load(open(os.path.join(ROOT, "updater", "registry.yaml"), encoding="utf-8"))
    es = d["sources"] if isinstance(d, dict) and "sources" in d else d
    es = es if isinstance(es, list) else list(es.values())
    return {(e.get("id") or e.get("source_id")): e for e in es}


def load_state():
    """{source_id: [row, ...]} from unit_state, plus the last few runs."""
    out, runs = {}, {}
    try:
        con = sqlite3.connect(f"file:{config.STATE_DB}?mode=ro", uri=True, timeout=120)
    except Exception:
        return out, runs
    try:
        for r in con.execute(
                "SELECT source_id, unit_id, status, last_success_utc, last_attempt_utc, "
                "obs_count, last_obs_date, last_error, upstream_vintage FROM unit_state"):
            out.setdefault(r[0], []).append(dict(
                unit=r[1], status=r[2], last_success=r[3], last_attempt=r[4],
                obs=r[5], last_obs=r[6], error=r[7], vintage=r[8]))
        for r in con.execute(
                "SELECT source_id, ts_utc, status, obs, dur_s, note FROM runs "
                "ORDER BY ts_utc DESC"):
            runs.setdefault(r[0], [])
            if len(runs[r[0]]) < 5:
                runs[r[0]].append(dict(ts=r[1], status=r[2], obs=r[3], dur=r[4], note=r[5]))
    except Exception:
        pass
    con.close()
    return out, runs


def load_catalog_counts():
    """{source_id: catalogued series} — range scan per source, uses the PK index."""
    out = {}
    p = os.path.join(ROOT, "data", "catalog.db")
    if not os.path.exists(p):
        return out
    try:
        con = sqlite3.connect(f"file:{p}?mode=ro", uri=True, timeout=300)
        for (sid,) in con.execute(
                "SELECT DISTINCT substr(series_id,1,instr(series_id,':')-1) FROM series"):
            if sid:
                out[sid] = con.execute(
                    "SELECT COUNT(*) FROM series WHERE series_id>=? AND series_id<?",
                    (sid + ":", sid + ";")).fetchone()[0]
        con.close()
    except Exception:
        pass
    return out


def load_served():
    """source ids listed in the worker's resolvable set."""
    try:
        ts = open(UTIL_TS, encoding="utf-8").read()
    except Exception:
        return set()
    return set(re.findall(r'^\s*"([a-z0-9_]+)",\s*$', ts, re.M))


def fetcher_doc(sid):
    """(path, docstring) for the source's fetcher module, or (None, None)."""
    p = os.path.join(ROOT, "updater", "strategies", "fetchers", f"{sid}.py")
    if not os.path.exists(p):
        return None, None
    src = open(p, encoding="utf-8").read()
    m = re.match(r'\s*(?:from __future__[^\n]*\n)?\s*"""(.*?)"""', src, re.S)
    return os.path.relpath(p, ROOT).replace(os.sep, "/"), (m.group(1).strip() if m else None)


def ledger_hits(sid):
    """Ledger entries whose body mentions this source. The repair history, in its own words."""
    try:
        txt = open(LEDGER, encoding="utf-8", errors="replace").read()
    except Exception:
        return []
    hits = []
    for m in re.finditer(r"^### (R\d+) — (.*?)$", txt, re.M):
        start = m.end()
        nxt = re.search(r"^### R\d+ — ", txt[start:], re.M)
        body = txt[start:start + (nxt.start() if nxt else 4000)]
        if re.search(rf"\b{re.escape(sid)}\b", body):
            hits.append((m.group(1), m.group(2).strip()))
    return hits


def licence_line(sid):
    try:
        txt = open(LICENCES, encoding="utf-8").read()
    except Exception:
        return None
    for line in txt.splitlines():
        if re.match(rf"^\|\s*\**{re.escape(sid)}\**\s*\|", line):
            return line.strip()
    return None


# ---------------------------------------------------------------- rendering
def _fmt(v, dash="—"):
    return dash if v in (None, "", [], {}) else v


def render(sid, reg, st, runs, cat, served, with_store=False):
    e = reg.get(sid, {})
    units = st.get(sid, [])
    L = []
    A = L.append

    A(f"# {sid}")
    A("")
    A(f"*Generated by `tools/gen_runbook.py` on {dt.datetime.now(dt.timezone.utc):%Y-%m-%d %H:%M} UTC "
      f"— every fact below is read from the system. Re-run the tool to refresh; do not hand-edit.*")
    A("")

    # ---- 1. AT A GLANCE
    A("## 1. At a glance")
    A("")
    A("| | |")
    A("|---|---|")
    A(f"| in registry | {'yes' if e else '**NO** — not scheduled at all'} |")
    A(f"| live (scheduled) | {_fmt(e.get('live'), 'false/absent')} |")
    A(f"| served to users | {'yes' if sid in served else 'no — absent from api/worker/src/util.ts'} |")
    A(f"| catalogued series | {cat.get(sid, 0):,} |")
    A(f"| strategy | `{_fmt(e.get('strategy'))}` |")
    A(f"| check cadence | {_fmt(e.get('cadence'))} |")
    if e.get("data_cadence"):
        A(f"| **data** cadence (lateness clock) | {e['data_cadence']} |")
    A(f"| runs where | {_fmt(e.get('run_location'), 'cloud (GitHub Actions)')} |")
    A(f"| refresh cost | {_fmt(e.get('refresh_cost'))} |")
    A(f"| store dir | `data/clean_full/{_fmt(e.get('out_dir'), sid)}/` |")
    if e.get("upstream_verified"):
        uv = e["upstream_verified"]
        A(f"| upstream declared COMPLETE | latest_obs `{uv.get('latest_obs')}`, checked "
          f"`{uv.get('checked')}` — lapses to ATTENTION after 180 days |")
    lic = licence_line(sid)
    if lic:
        A(f"| licence row | {lic[:200]} |")
    A("")

    # ---- 2. CURRENT STATE
    A("## 2. Current state (from `data/_aqueduct/state.db`)")
    A("")
    if not units:
        A("**No state rows.** This source has never produced state. If its adapter is built that is "
          "a real problem (the health gate calls it `RED-UNRUN`); if not, it is expected "
          "(`PENDING — no adapter built`).")
    for u in units:
        A(f"- **unit `{u['unit']}`** — status `{u['status']}`")
        A(f"  - last SUCCESS: `{_fmt(u['last_success'], 'NEVER')}`  ·  last attempt: "
          f"`{_fmt(u['last_attempt'], 'never')}`")
        A(f"  - obs_count: `{_fmt(u['obs'])}`  ·  last_obs_date: `{_fmt(u['last_obs'])}`  ·  "
          f"stored vintage: `{_fmt(u['vintage'])}`")
        if u["error"]:
            A(f"  - last error:")
            A(f"    ```")
            for ln in textwrap.wrap(u["error"], 96)[:12]:
                A(f"    {ln}")
            A(f"    ```")
    A("")
    A("> `obs_count` is **not comparable across runs** — most fetchers report rows merged this run, "
      "thirteen fall back to the whole store when nothing was written. To ask how big this source "
      "is, count the store. See ledger R326 and the comment in `updater/state.py`.")
    A("")
    A("> A `partial` NEVER sets `last_success_utc` (R231). A source reading "
      "`last SUCCESS: NEVER` may be ingesting millions of rows perfectly well and failing one "
      "sub-unit. Check `obs_count` and the run history before concluding it is dead.")
    A("")

    rr = runs.get(sid) or []
    if rr:
        A("### Recent runs")
        A("")
        A("| when (UTC) | status | obs | dur | note |")
        A("|---|---|---|---|---|")
        for r in rr:
            note = (r["note"] or "")[:110].replace("|", "\\|")
            A(f"| {str(r['ts'])[:19]} | {r['status']} | {_fmt(r['obs'])} | {_fmt(r['dur'])}s | {note} |")
        A("")

    # ---- 3. HOW IT WORKS
    A("## 3. How it works")
    A("")
    if e.get("strategy_reason"):
        A(f"**Why this strategy** (registry `strategy_reason`):")
        A("")
        A("> " + str(e["strategy_reason"]).replace("\n", "\n> "))
        A("")
    ad = e.get("adapter") or {}
    if ad:
        A("**Adapter contract** (registry `adapter`):")
        A("")
        for k in ("vintage_signal", "since_param", "out_paths_note", "rate_note"):
            if ad.get(k):
                A(f"- `{k}`: {ad[k]}")
        A("")
    for k in ("keys_or_blockers", "storage_layout", "supersession", "id_collision_warning",
              "refreshed_elsewhere", "license", "attribution"):
        if e.get(k):
            A(f"**{k}**: {e[k]}")
            A("")
    fp, doc = fetcher_doc(sid)
    if fp:
        A(f"**Fetcher module**: [`{fp}`]({os.path.relpath(os.path.join(ROOT, fp), OUT_DIR).replace(os.sep, '/')})")
        A("")
        if doc:
            A("<details><summary>Its own module docstring — the authoritative description, including "
              "every defect previously found and why the code looks the way it does</summary>")
            A("")
            A("```")
            A(doc)
            A("```")
            A("")
            A("</details>")
            A("")
    else:
        A("**No fetcher module** at `updater/strategies/fetchers/" + sid + ".py`. If this source is "
          "`live: true` the orchestrator will report `PENDING — no adapter built` and skip it "
          "forever, however it is scheduled.")
        A("")
    if e.get("scripts"):
        A(f"**Ingest script(s)**: {', '.join('`' + s + '`' for s in e['scripts'])}")
        A("")

    # ---- 4. DIAGNOSE
    A("## 4. If it stops updating — diagnose in this order")
    A("")
    A("Run everything from `E:\\research\\econfindatalibrary`.")
    A("")
    A("```bash")
    A("# 1. What does the system actually say? (not what you remember)")
    A(f"python -c \"import sys,sqlite3;sys.path.insert(0,'.');from updater import config;"
      f"con=sqlite3.connect(f'file:{{config.STATE_DB}}?mode=ro',uri=True);"
      f"print(*con.execute(\\\"SELECT unit_id,status,last_success_utc,last_attempt_utc,last_error "
      f"FROM unit_state WHERE source_id='{sid}'\\\"),sep=chr(10))\"")
    A("")
    A("# 2. Is it even being ATTEMPTED? 'scheduled' is not 'attempted' (R246).")
    A("#    A daily run reaches ~20 of ~106 cloud sources; most are skipped on cadence, which is normal.")
    A(f"python -m updater.run --source {sid} --dry")
    A("")
    A("# 3. Run it for real and READ THE REASON. Failures name themselves now.")
    A(f"AQUEDUCT_BACKEND=r2 python -u -m updater.run --source {sid} --force")
    A("")
    A("# 4. Is the PUBLISHER healthy, or is it us? Probe upstream directly, never a relay.")
    A("#    (DBnomics is BANNED — every source must be reached at its own publisher.)")
    A("")
    A("# 5. Is the store intact? obs_count in state is NOT the answer.")
    A(f"AQUEDUCT_BACKEND=r2 python -c \"import sys,os;sys.path.insert(0,'.');"
      f"from updater import config,blob;d=config.source_dir('{sid}');"
      f"fs=[f for f in blob.list_parquets(d) if not os.path.basename(f).startswith('_')];"
      f"print(len(fs),'files',sum(blob.row_count(os.path.join(d,os.path.basename(f))) for f in fs),'rows')\"")
    A("")
    A("# 6. Are its dates real? A counter read as a year is silent and has hit 7 sources.")
    A(f"python tools/audit_impossible_dates.py --r2 --source {sid}")
    A("```")
    A("")
    A("**Read the failure class before fixing anything.** From the 2026-08-04 audit of every "
      "unfetchable source, the causes were: `budget_deferral` (NOT broken — ran out of its time "
      "slice), `code_bug`, `rate_limited`, `gated_by_design`. **Zero** were an expired credential "
      "or a dead endpoint, although that is the usual first guess.")
    A("")
    A("If the error names sub-units as `deferred (budget N min)` or `budget spent`, **nothing has "
      "failed** — the source ran out of its slice and the rest is taken next tick (R303). Do not "
      "'fix' it.")
    A("")

    # ---- 5. HISTORY
    hits = ledger_hits(sid)
    A("## 5. Everything that has already gone wrong here")
    A("")
    if hits:
        A(f"{len(hits)} ledger entr{'y' if len(hits) == 1 else 'ies'} mention this source. "
          f"Read them in `D:\\research\\hfdatalibrary\\.claude\\MISTAKES.md` before changing "
          f"anything — several describe a fix that was tried and was WRONG.")
        A("")
        for rid, title in hits:
            A(f"- **{rid}** — {title}")
    else:
        A("No ledger entry mentions this source. That means no failure here has been diagnosed and "
          "written up yet — not that none has occurred.")
    A("")

    # ---- 6. RULES
    A("## 6. Rules that apply to every repair here")
    A("")
    A("- **A key/parser change is a RE-PULL, never a merge** (R22). Changing what `series_key` "
      "means leaves the old and new grains coexisting; dedup cannot collapse them because they no "
      "longer collide. Use `mode=\"replace\"`, or re-pull the table.")
    A("- **Never patch dates — re-pull** (R288). A naive date repair on `cso` would have destroyed "
      "11 of 12 rows; only a negative control caught it. And 41,091 of `stat_slovenia`'s fabricated "
      "dates land in 1900–2200, indistinguishable from real data, so no date test can find them.")
    A("- **Verify against the store you are actually serving** (R296/R36). Under "
      "`AQUEDUCT_BACKEND=r2` the local directory is a scratch mirror of the last run only; a local "
      "check can under-report or give the opposite answer.")
    A("- **When a probe reports ABSENCE, run it against something known PRESENT** (R316). An "
      "absence looks identical whether the data is missing or the probe is broken.")
    A("- **Compare (key, date) pairs, not just keys** (R314). A re-key check that compares "
      "identifiers verifies the half you changed on purpose; the half you changed by accident is "
      "in the values.")
    A("- **A bound over a fixed order is a truncation, not a budget** (R190). If work is capped, "
      "check it ROTATES — and specifically that it rotates when the item at the head never "
      "completes (R313).")
    A("- **Read the job's ARGV, not its progress** (R323). A `--dry-run` and a real run print the "
      "same numbers.")
    A("")
    return "\n".join(L) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source")
    ap.add_argument("--with-store", action="store_true")
    a = ap.parse_args()

    reg = load_registry()
    st, runs = load_state()
    cat = load_catalog_counts()
    served = load_served()

    ids = sorted(set(reg) | set(st) | set(cat))
    if a.source:
        print(render(a.source, reg, st, runs, cat, served, a.with_store))
        return 0

    os.makedirs(OUT_DIR, exist_ok=True)
    written = 0
    for sid in ids:
        if not sid:
            continue
        with open(os.path.join(OUT_DIR, f"{sid}.md"), "w", encoding="utf-8", newline="") as f:
            f.write(render(sid, reg, st, runs, cat, served, a.with_store))
        written += 1

    # ---- index
    idx = [
        "# Source runbook — one file per database",
        "",
        f"*Generated by `tools/gen_runbook.py` on {dt.datetime.now(dt.timezone.utc):%Y-%m-%d %H:%M} "
        f"UTC. Re-run it after any change; do not hand-edit these files.*",
        "",
        "**If a source has stopped updating, open its file below and work through §4.** Each file "
        "carries that source's real state, its adapter contract, its fetcher's own docstring "
        "(which records every defect previously found there), and the ledger entries about it.",
        "",
        "Before anything else, two facts that mislead people repeatedly:",
        "",
        "- A `partial` never sets `last_success_utc` (**R231**), so a source reading "
        "\"last SUCCESS: NEVER\" may be perfectly healthy and failing one sub-unit.",
        "- `obs_count` means \"rows this run\" on a productive run and \"whole store\" on a quiet "
        "one (**R326**) — a healthy source can appear to lose 168M rows. Count the store instead.",
        "",
        "| source | live | served | catalogued | status | last success |",
        "|---|---|---|---|---|---|",
    ]
    for sid in ids:
        if not sid:
            continue
        e, u = reg.get(sid, {}), (st.get(sid) or [{}])[0]
        idx.append(
            f"| [{sid}]({sid}.md) | {'yes' if e.get('live') else '—'} "
            f"| {'yes' if sid in served else '—'} | {cat.get(sid, 0):,} "
            f"| {_fmt(u.get('status'))} | {_fmt(u.get('last_success'), '**never**')} |")
    idx += ["", f"{len(ids)} sources.", ""]
    with open(os.path.join(OUT_DIR, "README.md"), "w", encoding="utf-8", newline="") as f:
        f.write("\n".join(idx))

    print(f"wrote {written} source files + README.md to {os.path.relpath(OUT_DIR, ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
