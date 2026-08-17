"""Apply the econdl_sweep human-readable series names to catalog.db (2026-08-16).

Deliverable: Ahmed's decoded-names sweep (WEBSITE_INTEGRATION_GUIDE.md) — one
csv/{source}.csv per source, one row per series, keyed by the canonical
series_id. This tool integrates them into the serving catalog.

Modes:
  --measure           stream EVERY csv, compare against catalog.db, write a
                      per-source report (rows, matched, title-diffs, unknown
                      ids) to stdout — NO writes. Full dataset, never sampled.
  --apply             perform the update. Per series:
                        * title: overwrite when csv title is non-empty and
                          differs from the current title;
                        * description: stored in metadata JSON under
                          "description" when non-empty and different from the
                          title (the guide sets description=title when no
                          fuller text exists — storing that would be noise);
                        * geography/unit: filled ONLY when the catalog's value
                          is NULL/empty (never clobbers curated values);
                        * series_id: NEVER touched (guide §6 — stable join key).
                      Changed ids are appended to
                      data/_aqueduct/pending_catalog_sync.txt so the standard
                      sync_catalog_d1 delta path pushes exactly them to D1.
  --source X          restrict either mode to one source.

R400: catalog.db is concurrently written by running jobs — busy_timeout applies.
"""
import argparse
import csv
import io
import json
import os
import sqlite3
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SWEEP = os.path.join(ROOT, "data", "_series_names_staging", "econdl_sweep")
PENDING = os.path.join(ROOT, "data", "_aqueduct", "pending_catalog_sync.txt")

csv.field_size_limit(10_000_000)


def sources():
    return sorted(f[:-4] for f in os.listdir(os.path.join(SWEEP, "csv"))
                  if f.endswith(".csv"))


def stream_rows(src):
    p = os.path.join(SWEEP, "csv", f"{src}.csv")
    with io.open(p, "r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            yield row


def load_catalog(con, src):
    """{series_id: (title, geography, unit, metadata)} for one source."""
    return {r[0]: (r[1], r[2], r[3], r[4]) for r in con.execute(
        "SELECT series_id, title, geography, unit, metadata FROM series "
        "WHERE source_id=?", (src,))}


def process(con, src, apply):
    cat = load_catalog(con, src)
    n_csv = matched = title_diff = unknown = filled_geo = filled_unit = desc_set = 0
    updates = []   # (title, geography, unit, metadata, series_id)
    changed_ids = []
    for row in stream_rows(src):
        n_csv += 1
        sid = row["series_id"]
        cur = cat.get(sid)
        if cur is None:
            unknown += 1
            continue
        matched += 1
        cur_title, cur_geo, cur_unit, cur_meta = cur
        new_title = (row.get("title") or "").strip()
        new_desc = (row.get("description") or "").strip()
        new_geo = (row.get("geography") or "").strip()
        new_unit = (row.get("unit") or "").strip()

        t = cur_title
        g = cur_geo
        u = cur_unit
        m = cur_meta
        changed = False
        if new_title and new_title != (cur_title or ""):
            t = new_title
            title_diff += 1
            changed = True
        if new_geo and not (cur_geo or "").strip():
            g = new_geo
            filled_geo += 1
            changed = True
        if new_unit and not (cur_unit or "").strip():
            u = new_unit
            filled_unit += 1
            changed = True
        if new_desc and new_desc != new_title:
            try:
                meta = json.loads(cur_meta) if cur_meta else {}
            except Exception:  # noqa: BLE001 — malformed metadata must not kill the run
                meta = {}
            if meta.get("description") != new_desc:
                meta["description"] = new_desc
                m = json.dumps(meta, ensure_ascii=False)
                desc_set += 1
                changed = True
        if changed and apply:
            updates.append((t, g, u, m, sid))
            changed_ids.append(sid)

    if apply and updates:
        con.executemany(
            "UPDATE series SET title=?, geography=?, unit=?, metadata=? "
            "WHERE series_id=?", updates)
        con.commit()
        os.makedirs(os.path.dirname(PENDING), exist_ok=True)
        with open(PENDING, "a", encoding="utf-8") as fh:
            for sid in changed_ids:
                fh.write(sid + "\n")
    return dict(csv=n_csv, matched=matched, title_diff=title_diff,
                unknown=unknown, geo=filled_geo, unit=filled_unit,
                desc=desc_set, updated=len(updates))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--measure", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--source")
    a = ap.parse_args()
    if a.measure == a.apply:
        ap.error("exactly one of --measure / --apply")
    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=7200)
    con.execute("PRAGMA busy_timeout=7200000")
    srcs = [a.source] if a.source else sources()
    tot = {"csv": 0, "matched": 0, "title_diff": 0, "unknown": 0,
           "geo": 0, "unit": 0, "desc": 0, "updated": 0}
    t0 = time.time()
    for i, src in enumerate(srcs, 1):
        r = process(con, src, a.apply)
        for k in tot:
            tot[k] += r[k]
        flag = " <-- CHANGES" if r["title_diff"] else ""
        print(f"[{i}/{len(srcs)}] {src}: csv={r['csv']:,} matched={r['matched']:,} "
              f"title_diff={r['title_diff']:,} unknown={r['unknown']:,} "
              f"desc={r['desc']:,}{flag}", flush=True)
    print(f"\nTOTAL ({time.time()-t0:,.0f}s): csv={tot['csv']:,} matched={tot['matched']:,} "
          f"title_diff={tot['title_diff']:,} unknown={tot['unknown']:,} "
          f"geo={tot['geo']:,} unit={tot['unit']:,} desc={tot['desc']:,} "
          f"updated={tot['updated']:,}", flush=True)


if __name__ == "__main__":
    main()
