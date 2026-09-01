"""Repair a LOCAL parquet mirror that has fallen behind the R2 store — safely.

WHY THIS EXISTS. `core/derive_csv` resolves every store file through
`clients/python/econdl/_resolve.py`, which builds a PLAIN LOCAL PATH and never goes through
`blob`. So a derive run on this desktop reads the LOCAL mirror no matter what
`AQUEDUCT_BACKEND` says, and for a cloud source — where CI writes R2 and the desktop only
mirrors — that means it publishes whatever the mirror happens to hold. On 2026-09-01 that had
put 1,384 store files behind and was serving users an older vintage: eurostat tec00108 served
5,328 rows where the store held 5,415, and ilostat CCF_XPPP_CUR_RT_A ended 2025-01-01 against
the store's 2026-01-01 (R548).

Every guard below was paid for by a specific failure; do not simplify one away without
reading its note.

  DIRECTION      A file where LOCAL is ahead is not a stale mirror, it is the STORE missing
                 data, and syncing it down destroys the only richer copy. fao_gf arrived here
                 labelled BEHIND while holding 110 MORE rows locally (R549 F3).

  CONTAINMENT    Never-shrink on a row COUNT proves nothing: a merge that adds rows to one
                 family and drops another passes it. Compare (key, date) SETS with a duckdb
                 ANTI JOIN — no size cap, because the first version capped at 3M rows and the
                 ten LARGEST files were therefore synced unchecked, after which the local copy
                 was gone and the question became permanently unanswerable (R549 F5, R550).

  KEY COLUMNS    Never guess the date column positionally. `cols[1]` is gleif's `LegalName`
                 and defillama's `name`, so renames were counted as lost observations and
                 THREE files were refused for no reason (R551). With no time axis, a row's
                 identity is its key alone.

  WITHDRAWALS    For a cloud source the R2 copy IS the publisher's current state, so
                 identities that vanish are usually a withdrawal, not breakage — ilostat
                 CCF_XPPP drops 9 of 5,645 pairs (0.16%) while gaining a whole year. Follow
                 the publisher when R2 is ahead on rows AND dates, and RECORD every withdrawn
                 identity to a file so nothing disappears unrecorded.

  RESTRUCTURE    A near-total turnover is not a withdrawal. ilostat EIP_NEET_SEX_AGE_RT_A came
                 through at 21,417 of 21,417 (100%) because the publisher replaced the age
                 classification outright. Correct to follow, but say so loudly.

  ATOMIC WRITE   Unique temp name (pid + uuid), retry with backoff, cleanup on every path.
                 A fixed `.syncing` name lost a race with a running derive and left orphans.

  RECEIPTS       The outcome lists are written from SUCCESSES and REFUSALS, never from the
                 plan. An earlier version wrote its "synced" list from the plan and named
                 three files whose replace had thrown (R549 F4).

Input TSV: source, relpath, local_rows, r2_rows, local_max, r2_max (header row required).
"""
from __future__ import annotations

import argparse
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

ROOTS = ("clean_full", "clean_grouped")


class _Skip(Exception):
    """This file is not to be replaced; a refusal has already been recorded."""


def lost_identities(local_path: str, new_path: str):
    """(count, description) of identities the LOCAL copy holds that the new copy does not.

    duckdb ANTI JOIN rather than Python sets: it streams and spills, so there is no size cap
    and therefore no population exempt from the check.
    """
    import duckdb
    q = duckdb.connect()
    lp = str(local_path).replace(os.sep, "/")
    rp = str(new_path).replace(os.sep, "/")
    cols = [r[0] for r in q.execute(f"describe select * from read_parquet('{lp}')").fetchall()]
    kc = next((c for c in ("series_key", "series_id", "key") if c in cols), cols[0])
    dc = next((c for c in ("obs_date", "date", "time_period") if c in cols), None)
    kq = kc.replace('"', '""')
    if dc is None:
        sel = "select \"%s\"::VARCHAR k from read_parquet('%s')"
        left, right = sel % (kq, lp), sel % (kq, rp)
        mode = f"key-only on {kc!r} (this schema has no date column)"
    else:
        dq = dc.replace('"', '""')
        sel = "select \"%s\"::VARCHAR k, \"%s\"::VARCHAR d from read_parquet('%s')"
        left, right = sel % (kq, dq, lp), sel % (kq, dq, rp)
        mode = f"({kc}, {dc})"
    n = q.execute(f"select count(*) from (({left}) except ({right}))").fetchone()[0]
    return n, mode


def source_dir_any_root(sid: str, repo: str) -> str | None:
    for root in ROOTS:
        d = os.path.join(repo, "data", root, sid)
        if os.path.isdir(d):
            return d
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tsv", required=True, help="source/relpath/local_rows/r2_rows/maxes")
    ap.add_argument("--apply", action="store_true", help="without this, measure only")
    ap.add_argument("--source", action="append")
    ap.add_argument("--out-ok", required=True, help="where the SUCCEEDED list is written")
    a = ap.parse_args()

    os.environ.setdefault("AQUEDUCT_BACKEND", "r2")
    from core import r2_util
    from updater import blob, config                                  # noqa: F401

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    s3 = r2_util.client(write=True)

    rows = []
    with open(a.tsv, encoding="utf-8") as fh:
        for ln in fh:
            f = ln.rstrip("\n").split("\t")
            if len(f) >= 6 and f[0] != "source" and (not a.source or f[0] in a.source):
                rows.append(f)
    print(f"{len(rows):,} behind-R2 file(s) to repair; mode: "
          f"{'APPLY' if a.apply else 'DRY RUN'}", flush=True)
    if not a.apply:
        return 0

    ok, refused, failed, withdrawn = [], [], [], []
    for i, (sid, rel, lrows, rrows, lmax, rmax) in enumerate(rows, 1):
        d = source_dir_any_root(sid, repo)
        if d is None:
            refused.append((sid, rel, "no local store directory under any known root"))
            continue
        p = os.path.join(d, rel.replace("/", os.sep))
        key = blob._path_to_key(d).rstrip("/") + "/" + rel
        try:
            payload = s3.get_object(Bucket="econ-data", Key=key)["Body"].read()
        except Exception as e:                                        # noqa: BLE001
            failed.append((sid, rel, f"download {type(e).__name__}"))
            continue

        tmp = f"{p}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp"
        wrote = refused_here = False
        try:
            os.makedirs(os.path.dirname(tmp), exist_ok=True)
            with open(tmp, "wb") as f:
                f.write(payload)
            try:
                lost, mode = lost_identities(p, tmp)
            except Exception as e:                                    # noqa: BLE001
                refused.append((sid, rel, f"containment check failed: {type(e).__name__}"))
                raise _Skip()
            if lost:
                net_ahead = (int(rrows) >= int(lrows)
                             and (not lmax or not rmax or str(rmax) >= str(lmax)))
                if not net_ahead:
                    refused.append((sid, rel, f"R2 lacks {lost:,} identities the local copy "
                                              f"holds AND is not ahead ({int(lrows):,} local "
                                              f"rows vs {int(rrows):,}) — MERGE, not sync"))
                    raise _Skip()
                frac = lost / max(int(lrows), 1)
                if frac >= 0.5:
                    print(f"  RESTRUCTURE {sid}/{rel}: {lost:,} of {int(lrows):,} identities "
                          f"({frac:.0%}) are absent from R2 — the publisher re-keyed this "
                          f"dataset; this is not a withdrawal", flush=True)
                withdrawn.append((sid, rel, lost, mode, int(lrows), int(rrows), lmax, rmax))
            if int(lrows) > int(rrows):
                refused.append((sid, rel, f"LOCAL IS AHEAD ({int(lrows):,} vs {int(rrows):,}) "
                                          f"— merge, never a sync-down"))
                raise _Skip()
            for attempt in range(6):
                try:
                    os.replace(tmp, p)
                    wrote = True
                    break
                except PermissionError:
                    time.sleep(1.5 * (attempt + 1))
                except Exception as e:                                # noqa: BLE001
                    failed.append((sid, rel, f"{type(e).__name__}: {str(e)[:60]}"))
                    break
        except _Skip:
            refused_here = True
        finally:
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass
        if wrote:
            ok.append((sid, rel))
        elif not refused_here and not any(r[1] == rel for r in failed):
            failed.append((sid, rel, "locked after 6 attempts"))
        if i % 100 == 0:
            print(f"  ... {i:,}/{len(rows):,}  synced {len(ok):,}  refused {len(refused):,}  "
                  f"failed {len(failed):,}", flush=True)

    print(f"\nsynced  {len(ok):,}\nrefused {len(refused):,}\nfailed  {len(failed):,}")
    for sid, rel, why in refused[:20]:
        print(f"  REFUSED {sid}/{rel}: {why}")
    for sid, rel, why in failed[:20]:
        print(f"  FAILED  {sid}/{rel}: {why}")

    with open(a.out_ok, "w", encoding="utf-8") as f:
        for sid, rel in ok:
            f.write(f"{sid}\t{rel}\n")
    # The NON-successes get a file too. A refusal list that lives in terminal scrollback does
    # not exist, and 233 of them did exactly that (R551).
    rpath = a.out_ok.replace(".tsv", "_refused.tsv")
    with open(rpath, "w", encoding="utf-8") as f:
        f.write("source\trelpath\treason\n")
        for sid, rel, why in refused + [(s, r, w) for s, r, w in failed]:
            f.write(f"{sid}\t{rel}\t{why}\n")
    print(f"\nwrote {len(ok):,} succeeded -> {a.out_ok}")
    print(f"wrote {len(refused) + len(failed):,} not-repaired -> {rpath}")
    if withdrawn:
        wpath = a.out_ok.replace(".tsv", "_withdrawals.tsv")
        with open(wpath, "w", encoding="utf-8") as f:
            f.write("source\trelpath\tidentities_withdrawn\tcompared_on\tlocal_rows\tr2_rows"
                    "\tlocal_max\tr2_max\n")
            for row in withdrawn:
                f.write("\t".join(str(x) for x in row) + "\n")
        print(f"PUBLISHER WITHDRAWALS: {len(withdrawn):,} file(s) lost "
              f"{sum(r[2] for r in withdrawn):,} identities upstream -> {wpath}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
