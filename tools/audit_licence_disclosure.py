"""Do the licences we SERVE match what the verbatim audit actually found?

WHY THIS EXISTS. Repairing seven FAO sources' downloadability relabelled 211,924
series as `cc-by-4.0` — commercially usable — while DATABASE_LICENSES_VERBATIM.md
classifies FAO as "redistributable_attribution_noncommercial ... a non-commercial /
anti-endorsement restriction that CC BY 4.0 does not impose". `catalog_complete` copies
the licence from existing rows, so unrelated repair work silently relicensed a
publisher's data, and a later sync carried it to the store users read.

Nothing caught it. A local-vs-D1 diff cannot: it treats one database as ground truth,
and for FAO the local one was the broken side. A diff finds DISAGREEMENT, never SHARED
error. The only authority is the audit.

TWO RULES ARE BUILT IN, both learned the hard way today:

  R119 — query the SERVING store. Three separate times a check run against
         data/catalog.db produced a confident finding that production did not have.
         The most recent reported a live breach on 18,838 idb series that D1 had
         right all along. This tool reads D1 by default and says so.

  R111 — parse the audit's STRUCTURED classification column, not free text. A first
         version matched "non-commercial" within a +/-1100 character window and
         returned 66 sources / 734,600 series, including sec_edgar (US public domain)
         and harvard_atlas (CC0). Against the table rows the same question yields one
         real answer.

WHAT IT CANNOT DO: confirm that a licence is RIGHT. It only finds served terms that
CONTRADICT a recorded classification. A source absent from the audit is reported
separately — unaudited is not the same as compliant.

Usage:
  python tools/audit_licence_disclosure.py            # read D1 (correct; needs wrangler)
  python tools/audit_licence_disclosure.py --local    # read catalog.db, clearly labelled
"""
from __future__ import annotations

import argparse
import io
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AUDIT = os.path.join(ROOT, "DATABASE_LICENSES_VERBATIM.md")
WORKER = os.path.join(ROOT, "api", "worker")

# The audit uses MORE THAN ONE table layout, and assuming one produced a phantom gap:
#   | `sid` | publisher | classification | status | action |          (original audit)
#   | sid   | licence    | CLEARED (...)  | "quote" | url    |          (later additions)
# Matching only the first reported 23 (source, licence) pairs / 495,878 series as
# "never audited" — including harvard_atlas, ksh_stadat and gapminder, every one of
# which is present with a verbatim quote and a CLEARED status in the second layout.
# So: accept the id with or without backticks, and keep the WHOLE row as the text to
# classify, rather than betting on which column holds the verdict (R112 — a matcher
# that assumes structure invents findings when the structure varies).
ROW = re.compile(r"^\|\s*`?([a-z0-9_]+)`?\s*\|(.*)$", re.M)
# MATCH THE CLASSIFICATION TOKEN, NEVER THE PROSE. The audit's rows argue ABOUT
# licence clauses, including refutations, so a bare keyword search inverts the meaning:
#   ksh_stadat — "The CC BY-NC carve-out on the same page is SCOPED and does not reach
#                 STADAT ... plain CC BY 4.0 governs what we host"   -> matched "NonCommercial"
#   dst        — "can be used free of charge commercially as well as non-commercially"
#                                                                    -> matched "non-commercially"
# Both were reported as violations while saying the opposite. The machine-readable
# signal is the snake_case classification (`redistributable_attribution_noncommercial`,
# `noncommercial_sharealike`, `noncommercial_no_derivatives`) or an explicit
# "CLEARED (NC ...)" verdict — never a hyphenated English phrase inside a quotation.
NC = re.compile(r"(?:^|_)noncommercial(?:_|\b)|CLEARED\s*\(\s*NC\b|"
                r"\(non-commercial,", re.I)
ND = re.compile(r"(?:^|_)no_derivatives(?:_|\b)|noncommercial_no_derivatives|"
                r"\bCC BY-NC-ND\b", re.I)
NOREDIST = re.compile(r"(?:^|_)non_redistributable(?:_|\b)|not-freely-redistributable|"
                      r"(?:^|_)no_open_redistribution(?:_|\b)", re.I)


def classifications():
    if not os.path.exists(AUDIT):
        return {}
    txt = io.open(AUDIT, encoding="utf-8").read()
    out = {}
    for m in ROW.finditer(txt):
        sid, rest = m.group(1), m.group(2)
        # Keep the LONGEST row seen for an id — the audit sometimes lists a source in a
        # short index table and again in a detailed one; the detailed row is the verdict.
        if len(rest) > len(out.get(sid, "")):
            out[sid] = rest
    return out


TRAIL = os.path.join(ROOT, "REDISTRIBUTION_EMAIL_TRAIL.md")
DENYGEN = os.path.join(ROOT, "core", "gen_denylist.py")


def granted():
    """Publisher keys with WRITTEN redistribution permission on file.

    An audit classification is the PUBLISHER'S DEFAULT terms. A later written grant
    overrides it, and several sources are hosted on exactly that basis — Bundesbank
    ("distribute/reproduce OK free of charge, unaltered, exact credit", inquiry
    2026/005812, 2026-07-15), IDB, kof_globalization, comtrade.

    Without this, the tool reports every one of them as a contradiction forever. An
    alarm that cannot be cleared by correct behaviour is worse than no alarm: it
    trains the reader to skip the whole report, which is how a real breach gets
    missed. Same lesson as the health gate's unclearable RED-DATA earlier today.

    Deliberately NOT authoritative on its own — it lowers a finding from CONTRADICTION
    to "permission on file, verify the conditions are honoured", it never hides one.
    """
    keys = set()
    for path in (TRAIL, DENYGEN):
        if not os.path.exists(path):
            continue
        txt = io.open(path, encoding="utf-8", errors="replace").read()
        for line in txt.splitlines():
            if re.search(r"GRANTED|GRANTED_EXCEPTIONS|written permission", line, re.I):
                for tok in re.findall(r"[A-Za-z][A-Za-z0-9_\- ]{3,40}", line):
                    keys.add(tok.strip().lower())
    return keys


def has_permission(sid, name, perm_keys):
    if sid.lower() in perm_keys:
        return True
    n = (name or "").strip().lower()
    if len(n) >= 6 and any(n in k or k in n for k in perm_keys if len(k) >= 6):
        return True
    return False


def served_from_d1():
    """(source_id, license_id, n, commercial_ok, no_modify, reservable) from D1."""
    sql = ("SELECT se.source_id, se.license_id, COUNT(*) n, "
           "COALESCE(l.commercial_ok,-1) c, COALESCE(l.no_modify,-1) nm, "
           "COALESCE(l.reservable,-1) r "
           "FROM series se LEFT JOIN license l ON l.license_id=se.license_id "
           "GROUP BY se.source_id, se.license_id")
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    from core import d1_remote                                           # noqa: PLC0415 - plan step 1
    try:
        rows, _ = d1_remote.rows("econ-catalog", sql)
    except RuntimeError as e:
        print("could not read D1 (is wrangler authenticated?). "
              "Re-run with --local, understanding it is NOT what users see.")
        print(str(e)[-400:])
        return None
    return [(r["source_id"], r["license_id"], r["n"], r["c"], r["nm"], r["r"])
            for r in rows]


def served_from_local():
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    from core import catalog_path                                        # noqa: PLC0415 - plan step 1
    con = catalog_path.connect()
    return list(con.execute(
        "SELECT se.source_id, se.license_id, COUNT(*), COALESCE(l.commercial_ok,-1), "
        "COALESCE(l.no_modify,-1), COALESCE(l.reservable,-1) "
        "FROM series se LEFT JOIN license l ON l.license_id=se.license_id "
        "GROUP BY se.source_id, se.license_id"))


def served_from_build(chunk: int = 200_000):
    """AFTER T0: served_from_local's rows, read from the LIVE build in primary-key chunks (R1243). One GROUP BY
    over ~14M rows holds a read lock for its whole run, and the build keeps a rollback journal, so the updater's
    writes wait behind it (R715/R721). Each chunk's read ends before the next starts, so a writer gets in between;
    the aggregation is done here. Every row is read exactly once (series_id is the primary key)."""
    import collections                                                   # noqa: PLC0415
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    from core import catalog_path                                        # noqa: PLC0415
    con = catalog_path.connect()
    try:
        lic = {r[0]: r[1:] for r in con.execute(
            "SELECT license_id, commercial_ok, no_modify, reservable FROM license")}
        counts: collections.Counter = collections.Counter(
            catalog_path.iter_series(con, ("source_id", "license_id"), chunk=chunk))
    finally:
        con.close()
    out = []
    for (src, lid), n in counts.items():
        vals = lic.get(lid, (None, None, None))
        out.append((src, lid, n, *(-1 if v is None else v for v in vals)))   # COALESCE(l.x, -1), as the JOIN did
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--local", action="store_true",
                    help="read the local catalogue instead of D1 — NOT what users see")
    a = ap.parse_args()

    cls = classifications()
    if not cls:
        print("no structured classifications found in DATABASE_LICENSES_VERBATIM.md")
        return 2
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    from core import cutover                                             # noqa: PLC0415
    # AFTER T0 (plan step 6d) the origin serves copies of the catalogue BUILD made at each swap (D1 is frozen). The
    # build is read here - in chunks, so the updater is not blocked - and between swaps it can be AHEAD of what is
    # served; the label says so (R1243)
    selfhosted = cutover.is_cut_over()
    served = (served_from_build() if selfhosted else served_from_local() if a.local else served_from_d1())
    if served is None:
        return 2
    where = ("the catalogue BUILD (the origin serves the copy made at the last swap; a source changed since then "
             "reaches users at the next swap)" if selfhosted
             else "the LOCAL catalogue (NOT the serving store)" if a.local else "D1 (serving store)")
    print(f"audit classifications: {len(cls)}   |   read from: {where}")
    print()

    perm = granted()
    names = {}
    try:
        if ROOT not in sys.path:
            sys.path.insert(0, ROOT)
        from core import catalog_path                                    # noqa: PLC0415 - plan step 1
        _c = catalog_path.connect()
        names = {r[0]: r[1] for r in _c.execute("SELECT source_id, name FROM source")}
    except Exception:                                         # noqa: BLE001
        pass
    contra, unaudited, permitted = [], [], []
    for sid, lid, n, comm, nomod, res in served:
        c = cls.get(sid)
        if not c:
            unaudited.append((sid, lid, n))
            continue
        why = None
        if comm == 1 and NC.search(c):
            why = "served COMMERCIAL-OK; audit says non-commercial"
        elif nomod == 0 and ND.search(c):
            why = "served modifiable; audit says no-derivatives"
        elif res == 1 and NOREDIST.search(c):
            why = "served as redistributable; audit says NOT"
        if why is None:
            continue
        # A written grant overrides the publisher's default terms. Downgrade, never hide.
        if has_permission(sid, names.get(sid), perm):
            permitted.append((sid, lid, n, why))
        else:
            contra.append((sid, lid, n, why))

    print(f"CONTRADICTIONS (served terms conflict with the audit): {len(contra)}")
    for sid, lid, n, why in sorted(contra, key=lambda x: -x[2]):
        print(f"   {sid:<18} {str(lid)[:22]:<22} {n:>9,}  {why}")
    if not contra:
        print("   none")
    print()
    if permitted:
        print(f"AUDIT DEFAULT OVERRIDDEN BY WRITTEN PERMISSION: {len(permitted)} — verify "
              f"the granted CONDITIONS are what we serve, then move on")
        for sid, lid, n, why in sorted(permitted, key=lambda x: -x[2]):
            print(f"   {sid:<18} {str(lid)[:22]:<22} {n:>9,}  {why}")
        print()
    print(f"SERVED BUT NOT IN THE AUDIT: {len(unaudited)} (source, licence) pair(s), "
          f"{sum(x[2] for x in unaudited):,} series")
    for sid, lid, n in sorted(unaudited, key=lambda x: -x[2])[:10]:
        print(f"   {sid:<18} {str(lid)[:22]:<22} {n:>9,}")
    if len(unaudited) > 10:
        print(f"   ... +{len(unaudited)-10} more")
    print()
    print("Unaudited is NOT a pass — it means nobody has read that publisher's terms. "
          "A licence flag is a column somebody set; the audit is the evidence (R113).")
    return 1 if contra else 0


if __name__ == "__main__":
    sys.exit(main())
