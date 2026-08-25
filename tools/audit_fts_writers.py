#!/usr/bin/env python3
"""Which files can duplicate series_fts, and which cannot.

FTS5 has no unique constraint, so an INSERT into series_fts that is not preceded by a DELETE of
the same ids adds a copy every time it runs. Two such writers are now fixed
(core/sync_catalog_d1.py, tools/catalog_complete.py). This finds the rest, because a third
unfixed writer would put the duplication straight back after any remediation.

TWO SEVERITIES, and the distinction is what matters:

  UNBOUNDED  the writer re-inserts a WHOLE source on every run. core/sync_catalog_d1.py did
             this and is the source of the fleet-wide ratios - boc 8.00x, cepii_gravity every
             id >=3 copies plus exactly 50,000 with a 4th, global 2.31x. Fixed.
  BOUNDED    the writer inserts only rows NOT already catalogued, so it can add at most ONE
             extra copy, and only for an id deleted from `series` and re-catalogued.
             tools/catalog_complete.py was this shape - damodaran's 384 ids at exactly 2.00
             rows each. Fixed. Eighteen sibling cataloguers still carry it; verified bounded
             by reading catalog_table_grain.py and refresh_sec_edgar.py, which insert per new
             row only.

The DROP-TABLE case below exists because the first version of this audit reported
core/export_d1.py as a duplication source. It DROPs and recreates the table, which is a
rebuild. Check the instrument before reporting the count.

Classification is by what precedes each INSERT in the same function, not by grep alone:
  SAFE-DELETE   a DELETE FROM series_fts appears before the INSERT in the same function
  SAFE-REBUILD  the whole table is emptied (DELETE FROM series_fts with no WHERE) first
  RISK          an INSERT with neither
"""
import io
import os
import re

ROOT = r"E:\research\econfindatalibrary"
INSERT = re.compile(r"INSERT(?:\s+OR\s+\w+)?\s+INTO\s+series_fts", re.I)
DELETE = re.compile(r"DELETE\s+FROM\s+series_fts", re.I)
DELETE_ALL = re.compile(r"DELETE\s+FROM\s+series_fts\s*(?:;|\"|')", re.I)
# A DROP TABLE + CREATE VIRTUAL TABLE is also a full rebuild and is SAFE. My first pass
# looked only for DELETE and flagged core/export_d1.py as a duplication source when it
# actually drops and recreates the table. Checking before reporting the count.
DROP_FTS = re.compile(r"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?series_fts", re.I)
DEFLINE = re.compile(r"^\s*(?:def|class)\s+\w+")

rows = []
for dirpath, _dn, files in os.walk(ROOT):
    if any(seg in dirpath for seg in (".git", "__pycache__", "node_modules", "\\data\\", "/data/")):
        continue
    for fn in files:
        if not fn.endswith(".py"):
            continue
        p = os.path.join(dirpath, fn)
        try:
            src = io.open(p, encoding="utf-8", errors="replace").read()
        except OSError:
            continue
        if not INSERT.search(src):
            continue
        lines = src.split(chr(10))
        # function boundaries
        bounds = [i for i, l in enumerate(lines) if DEFLINE.match(l)] + [len(lines)]
        verdicts = []
        for i, l in enumerate(lines):
            if not INSERT.search(l):
                continue
            start = max([b for b in bounds if b <= i] or [0])
            end = min([b for b in bounds if b > i] or [len(lines)])
            body = chr(10).join(lines[start:end])
            before = chr(10).join(lines[start:i])
            if DELETE_ALL.search(body) or DROP_FTS.search(body):
                verdicts.append(("SAFE-REBUILD", i + 1))
            elif DELETE.search(before):
                verdicts.append(("SAFE-DELETE", i + 1))
            else:
                verdicts.append(("RISK", i + 1))
        rel = os.path.relpath(p, ROOT).replace(os.sep, "/")
        rows.append((rel, verdicts))

risk = [(f, v) for f, v in rows if any(k == "RISK" for k, _ in v)]
safe = [(f, v) for f, v in rows if not any(k == "RISK" for k, _ in v)]

print("   files writing series_fts: %d   with a RISK insert: %d" % (len(rows), len(risk)))
print()
print("   RISK — an INSERT with no preceding DELETE (each run adds a copy):")
for f, v in sorted(risk):
    marks = ", ".join("L%d" % ln for k, ln in v if k == "RISK")
    print("     %-46s %s" % (f, marks))
print()
print("   SAFE:")
for f, v in sorted(safe):
    kinds = sorted({k for k, _ in v})
    print("     %-46s %s" % (f, "/".join(kinds)))
