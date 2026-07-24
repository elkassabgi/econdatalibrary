"""Refresh the updater's coherence reference: upload the curated local catalog.db to R2.

The CI pulls `_aqueduct/catalog.db.zst` read-only and the derive/coherence step maps each
changed store series_key to a catalog series_id. That R2 copy is STALE: it lacks the 13 sources
catalogued since (ssb/scb/stat_estonia/bfs/dst/statfin/hagstofa/stat_latvia/stat_slovenia + the
IEP set) and still carries the ~20 purged/gated sources (wto_hs_a_*, cow, irena, polity, sipri…).
That staleness is exactly why bfs/stat_estonia/ssb/stat_latvia demote to "csv coherence unmet".

Verified safe before writing: the 4 non-scb live sources are byte-identical local vs R2, scb only
GAINS its 2,550 series, and every source R2 has that local lacks is gated/purged (not hosted).
Reversible: the current R2 object is copied to a dated .bak key first.
"""
import io, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core import r2_util
import zstandard

BUCKET = "econ-data"
KEY = "_aqueduct/catalog.db.zst"
LOCAL = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "catalog.db")
STAMP = sys.argv[1] if len(sys.argv) > 1 else "manual"   # pass a date stamp; no Date.now in scripts

c = r2_util.client(write=True)

# 1. backup the current R2 object (reversibility)
cur = c.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
bak = f"{KEY}.bak-{STAMP}"
c.put_object(Bucket=BUCKET, Key=bak, Body=cur)
print(f"  backed up current R2 catalog -> {bak}  ({len(cur):,} B)")

# 2. compress local catalog.db
with open(LOCAL, "rb") as f:
    raw = f.read()
z = zstandard.ZstdCompressor(level=10).compress(raw)
print(f"  local catalog.db {len(raw):,} B -> zst {len(z):,} B")

# 3. upload (single PUT; ~0.5 GB, well under the 5 GB multipart threshold)
c.put_object(Bucket=BUCKET, Key=KEY, Body=z, ContentType="application/zstd")
print(f"  uploaded new {KEY}")

# 4. verify by re-download + decompress + spot-check counts
back = c.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
dctx = zstandard.ZstdDecompressor()
dec = dctx.decompress(back, max_output_size=len(raw) + 1)
tmp = os.path.join(os.environ.get("TEMP", "/tmp"), "verify_catalog.db")
with open(tmp, "wb") as out:
    out.write(dec)
import sqlite3
con = sqlite3.connect(f"file:{tmp}?mode=ro", uri=True)
print("\n  verify re-download:")
for s in ("bcb", "cnb", "frankfurter", "treasury", "scb", "ssb", "stat_estonia", "bfs"):
    n = con.execute("SELECT COUNT(*) FROM series WHERE source_id=?", (s,)).fetchone()[0]
    print(f"    {s:14} {n:>6,}")
for s in ("cow", "sipri", "polity"):
    n = con.execute("SELECT COUNT(*) FROM series WHERE source_id=?", (s,)).fetchone()[0]
    print(f"    purged {s:8} {n}  ({'OK gone' if n == 0 else '*** still present'})")
tot = con.execute("SELECT COUNT(*) FROM series").fetchone()[0]
print(f"  total series now on R2 catalog: {tot:,}")
con.close()
print("\n  DONE. Rollback if needed: copy the .bak key back over the live key.")
