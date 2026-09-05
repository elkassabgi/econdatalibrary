"""Byte-verify a random sample of the restored statcan cubes against the local originals.

    python tools/verify_statcan_store_bytes.py [N]

upload_statcan_store.py compares ContentLength after each PUT, which catches a truncated upload but not
a corrupted one. This downloads a random sample and compares MD5 with the local file, and additionally
opens the remote bytes as parquet so a file that transferred intact but is unreadable cannot pass.
Read-only. A control is included: one local file is compared against a DIFFERENT remote object, which
must MISMATCH, so a run in which everything "matches" cannot be the comparison silently succeeding."""
import hashlib
import io
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from core import r2_util   # noqa: E402

BUCKET = "econ-data"
PREFIX = "clean_full/statcan/"
LOCAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "clean_full", "statcan")
N = int(sys.argv[1]) if len(sys.argv) > 1 else 12


def md5(b):
    return hashlib.md5(b).hexdigest()


c = r2_util.client()
remote = {}
tok = None
while True:
    kw = {"Bucket": BUCKET, "Prefix": PREFIX}
    if tok:
        kw["ContinuationToken"] = tok
    p = c.list_objects_v2(**kw)
    for o in p.get("Contents", []):
        remote[o["Key"].split("/")[-1]] = o["Size"]
    if not p.get("IsTruncated"):
        break
    tok = p.get("NextContinuationToken")
print(f"objects under {PREFIX}: {len(remote):,}")

# EVERY file's size, not just the sample's. This is one listing plus a local stat each, so it costs
# nothing, and it is the difference between "10 files are fine" and "no file is truncated or missing".
local_names = {os.path.basename(p): os.path.getsize(p)
               for p in __import__("glob").glob(os.path.join(LOCAL, "*.parquet"))
               if not os.path.basename(p).startswith("_")}
missing = sorted(n for n in local_names if n not in remote)
sized = sorted(n for n, s in local_names.items() if n in remote and remote[n] != s)
extra = sorted(n for n in remote if n.endswith(".parquet") and n not in local_names)
print(f"  local cubes {len(local_names):,}   on R2 {sum(1 for n in remote if n.endswith('.parquet')):,}")
print(f"  MISSING from R2      : {len(missing):,}" + (f"   e.g. {missing[:5]}" if missing else ""))
print(f"  SIZE MISMATCH        : {len(sized):,}" + (f"   e.g. {sized[:5]}" if sized else ""))
print(f"  on R2 with no local  : {len(extra):,}" + (f"   e.g. {extra[:5]}" if extra else ""))
if missing or sized:
    print("  ^ the restore is INCOMPLETE — re-run tools/upload_statcan_store.py (it resumes)")

# sample from the SMALLER half so the check is cheap; the giants are size-verified at upload
cands = sorted((s, n) for n, s in remote.items() if n.endswith(".parquet") and s < 20_000_000)
pick = random.sample(cands, min(N, len(cands)))
import pyarrow.parquet as pq

ok = bad = 0
for size, name in pick:
    lp = os.path.join(LOCAL, name)
    if not os.path.exists(lp):
        print(f"  {name}: no local original"); bad += 1; continue
    body = c.get_object(Bucket=BUCKET, Key=PREFIX + name)["Body"].read()
    lb = open(lp, "rb").read()
    same = md5(body) == md5(lb)
    try:
        nrows = pq.ParquetFile(io.BytesIO(body)).metadata.num_rows
        readable = True
    except Exception as e:                                        # noqa: BLE001
        nrows, readable = f"{type(e).__name__}", False
    print(f"  {name:22} {size:>10,} B  md5 {'match' if same else 'DIFFER'}  parquet {'ok' if readable else 'UNREADABLE'}  rows {nrows}")
    if same and readable:
        ok += 1
    else:
        bad += 1

# the control: a local file compared against a DIFFERENT remote object must differ
if len(pick) >= 2:
    a_name, b_name = pick[0][1], pick[1][1]
    body = c.get_object(Bucket=BUCKET, Key=PREFIX + b_name)["Body"].read()
    lb = open(os.path.join(LOCAL, a_name), "rb").read()
    ctrl = md5(body) != md5(lb)
    print(f"\ncontrol ({a_name} local vs {b_name} remote): {'DIFFER as required' if ctrl else 'MATCHED — the comparison is broken'}")
    if not ctrl:
        bad += 1

print(f"\n{ok} verified byte-identical and readable, {bad} problem(s)")
sys.exit(1 if bad else 0)
