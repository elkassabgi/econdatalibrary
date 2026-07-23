"""Delete `polity` from R2 -- and ONLY polity, of the 15 sources holding residue.

Why polity and not the other 14: the canonical verbatim audit gives polity a direct row --
`permission_required` -> "RESTRICTED (keep gated)" -- so it is data we are not permitted to
re-host, and Ahmed's standing rule is delete, not gate. The other 14 have NO row in the audit at
all: they are UNASSESSED, not proven prohibited, and several (UNESCO = CC BY-SA, WHO GHO =
CC BY-NC-SA 3.0 IGO) look positively redistributable. Deleting unassessed data would destroy the
option to publish it once assessed, which is the opposite of the goal.

Archive first (same contract as the ten): every PRIMARY object md5-verified on disk before any
delete. The derived CSVs are regenerable from that parquet.
"""
import sys, hashlib, os, shutil, urllib.parse
sys.path.insert(0, r"D:/research/econfindatalibrary")
from core import r2_util  # noqa

BUCKET = "econ-data"
LOCAL_ROOT = r"D:/research/econfindatalibrary/data"
read = r2_util.client()
write = r2_util.client(write=True)


def walk(prefix):
    tok = None
    while True:
        kw = dict(Bucket=BUCKET, Prefix=prefix, MaxKeys=1000)
        if tok:
            kw["ContinuationToken"] = tok
        r = read.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            yield o["Key"], o["Size"]
        if not r.get("IsTruncated"):
            return
        tok = r.get("NextContinuationToken")


def md5(p):
    h = hashlib.md5()
    with open(p, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


print("=== 1. archive PRIMARY objects ===")
prim = list(walk("clean_full/polity/"))
ok = 0
for key, size in prim:
    dest = os.path.join(LOCAL_ROOT, key.replace("/", os.sep))
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    etag = read.head_object(Bucket=BUCKET, Key=key)["ETag"].strip('"')
    if os.path.exists(dest) and "-" not in etag and md5(dest) == etag:
        print(f"  IDENTICAL {key}")
        ok += 1
        continue
    if os.path.exists(dest) and not os.path.exists(dest + ".stale"):
        shutil.copy2(dest, dest + ".stale")
    tmp = dest + ".dl"
    read.download_file(BUCKET, key, tmp)
    good = (md5(tmp) == etag) if "-" not in etag else (os.path.getsize(tmp) == size)
    if good:
        os.replace(tmp, dest)
        ok += 1
        print(f"  FETCHED   {key}")
    else:
        os.remove(tmp)
        print(f"  *** FAIL  {key}")
print(f"  archived {ok}/{len(prim)}")
if ok != len(prim):
    raise SystemExit("archive incomplete -- refusing to delete")

print("\n=== 2. delete from R2 ===")
keys = [k for k, _ in walk("clean_full/polity/")]
keys += [k for k, _ in walk("clean_grouped/polity/")]
keys += [k for k, _ in walk("series/polity%3A")]
keys += [k for k, _ in walk("series/polity:")]
keys = sorted(set(keys))


def safe(k):
    d = urllib.parse.unquote(k)
    return (d.startswith(("clean_full/polity/", "clean_grouped/polity/"))
            or (d.startswith("series/") and d.split("/", 1)[1].startswith("polity:")))


bad = [k for k in keys if not safe(k)]
if bad:
    raise SystemExit(f"boundary check failed: {bad[:5]}")

deleted = 0
for i in range(0, len(keys), 1000):
    batch = keys[i:i + 1000]
    assert all(safe(k) for k in batch)
    r = write.delete_objects(Bucket=BUCKET,
                             Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True})
    errs = r.get("Errors", [])
    deleted += len(batch) - len(errs)
    for e in errs[:5]:
        print("  ERROR", e)
print(f"  deleted {deleted:,} of {len(keys):,}")

print("\n=== 3. verify zero residue ===")
n = sum(1 for _ in walk("clean_full/polity/")) + sum(1 for _ in walk("clean_grouped/polity/")) \
    + sum(1 for _ in walk("series/polity%3A"))
print(f"  polity objects remaining: {n}", "CLEAN" if n == 0 else "*** STILL PRESENT ***")
print(f"  local archive: {sorted(os.listdir(os.path.join(LOCAL_ROOT, 'clean_full', 'polity')))}")
