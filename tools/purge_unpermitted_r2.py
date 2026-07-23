"""Delete from R2 every source we may not host, or asked to host and never heard back about.

Ahmed's ruling (2026-07-23): permission emails went out ~2026-07-08. Two weeks of silence is a NO.
Refusal and non-response are treated identically -- DELETE. Gating is not compliance. And deletion
is recoverable: every one of these has an ingest script and a public upstream, so a mistake costs a
re-crawl, while hosting without permission is real legal exposure.

Fifteen sources, 14,469 objects, 1.24 GB. All are gated with 0 catalog series, so nothing that is
being served is touched.

Still archives the PRIMARY parquet first -- cheap insurance, and it means a re-publish after a late
grant does not even need a re-crawl. Derived CSVs are not archived (regenerable from the parquet).

Safety: every prefix is TERMINATED (`fred/`, `fred%3A`) so a sibling id sharing a name prefix can
never be swept in; re-asserted on every batch immediately before the delete call.
"""
import sys, os, hashlib, shutil, urllib.parse, collections

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core import r2_util  # noqa

BUCKET = "econ-data"
LOCAL_ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

# The 15 holding R2 residue: gated, 0 catalog series, no permission (refused, silent, or unassessed).
TARGETS = ["fred", "gus", "ibge", "ine_spain", "norgesbank", "polity", "qog",
           "unesco_natmon", "unesco_sci", "unesco_sdg", "unicef", "unsdg",
           "vdem", "who_gho", "wid"]
# Ids that share a name prefix with a target but are NOT targets.
GUARD = ["sipri_polity", "fred_releases", "unesco_clte", "unesco_cltt", "unesco_dem",
         "unesco_film", "unesco_inno", "imf_unsdg_imf_inputs"]

MODE = sys.argv[1] if len(sys.argv) > 1 else "all"   # archive | purge | all

read = r2_util.client()
write = r2_util.client(write=True) if MODE in ("purge", "all") else None


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


def prefixes(src):
    return [f"clean_full/{src}/", f"clean_grouped/{src}/",
            f"series/{src}%3A", f"series/{src}:"]


def safe(key, src):
    """Deletable only if the key belongs to `src` at a terminated boundary."""
    d = urllib.parse.unquote(key)
    for g in GUARD:
        if g != src and (d.startswith(f"clean_full/{g}/") or d.startswith(f"clean_grouped/{g}/")
                         or (d.startswith("series/") and d.split("/", 1)[1].startswith(f"{g}:"))):
            return False
    if d.startswith(f"clean_full/{src}/") or d.startswith(f"clean_grouped/{src}/"):
        return True
    return d.startswith("series/") and d.split("/", 1)[1].startswith(f"{src}:")


print("=" * 78)
print(f"MODE={MODE}")
print("STEP 1 - archive PRIMARY parquet (download only what is missing or mismatched)")
print("=" * 78)
arch_ok = arch_dl = 0
arch_fail = []
for src in TARGETS:
    prim = list(walk(f"clean_full/{src}/"))
    for key, size in prim:
        dest = os.path.join(LOCAL_ROOT, key.replace("/", os.sep))
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        etag = read.head_object(Bucket=BUCKET, Key=key)["ETag"].strip('"')
        simple = "-" not in etag
        if os.path.exists(dest) and ((simple and md5(dest) == etag)
                                     or (not simple and os.path.getsize(dest) == size)):
            arch_ok += 1
            continue
        if os.path.exists(dest) and not os.path.exists(dest + ".stale"):
            shutil.copy2(dest, dest + ".stale")
        tmp = dest + ".dl"
        read.download_file(BUCKET, key, tmp)
        good = (md5(tmp) == etag) if simple else (os.path.getsize(tmp) == size)
        if good:
            os.replace(tmp, dest)
            arch_ok += 1
            arch_dl += 1
        else:
            os.remove(tmp)
            arch_fail.append(key)
    print(f"  {src:16} primary={len(prim):5,}  archived-ok so far={arch_ok:,}")
print(f"\n  archived {arch_ok:,} primary objects ({arch_dl:,} newly downloaded), "
      f"{len(arch_fail)} failure(s)")
if arch_fail:
    for k in arch_fail[:10]:
        print("    FAIL", k)
    raise SystemExit("archive incomplete -- refusing to delete")

if MODE == "archive":
    print("\n  archive-only mode: R2 untouched. Re-run with `purge` to remove them.")
    raise SystemExit(0)

print("\n" + "=" * 78)
print("STEP 2 - delete from R2")
print("=" * 78)
deleted = collections.Counter()
errors = []
for src in TARGETS:
    keys = sorted({k for p in prefixes(src) for k, _ in walk(p)})
    bad = [k for k in keys if not safe(k, src)]
    if bad:
        print(f"  *** ABORT {src}: {len(bad)} key(s) failed the boundary check: {bad[:3]}")
        errors.append((src, "boundary"))
        continue
    for i in range(0, len(keys), 1000):
        batch = keys[i:i + 1000]
        assert all(safe(k, src) for k in batch), "batch boundary re-check failed"
        r = write.delete_objects(Bucket=BUCKET,
                                 Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True})
        errs = r.get("Errors", [])
        errors += [(src, e) for e in errs]
        deleted[src] += len(batch) - len(errs)
    print(f"  {src:16} deleted {deleted[src]:7,}")
print(f"\n  TOTAL DELETED: {sum(deleted.values()):,}   errors: {len(errors)}")
for e in errors[:10]:
    print("   ", e)

print("\n" + "=" * 78)
print("STEP 3 - verify zero residue, and that guarded siblings survived")
print("=" * 78)
resid = 0
for src in TARGETS:
    n = sum(1 for p in prefixes(src) for _ in walk(p))
    resid += n
    print(f"  {src:16} {n}")
print(f"\n  residual: {resid}", "CLEAN" if resid == 0 else "*** STILL PRESENT ***")
for g in GUARD:
    n = sum(1 for _ in walk(f"clean_full/{g}/")) + sum(1 for _ in walk(f"series/{g}%3A"))
    print(f"  guard {g:24} objects: {n}")
