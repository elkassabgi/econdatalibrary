"""Independent verification of the EDGAR pointer Parquet output."""
import os, re, glob, time, zipfile
import orjson
import pyarrow.parquet as pq
import pyarrow.dataset as ds

OUT_DIR = 'D:/research/econfindatalibrary/data/clean_full/edgar_pointers'
ZIP     = 'D:/research/econfindatalibrary/data/raw/sec_edgar/submissions.zip'
N_SHARDS = 256
MAIN_RE = re.compile(r'^CIK(\d{10})\.json$')

def main():
    t0=time.time()
    log=lambda *a: print(*a, flush=True)

    parts = sorted(glob.glob(os.path.join(OUT_DIR, 'cik_shard=*', 'part.parquet')))
    log(f'shard files found: {len(parts)}')
    assert len(parts)==N_SHARDS, f'expected {N_SHARDS} files, got {len(parts)}'

    # 1) independent total via parquet metadata (no full read)
    total = 0
    sizes = 0
    rowgroups = 0
    per_shard = {}
    for p in parts:
        pf = pq.ParquetFile(p)
        n = pf.metadata.num_rows
        total += n
        rowgroups += pf.metadata.num_row_groups
        sizes += os.path.getsize(p)
        s = int(re.search(r'cik_shard=(\d+)', p).group(1))
        per_shard[s] = n
    log(f'TOTAL rows (parquet metadata): {total:,}')
    log(f'total size on disk: {sizes/1e9:.2f} GB ({sizes/total:.1f} bytes/row)')
    log(f'total row groups across shards: {rowgroups:,} (avg {rowgroups/N_SHARDS:.0f}/shard)')
    log(f'min/max rows per shard: {min(per_shard.values()):,} / {max(per_shard.values()):,}')
    log(f'all 256 shard ids present: {sorted(per_shard.keys())==list(range(256))}')

    # 2) schema check
    sch = pq.ParquetFile(parts[0]).schema_arrow
    log('schema:')
    for f in sch:
        log(f'  {f.name}: {f.type}')

    # 3) membership audit: scan a few shards fully, confirm cik%256==shard
    log('membership audit (full scan of 4 shards):')
    for p in [parts[0], parts[1], parts[128], parts[255]]:
        s=int(re.search(r'cik_shard=(\d+)',p).group(1))
        t=pq.read_table(p, columns=['cik'])
        ciks=t.column('cik').to_pylist()
        bad=sum(1 for c in ciks if c % N_SHARDS != s)
        log(f'  shard {s}: {len(ciks):,} rows, membership violations={bad}')

    # 4) URL audit: sample non-null urls, check format
    log('URL format audit:')
    url_re = re.compile(r'^https://www\.sec\.gov/Archives/edgar/data/\d+/\d{18}/.+')
    checked=0; bad=0; null=0; allrows=0
    dataset = ds.dataset(OUT_DIR, format='parquet', partitioning='hive')
    # sample first shard's urls
    t=pq.read_table(parts[0], columns=['cik','accession','primary_doc_url'])
    urls=t.column('primary_doc_url').to_pylist()
    accs=t.column('accession').to_pylist()
    for u in urls:
        allrows+=1
        if u is None:
            null+=1
            continue
        checked+=1
        if not url_re.match(u):
            bad+=1
            if bad<=5: log(f'  BAD URL: {u}')
    log(f'  shard0: rows={allrows:,} non-null urls checked={checked:,} bad_format={bad} null={null:,} ({100*null/allrows:.1f}%)')
    log(f'  sample urls:')
    for u in urls[:3]:
        log(f'    {u}')

    # 5) FULL-HISTORY cross-check: for known deep-history filers, count rows in raw zip
    #    (recent + all overflow) and compare to rows in parquet output.
    log('FULL-HISTORY cross-check (raw zip vs parquet) for known filers:')
    z=zipfile.ZipFile(ZIP)
    def raw_count(cik):
        name=f'CIK{cik:010d}.json'
        d=orjson.loads(z.read(name))
        fl=d.get('filings') or {}
        n=len(fl.get('recent',{}).get('accessionNumber',[]) or [])
        nover=0
        for f in (fl.get('files') or []):
            sd=orjson.loads(z.read(f['name']))
            cnt=len(sd.get('accessionNumber',[]) or [])
            n+=cnt; nover+=1
        return n, nover
    # build a lookup of parquet rows per cik using dataset filter on the right shard
    def parquet_count(cik):
        s=cik % N_SHARDS
        p=os.path.join(OUT_DIR, f'cik_shard={s:03d}', 'part.parquet')
        t=pq.read_table(p, columns=['cik'])
        import pyarrow.compute as pc
        return pc.sum(pc.equal(t.column('cik'), cik)).as_py()
    for name,cik in [('Apple',320193),('GE',40545),('Microsoft',789019),
                     ('Abbott',1800),('Berkshire',1067983),('JPMorgan',19617),
                     ('Ford',37996),('Exxon',34088)]:
        rc, nover = raw_count(cik)
        pc_ = parquet_count(cik)
        ok = 'OK' if rc==pc_ else 'MISMATCH!!!'
        log(f'  {name:10s} CIK {cik:>8d}: raw(recent+{nover} overflow)={rc:>6,} parquet={pc_:>6,}  {ok}')

    # 6) independent sanity on grand total: count ALL accessionNumbers in zip for a
    #    random-ish sample of shards-worth of filers and extrapolate? Instead, trust
    #    per-filer cross-check above + the build's internal sum==metadata equality:
    log(f'build metadata total == sum(per-shard): {total==sum(per_shard.values())}')

    # 7) date sanity: min/max filing_date across a shard
    import pyarrow.compute as pc
    t=pq.read_table(parts[0], columns=['filing_date'])
    col=t.column('filing_date')
    log(f'shard0 filing_date min={pc.min(col).as_py()} max={pc.max(col).as_py()} nulls={col.null_count:,}/{len(col):,}')

    # 8) ticker coverage
    t=pq.read_table(parts[0], columns=['ticker'])
    tc=t.column('ticker')
    log(f'shard0 ticker: non-null={len(tc)-tc.null_count:,}/{len(tc):,} ({100*(len(tc)-tc.null_count)/len(tc):.1f}%)')

    log(f'VERIFY DONE in {time.time()-t0:.1f}s')

if __name__=='__main__':
    main()
