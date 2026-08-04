"""Dry-run validation of the EDGAR pointer build on a small slice -> temp dir."""
import os, re, time, zipfile, shutil
import orjson
import pyarrow as pa
import pyarrow.parquet as pq

import edgar_pointers_build as B

LIMIT = 6000

# Repo root derived from this file, never a drive letter. The store moved D: -> E: in
# the workstation cutover; a stale root here silently writes into, or reports on, a
# tree that is not there. R330.
def _RD(*parts):
    _r = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(_r, *parts) if parts else _r

TEST_OUT = _RD('data', '_edgar_test')

def main():
    t0 = time.time()
    if os.path.exists(TEST_OUT):
        shutil.rmtree(TEST_OUT)
    os.makedirs(TEST_OUT, exist_ok=True)
    tmap = B.load_ticker_map()
    z = zipfile.ZipFile(B.ZIP)
    infos = z.infolist()
    mains = [i for i in infos if B.MAIN_RE.match(i.filename)][:LIMIT]
    print(f'testing on {len(mains)} mains', flush=True)

    writers = []
    for s in range(B.N_SHARDS):
        d = os.path.join(TEST_OUT, f'cik_shard={s:03d}')
        os.makedirs(d, exist_ok=True)
        writers.append(pq.ParquetWriter(os.path.join(d, 'part.parquet'), B.SCHEMA,
                       compression='zstd', compression_level=7,
                       use_dictionary=['ticker','form','primary_doc_url'], version='2.6'))
    buf = B.new_buffers(); counts=[0]*B.N_SHARDS; buffered=0; total=0; nover=0
    # capture a few sample rows for manual URL check
    samples=[]
    for info in mains:
        cik_int=int(B.MAIN_RE.match(info.filename).group(1))
        d=orjson.loads(z.read(info))
        tks=d.get('tickers') or []; ticker=tks[0] if tks else tmap.get(cik_int)
        filings=d.get('filings') or {}; rec=filings.get('recent') or {}
        accs=rec.get('accessionNumber')
        if accs:
            r=B.emit_rows(cik_int,ticker,rec.get('form') or [],rec.get('filingDate') or [],accs,rec.get('primaryDocument') or [],buf)
            buffered+=r; total+=r
        for f in (filings.get('files') or []):
            nm=f.get('name')
            if not nm: continue
            sd=orjson.loads(z.read(nm)); nover+=1
            sa=sd.get('accessionNumber')
            if sa:
                r=B.emit_rows(cik_int,ticker,sd.get('form') or [],sd.get('filingDate') or [],sa,sd.get('primaryDocument') or [],buf)
                buffered+=r; total+=r
        if buffered>=B.FLUSH_ROWS:
            B.flush(buf,writers,counts); buffered=0
    B.flush(buf,writers,counts)
    for w in writers: w.close()

    print(f'total rows: {total:,}  sum(counts): {sum(counts):,}  match={total==sum(counts)}', flush=True)
    nonempty=sum(1 for c in counts if c>0)
    print(f'shards with data: {nonempty}/{B.N_SHARDS}', flush=True)
    print(f'overflow read: {nover}', flush=True)

    # read back a couple shards, validate schema + show sample rows + verify shard membership
    import glob
    parts=sorted(glob.glob(os.path.join(TEST_OUT,'cik_shard=*','part.parquet')))
    # total via parquet metadata
    meta_rows=sum(pq.ParquetFile(p).metadata.num_rows for p in parts)
    print(f'parquet metadata total rows: {meta_rows:,}  match={meta_rows==total}', flush=True)

    # validate membership: every row in shard s has cik%256==s ; check 3 shards fully
    for p in parts[:3]:
        s=int(re.search(r'cik_shard=(\d+)',p).group(1))
        t=pq.read_table(p, columns=['cik'])
        ciks=t.column('cik').to_pylist()
        bad=[c for c in ciks if c % B.N_SHARDS != s]
        print(f'  shard {s}: {len(ciks):,} rows, membership bad={len(bad)}', flush=True)

    # print 5 sample full rows from shard containing Apple (320193 % 256)
    appshard=320193 % B.N_SHARDS
    pth=os.path.join(TEST_OUT,f'cik_shard={appshard:03d}','part.parquet')
    if os.path.exists(pth):
        t=pq.read_table(pth)
        df=t.to_pandas()
        ap=df[df['cik']==320193]
        print(f'Apple rows in test slice: {len(ap)}', flush=True)
        if len(ap):
            print(ap.head(3).to_string(), flush=True)
    # show schema + dtypes
    pf=pq.ParquetFile(parts[0])
    print('schema:', pf.schema_arrow, flush=True)
    # null/empty stats on url
    allurl_null=0; allrows=0
    for p in parts:
        t=pq.read_table(p, columns=['primary_doc_url','accession'])
        u=t.column('primary_doc_url'); allrows+=len(u); allurl_null+=u.null_count
    print(f'rows={allrows:,} url null={allurl_null:,} ({100*allurl_null/allrows:.2f}%)', flush=True)

    size=sum(os.path.getsize(p) for p in parts)
    print(f'test output size: {size/1e6:.1f} MB for {total:,} rows -> {size/total:.1f} bytes/row', flush=True)
    print(f'projected full size @415M rows: {size/total*415e6/1e9:.1f} GB', flush=True)
    print(f'test elapsed {time.time()-t0:.1f}s', flush=True)

if __name__=='__main__':
    main()
