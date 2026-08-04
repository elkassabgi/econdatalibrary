"""
Rebuild EDGAR filing pointers — FULL history — as sharded grouped Parquet.

Source : D:/research/econfindatalibrary/data/raw/sec_edgar/submissions.zip
Output : D:/research/econfindatalibrary/data/clean_full/edgar_pointers/cik_shard=NNN/part.parquet  (256 shards)

For EACH filer (CIK##########.json) we emit one row per filing from BOTH:
  - filings.recent  (parallel arrays: form, filingDate, accessionNumber, primaryDocument, ...)
  - every overflow JSON listed in filings.files[]  (CIK##########-submissions-NNN.json, same zip)

Columns: cik (int32), ticker (str|null), form (str), filing_date (date32),
         accession (str), primary_doc_url (str)
primary_doc_url = https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_dashes}/{primaryDocument}

License: us-public-domain
Sharding: cik % 256  -> ~256 files total, each holding MANY filers' rows (NOT one file per filer).

Memory is bounded: 256 column buffers accumulate rows; when the global buffered row
count crosses FLUSH_ROWS, every non-empty shard buffer is written as a Parquet row group
to that shard's already-open ParquetWriter, then cleared.
"""
import os, re, sys, time, zipfile, json
import orjson
import pyarrow as pa
import pyarrow.parquet as pq


# Repo root derived from this file, never a drive letter. The store moved D: -> E: in
# the workstation cutover; a stale root here silently writes into, or reports on, a
# tree that is not there. R330.
def _RD(*parts):
    _r = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(_r, *parts) if parts else _r

ZIP      = _RD('data', 'raw', 'sec_edgar', 'submissions.zip')
OUT_DIR  = _RD('data', 'clean_full', 'edgar_pointers')
TICKERS  = _RD('data', 'raw', 'sec_edgar', 'company_tickers.json')
N_SHARDS = 256
FLUSH_ROWS = 3_000_000          # flush all buffers when this many rows are buffered globally
ARCH_PREFIX = 'https://www.sec.gov/Archives/edgar/data/'
LICENSE = 'us-public-domain'

MAIN_RE = re.compile(r'^CIK(\d{10})\.json$')

SCHEMA = pa.schema([
    ('cik',             pa.int32()),
    ('ticker',          pa.string()),
    ('form',            pa.string()),
    ('filing_date',     pa.date32()),
    ('accession',       pa.string()),
    ('primary_doc_url', pa.string()),
])

# ---- date parsing: 'YYYY-MM-DD' -> days since epoch (date32 stores int days) ----
import datetime
_EPOCH = datetime.date(1970, 1, 1)
_date_cache = {}
def date_to_days(s):
    if not s:
        return None
    d = _date_cache.get(s)
    if d is None:
        try:
            y = int(s[0:4]); m = int(s[5:7]); day = int(s[8:10])
            d = (datetime.date(y, m, day) - _EPOCH).days
        except Exception:
            d = None
        _date_cache[s] = d
    return d


def load_ticker_map():
    """CIK(int) -> ticker, from company_tickers.json (public companies with tickers)."""
    with open(TICKERS, 'rb') as fh:
        d = orjson.loads(fh.read())
    m = {}
    for v in d.values():
        try:
            m[int(v['cik_str'])] = v['ticker']
        except Exception:
            pass
    return m


def emit_rows(cik_int, ticker, forms, dates, accs, docs, buf):
    """Append one row per filing into the correct shard buffer."""
    shard = cik_int % N_SHARDS
    b = buf[shard]
    b_cik, b_tic, b_form, b_date, b_acc, b_url = b
    n = len(accs)
    # forms/dates/docs should be same length; guard against ragged arrays
    lf = len(forms); ld = len(dates); ldoc = len(docs)
    for i in range(n):
        acc = accs[i]
        b_cik.append(cik_int)
        b_tic.append(ticker)
        b_form.append(forms[i] if i < lf else None)
        b_date.append(date_to_days(dates[i]) if i < ld else None)
        b_acc.append(acc)
        if acc and i < ldoc:
            doc = docs[i]
            if doc:
                b_url.append(f'{ARCH_PREFIX}{cik_int}/{acc.replace("-", "")}/{doc}')
            else:
                b_url.append(None)
        else:
            b_url.append(None)
    return n


def new_buffers():
    # each shard: 6 parallel python lists
    return [([], [], [], [], [], []) for _ in range(N_SHARDS)]


def flush(buf, writers, counts):
    wrote = 0
    for shard in range(N_SHARDS):
        b = buf[shard]
        if not b[0]:
            continue
        arrays = [
            pa.array(b[0], type=pa.int32()),
            pa.array(b[1], type=pa.string()),
            pa.array(b[2], type=pa.string()),
            pa.array(b[3], type=pa.date32()),
            pa.array(b[4], type=pa.string()),
            pa.array(b[5], type=pa.string()),
        ]
        tbl = pa.Table.from_arrays(arrays, schema=SCHEMA)
        writers[shard].write_table(tbl)
        counts[shard] += len(b[0])
        wrote += len(b[0])
        # clear in place
        for lst in b:
            lst.clear()
    return wrote


def main():
    t0 = time.time()
    os.makedirs(OUT_DIR, exist_ok=True)
    log = lambda *a: print(*a, flush=True)

    log('loading ticker map...')
    tmap = load_ticker_map()
    log(f'ticker map: {len(tmap)} entries')

    log('opening zip + reading central directory...')
    z = zipfile.ZipFile(ZIP)
    infos = z.infolist()
    log(f'zip entries: {len(infos)} ({time.time()-t0:.1f}s)')

    mains = [i for i in infos if MAIN_RE.match(i.filename)]
    log(f'main CIK files: {len(mains)}')

    # open 256 parquet writers with zstd + dictionary on string cols
    writers = []
    for s in range(N_SHARDS):
        d = os.path.join(OUT_DIR, f'cik_shard={s:03d}')
        os.makedirs(d, exist_ok=True)
        w = pq.ParquetWriter(
            os.path.join(d, 'part.parquet'),
            SCHEMA,
            compression='zstd',
            compression_level=7,
            use_dictionary=['ticker', 'form', 'primary_doc_url'],
            version='2.6',
        )
        writers.append(w)
    log(f'opened {N_SHARDS} parquet writers (zstd)')

    buf = new_buffers()
    counts = [0] * N_SHARDS
    buffered = 0
    total_rows = 0
    n_filers = 0
    n_overflow = 0
    n_filers_with_filings = 0
    zread = z.read

    last_log = time.time()
    for idx, info in enumerate(mains):
        m = MAIN_RE.match(info.filename)
        cik_int = int(m.group(1))
        try:
            d = orjson.loads(zread(info))
        except Exception as e:
            log(f'WARN parse fail {info.filename}: {e!r}')
            continue
        n_filers += 1

        # ticker: prefer the filer's own tickers[], fallback to company_tickers map
        tks = d.get('tickers') or []
        ticker = tks[0] if tks else tmap.get(cik_int)

        filings = d.get('filings') or {}
        rec = filings.get('recent') or {}
        accs = rec.get('accessionNumber')
        if accs:
            r = emit_rows(cik_int, ticker,
                          rec.get('form') or [],
                          rec.get('filingDate') or [],
                          accs,
                          rec.get('primaryDocument') or [],
                          buf)
            buffered += r; total_rows += r
            if r:
                n_filers_with_filings += 1

        # overflow submission files in the SAME zip
        for f in (filings.get('files') or []):
            nm = f.get('name')
            if not nm:
                continue
            try:
                sd = orjson.loads(zread(nm))
            except KeyError:
                log(f'WARN overflow missing in zip: {nm}')
                continue
            except Exception as e:
                log(f'WARN overflow parse fail {nm}: {e!r}')
                continue
            n_overflow += 1
            saccs = sd.get('accessionNumber')
            if saccs:
                r = emit_rows(cik_int, ticker,
                              sd.get('form') or [],
                              sd.get('filingDate') or [],
                              saccs,
                              sd.get('primaryDocument') or [],
                              buf)
                buffered += r; total_rows += r

        if buffered >= FLUSH_ROWS:
            w = flush(buf, writers, counts)
            buffered = 0

        now = time.time()
        if now - last_log >= 15:
            rate = n_filers / (now - t0)
            eta = (len(mains) - n_filers) / rate / 60 if rate else 0
            log(f'  {n_filers:,}/{len(mains):,} filers | {total_rows:,} rows | '
                f'{n_overflow:,} overflow | {rate:.0f} filers/s | ETA {eta:.1f} min | '
                f'elapsed {(now-t0)/60:.1f} min')
            last_log = now

    # final flush
    flush(buf, writers, counts)
    for w in writers:
        w.close()

    nonempty = sum(1 for c in counts if c > 0)
    log('=' * 60)
    log(f'DONE in {(time.time()-t0)/60:.1f} min')
    log(f'filers processed     : {n_filers:,}')
    log(f'filers with filings  : {n_filers_with_filings:,}')
    log(f'overflow JSONs read  : {n_overflow:,}')
    log(f'TOTAL ROWS WRITTEN   : {total_rows:,}')
    log(f'shards with data     : {nonempty}/{N_SHARDS}')
    log(f'sum of shard counts  : {sum(counts):,}  (must equal total rows)')
    log(f'min/max rows per shard: {min(counts):,} / {max(counts):,}')

    # write a small manifest
    manifest = {
        'source': ZIP,
        'license': LICENSE,
        'n_shards': N_SHARDS,
        'partition_scheme': 'cik % 256, dir cik_shard=NNN',
        'total_rows': total_rows,
        'filers_processed': n_filers,
        'overflow_jsons': n_overflow,
        'shards_with_data': nonempty,
        'columns': [f.name for f in SCHEMA],
        'archives_url_format': ARCH_PREFIX + '{cik}/{accession_no_dashes}/{primaryDocument}',
        'built_utc': datetime.datetime.utcnow().isoformat() + 'Z',
    }
    with open(os.path.join(OUT_DIR, '_manifest.json'), 'wb') as fh:
        fh.write(orjson.dumps(manifest, option=orjson.OPT_INDENT_2))
    log('wrote _manifest.json')


if __name__ == '__main__':
    main()
