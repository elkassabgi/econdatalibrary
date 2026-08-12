"""Measure depth-1/2/3 grain over the FULL unctad_biotrademerch store.

The serve chain's first step (critmin/tradeservcat precedent): count distinct
dot-prefix ids at each depth over EVERY row, so the catalogue grain is chosen
from the whole store, never a sample. Key shape:
  Product.Flow.Economy.Partner.M<code>
depth-1 = Product; depth-2 = Product.Flow; depth-3 = Product.Flow.Economy.
Also counts full series keys and rows for the record.
"""
import duckdb

P = r"E:\research\econfindatalibrary\data\clean_full\unctad_biotrademerch\unctad_biotrademerch.parquet"
con = duckdb.connect()
q = f"""
SELECT
  count(*)                                                          AS rows,
  count(DISTINCT series_key)                                        AS series,
  count(DISTINCT split_part(series_key, '.', 1))                    AS depth1,
  count(DISTINCT split_part(series_key, '.', 1) || '.' ||
                 split_part(series_key, '.', 2))                    AS depth2,
  count(DISTINCT split_part(series_key, '.', 1) || '.' ||
                 split_part(series_key, '.', 2) || '.' ||
                 split_part(series_key, '.', 3))                    AS depth3
FROM read_parquet('{P}')
"""
r = con.execute(q).fetchone()
print(f"rows={r[0]:,}  series={r[1]:,}  depth1={r[2]:,}  depth2={r[3]:,}  depth3={r[4]:,}",
      flush=True)
