"""Measure depth-1/2/3 grain over the FULL unctad_tradefoodcatbyproc store (giant #14).

The serve chain's first step (biotrademerch/critmin precedent): count distinct
dot-prefix ids at each depth over EVERY row, so the catalogue grain is chosen
from the whole store, never a sample. Key shape (from the ingest log 2026-08-16):
  ProcessFoodCategory.Economy.Partner.Flow.M<code>
depth-1 = Category; depth-2 = Category.Economy; depth-3 = Category.Economy.Partner.
Store: 394,118,603 obs / 26,579,759 series (4 measures, sum exact).
"""
import duckdb

P = r"E:\research\econfindatalibrary\data\clean_full\unctad_tradefoodcatbyproc\unctad_tradefoodcatbyproc.parquet"
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
