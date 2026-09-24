-- migrations/users_selfhost.sql -- econ's two new tables in the SHARED users db (hfdatalibrary-db,
-- binding USERS), for the self-hosting cutover (docs/ECON_SELF_HOSTING_PLAN.md).
--
-- Once FORWARD = "on", the edge no longer touches econ D1 or the econ R2 bucket. The two things it still
-- WRITES move here, beside econ_download_log, which already lives in this db:
--   econ_pageview    - the page-view beacon (src/pageview.ts); same columns as econ D1's `pageview`.
--   econ_ops_status  - the cost guard's status record (src/costGuard.ts); one row per key.
--
-- Additive and idempotent: IF NOT EXISTS, no change to any hf table. Apply BEFORE FORWARD is set:
--   npx wrangler d1 execute hfdatalibrary-db --remote --file migrations/users_selfhost.sql
-- (a deploy-class step: Ahmed's). With FORWARD unset nothing reads or writes these tables.
--
-- The old page-view rows are copied in AFTER the flip, so the rows counted between the copy and the flip
-- are not lost and none is counted twice (econ D1 stops receiving at the flip):
--   INSERT INTO econ_pageview (path, day, hits) VALUES (...)
--     ON CONFLICT(path, day) DO UPDATE SET hits = hits + excluded.hits;

CREATE TABLE IF NOT EXISTS econ_pageview (
  path TEXT NOT NULL,
  day  TEXT NOT NULL,
  hits INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (path, day)
);

CREATE TABLE IF NOT EXISTS econ_ops_status (
  key        TEXT PRIMARY KEY,
  body       TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
