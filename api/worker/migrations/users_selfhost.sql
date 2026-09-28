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
-- (a deploy-class step: Ahmed's). With FORWARD unset nothing reads or writes these tables. The worker
-- also creates either table on first use if this step was missed, so a flip made first loses nothing.
-- The DDL below is pinned equal to src/pageview.ts and src/costGuard.ts by test/edge.test.ts.
--
-- ONE-TIME MERGE of econ D1's old page views (review R1172: a plain upsert-add doubles on a re-run).
-- Run it only after FORWARD has been on long enough that no old worker version still writes econ D1
-- (at least 10 minutes after the deploy that turned it on):
--   1. export econ D1 `pageview` into a staging table here, econ_pageview_import (same columns);
--   2. in ONE batch, guarded by a marker so a second run does nothing:
--        INSERT INTO econ_pageview (path, day, hits)
--          SELECT path, day, hits FROM econ_pageview_import
--          WHERE NOT EXISTS (SELECT 1 FROM econ_ops_status WHERE key = 'pageview_import_merged')
--          ON CONFLICT(path, day) DO UPDATE SET hits = hits + excluded.hits;
--        INSERT INTO econ_ops_status (key, body, updated_at)
--          VALUES ('pageview_import_merged', '{}', datetime('now'));      -- fails on a second run: PK
--   3. check SUM(hits) per day: econ D1 `pageview` + hits counted here since the flip = econ_pageview.
-- A rollback to FORWARD unset sends new hits back to econ D1; a later second flip needs a fresh staging
-- export of ONLY the days after the first flip, merged under a new marker key.

CREATE TABLE IF NOT EXISTS econ_pageview (path TEXT NOT NULL, day TEXT NOT NULL, hits INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (path, day));

CREATE TABLE IF NOT EXISTS econ_ops_status (key TEXT PRIMARY KEY, body TEXT NOT NULL, updated_at TEXT NOT NULL);
