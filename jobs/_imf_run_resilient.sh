#!/usr/bin/env bash
# Resilient IMF ingest runner: re-launches the streamed full pull with
# --skip-existing whenever it is killed (e.g. by a global OOM event from the
# many concurrent sibling ingest jobs), until the run reports DONE or we hit
# the attempt cap. Each restart resumes by skipping already-written parquet.
set -u
cd /d/research/econfindatalibrary 2>/dev/null || cd "D:/research/econfindatalibrary"
LOG="D:/research/econfindatalibrary/data/ingest_imf_full.log"
MAXTRY=15
for try in $(seq 1 $MAXTRY); do
  if grep -q "DONE in" "$LOG" 2>/dev/null; then
    echo "[runner] DONE detected, stopping (attempt $try)"
    break
  fi
  echo "[runner] === attempt $try/$MAXTRY $(date -u +%H:%M:%S) ==="
  # clean any orphaned temp from a previous kill
  rm -f D:/research/econfindatalibrary/data/tmp*.csv.gz D:/research/econfindatalibrary/data/clean_full/imf/*.tmp 2>/dev/null
  PYTHONIOENCODING=utf-8 python jobs/ingest_imf_full.py --skip-existing >> "$LOG" 2>&1
  rc=$?
  echo "[runner] attempt $try exited rc=$rc"
  if grep -q "DONE in" "$LOG" 2>/dev/null; then
    echo "[runner] DONE after attempt $try"
    break
  fi
  echo "[runner] not done; backing off before retry"
  sleep 8
done
echo "[runner] FINISHED runner loop"
