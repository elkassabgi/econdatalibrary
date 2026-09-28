#!/usr/bin/env bash
# The ONE way to deploy the econ edge worker (econdl-api) during the self-hosting move
# (docs/ECON_SELF_HOSTING_PLAN.md, change 6; review R1174 B2). Run by Ahmed; deploys are his.
#
# Why a wrapper: the off-machine check (tools/selfhost/watch_edge.py) compares the DEPLOYED edge with what
# main commits, and it needs the deployed commit id to say which code is live. A hand-typed
# `wrangler deploy --var GIT_COMMIT:...` is easy to forget or get wrong, and a deploy from a stale worktree
# (dozens exist, several with an old api/worker) would quietly put back a worker that serves or writes the
# frozen cloud copy. This script refuses both:
#   * the checkout must be main, clean under api/worker, and equal to origin/main;
#   * GIT_COMMIT is set from `git rev-parse HEAD`, never typed;
#   * afterwards /v1/edge-status must answer that commit, or the script fails.
set -euo pipefail

EDGE="${SELFHOST_EDGE:-https://econdl-api.elkassabgi.workers.dev}"
top="$(git rev-parse --show-toplevel)"
cd "$top"

branch="$(git rev-parse --abbrev-ref HEAD)"
if [ "$branch" != "main" ]; then
  echo "refused: the checkout is on '$branch', not main" >&2; exit 1
fi
if [ -n "$(git status --porcelain -- api/worker)" ]; then
  echo "refused: api/worker has uncommitted changes" >&2; git status --short -- api/worker >&2; exit 1
fi
git fetch --quiet origin main
if [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]; then
  echo "refused: HEAD is not origin/main (pull or push first)" >&2; exit 1
fi
commit="$(git rev-parse HEAD)"

echo "deploying econdl-api at $commit"
(cd api/worker && npx wrangler deploy --config wrangler.toml --var "GIT_COMMIT:$commit")   # --config: wrangler 3 obeys a stray .wrangler/deploy/config.json otherwise (AR-151)

for i in 1 2 3 4 5 6; do
  got="$(curl -s "$EDGE/v1/edge-status" | python -c 'import sys,json; print(json.load(sys.stdin).get("commit") or "")' 2>/dev/null || true)"
  if [ "$got" = "$commit" ]; then
    echo "verified: $EDGE/v1/edge-status answers $commit"
    curl -s "$EDGE/v1/edge-status"; echo
    exit 0
  fi
  sleep 10
done
echo "FAILED: $EDGE/v1/edge-status does not answer $commit (got '${got:-nothing}')" >&2
exit 1
