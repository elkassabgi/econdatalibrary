#!/usr/bin/env bash
# The ONE way to deploy the SOAK worker (econdl-api-soak; api/worker/wrangler.soak.toml; self-hosting plan
# step 4). It is a second, public test address; the production worker is econdl-api and has its own wrapper
# (deploy_edge.sh). Run by the owner; deploys are his.
#
# What it refuses, and why:
#   * a checkout that is not main, not clean under api/worker, or not equal to origin/main (as deploy_edge.sh);
#   * any argument: the worker's name and its config are fixed here, never typed;
#   * afterwards the soak address must answer this commit with soak = true and forward = true, AND the
#     production worker's /v1/edge-status must be what it was before (same commit, not a soak worker) -
#     a deploy that landed on the wrong worker fails here, loudly.
set -euo pipefail

SOAK_NAME="econdl-api-soak"
SOAK_CONFIG="wrangler.soak.toml"
SOAK="${SELFHOST_SOAK:-https://econdl-api-soak.elkassabgi.workers.dev}"
EDGE="${SELFHOST_EDGE:-https://econdl-api.elkassabgi.workers.dev}"

if [ "$#" -ne 0 ]; then
  echo "refused: this script takes no argument (the worker is $SOAK_NAME, the config $SOAK_CONFIG)" >&2; exit 1
fi

# one field of a worker's /v1/edge-status, as ONE word: a string as it is, true/false as yes/no, null as
# "none", and "unknown" when the address does not answer JSON (a curl error included)
status_field() {
  local body
  body="$(curl -s "$1/v1/edge-status" 2>/dev/null || true)"
  printf '%s' "$body" | python -c 'import sys,json
try:
    v = json.load(sys.stdin).get(sys.argv[1])
except Exception:
    v = Ellipsis
print("yes" if v is True else "no" if v is False else "none" if v is None else v if isinstance(v, str) and v.strip() and len(v.split()) == 1 else "unknown")' "$2" 2>/dev/null || echo unknown
}

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

prod_before="$(status_field "$EDGE" commit)"
if [ "$prod_before" = "unknown" ]; then
  echo "refused: the production worker's /v1/edge-status cannot be read at $EDGE, so this script could not tell afterwards that production is untouched" >&2; exit 1
fi

echo "deploying $SOAK_NAME at $commit (production is at commit: $prod_before)"
(cd api/worker && npx wrangler deploy --config "$SOAK_CONFIG" --name "$SOAK_NAME" --var "GIT_COMMIT:$commit")

ok=""
got=""
for i in 1 2 3 4 5 6; do
  got="$(status_field "$SOAK" commit)"
  if [ "$got" = "$commit" ]; then ok=1; break; fi
  sleep 10
done
if [ -z "$ok" ]; then
  echo "FAILED: $SOAK/v1/edge-status does not answer $commit (got '$got')" >&2; exit 1
fi
soak="$(status_field "$SOAK" soak)"
forward="$(status_field "$SOAK" forward)"
if [ "$soak" != "yes" ] || [ "$forward" != "yes" ]; then
  echo "FAILED: $SOAK/v1/edge-status says soak=$soak forward=$forward (expected yes and yes): this is not the soak worker's config" >&2; exit 1
fi

prod_after="$(status_field "$EDGE" commit)"
prod_soak="$(status_field "$EDGE" soak)"
# "none" = an edge deployed before the field existed: not a soak worker
if [ "$prod_after" != "$prod_before" ] || { [ "$prod_soak" != "no" ] && [ "$prod_soak" != "none" ]; }; then
  echo "FAILED: the PRODUCTION worker changed or cannot be read: commit before '$prod_before', after '$prod_after', soak=$prod_soak. Check $EDGE/v1/edge-status now." >&2; exit 1
fi
echo "verified: $SOAK answers $commit as a soak worker; production is unchanged ($prod_after)"
