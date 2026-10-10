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

# ONE answer of a worker's /v1/edge-status as ONE line of four words: commit soak forward edge_state.
# All four come from the same body, so they describe the same version of the worker (while a deploy
# spreads, two requests can be answered by two versions). A word is: a string as it is, true/false as
# yes/no, null or a missing key as "none", anything else as "odd". Instead of the line:
#   "unreachable" when curl fails (no answer at all), "notjson" when the answer is not a JSON object.
status_line() {
  local body
  if ! body="$(curl -s --max-time 30 "$1/v1/edge-status" 2>/dev/null)"; then echo unreachable; return 0; fi
  printf '%s' "$body" | python -c 'import sys,json
try:
    d = json.load(sys.stdin)
    assert isinstance(d, dict)
except Exception:
    print("notjson")
    raise SystemExit(0)
def w(v):
    return "yes" if v is True else "no" if v is False else "none" if v is None else v if isinstance(v, str) and v.strip() and len(v.split()) == 1 else "odd"
print(w(d.get("commit")), w(d.get("soak")), w(d.get("forward")), w(d.get("edge_state")))' 2>/dev/null || echo notjson
}

# wrangler 3.114.17 lets the variable WRANGLER_CI_OVERRIDE_NAME replace --name on `wrangler deploy`
# (cli.js 121023), and it loads api/worker/.env into its environment (cli.js 153152); .env is git-ignored,
# so the clean-checkout test cannot see it. Either would send this deploy to another worker.
refuse_a_name_override() {
  if [ -n "${WRANGLER_CI_OVERRIDE_NAME+x}" ]; then
    echo "refused: WRANGLER_CI_OVERRIDE_NAME is set in the environment; wrangler would deploy to that name, not to the one fixed here" >&2; exit 1
  fi
  if grep -qs 'WRANGLER_CI_' "$1"/.env "$1"/.env.* 2>/dev/null; then
    echo "refused: a .env file in $1 sets a WRANGLER_CI_ variable; wrangler would read it" >&2; exit 1
  fi
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

refuse_a_name_override api/worker
echo "soak address: $SOAK   production address: $EDGE"
prod_before="$(status_line "$EDGE")"
# production must carry a commit id (40 hex) and must not be a soak worker NOW; else nothing can be compared after
if ! printf '%s' "$prod_before" | grep -Eq '^[0-9a-f]{40} (no|none) (yes|no) [a-z]+$'; then
  echo "refused: the production worker's status cannot be read as a deployed commit: $EDGE/v1/edge-status reads '$prod_before' (expected: <40 hex> no|none yes|no <state>), so this script could not tell afterwards that production is untouched" >&2; exit 1
fi

echo "deploying $SOAK_NAME at $commit (production reads: $prod_before)"
(cd api/worker && npx wrangler deploy --config "$SOAK_CONFIG" --name "$SOAK_NAME" --var "GIT_COMMIT:$commit")

ok=""
got=""
for i in 1 2 3 4 5 6; do
  got="$(status_line "$SOAK")"
  if [ "${got%% *}" = "$commit" ]; then ok=1; break; fi
  sleep 10
done
if [ -z "$ok" ]; then
  echo "FAILED: the deploy command ended without an error, but $SOAK/v1/edge-status does not answer $commit (it reads '$got')" >&2; exit 1
fi
# the SAME answer that carried the commit: this is the version just deployed
if [ "$got" != "$commit yes yes users" ]; then
  echo "FAILED: the deploy is LIVE at $commit, and $SOAK/v1/edge-status reads '$got' (expected '$commit yes yes users' = commit, soak, forward, edge_state): this is not the soak worker's config" >&2; exit 1
fi

# production: three answers, 5 s apart, each equal to the one before the deploy in all four words. One answer
# can come from the older version while a deploy spreads; three make a deploy that landed there hard to miss.
for i in 1 2 3; do
  prod_after="$(status_line "$EDGE")"
  if [ "$prod_after" != "$prod_before" ]; then
    echo "FAILED: the PRODUCTION worker changed or cannot be read: before '$prod_before', now '$prod_after' (commit soak forward edge_state). Check $EDGE/v1/edge-status now." >&2; exit 1
  fi
  sleep 5
done
echo "verified: $SOAK answers $commit as a soak worker; production is unchanged ($prod_after)"
