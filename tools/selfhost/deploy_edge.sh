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

# wrangler 3.114.17 lets the variable WRANGLER_CI_OVERRIDE_NAME replace --name on `wrangler deploy`
# (cli.js 121023), and it loads api/worker/.env into its environment (cli.js 153152); .env is git-ignored,
# so the clean-checkout test cannot see it. Either would send this deploy to another worker.
# ANY LETTER CASE: on Windows node reads environment names without regard to case, so
# `wrangler_ci_override_name` is the same variable to wrangler (measured in review AR-275). The names are read
# from a here-string, not through a pipe: with `pipefail`, `env | grep -q` can end non-zero ON a match.
# LC_ALL=C: in a Turkish locale `grep -i` does not pair i with I, and node on Windows still does (review AR-278).
refuse_a_name_override() {
  local names
  names="$(compgen -e || true)"      # under `set -e` a failing compgen would end the script with no message
  # PATH is always exported. A list without it was not read (no compgen, no here-string): refuse, do not pass.
  if ! LC_ALL=C grep -qx 'PATH' <<<"$names"; then
    echo "refused: the list of exported variable names could not be read, so a WRANGLER_CI_OVERRIDE_NAME cannot be ruled out" >&2; exit 1
  fi
  if LC_ALL=C grep -qix 'WRANGLER_CI_OVERRIDE_NAME' <<<"$names"; then
    echo "refused: WRANGLER_CI_OVERRIDE_NAME is set in the environment (in some letter case); wrangler would deploy to that name, not to the one fixed here" >&2; exit 1
  fi
  if LC_ALL=C grep -qsi 'WRANGLER_CI_' "$1"/.env "$1"/.env.* 2>/dev/null; then
    echo "refused: a .env file in $1 holds the text WRANGLER_CI_ (wrangler reads $1/.env; every .env* file is searched, comments too). Take that name out of the file" >&2; exit 1
  fi
}
refuse_a_name_override api/worker

echo "deploying econdl-api at $commit"
(cd api/worker && npx wrangler deploy --config wrangler.toml --var "GIT_COMMIT:$commit")   # --config: wrangler 3 obeys a stray .wrangler/deploy/config.json otherwise (AR-151)

for i in 1 2 3 4 5 6; do
  # ONE answer per try: the commit and the soak flag are read from the SAME body, so they describe the same
  # version of the worker (while a deploy spreads, two requests can be answered by two versions).
  body="$(curl -s --max-time 30 "$EDGE/v1/edge-status" || true)"
  got="$(printf '%s' "$body" | python -c 'import sys,json; print(json.load(sys.stdin).get("commit") or "")' 2>/dev/null || true)"
  if [ "$got" = "$commit" ]; then
    echo "verified: $EDGE/v1/edge-status answers $commit"
    printf '%s\n' "$body"
    # SOAK = "1" is for the soak worker only (wrangler.soak.toml). On this worker it would turn the page-view
    # routes into 404s with no error anywhere. `wrangler deploy` replaces plain variables with the ones in
    # wrangler.toml, but a SECRET named SOAK (dashboard or `wrangler secret put`) survives every deploy, and
    # a `--var SOAK:1` on the command above would set it again at every deploy.
    soak="$(printf '%s' "$body" | python -c 'import sys,json; v=json.load(sys.stdin).get("soak"); print("yes" if v is True else "no" if v in (False, None) else "unknown")' 2>/dev/null || echo unknown)"
    if [ "$soak" = "yes" ]; then
      echo "FAILED: the deploy is LIVE, and $EDGE/v1/edge-status says soak=true. Its page-view routes answer 404 now. Remove the SOAK secret or variable from econdl-api (wrangler secret delete SOAK --name econdl-api --config wrangler.toml) and deploy again." >&2
      exit 1
    fi
    if [ "$soak" != "no" ]; then
      echo "FAILED: the deploy is LIVE at $commit, but the soak field of $EDGE/v1/edge-status could not be read (soak=$soak). Read the status by hand before anything else; nothing says the worker is a soak worker." >&2
      exit 1
    fi
    exit 0
  fi
  sleep 10
done
echo "FAILED: $EDGE/v1/edge-status does not answer $commit (got '${got:-nothing}')" >&2
exit 1
