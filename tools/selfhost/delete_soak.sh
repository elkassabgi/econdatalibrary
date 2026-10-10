#!/usr/bin/env bash
# The ONE way to delete the SOAK worker (econdl-api-soak) when the soak ends. Run by the owner: deleting a
# cloud worker is his.
#
# Why a wrapper: `wrangler delete` without --name deletes the worker named in the config it reads - in
# api/worker that is the PRODUCTION worker econdl-api, after one question. Here the name and the config are
# fixed, no argument is accepted, a dry run comes first, wrangler still asks its own question, and
# afterwards the production worker's /v1/edge-status must be what it was before.
# It deletes the worker only. Rows in the users database, the tunnel, the Access application and the DNS
# record are not touched.
set -euo pipefail

SOAK_NAME="econdl-api-soak"
SOAK_CONFIG="wrangler.soak.toml"
SOAK="${SELFHOST_SOAK:-https://econdl-api-soak.elkassabgi.workers.dev}"
EDGE="${SELFHOST_EDGE:-https://econdl-api.elkassabgi.workers.dev}"

if [ "$#" -ne 0 ]; then
  echo "refused: this script takes no argument (it deletes $SOAK_NAME and nothing else)" >&2; exit 1
fi
if [ "$SOAK_NAME" != "econdl-api-soak" ]; then
  echo "refused: the name in this script is not econdl-api-soak" >&2; exit 1
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

top="$(git rev-parse --show-toplevel)"
cd "$top/api/worker"
if [ ! -f "$SOAK_CONFIG" ]; then
  echo "refused: api/worker/$SOAK_CONFIG is missing; without it wrangler would read wrangler.toml" >&2; exit 1
fi

echo "soak address: $SOAK   production address: $EDGE"
prod_before="$(status_line "$EDGE")"
if ! printf '%s' "$prod_before" | grep -Eq '^[0-9a-f]{40} (no|none) (yes|no) [a-z]+$'; then
  echo "refused: the production worker's status cannot be read as a deployed commit: $EDGE/v1/edge-status reads '$prod_before' (expected: <40 hex> no|none yes|no <state>), so this script could not tell afterwards that production is untouched" >&2; exit 1
fi

# THE QUESTION IS ASKED HERE. wrangler asks its own only when it sees a terminal on both sides; in any other
# context (a pipe, a tool, CI) it answers "yes" by itself (wrangler 3.114.17, cli.js 91931).
printf 'This deletes the cloud worker %s. Type its name to go on: ' "$SOAK_NAME"
typed=""
read -r typed || true
typed="${typed%$'\r'}"
if [ "$typed" != "$SOAK_NAME" ]; then
  echo "refused: the name was not typed; nothing was deleted" >&2; exit 1
fi

echo "dry run first (it reads the config and deletes nothing):"
npx wrangler delete --config "$SOAK_CONFIG" --name "$SOAK_NAME" --dry-run
echo "now the delete of $SOAK_NAME (in a terminal wrangler asks once more):"
npx wrangler delete --config "$SOAK_CONFIG" --name "$SOAK_NAME"

for i in 1 2 3; do
  prod_after="$(status_line "$EDGE")"
  if [ "$prod_after" != "$prod_before" ]; then
    echo "FAILED: the PRODUCTION worker's status changed or cannot be read: before '$prod_before', now '$prod_after' (commit soak forward edge_state). Check $EDGE/v1/edge-status now." >&2; exit 1
  fi
  sleep 5
done
# gone = the address ANSWERS, and not with JSON. No answer at all (curl failed) proves nothing.
soak_after=""
for i in 1 2 3 4 5 6; do
  soak_after="$(status_line "$SOAK")"
  if [ "$soak_after" = "notjson" ]; then break; fi
  sleep 10
done
if [ "$soak_after" = "unreachable" ]; then
  echo "FAILED: $SOAK could not be reached, so nothing shows that the soak worker is gone. Production is unchanged ($prod_after). Read $SOAK/v1/edge-status by hand." >&2; exit 1
fi
if [ "$soak_after" != "notjson" ]; then
  echo "FAILED: $SOAK/v1/edge-status still answers JSON ('$soak_after'): the soak worker is not gone (wrangler deleted nothing, or the answer to its question was no)" >&2; exit 1
fi
echo "done: $SOAK_NAME no longer answers; production is unchanged ($prod_after)"
