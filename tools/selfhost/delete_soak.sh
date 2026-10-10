#!/usr/bin/env bash
# The ONE way to delete the SOAK worker (econdl-api-soak) when the soak ends. Run by the owner: deleting a
# cloud worker is his.
#
# Why a wrapper: `wrangler delete` without --name deletes the worker named in the config it reads - in
# api/worker that is the PRODUCTION worker econdl-api, after one question in a terminal and after none
# anywhere else (wrangler answers its own question with yes there). Here the name and the config are
# fixed, no argument is accepted, the script asks for the worker's name (wrangler asks its own question only
# in a terminal), a dry run comes before the delete, and afterwards the production worker's
# /v1/edge-status must be what it was before.
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
if ! printf '%s' "$prod_before" | LC_ALL=C grep -Eq '^[0-9a-f]{40} (no|none) (yes|no) [a-z]+$'; then
  echo "refused: the production worker's status cannot be read as a deployed commit: $EDGE/v1/edge-status reads '$prod_before' (expected: <40 hex> no|none yes|no <state>), so this script could not tell afterwards that production is untouched" >&2; exit 1
fi
# The soak address BEFORE the delete. Only a change can be shown: an address that answers with something that is
# not the soak worker's JSON now (a filter's page for a new host name, an error page, another worker) answers
# the same after a delete that deleted nothing (review AR-278, case K04: the script said "done").
soak_before="$(status_line "$SOAK")"
soak_seen=""
if printf '%s' "$soak_before" | LC_ALL=C grep -Eq '^[0-9a-f]{40} yes (yes|no) [a-z]+$'; then
  soak_seen=1
else
  echo "note: $SOAK/v1/edge-status does not answer as a soak worker now (it reads '$soak_before'; expected: <40 hex> yes yes|no <state>). The delete can still run, but this script will not be able to show that it worked."
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
if [ -z "$soak_seen" ]; then
  soak_after="$(status_line "$SOAK")"
  echo "NOT SHOWN: wrangler's delete ended without an error and production is unchanged ($prod_after), but $SOAK/v1/edge-status did not answer as a soak worker before the delete either (before '$soak_before', now '$soak_after'), so its answers show nothing about the delete. Look for $SOAK_NAME in the account's list of workers." >&2; exit 1
fi
# What counts as the change: the address answered as the soak worker before, and now it ANSWERS, and not with
# JSON, TWICE IN A ROW. No answer at all (curl failed) proves nothing, and ONE answer that is not JSON can come
# from a worker that is still there (an error page of the network or of Cloudflare; measured with stand-ins in
# review AR-275: the script said "done" with the worker in place). Two such pages in a row from a worker that
# is still there end "done" all the same (AR-278, case K03): the line below says what was seen, no more.
soak_after=""
waited=0
for i in 1 2 3 4 5 6; do
  soak_after="$(status_line "$SOAK")"
  if [ "$soak_after" = "notjson" ]; then
    sleep 5; waited=$((waited + 5))
    soak_after="$(status_line "$SOAK")"
    if [ "$soak_after" = "notjson" ]; then break; fi
  fi
  sleep 10; waited=$((waited + 10))
done
if [ "$soak_after" = "unreachable" ]; then
  echo "FAILED: $SOAK could not be reached, so nothing shows that the soak worker is gone. Production is unchanged ($prod_after). Read $SOAK/v1/edge-status by hand." >&2; exit 1
fi
if [ "$soak_after" != "notjson" ]; then
  echo "FAILED: $SOAK/v1/edge-status still answers JSON ('$soak_after') after ${waited} s of waits: nothing shows that the soak worker is gone. Either nothing was deleted (the answer to wrangler's own question was no), or the delete has not reached this address yet: read it again in a minute. Production is unchanged ($prod_after)." >&2; exit 1
fi
echo "done: production is unchanged ($prod_after). Seen: $SOAK/v1/edge-status answered as a soak worker before the delete ('$soak_before') and twice in a row with something that is not JSON after it."
