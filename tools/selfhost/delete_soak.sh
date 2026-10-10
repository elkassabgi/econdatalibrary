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
cd "$top/api/worker"
if [ ! -f "$SOAK_CONFIG" ]; then
  echo "refused: api/worker/$SOAK_CONFIG is missing; without it wrangler would read wrangler.toml" >&2; exit 1
fi

prod_before="$(status_field "$EDGE" commit)"
if [ "$prod_before" = "unknown" ]; then
  echo "refused: the production worker's /v1/edge-status cannot be read at $EDGE, so this script could not tell afterwards that production is untouched" >&2; exit 1
fi

echo "dry run first:"
npx wrangler delete --config "$SOAK_CONFIG" --name "$SOAK_NAME" --dry-run
echo "now the delete of $SOAK_NAME (wrangler asks once more):"
npx wrangler delete --config "$SOAK_CONFIG" --name "$SOAK_NAME"

prod_after="$(status_field "$EDGE" commit)"
if [ "$prod_after" != "$prod_before" ]; then
  echo "FAILED: the PRODUCTION worker's status changed: commit before '$prod_before', after '$prod_after'. Check $EDGE/v1/edge-status now." >&2; exit 1
fi
soak_after="$(status_field "$SOAK" soak)"
if [ "$soak_after" != "unknown" ]; then
  echo "FAILED: $SOAK/v1/edge-status still answers JSON (soak=$soak_after): the soak worker is not gone" >&2; exit 1
fi
echo "done: $SOAK_NAME no longer answers; production is unchanged ($prod_after)"
