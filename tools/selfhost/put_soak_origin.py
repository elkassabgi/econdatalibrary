"""Sets the two origin values of the SOAK worker (econdl-api-soak) without anyone typing or seeing them.

    python tools/selfhost/put_soak_origin.py --url-file <file> --dev-vars <file> --check     (sends nothing)
    python tools/selfhost/put_soak_origin.py --url-file <file> --dev-vars <file>

The edge sends the origin secret - and then the Access token - to whatever host ORIGIN_URL names
(api/worker/src/edge.ts). So neither value is typed: ORIGIN_URL is read from a one-line file and must be
exactly `https://<one label>.econdatalibrary.com` (a wrong ZONE is the mistake that would send two secrets
to a stranger); ORIGIN_SECRET is read from the `ORIGIN_SECRET=` line of the origin's `.dev.vars` and must be
64 lower-case hex characters. Each value goes to `wrangler secret put` on its STANDARD INPUT - never on a
command line, where a process list would show it. The worker's name and config are fixed here:
without `--name`, `wrangler secret put` writes on the worker named in the config it reads.

It prints the two NAMES it set, wrangler's exit codes and wrangler's own lines (in ASCII). It never prints a
value; if wrangler's output holds a whole value, that output is not shown.

It REFUSES BEFORE deploy_soak.sh: when the worker does not exist, `wrangler secret put` creates it by itself
(it is not asked: standard input is a pipe). So the soak address is read first, and its /v1/edge-status must
read as deploy_soak.sh leaves it: a 40-hex commit, soak yes, forward yes, edge_state users. After both values
the address is read again until `origin_configured` is true. That field says only that both names hold SOME
value on the worker: it does not show that the values are the right ones, and when it was true before this
run it shows no change. `--check` reads the two files and the address, and sends nothing.
The two Access values (ORIGIN_ACCESS_ID, ORIGIN_ACCESS_SECRET) are NOT handled here: the owner types them at
wrangler's prompt, which is masked only when wrangler's standard input is a terminal.

Exit code 0 only when both values were set and the address then showed `origin_configured` true
(`--check`: when both files are well-formed and the address answers as the deployed soak worker).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

SOAK_NAME = "econdl-api-soak"
SOAK_CONFIG = "wrangler.soak.toml"
SOAK = "https://econdl-api-soak.elkassabgi.workers.dev"
UA = "econdatalibrary-selfhost-tools/1.0"
DEPLOYED_RE = re.compile(r"[0-9a-f]{40} yes yes users")        # what deploy_soak.sh verifies: commit soak forward edge_state
TRIES_AFTER, WAIT_AFTER = 6, 10
URL_RE = re.compile(r"https://[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.econdatalibrary\.com")
SECRET_RE = re.compile(r"[0-9a-f]{64}")
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class Refused(Exception):
    pass


def _text(path: str) -> str:
    """The file as text. A byte-order mark is dropped (Notepad and PowerShell 5 write one); a file that is not
    UTF-8 (PowerShell 5's `>` writes UTF-16) is a refusal that says so, not a traceback."""
    try:
        with open(path, encoding="utf-8-sig") as fh:
            return fh.read()
    except UnicodeDecodeError:
        raise Refused(f"{os.path.basename(path)} is not UTF-8 text (PowerShell's `>` writes UTF-16: write the "
                      "file again as UTF-8)") from None


def read_url(path: str) -> str:
    lines = [ln.strip() for ln in _text(path).splitlines() if ln.strip()]
    if len(lines) != 1:
        raise Refused(f"the URL file must hold exactly one line; it holds {len(lines)}")
    if not URL_RE.fullmatch(lines[0]):
        raise Refused("the URL is not exactly https://<one label>.econdatalibrary.com (no path, no port, "
                      "no other zone)")
    return lines[0]


def read_secret(path: str) -> str:
    found = [ln.split("=", 1)[1].strip() for ln in _text(path).splitlines()
             if ln.split("=", 1)[0].strip() == "ORIGIN_SECRET" and "=" in ln]
    if len(found) != 1:
        raise Refused(f"the .dev.vars file must hold exactly one ORIGIN_SECRET line; it holds {len(found)}")
    value = found[0].strip('"')
    if not SECRET_RE.fullmatch(value):
        raise Refused("ORIGIN_SECRET is not 64 lower-case hex characters")       # the value is never shown
    return value


def fetch_status(soak: str, timeout: int = 30) -> object:
    """ONE answer of <soak>/v1/edge-status: the parsed JSON, or a word that says why there is none."""
    req = urllib.request.Request(soak + "/v1/edge-status", headers={"User-Agent": UA, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
    except urllib.error.HTTPError as e:
        return f"http-{e.code}"
    except Exception as e:  # noqa: BLE001 - reported with its type; no answer is never a pass
        return f"unreachable-{type(e).__name__}"
    try:
        return json.loads(body)
    except ValueError:
        return "notjson"


def soak_status(soak: str, fetch) -> tuple[str, bool]:
    """(the four words of deploy_soak.sh from ONE answer, whether that answer says origin_configured true)."""
    d = fetch(soak)
    if not isinstance(d, dict):
        return (d if isinstance(d, str) and d else "notjson"), False

    def w(v):                                   # the STRINGS "yes"/"no"/"none" are not a boolean and not a null
        if isinstance(v, bool):
            return "yes" if v else "no"
        if v is None:
            return "none"
        return v if isinstance(v, str) and len(v.split()) == 1 and v == v.strip() and v not in ("yes", "no", "none", "odd") else "odd"
    line = " ".join(w(d.get(k)) for k in ("commit", "soak", "forward", "edge_state"))
    return line, d.get("origin_configured") is True


def command(name: str) -> list[str]:
    npx = shutil.which("npx")
    if npx is None:
        raise Refused("npx is not on PATH")
    return [npx, "wrangler", "secret", "put", name, "--config", SOAK_CONFIG, "--name", SOAK_NAME]


def put(name: str, value: str, worker_dir: str, run=subprocess.run) -> int:
    """One `wrangler secret put`, the value on stdin. Returns wrangler's exit code."""
    argv = command(name)
    assert value not in " ".join(argv), "a value never goes on a command line"
    # BYTES both ways. wrangler writes UTF-8 (its banner holds U+26C5 U+FE0F); in text mode Python on Windows
    # decodes with the ANSI code page, the byte 0x8F has no character there, the reader thread dies with a
    # traceback and ALL of wrangler's output is lost (review AR-274, measured on Python 3.11, 3.12 and 3.14).
    r = run(argv, input=(value + "\n").encode("ascii"), cwd=worker_dir, capture_output=True)
    shown = b"".join(x if isinstance(x, bytes) else str(x or "").encode("utf-8") for x in (r.stdout, r.stderr))
    shown = shown.decode("utf-8", "replace")
    if value in shown:
        print(f"{name}: wrangler's output held the value and is not shown")
    else:
        for line in shown.splitlines():
            if line.strip():                              # ASCII only: a console code page cannot stop the receipt
                print("  wrangler: " + line.strip()[:200].encode("ascii", "replace").decode("ascii"))
    print(f"{name}: wrangler exit code {r.returncode}")
    return r.returncode


def main(argv=None, run=subprocess.run, fetch=None, sleep=time.sleep) -> int:
    ap = argparse.ArgumentParser(description="Set ORIGIN_URL and ORIGIN_SECRET on the soak worker from two files.")
    ap.add_argument("--url-file", required=True, help="a file with one line: the origin's https address")
    ap.add_argument("--dev-vars", required=True, help="the origin's .dev.vars (its ORIGIN_SECRET= line is read)")
    ap.add_argument("--worker-dir", default=os.path.join(ROOT, "api", "worker"))
    ap.add_argument("--soak", default=os.environ.get("SELFHOST_SOAK") or SOAK, help="the soak worker's address")
    ap.add_argument("--check", action="store_true", help="read the two files and the address; send nothing")
    a = ap.parse_args(argv)
    fetch = fetch or fetch_status
    try:
        if not os.path.isfile(os.path.join(a.worker_dir, SOAK_CONFIG)):
            raise Refused(f"{SOAK_CONFIG} is not in {a.worker_dir}: without it wrangler would read wrangler.toml")
        url, secret = read_url(a.url_file), read_secret(a.dev_vars)
        print(f"soak address: {a.soak}")
        before, was_set = soak_status(a.soak, fetch)
        if not DEPLOYED_RE.fullmatch(before):
            raise Refused(f"{a.soak}/v1/edge-status does not answer as the deployed soak worker: it reads "
                          f"'{before}' (expected: <40 hex> yes yes users = commit soak forward edge_state). "
                          "Run deploy_soak.sh first: on a worker that does not exist, `wrangler secret put` "
                          "creates one by itself. Nothing was sent")
        yn = "true" if was_set else "not true"
        if a.check:
            print(f"check: the URL file and the ORIGIN_SECRET line are well-formed; the address answers "
                  f"'{before}'; origin_configured is {yn}. Nothing was sent.")
            return 0
        codes = [put("ORIGIN_URL", url, a.worker_dir, run)]
        if codes[0] != 0:
            raise Refused("ORIGIN_URL was not set, so ORIGIN_SECRET is not sent (it would have no host to go to)")
        codes.append(put("ORIGIN_SECRET", secret, a.worker_dir, run))
        if codes == [0, 0]:
            after, now_set = "", False
            for i in range(TRIES_AFTER):
                after, now_set = soak_status(a.soak, fetch)
                if now_set and after == before:
                    break
                if i + 1 < TRIES_AFTER:
                    sleep(WAIT_AFTER)
            if not (now_set and after == before):
                print(f"NOT SHOWN: wrangler ended with 0 for both names, but after {TRIES_AFTER} answers the address "
                      f"reads '{after}' with origin_configured {'true' if now_set else 'not true'} (before: "
                      f"'{before}', origin_configured {yn}). Read {a.soak}/v1/edge-status again in a minute.",
                      file=sys.stderr)
                return 1
            print(f"seen after both values: the address answers '{after}' with origin_configured true "
                  f"(before this run it was {yn}). That shows that both names hold a value, not which value.")
    except Refused as e:
        print(f"refused: {e}", file=sys.stderr)
        return 1
    except OSError as e:
        print(f"refused: {type(e).__name__} on a file or on npx ({e.strerror})", file=sys.stderr)
        return 1
    return 0 if codes == [0, 0] else 1


if __name__ == "__main__":
    raise SystemExit(main())
