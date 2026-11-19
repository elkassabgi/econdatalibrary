"""Mutation check for the release watch (release-watch design Revision 5, B8 "Mutants (each must be killed)").

Each mutant is applied ON DISK, one at a time, because the probe child imports updater/release_watch.py from the
file (an in-memory patch would never reach it). The procedure, which refuses to start on a dirty tree:
  1. control: every mutant's test selection passes on the committed code;
  2. per mutant: apply its edit (each `old` must occur exactly once), run its selection, and count it KILLED only
     when pytest reports a failed test (an import or syntax error is not a kill);
  3. restore the file with `git checkout -- <file>` and require `git diff --exit-code` to be clean before the next
     mutant (ledger M-20261119-11).

    python tools/release_watch_mutants.py --basetemp F:/somewhere/bt          # all mutants
    python tools/release_watch_mutants.py --basetemp F:/somewhere/bt --only no-size-cap

Exit 0 only when the control passes and every mutant is killed. Never run it in the live checkout: it edits
tracked files (and restores them) while it runs.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RW = "updater/release_watch.py"
T_MAIN, T_BOX, T_BEAT = "tests/test_release_watch.py", "tests/test_release_watch_sandbox.py", \
    "tests/test_release_watch_beat.py"

# (name, file, [(old, new), ...], pytest selection)
MUTANTS = [
    ("bare-same", RW,
     [("f\"release: same as of {_day(u.get('last_probe_utc'))}\"", "\"release: same\"")],
     [T_MAIN, "-k", "b8_1 or b8_9 or b8_13"]),
    ("late-on-stale-beat", RW,
     [("    if not stale:                                        # LATE is judged only from a fresh beat (C1)",
       "    if True:")],
     [T_MAIN, "-k", "b8_2"]),
    ("denylist-env", RW,
     [("    env = {k: up[k] for k in _ENV_PASS if k in up}",
       "    env = {k: v for k, v in up.items() if not k.startswith(('UNCTAD_', 'R2_', 'CLOUDFLARE_', 'COMTRADE_', "
       "'BEA_', 'CENSUS_', 'FRED_', 'RESEND_', 'GH_TOKEN', 'GITHUB_TOKEN'))}")],
     [T_BOX, "-k", "b8_6"]),
    ("trust-env-true", RW,
     [("    session.trust_env = False", "    pass")],
     [T_BOX, "-k", "b8_5"]),
    ("env-guard-builtins-open-only", RW,
     [("    builtins.open = _path_guard(builtins.open, \"open\")\n",
       "    builtins.open = _path_guard(builtins.open, \"open\")\n    _GUARD[\"on\"] = True\n    return\n")],
     [T_BOX, "-k", "b8_4"]),
    ("non-atomic-write", RW,
     [("    tmp = os.path.join(d, f\".{os.path.basename(path)}.{os.getpid()}.tmp\")", "    tmp = path"),
      ("                os.replace(tmp, path)\n                break", "                break"),
      ("        if os.path.exists(tmp):\n            try:", "        if False:\n            try:")],
     [T_MAIN, "-k", "b8_10"]),
    ("no-size-cap", RW,
     [("    if len(json.dumps(out, separators=(\",\", \":\"))) > BEAT_MAX_BYTES:", "    if False:")],
     [T_BEAT, "-k", "size_cap"]),
    ("clear-on-p-equals-previous-p", RW,
     [("        mv = open_move(u.get(\"events\"), None if isinstance(unit_rows, BaseException) else T, "
       "u.get(\"trimmed\"))",
       "        mv = open_move(u.get(\"events\"), None if isinstance(unit_rows, BaseException) else T, "
       "u.get(\"trimmed\")) if u.get(\"first_seen_utc\") == u.get(\"last_probe_utc\") else None")],
     [T_MAIN, "-k", "b8_9"]),
    ("failure-creates-the-record", RW,
     [("        if not real:\n            return None\n        rec = {", "        rec = {")],
     [T_MAIN, "-k", "b8_8 or driver"]),
    ("single-scrub-digest-trusts-the-beat", RW,
     [("            continue                                     # neither named nor counted (R888)",
       "            pass")],
     [T_MAIN, "-k", "b8_12 or b8_13"]),
    # beyond the ten named in B8
    ("single-scrub-beat-trusts-the-digest", RW,
     [("        return str(key).split(\"/\", 1)[0].lower() not in gated", "        return True")],
     [T_BEAT, "-k", "b8_12"]),
    ("trimmed-said-only-when-t-is-missing", RW,
     [("            \"trimmed\": bool(trimmed),",
       "            \"trimmed\": bool(trimmed) and T not in [e.get(\"to\") for e in ev],")],
     [T_MAIN, "-k", "r5_note"]),
    ("a-swallowed-refusal-reads-as-success", RW,
     [("        if _GUARD[\"tripped\"]:\n            out[\"error\"]", "        if False:\n            out[\"error\"]")],
     [T_BOX, "-k", "swallow"]),
    ("no-audit-hook", RW,
     [("    sys.addaudithook(_audit)\n", "")],
     [T_BOX, "-k", "audit_hook_closes"]),
    ("no-token-shape-check", RW,
     [("    if not _token_ok(p, token_re):", "    if False:")],
     [T_BOX, "-k", "not_a_release_token"]),
    # verification of #308 (C1, C2, optional since-date)
    ("no-store-token-counted-as-same", RW,
     [("        elif u.get(\"last_probe_ok\") is True and T is None:", "        elif False:")],
     [T_MAIN, "-k", "no_store_token or counter"]),
    ("beat-from-not-rebuilt", RW,
     [("    ev = [dict(e, **({\"from\": ev[i - 1].get(\"to\")} if (i > 0 and \"from\" not in e) else {}))\n"
       "          for i, e in enumerate(ev)]\n", "")],
     [T_BEAT, "-k", "rebuilds"]),
    ("beat-carries-every-from", RW,
     [("                                      **({\"from\": _t(e.get(\"from\"))} if (i == 1 and trimmed) else {}))",
       "                                      **({\"from\": _t(e.get(\"from\"))}))")],
     [T_BEAT, "-k", "worst_case or rebuilds"]),
    ("trimmed-start-given-as-a-date", RW,
     [("    bound = bool(trimmed) and (not hits or hits[-1] == 0)", "    bound = False")],
     [T_MAIN, "-k", "b8_9"]),
    ("run-with-no-real-p-is-ok", RW,
     [("    rc = 0 if real else 3", "    rc = 0")],
     [T_MAIN, "-k", "driver or no_real_p"]),
]


def _git(*a):
    return subprocess.run(["git", "-C", ROOT, *a], capture_output=True, text=True)


def _clean(path=None) -> bool:
    args = ["diff", "--exit-code", "--quiet"] + (["--", path] if path else [])
    return _git(*args).returncode == 0 and _git("diff", "--cached", "--exit-code", "--quiet").returncode == 0


def _pytest(sel, basetemp):
    cmd = [sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", f"--basetemp={basetemp}", *sel]
    r = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    tail = (r.stdout + r.stderr).strip().splitlines()[-1:] or [""]
    return r.returncode, tail[0], r.stdout + r.stderr


def _apply(m):
    _name, f, edits, _sel = m
    p = os.path.join(ROOT, f)
    with open(p, encoding="utf-8", newline="") as fh:
        src = fh.read()
    for old, new in edits:
        n = src.count(old)
        if n != 1:
            raise SystemExit(f"mutant {m[0]}: expected 1 occurrence, found {n}: {old[:80]!r}")
        src = src.replace(old, new)
    with open(p, "w", encoding="utf-8", newline="") as fh:
        fh.write(src)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--basetemp", required=True)
    ap.add_argument("--only", action="append")
    a = ap.parse_args(argv)
    todo = [m for m in MUTANTS if not a.only or m[0] in a.only]
    if not _clean():
        print("refused: the tree has uncommitted changes; mutants restore from the committed reference")
        return 2
    started = time.time()
    for m in todo:                                       # 1. control
        rc, tail, _out = _pytest(m[3], a.basetemp)
        print(f"control  {m[0]:40} rc={rc}  {tail}", flush=True)
        if rc != 0:
            print("refused: a control selection fails on the committed code")
            return 3
    killed = 0
    for m in todo:                                       # 2. one mutant at a time
        _apply(m)
        try:
            rc, tail, out = _pytest(m[3], a.basetemp)
        finally:
            _git("checkout", "--", m[1])                 # 3. restore from the committed reference
        if not _clean(m[1]):
            print(f"STOP: {m[1]} is not clean after restoring mutant {m[0]}")
            return 4
        ok = rc != 0 and " failed" in tail
        killed += ok
        print(f"mutant   {m[0]:40} {'KILLED' if ok else 'SURVIVED'} rc={rc}  {tail}", flush=True)
    print(f"{killed}/{len(todo)} mutants killed; tree clean (git diff --exit-code); {time.time() - started:.0f} s")
    return 0 if killed == len(todo) else 1


if __name__ == "__main__":
    sys.exit(main())
