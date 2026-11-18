"""catalog/site/assets/sso.js must paint the nav pill on EVERY page a signed-in visitor sees.

The bug (reported 2026-11-18): a family-only visitor -- signed in through the pop-up, so
ekd_rt is stored but no edl_key -- saw their name on one page and "Sign in" on the next.
sso.js called updateUI() on some exits only. Step 4 ("skipped" inside RECHECK_MS, "capped"
after MAX_TRIES) and step 5 (non-production host) returned without painting, and a family-only
visitor reaches step 4 on every nav click inside 20 s and on every page after five checks.

The fix paints once, on the path every exit after step 1 reaches. These tests run the real
sso.js under node's `vm` against a hand-made fake DOM and fake storage: no network, no server,
no browser. Node is already a CI requirement of this suite (tests.yml sets up node 22 for
test_worker_geo_projection.py); this test needs only plain node, no type stripping.

Controls, so the tests can fail: a signed-out visitor stays "Sign in"; hasKey() still decides
the bounce (a family-only visitor with no key is still bounced to fetch it); a stale ekd_rt
next to the sign-out marker stays signed out and fetches nothing; a key or edl_family wins
over the marker.

The harness takes the path of sso.js as argv[2], so a mutated copy can be run through it.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SSO_JS = REPO / "catalog" / "site" / "assets" / "sso.js"

HARNESS = r"""
'use strict';
const fs = require('fs');
const vm = require('vm');
const SRC = fs.readFileSync(process.argv[2], 'utf8');

class ClassList {
  constructor() { this.s = new Set(); }
  add(c) { this.s.add(c); }
  contains(c) { return this.s.has(c); }
}
class El {
  constructor(tag, attrs) {
    this.tagName = tag; this.attrs = Object.assign({}, attrs || {});
    this.children = []; this.classList = new ClassList(); this._text = '';
    this.id = this.attrs.id || ''; this.style = {}; this.title = '';
    for (const c of (this.attrs['class'] || '').split(/\s+/).filter(Boolean)) this.classList.add(c);
  }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  appendChild(c) { this.children.push(c); return c; }
  insertBefore(c) { this.children.unshift(c); return c; }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map(c => c.textContent).join(''); }
  set textContent(v) { this._text = String(v); this.children = []; }
  remove() {}
}
function textNode(t) { return { textContent: String(t) }; }

function makeStore(init) {
  const m = new Map(Object.entries(init || {}));
  return {
    getItem: k => (m.has(k) ? m.get(k) : null),
    setItem: (k, v) => { m.set(k, String(v)); },
    removeItem: k => { m.delete(k); },
    _m: m,
  };
}

async function run(sc) {
  const anchor = new El('a', { href: 'account.html', 'class': 'signin' });
  anchor.textContent = 'Sign in';
  const body = new El('body');
  const head = new El('head');
  const byId = {};
  const listeners = [];
  const fetches = [];
  const replaced = [];
  const document = {
    readyState: sc.ready,
    referrer: sc.referrer || '',
    title: 't',
    body, head, documentElement: head,
    addEventListener: (ev, fn) => { if (ev === 'DOMContentLoaded') listeners.push(fn); },
    querySelector: sel => (/\.nav a\.signin|\.nav a\[href="account\.html"\]|\.nav \.signin/.test(sel) ? anchor : null),
    querySelectorAll: () => [anchor],
    getElementById: id => byId[id] || null,
    createElement: tag => {
      const e = new El(tag);
      return new Proxy(e, { set(t, k, v) { t[k] = v; if (k === 'id') byId[v] = t; return true; } });
    },
    createTextNode: textNode,
  };
  const location = {
    hostname: sc.host, origin: 'https://' + sc.host, pathname: '/catalog.html',
    search: '', hash: sc.hash || '',
    replace: u => replaced.push(u),
  };
  const ls = makeStore(sc.local);
  const ss = makeStore(sc.session);
  const fetch = (url, opts) => {
    fetches.push(url);
    return Promise.resolve({ ok: true, json: () => Promise.resolve(sc.me || {}) });
  };
  const ctx = {
    document, location, localStorage: ls, sessionStorage: ss, fetch,
    history: { replaceState: () => {} },
    navigator: { userAgent: 'Mozilla/5.0 test', webdriver: false },
    isSecureContext: true,
    crypto: globalThis.crypto, TextEncoder, URLSearchParams, Promise, JSON, Date, Math,
    String, parseInt, encodeURIComponent, Uint8Array, btoa, Set, Map, Object, RegExp,
    console,
  };
  ctx.window = ctx;
  vm.createContext(ctx);
  vm.runInContext(SRC, ctx, { filename: 'sso.js' });
  if (sc.ready === 'loading') { document.readyState = 'interactive'; for (const fn of listeners) fn(); }
  for (let i = 0; i < 20; i++) await new Promise(r => setImmediate(r));   // drain fetch/digest
  const note = ss.getItem('edl_sso_note');
  return {
    text: anchor.textContent,
    chip: anchor.classList.contains('chip'),
    signedAttr: body.getAttribute('data-signed-in'),
    fetches, replaced,
    note: note ? JSON.parse(note).state : null,
    name: ls.getItem('edl_name'),
  };
}

(async () => {
  const scenarios = JSON.parse(fs.readFileSync(0, 'utf8'));
  const out = {};
  for (const [k, sc] of Object.entries(scenarios)) out[k] = await run(sc);
  process.stdout.write(JSON.stringify(out));
})().catch(e => { console.error(e && e.stack || e); process.exit(2); });
"""



def _scenarios(now_ms: int) -> dict:
    fresh = str(now_ms - 2_000)           # 2 s old: inside RECHECK_MS (20 s)
    stale = str(now_ms - 60_000)          # 60 s old: outside RECHECK_MS
    at_ok = json.dumps({"t": "tok", "e": now_ms + 3_600_000})
    prod = "econdatalibrary.com"
    preview = "abc123.econdatalibrary.pages.dev"
    fam = {"edl_family": "1", "ekd_rt": "rt", "edl_name": "Ahmed Elkassabgi"}
    popup = {"ekd_rt": "rt", "edl_name": "Ahmed Elkassabgi"}   # pop-up sign-in: no edl_family
    s = {}
    for ready in ("complete", "loading"):
        r = ready[0]
        # Step 4 "skipped": checked 2 s ago.
        s[f"skipped_family_{r}"] = dict(ready=ready, host=prod, local=fam,
                                        session={"edl_sso_checked": fresh, "edl_sso_tries": "1"})
        s[f"skipped_popup_{r}"] = dict(ready=ready, host=prod, local=popup,
                                       session={"edl_sso_checked": fresh, "edl_sso_tries": "1",
                                                "ekd_silent_done": fresh})
        # Step 4 "capped": five tries used, last check stale.
        s[f"capped_family_{r}"] = dict(ready=ready, host=prod, local=fam,
                                       session={"edl_sso_checked": stale, "edl_sso_tries": "5"})
        # Step 5: preview host, nothing checked yet.
        s[f"preview_family_{r}"] = dict(ready=ready, host=preview, local=fam, session={})
        # Name backfill on the step-4 and step-5 exits: no stored name, valid access token.
        s[f"skipped_backfill_{r}"] = dict(
            ready=ready, host=prod, local={"edl_family": "1", "ekd_rt": "rt", "ekd_at": at_ok},
            session={"edl_sso_checked": fresh, "edl_sso_tries": "1"}, me={"name": "Ahmed Elkassabgi"})
        s[f"preview_backfill_{r}"] = dict(
            ready=ready, host=preview, local={"edl_family": "1", "ekd_rt": "rt", "ekd_at": at_ok},
            session={}, me={"name": "Ahmed Elkassabgi"})
        # Key holder (step 2): painted once, /me fetched at most once.
        s[f"key_backfill_{r}"] = dict(
            ready=ready, host=prod,
            local={"edl_key": "k", "edl_family": "1", "ekd_rt": "rt", "ekd_at": at_ok},
            session={}, me={"name": "Ahmed Elkassabgi"})
        # CONTROLS: signed out on each of the three exits.
        s[f"ctl_out_skipped_{r}"] = dict(ready=ready, host=prod, local={},
                                         session={"edl_sso_checked": fresh, "edl_sso_tries": "1",
                                                  "ekd_silent_done": fresh})
        s[f"ctl_out_capped_{r}"] = dict(ready=ready, host=prod, local={},
                                        session={"edl_sso_checked": stale, "edl_sso_tries": "5",
                                                 "ekd_silent_done": fresh})
        s[f"ctl_out_preview_{r}"] = dict(ready=ready, host=preview, local={}, session={})
        # CONTROL hasKey(): family-only, no key, nothing checked -> step 6 must still bounce.
        s[f"ctl_haskey_bounce_{r}"] = dict(ready=ready, host=prod, local=fam, session={})
        # SIGN-OUT MARKER (C2): stale ekd_rt + marker, no key, no edl_family.
        for where in ("session", "local"):
            loc = {"ekd_rt": "rt", "ekd_at": at_ok}
            ses = {"edl_sso_checked": fresh, "edl_sso_tries": "1", "ekd_silent_done": fresh}
            (ses if where == "session" else loc)["ekd_signed_out"] = "1"
            s[f"marker_{where}_{r}"] = dict(ready=ready, host=prod, local=loc, session=ses,
                                            me={"name": "Ahmed Elkassabgi"})
        # Marker does NOT override edl_family or a key.
        s[f"marker_family_{r}"] = dict(
            ready=ready, host=prod, local={"edl_family": "1", "ekd_rt": "rt", "ekd_signed_out": "1",
                                           "edl_name": "Ahmed Elkassabgi"},
            session={"edl_sso_checked": fresh, "edl_sso_tries": "1"})
        s[f"marker_key_{r}"] = dict(
            ready=ready, host=prod, local={"edl_key": "k", "ekd_rt": "rt", "ekd_signed_out": "1",
                                           "ekd_at": at_ok},
            session={}, me={"name": "Ahmed Elkassabgi"})
    return s


def run_harness(sso_path: Path, tmp_path: Path) -> dict:
    import time
    harness = tmp_path / "sso_harness.cjs"
    harness.write_text(HARNESS, encoding="utf-8")
    scen = json.dumps(_scenarios(int(time.time() * 1000)))
    p = subprocess.run(["node", str(harness), str(sso_path)], input=scen,
                       capture_output=True, text=True, timeout=120)
    assert p.returncode == 0, f"harness failed: stderr={p.stderr!r}"
    return json.loads(p.stdout)


def check(res: dict) -> list[str]:
    """Every expectation, returned as a list of failures (empty = pass)."""
    bad = []
    me = "/v1/auth/me"

    def chip(k, name="Ahmed"):
        r = res[k]
        if not (r["chip"] and name in r["text"] and r["signedAttr"] == "1"):
            bad.append(f"{k}: expected signed-in chip '{name}', got chip={r['chip']} text={r['text']!r}")

    def signin(k):
        r = res[k]
        if r["chip"] or r["text"] != "Sign in" or r["signedAttr"] is not None:
            bad.append(f"{k}: expected 'Sign in', got chip={r['chip']} text={r['text']!r}")

    for r in ("c", "l"):
        for k in ("skipped_family", "skipped_popup", "capped_family", "preview_family",
                  "skipped_backfill", "preview_backfill", "key_backfill",
                  "marker_family", "marker_key"):
            chip(f"{k}_{r}")
        for k in ("skipped_family", "skipped_popup", "capped_family", "preview_family",
                  "ctl_out_skipped", "ctl_out_capped", "ctl_out_preview"):
            if res[f"{k}_{r}"]["replaced"]:
                bad.append(f"{k}_{r}: navigated away {res[f'{k}_{r}']['replaced']}")
        exp_note = {"skipped_family": "skipped", "skipped_popup": "skipped", "capped_family": "capped"}
        for k, n in exp_note.items():
            if res[f"{k}_{r}"]["note"] != n:
                bad.append(f"{k}_{r}: note {res[f'{k}_{r}']['note']!r} != {n!r} (wrong path exercised)")
        for k in ("skipped_backfill", "preview_backfill", "key_backfill", "marker_key"):
            n = sum(1 for u in res[f"{k}_{r}"]["fetches"] if me in u)
            if n != 1:
                bad.append(f"{k}_{r}: /me fetched {n} times, expected exactly 1")
        for k in ("ctl_out_skipped", "ctl_out_capped", "ctl_out_preview",
                  "marker_session", "marker_local"):
            signin(f"{k}_{r}")
            if res[f"{k}_{r}"]["fetches"]:
                bad.append(f"{k}_{r}: fetched {res[f'{k}_{r}']['fetches']} while signed out")
        for k in ("marker_session", "marker_local"):
            if res[f"{k}_{r}"]["name"] is not None:
                bad.append(f"{k}_{r}: edl_name re-written after sign-out")
        b = res[f"ctl_haskey_bounce_{r}"]["replaced"]
        if not (len(b) == 1 and b[0].startswith("https://api.hfdatalibrary.com/v1/auth/sso?")):
            bad.append(f"ctl_haskey_bounce_{r}: hasKey() changed - expected one bounce to /v1/auth/sso, got {b}")
    return bad


def _node_or_skip():
    if shutil.which("node"):
        return
    if os.environ.get("CI"):
        pytest.fail("node is required on CI (tests.yml sets it up); refusing to skip")
    pytest.skip("node not available")


def test_sso_nav_paints_on_every_exit(tmp_path):
    _node_or_skip()
    bad = check(run_harness(SSO_JS, tmp_path))
    assert not bad, "\n".join(bad)


def test_has_key_is_key_only():
    # The file's own §hasKey comment: widening it once broke downloads. Pin its body.
    src = SSO_JS.read_text(encoding="utf-8")
    assert "function hasKey()   { return !!localStorage.getItem(K); }" in src
