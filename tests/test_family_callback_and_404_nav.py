"""Three sign-in defects from the family-site audit of 2026-11-18, tested by RUNNING the shipped pages.

1. portal/auth/callback.html had a second, unguarded <script>. On the silent prompt=none return (no
   window.opener) it still broadcast an empty code to other portal tabs and called window.close()
   after 200 ms, which is shorter than the token exchange, so the tab closed before ekd_rt was
   stored. Every portal visit with a family session closed the tab (3 of 3 in the audit); a
   signed-out visitor (login_required) lost the tab too. The page is now ONE script with one
   hasOpener branch, the structure of the econ and ip callbacks.
2. portal/404.html had no nav paint, so its nav always read "Create free account".
3. catalog/site/404.html never loaded assets/sso.js, so its nav always read "Sign in".

Everything runs under node's `vm` against a hand-made fake DOM, fake storage, a fake fetch and fake
timers: no network, no server, no browser (the pattern of tests/test_sso_nav_paint.py). The family
SDK (accounts.elkassabgidata.com/sdk/ekd-sso.js) is external, so a stub stands in for it; every
other script on the page runs as shipped, in page order, including catalog/site/assets/sso.js.

Controls, so the tests can fail: the pop-up path must still post the code and close; a signed-out
visitor must still read "Sign in" / "Create free account"; the portal index must still bounce a
fresh signed-out visitor (proves the harness sees a bounce) while the 404 must not; the econ
callback, run through the same harness, must give the same silent-path result as the portal one.
The harness takes every file path from its input, so a mutated copy can be run through it.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
PORTAL_CALLBACK = REPO / "portal" / "auth" / "callback.html"
ECON_CALLBACK = REPO / "catalog" / "site" / "auth" / "callback.html"
PORTAL_404 = REPO / "portal" / "404.html"
PORTAL_INDEX = REPO / "portal" / "index.html"
ECON_404 = REPO / "catalog" / "site" / "404.html"
ECON_SITE = REPO / "catalog" / "site"
GEN_SITE = REPO / "catalog" / "gen_site.py"

HARNESS = r"""
'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');

function makeStore(init, events, tag) {
  const m = new Map(Object.entries(init || {}));
  return {
    getItem: k => (m.has(k) ? m.get(k) : null),
    setItem: (k, v) => { m.set(k, String(v)); if (events) events.push(tag + ':' + k); },
    removeItem: k => { m.delete(k); },
    _m: m,
  };
}
function scriptsOf(html) {
  const out = [];
  const re = /<script([^>]*)>([\s\S]*?)<\/script>/g;
  let m;
  while ((m = re.exec(html))) {
    const attrs = m[1];
    if (/type\s*=\s*"application\/ld\+json"/.test(attrs)) continue;
    const src = /\ssrc\s*=\s*"([^"]*)"/.exec(attrs);
    out.push({ src: src ? src[1] : null, code: m[2] });
  }
  return out;
}
const drain = async n => { for (let i = 0; i < (n || 30); i++) await new Promise(r => setImmediate(r)); };
const BASE_GLOBALS = () => ({
  JSON, Date, Math, String, Number, Boolean, Array, Object, RegExp, Error, Set, Map, Promise,
  parseInt, encodeURIComponent, decodeURIComponent, Uint8Array, btoa, atob, TextEncoder,
  URLSearchParams, console, crypto: globalThis.crypto,
});

// ---------------------------------------------------------------- auth callback
async function runCallback(sc) {
  const html = fs.readFileSync(sc.file, 'utf8');
  const events = [], posted = [], bcast = [], fetches = [], replaced = [], timers = [], pending = [];
  const ls = makeStore(sc.local, events, 'ls');
  const ss = makeStore(sc.session);
  let opener = null;
  if (sc.opener) opener = {
    closed: false,
    postMessage: (m, o) => { if (sc.opener === 'throws') throw new Error('blocked'); posted.push({ m, o }); events.push('post'); },
  };
  class BroadcastChannel {
    constructor(n) { this.n = n; }
    postMessage(m) { bcast.push({ n: this.n, m }); events.push('broadcast'); }
    close() {}
  }
  const location = {
    hash: sc.hash, pathname: '/auth/callback', origin: 'https://' + sc.host, href: '',
    replace: u => { replaced.push(u); events.push('replace'); },
  };
  const ctx = Object.assign(BASE_GLOBALS(), {
    location, localStorage: ls, sessionStorage: ss, BroadcastChannel,
    history: { replaceState: () => { location.hash = ''; } },
    document: { getElementById: () => ({ hidden: true }) },
    setTimeout: (fn, ms) => { timers.push({ fn, ms: ms || 0 }); return timers.length; },
    fetch: (url, opts) => {
      fetches.push({ url, body: opts && opts.body ? JSON.parse(opts.body) : null });
      events.push('fetch');
      return new Promise(res => pending.push(() => res({ ok: true, json: () => Promise.resolve(sc.tokens || {}) })));
    },
  });
  ctx.window = ctx;
  ctx.opener = opener;
  ctx.close = () => events.push('close');
  vm.createContext(ctx);
  const errors = [];
  for (const s of scriptsOf(html)) {
    if (s.src) { errors.push('unexpected src ' + s.src); continue; }
    try { vm.runInContext(s.code, ctx, { filename: path.basename(sc.file) }); } catch (e) { errors.push(String(e)); }
  }
  // The race the audit saw: every timer up to 1 s fires BEFORE the exchange answers.
  const fire = () => { timers.sort((a, b) => a.ms - b.ms); while (timers.length) timers.shift().fn(); };
  await drain(); fire(); await drain();
  while (pending.length) pending.shift()();
  await drain(); fire(); await drain();
  return {
    events, posted, bcast, fetches, replaced, errors,
    rt: ls.getItem('ekd_rt'), at: ls.getItem('ekd_at'),
    done: ss.getItem('ekd_silent_done'),
    leftover: ['ekd_silent_v', 'ekd_silent_s', 'ekd_silent_r'].filter(k => ss.getItem(k) !== null),
  };
}

// ---------------------------------------------------------------- page nav (404 / index)
function attrsOf(s) {
  const a = {}; const re = /([a-zA-Z-]+)\s*=\s*"([^"]*)"/g; let m;
  while ((m = re.exec(s))) a[m[1]] = m[2];
  return a;
}
async function runPage(sc) {
  const html = fs.readFileSync(sc.file, 'utf8');
  const byId = {};
  class El {
    constructor(tag, attrs) {
      attrs = attrs || {};
      this.tagName = tag; this.attrs = Object.assign({}, attrs); this.children = []; this.parentNode = null;
      this._text = ''; this.style = {}; this.dataset = {}; this.title = ''; this._cls = new Set();
      for (const c of (attrs['class'] || '').split(/\s+/).filter(Boolean)) this._cls.add(c);
      const self = this;
      this.classList = { add: c => self._cls.add(c), contains: c => self._cls.has(c),
                         toggle: c => (self._cls.has(c) ? self._cls.delete(c) : self._cls.add(c)) };
      if (attrs.id) this.id = attrs.id;
      this.href = attrs.href || '';
    }
    set id(v) { this._id = v; if (v) byId[v] = this; }
    get id() { return this._id || ''; }
    set className(v) { this._cls = new Set(String(v).split(/\s+/).filter(Boolean)); }
    get className() { return [...this._cls].join(' '); }
    set innerHTML(v) {
      this.children = []; this._text = '';
      const re = /<([a-z]+)([^>]*)>([^<]*)/g; let m;
      while ((m = re.exec(String(v)))) { const c = new El(m[1], attrsOf(m[2])); c._text = m[3]; this.appendChild(c); }
    }
    getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
    setAttribute(k, v) { this.attrs[k] = String(v); if (k === 'id') this.id = v; }
    appendChild(c) { this.children.push(c); c.parentNode = this; return c; }
    insertBefore(c) { this.children.unshift(c); c.parentNode = this; return c; }
    replaceChild(n, o) { const i = this.children.indexOf(o); if (i >= 0) { this.children[i] = n; n.parentNode = this; o.parentNode = null; } return o; }
    get firstChild() { return this.children[0] || null; }
    get textContent() { return this._text + this.children.map(c => c.textContent).join(''); }
    set textContent(v) { this._text = String(v); this.children = []; }
    addEventListener() {}
    contains(x) { return x === this || this.children.some(c => c.contains && c.contains(x)); }
    remove() {}
  }
  // Build the DOM the scripts touch from the shipped HTML: the first nav-links list (each anchor
  // in its own <li>), plus every .js-ekd-register anchor elsewhere on the page, in page order.
  const body = new El('body'), head = new El('head');
  const ulM = /<ul class="nav-links">([\s\S]*?)<\/ul>/.exec(html);
  if (!ulM) throw new Error('no nav-links list in ' + sc.file);
  const navOpen = html.lastIndexOf('<nav', ulM.index);
  const navIsDotNav = navOpen >= 0 && /^<nav class="nav"/.test(html.slice(navOpen));
  const ul = new El('ul', { 'class': 'nav-links' }); body.appendChild(ul);
  const navAnchors = [];
  const aRe = /<a ([^>]*)>([^<]*)<\/a>/g; let m;
  while ((m = aRe.exec(ulM[1]))) {
    const li = new El('li'); const a = new El('a', attrsOf(m[1])); a._text = m[2];
    li.appendChild(a); ul.appendChild(li); navAnchors.push(a);
  }
  const rest = html.slice(0, ulM.index) + html.slice(ulM.index + ulM[0].length);
  const otherReg = [];
  aRe.lastIndex = 0;
  while ((m = aRe.exec(rest))) {
    const at = attrsOf(m[1]);
    if (!/(^|\s)js-ekd-register(\s|$)/.test(at['class'] || '')) continue;
    const li = new El('li'); const a = new El('a', at); a._text = m[2]; li.appendChild(a); body.appendChild(li);
    otherReg.push({ a, before: m.index < ulM.index });
  }
  const isReg = a => a.classList.contains('js-ekd-register');
  const allReg = [...otherReg.filter(o => o.before).map(o => o.a), ...navAnchors.filter(isReg),
                  ...otherReg.filter(o => !o.before).map(o => o.a)];
  const target = navAnchors.find(a => isReg(a) || a.classList.contains('signin'));
  if (!target) throw new Error('no sign-in anchor in the nav of ' + sc.file);
  const targetLi = target.parentNode;
  const wrap = /class="wrap"/.test(html) ? new El('div', { 'class': 'wrap' }) : null;
  // Every other element with an id in the markup (status strip etc.), so page scripts find it.
  const idRe = /\sid="([^"]+)"/g;
  const markup = html.replace(/<script[\s\S]*?<\/script>/g, '');
  while ((m = idRe.exec(markup))) if (!byId[m[1]]) new El('span', { id: m[1] });

  const listeners = [], replaced = [], fetches = [], errors = [];
  const document = {
    readyState: sc.ready, referrer: sc.referrer || '', title: 't', body, head, documentElement: head,
    addEventListener: (ev, fn) => { if (ev === 'DOMContentLoaded') listeners.push(fn); },
    getElementById: id => byId[id] || null,
    createElement: tag => new El(tag),
    createTextNode: t => ({ textContent: String(t) }),
    querySelector: sel => {
      if (sel === '.js-ekd-register') return allReg[0] || null;
      if (sel === '.nav-links') return ul;
      if (sel === '.wrap') return wrap;
      if (navIsDotNav && (sel === '.nav a.signin' || sel === '.nav .signin'))
        return navAnchors.find(a => a.classList.contains('signin')) || null;
      if (navIsDotNav && sel === '.nav a[href="account.html"]')
        return navAnchors.find(a => a.getAttribute('href') === 'account.html') || null;
      return null;
    },
    querySelectorAll: sel => {
      if (sel === '.js-ekd-register') return allReg;
      if (sel === '.nav-links .js-ekd-register') return navAnchors.filter(isReg);
      if (sel === '.nav-links a, .nav a') return navAnchors;
      return [];
    },
  };
  const location = {
    hostname: sc.host, origin: 'https://' + sc.host, pathname: '/no-such-page', search: '', hash: '', href: '',
    replace: u => replaced.push(u), reload: () => {},
  };
  const ls = makeStore(sc.local), ss = makeStore(sc.session);
  const ctx = Object.assign(BASE_GLOBALS(), {
    document, location, localStorage: ls, sessionStorage: ss,
    history: { replaceState: () => {} },
    navigator: { userAgent: 'Mozilla/5.0 test', webdriver: false },
    isSecureContext: true,
    addEventListener: () => {},
    setTimeout: () => 0, clearTimeout: () => {}, setInterval: () => 0,
    Image: class { set src(v) {} },
    fetch: (url) => {
      fetches.push(String(url));
      const me = /\/v1\/(auth\/)?me(\?|$)/.test(String(url));
      return Promise.resolve({ ok: true, json: () => Promise.resolve(me ? { name: sc.meName || null } : {}) });
    },
  });
  ctx.window = ctx;
  vm.createContext(ctx);
  for (const s of scriptsOf(html)) {
    let code = s.code, name = path.basename(sc.file) + ':inline';
    if (s.src) {
      if (s.src === 'https://accounts.elkassabgidata.com/sdk/ekd-sso.js') {
        if (sc.sdk === false) continue;
        // Stand-in for the family SDK: logged in means a refresh token is stored (its isLoggedIn()).
        ctx.EKD = {
          init() {}, on() {}, logout() {}, login() { return Promise.resolve(); },
          isLoggedIn() { return !!ls.getItem('ekd_rt'); },
          getAccessToken() { return Promise.resolve(ls.getItem('ekd_rt') ? 'tok' : null); },
        };
        continue;
      }
      const local = /^\/?(assets\/[A-Za-z0-9_.-]+\.js)(\?.*)?$/.exec(s.src);
      if (!local || !sc.siteRoot) { errors.push('unhandled src ' + s.src); continue; }
      code = fs.readFileSync(path.join(sc.siteRoot, local[1]), 'utf8');
      name = local[1];
    }
    try { vm.runInContext(code, ctx, { filename: name }); } catch (e) { errors.push(name + ': ' + String(e)); }
  }
  if (sc.ready === 'loading') { document.readyState = 'interactive'; for (const fn of listeners) fn(); }
  await drain(40);
  const node = targetLi.children[0];
  return {
    id: node.id, text: node.textContent, classes: node.className,
    signedAttr: body.getAttribute('data-signed-in'),
    replaced, fetches, errors,
  };
}

(async () => {
  const jobs = JSON.parse(fs.readFileSync(0, 'utf8'));
  const out = {};
  for (const [k, j] of Object.entries(jobs)) out[k] = j.kind === 'callback' ? await runCallback(j) : await runPage(j);
  process.stdout.write(JSON.stringify(out));
})().catch(e => { console.error(e && e.stack || e); process.exit(2); });
"""


def _node_or_skip():
    if shutil.which("node"):
        return
    if os.environ.get("CI"):
        pytest.fail("node is required on CI (tests.yml sets it up); refusing to skip")
    pytest.skip("node not available")


def run_jobs(jobs: dict, workdir: Path) -> dict:
    harness = workdir / "family_nav_harness.cjs"
    harness.write_text(HARNESS, encoding="utf-8")
    p = subprocess.run(["node", str(harness)], input=json.dumps(jobs),
                       capture_output=True, text=True, timeout=120)
    assert p.returncode == 0, f"harness failed: stderr={p.stderr!r}"
    return json.loads(p.stdout)


# ------------------------------------------------------------------------------ 1. callback
def callback_jobs(portal_cb: Path, econ_cb: Path) -> dict:
    tokens = {"access_token": "at1", "refresh_token": "rt1", "expires_in": 900}
    silent = {"ekd_silent_v": "ver1", "ekd_silent_s": "s1", "ekd_silent_r": "/about"}
    jobs = {}
    for tag, f, host in (("portal", portal_cb, "elkassabgidata.com"), ("econ", econ_cb, "econdatalibrary.com")):
        base = dict(kind="callback", file=str(f), host=host, tokens=tokens)
        jobs[f"{tag}_silent_family"] = dict(base, hash="#code=c1&state=s1", session=silent)
        jobs[f"{tag}_silent_signed_out"] = dict(base, hash="#error=login_required&state=s1",
                                                session={"ekd_silent_v": "ver1", "ekd_silent_s": "s1"})
        jobs[f"{tag}_silent_foreign_state"] = dict(base, hash="#code=c1&state=other", session=silent)
        jobs[f"{tag}_popup"] = dict(base, hash="#code=c1&state=s1", session={}, opener="ok")
        jobs[f"{tag}_popup_post_blocked"] = dict(base, hash="#code=c1&state=s1", session={}, opener="throws")
    return jobs


def check_callback(res: dict, tags=("portal", "econ")) -> list[str]:
    bad = []
    for t in tags:
        origin = "https://elkassabgidata.com" if t == "portal" else "https://econdatalibrary.com"
        for k in ("silent_family", "silent_signed_out", "silent_foreign_state", "popup", "popup_post_blocked"):
            r = res[f"{t}_{k}"]
            if r["errors"]:
                bad.append(f"{t}_{k}: script errors {r['errors']}")
            # No path, ever, may hand out an empty code.
            for b in r["bcast"]:
                if not b["m"].get("code"):
                    bad.append(f"{t}_{k}: broadcast an EMPTY code {b}")
            for p in r["posted"]:
                if not p["m"].get("code"):
                    bad.append(f"{t}_{k}: posted an EMPTY code {p}")
        for k in ("silent_family", "silent_signed_out", "silent_foreign_state"):
            r = res[f"{t}_{k}"]
            if "close" in r["events"]:
                bad.append(f"{t}_{k}: silent path CLOSED the tab (events {r['events']})")
            if r["bcast"] or r["posted"]:
                bad.append(f"{t}_{k}: silent path broadcast/posted {r['bcast']} {r['posted']}")
            if not r["done"]:
                bad.append(f"{t}_{k}: ekd_silent_done not spent (redirect-loop risk)")
            if r["leftover"]:
                bad.append(f"{t}_{k}: PKCE scratch left in sessionStorage {r['leftover']}")
        r = res[f"{t}_silent_family"]
        if r["rt"] != "rt1" or not r["at"] or json.loads(r["at"]).get("t") != "at1":
            bad.append(f"{t}_silent_family: session not stored (ekd_rt={r['rt']!r} ekd_at={r['at']!r})")
        if r["replaced"] != ["/about"]:
            bad.append(f"{t}_silent_family: expected one return to /about, got {r['replaced']}")
        if len(r["fetches"]) != 1 or r["fetches"][0]["url"] != "https://accounts.elkassabgidata.com/token/exchange" \
                or r["fetches"][0]["body"] != {"code": "c1", "code_verifier": "ver1", "client_origin": origin}:
            bad.append(f"{t}_silent_family: wrong exchange {r['fetches']}")
        ev = r["events"]
        if "ls:ekd_rt" in ev and "replace" in ev and ev.index("ls:ekd_rt") > ev.index("replace"):
            bad.append(f"{t}_silent_family: navigated away before storing ekd_rt {ev}")
        for k in ("silent_signed_out", "silent_foreign_state"):
            r = res[f"{t}_{k}"]
            if r["replaced"] != ["/"] and k == "silent_signed_out":
                bad.append(f"{t}_{k}: expected to land on '/', got {r['replaced']}")
            if r["fetches"] or r["rt"]:
                bad.append(f"{t}_{k}: exchanged/stored without a valid code+state {r['fetches']} rt={r['rt']!r}")
        r = res[f"{t}_silent_foreign_state"]
        if r["replaced"] != ["/about"]:
            bad.append(f"{t}_silent_foreign_state: expected the stored return /about, got {r['replaced']}")
        r = res[f"{t}_popup"]
        want = {"type": "ekd_auth", "code": "c1", "state": "s1"}
        if len(r["posted"]) != 1 or r["posted"][0]["m"] != want or r["posted"][0]["o"] != origin:
            bad.append(f"{t}_popup: expected one same-origin post of {want}, got {r['posted']}")
        if r["events"].count("close") != 1:
            bad.append(f"{t}_popup: expected the pop-up to close once, events {r['events']}")
        if r["fetches"] or r["replaced"] or r["rt"] or r["bcast"]:
            bad.append(f"{t}_popup: pop-up did silent-path work {r['fetches']} {r['replaced']} {r['bcast']}")
        r = res[f"{t}_popup_post_blocked"]
        if len(r["bcast"]) != 1 or r["bcast"][0]["m"] != want or r["bcast"][0]["n"] != "ekd_auth":
            bad.append(f"{t}_popup_post_blocked: expected BroadcastChannel fallback with {want}, got {r['bcast']}")
        if r["events"].count("close") != 1:
            bad.append(f"{t}_popup_post_blocked: expected close once, events {r['events']}")
    return bad


def test_portal_callback_matches_econ_and_never_closes_a_silent_tab(tmp_path):
    _node_or_skip()
    bad = check_callback(run_jobs(callback_jobs(PORTAL_CALLBACK, ECON_CALLBACK), tmp_path))
    assert not bad, "\n".join(bad)


def test_portal_callback_is_one_script():
    html = PORTAL_CALLBACK.read_text(encoding="utf-8")
    assert len(re.findall(r"<script[ >]", html)) == 1, "two scripts is how the unguarded close came back"


# ------------------------------------------------------------------------------ 2+3. 404 nav
def page_jobs(portal_404: Path, portal_index: Path, econ_404: Path, econ_site: Path) -> dict:
    now = int(time.time() * 1000)
    fresh = str(now - 2_000)
    jobs = {}
    for ready in ("complete", "loading"):
        r = ready[0]
        for tag, f in (("p404", portal_404), ("pindex", portal_index)):
            base = dict(kind="page", file=str(f), host="elkassabgidata.com", ready=ready,
                        meName="Ahmed Elkassabgi")
            spent = {"ekd_silent_done": fresh, "ekd_silent_tries": "1"}
            jobs[f"{tag}_in_{r}"] = dict(base, local={"ekd_rt": "rt"}, session=spent)
            jobs[f"{tag}_out_{r}"] = dict(base, local={}, session=spent)
            jobs[f"{tag}_out_fresh_{r}"] = dict(base, local={}, session={})
            jobs[f"{tag}_sdk_blocked_{r}"] = dict(base, local={"ekd_rt": "rt"}, session=spent, sdk=False)
        base = dict(kind="page", file=str(econ_404), siteRoot=str(econ_site), host="econdatalibrary.com",
                    ready=ready, meName="Ahmed Elkassabgi")
        jobs[f"e404_key_{r}"] = dict(base, local={"edl_key": "k", "edl_name": "Ahmed Elkassabgi"}, session={})
        jobs[f"e404_out_{r}"] = dict(base, local={}, session={"edl_sso_checked": fresh, "edl_sso_tries": "1",
                                                               "ekd_silent_done": fresh})
    return jobs


def check_pages(res: dict) -> list[str]:
    bad = []
    for r in ("c", "l"):
        for tag in ("p404", "pindex"):
            x = res[f"{tag}_in_{r}"]
            if x["id"] != "ekd-chip" or "Ahmed" not in x["text"]:
                bad.append(f"{tag}_in_{r}: expected the signed-in chip 'Ahmed', got id={x['id']!r} text={x['text']!r}")
            # Signed out, the nav keeps the page's own label: "Sign in" on the index,
            # "Create free account" on the 404 (the markup was not changed here).
            label = {"p404": "Create free account", "pindex": "Sign in"}[tag]
            for k in ("out", "out_fresh", "sdk_blocked"):
                x = res[f"{tag}_{k}_{r}"]
                if x["id"] == "ekd-chip" or x["text"] != label:
                    bad.append(f"{tag}_{k}_{r}: expected {label!r}, got id={x['id']!r} text={x['text']!r}")
            for k in ("in", "out", "sdk_blocked"):
                if res[f"{tag}_{k}_{r}"]["replaced"]:
                    bad.append(f"{tag}_{k}_{r}: navigated away {res[f'{tag}_{k}_{r}']['replaced']}")
        # Identical behaviour: the 404 nav reads exactly what the index nav reads.
        for k in ("in",):
            a, b = res[f"p404_{k}_{r}"], res[f"pindex_{k}_{r}"]
            if (a["id"], a["text"]) != (b["id"], b["text"]):
                bad.append(f"p404_{k}_{r}: differs from index: {(a['id'], a['text'])} vs {(b['id'], b['text'])}")
        # Control: the index bounces a fresh signed-out visitor (the harness can see a bounce) ...
        b = res[f"pindex_out_fresh_{r}"]["replaced"]
        if not (len(b) == 1 and b[0].startswith("https://accounts.elkassabgidata.com/authorize?")):
            bad.append(f"pindex_out_fresh_{r}: control lost - index did not bounce: {b}")
        # ... and the 404 does not: only the paint was copied, not the resume.
        if res[f"p404_out_fresh_{r}"]["replaced"]:
            bad.append(f"p404_out_fresh_{r}: 404 started a sign-in round trip {res[f'p404_out_fresh_{r}']['replaced']}")
        x = res[f"e404_key_{r}"]
        if "chip" not in x["classes"].split() or "Ahmed" not in x["text"] or x["signedAttr"] != "1":
            bad.append(f"e404_key_{r}: expected the signed-in chip 'Ahmed', got classes={x['classes']!r} text={x['text']!r}")
        x = res[f"e404_out_{r}"]
        if "chip" in x["classes"].split() or x["text"] != "Sign in" or x["signedAttr"] is not None:
            bad.append(f"e404_out_{r}: expected 'Sign in', got classes={x['classes']!r} text={x['text']!r}")
        for k in ("e404_key", "e404_out"):
            if res[f"{k}_{r}"]["replaced"]:
                bad.append(f"{k}_{r}: navigated away {res[f'{k}_{r}']['replaced']}")
    for k, x in res.items():
        if (k.startswith("p404") or k.startswith("e404")) and x["errors"]:
            bad.append(f"{k}: script errors {x['errors']}")
    return bad


def test_both_404_navs_paint_the_family_session(tmp_path):
    _node_or_skip()
    bad = check_pages(run_jobs(page_jobs(PORTAL_404, PORTAL_INDEX, ECON_404, ECON_SITE), tmp_path))
    assert not bad, "\n".join(bad)


def test_econ_404_loads_sso_js_absolute_with_the_generator_pin():
    pins = set(re.findall(r"assets/sso\.js\?v=([0-9a-z]+)", GEN_SITE.read_text(encoding="utf-8")))
    assert len(pins) == 1, pins
    tags = re.findall(r'<script src="([^"]*sso\.js[^"]*)"', ECON_404.read_text(encoding="utf-8"))
    # Absolute: a 404 is served at whatever nested path was requested, where a relative
    # "assets/sso.js" resolves under that path and 404s itself.
    assert tags == [f"/assets/sso.js?v={pins.pop()}"], tags


def test_portal_404_paint_is_the_index_paint_verbatim():
    def paint(p: Path) -> str:
        s = p.read_text(encoding="utf-8")
        i = s.index("  function ready(fn) {")
        j = s.index("\n  });\n", s.index("    paint();\n", i)) + len("\n  });\n")
        return s[i:j]
    assert paint(PORTAL_404) == paint(PORTAL_INDEX)
