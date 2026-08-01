// Cross-site single sign-on for the ElkassabgiData family.
// Accounts live on HF's auth server. When you're signed in there, this makes you
// appear signed in on econdatalibrary.com too, using a one-time redirect through
// HF's auth domain (so it works in every browser — no third-party cookies).
//
// Flow:
//   1. First econ page of a session, if we have no local key: redirect to
//      HF /v1/auth/sso?return=<this page>. HF reads its first-party session
//      cookie and bounces back with the key in the URL fragment (or sso_key=none).
//   2. On return, we store the key locally, strip the fragment, and flip the nav
//      "Sign in" to "Account". Subsequent pages use the stored key — no redirect.
//   3. The "checked" flag is re-armed (we check AGAIN) when you arrive from a
//      family site or with #sso_recheck — otherwise signing in on hfdatalibrary
//      AFTER the silent check ran would leave econ stuck on "signed out" for the
//      rest of the browser session.
// ── Notice banner (auto-expires; mirrors hfdatalibrary.com's site.js banner) ──
(function () {
  try {
    var EXP = Date.UTC(2026, 7, 1, 0, 0, 0); // 2026-08-01 00:00Z
    if (Date.now() > EXP) return;
    if (sessionStorage.getItem('apinotice-dismissed') === '1') return;
    function inject() {
      try {
        if (document.getElementById('maint-banner')) return;
        var bar = document.createElement('div');
        bar.id = 'maint-banner';
        bar.style.cssText = 'background:#1e3a5f;color:#fff;padding:0.6rem 2.2rem 0.6rem 1rem;' +
          'font-size:0.88rem;line-height:1.45;text-align:center;position:relative;z-index:1500;';
        bar.textContent = '⚙️ API access will be temporarily unavailable during a scheduled upgrade.';
        var x = document.createElement('button');
        x.textContent = '×'; x.setAttribute('aria-label', 'Dismiss');
        x.style.cssText = 'position:absolute;right:0.7rem;top:50%;transform:translateY(-50%);' +
          'background:none;border:none;color:#fff;font-size:1.1rem;cursor:pointer;';
        x.onclick = function () { bar.remove(); sessionStorage.setItem('apinotice-dismissed', '1'); };
        bar.appendChild(x);
        document.body.insertBefore(bar, document.body.firstChild);
      } catch (e) { /* banner must never break the page */ }
    }
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', inject); else inject();
  } catch (e) { /* never break the page */ }
})();

(function () {
  var API = 'https://api.hfdatalibrary.com';
  var K = 'edl_key', N = 'edl_name', C = 'edl_sso_checked', F = 'edl_family';

  // Highlight the current page's nav link (parity with hfdatalibrary.com,
  // whose .nav-links a.active gets the same pill background as :hover).
  // Runs on every page regardless of which SSO branch returns below.
  (function () {
    function mark() {
      var here = (location.pathname.split('/').pop() || 'index').replace(/\.html$/, '') || 'index';
      var links = document.querySelectorAll('.nav-links a, .nav a');
      for (var i = 0; i < links.length; i++) {
        var href = (links[i].getAttribute('href') || '').split('?')[0].split('#')[0].replace(/\.html$/, '');
        if (href && href === here) links[i].classList.add('active');
      }
    }
    if (document.readyState !== 'loading') mark();
    else document.addEventListener('DOMContentLoaded', mark);
  })();

  // "Signed in" used to mean ONLY "an api_key is in this browser". That was true when the
  // api_key was the only credential there was. Family sign-in changed it: a visitor can now
  // complete Google on accounts.elkassabgidata.com, download successfully on every page —
  // and still be told "Sign in" in the nav, because no api_key was ever stored. The owner
  // hit exactly that and reported it.
  //
  // F is written by account.html only when the account SERVER confirmed the family session
  // (its 'login' event), and deleted on logout and on sign-out, so it cannot outlive the
  // session. Deliberately not a read of the SDK's own storage: that is private to the SDK,
  // and a bare token read reports true for a session the server has already refused —
  // the mistake that produced a lockout on the account page earlier in this work.
  // TWO DIFFERENT QUESTIONS. They were briefly one function and that broke downloads.
  //
  // hasKey()   — "is the api_key in this browser?" This is what the silent-SSO logic below
  //              must ask. Step 2 returns early when it is true, skipping the one-per-session
  //              bounce to HF's /v1/auth/sso that FETCHES the key. Widening this to include a
  //              family session meant a signed-in visitor short-circuited that step, the key
  //              was never fetched, and downloads stopped working — the regression the owner
  //              reported as "back to sign-in and I can't download any more".
  //
  // signedIn() — "should the page present this visitor as signed in?" Used only by the nav.
  //              A confirmed family session counts here, because it genuinely is one.
  function hasKey()   { return !!localStorage.getItem(K); }
  function signedIn() { return !!(localStorage.getItem(K) || localStorage.getItem(F)); }

  function updateUI() {
    if (!signedIn()) return;
    var a = document.querySelector('.nav a.signin')
         || document.querySelector('.nav a[href="account.html"]')
         || document.querySelector('.nav .signin');
    if (a) {
      // Show the person's FIRST NAME rather than the word "Account". The name was already
      // stored and used only for a tooltip, which nobody hovers — so a signed-in visitor
      // got a generic label indistinguishable at a glance from the "Sign in" it replaced.
      // A name is the clearest possible confirmation that the family sign-in worked, and
      // this pill is the only signed-in indicator on most pages.
      //
      // Falls back to "Account" whenever there is no usable name (an older browser that
      // stored a key before names were kept, or a name that is blank/whitespace), so the
      // nav can never end up empty. First name only: the nav is a single line and long
      // full names push the other links off narrow screens. textContent, never innerHTML —
      // this value came from a profile field the user types.
      var nm = (localStorage.getItem(N) || '').trim();
      var first = nm ? nm.split(/\s+/)[0] : '';
      if (first.length > 14) first = first.slice(0, 13) + '…';
      a.textContent = first || 'Account';
      if (nm) a.title = 'Signed in as ' + nm;
    }
    if (document.body) document.body.setAttribute('data-signed-in', '1');
  }
  function onReady(fn) {
    if (document.readyState !== 'loading') fn();
    else document.addEventListener('DOMContentLoaded', fn);
  }

  // Put the note where the visitor already looks — under the API-key bar on the download
  // page, or at the top of the account page. Silent when there is nothing to say, and it
  // never replaces or hides an existing element: it appends one line of its own.
  function showNote() {
    try {
      var raw = sessionStorage.getItem('edl_sso_note');
      if (!raw) return;
      var n = JSON.parse(raw);
      if (!n || !n.state) return;
      var host = document.querySelector('.keybar') || document.querySelector('.wrap') || document.body;
      if (!host || document.getElementById('edl-sso-note')) return;
      var p = document.createElement('p');
      p.id = 'edl-sso-note';
      p.style.cssText = 'margin:.55rem 0 0;font-size:.8rem;line-height:1.5;color:#6b7280';
      // textContent: every value here is server- or state-derived, never markup.
      p.textContent = 'Family sign-in check: ' + n.state + ' — ' + (n.detail || '');
      host.appendChild(p);
    } catch (e) { /* a diagnostic must never break the page */ }
  }
  function bounce() {
    location.replace(API + '/v1/auth/sso?return=' + encodeURIComponent(location.origin + location.pathname));
  }

  var hp = new URLSearchParams((location.hash || '').replace(/^#/, ''));

  // Records what the silent check actually did, so a failure can be read off the page
  // instead of out of the dev-tools network tab. Six rounds of this were diagnosed by
  // inference because the only observable was "signed out", which every distinct failure
  // produces. Written to sessionStorage so it survives the redirect and can be shown on
  // whatever page the visitor lands on.
  function note(state, detail) {
    try { sessionStorage.setItem('edl_sso_note', JSON.stringify({ state: state, detail: detail, at: Date.now() })); } catch (e) {}
  }

  // 1) Returning from the SSO redirect?
  if (hp.has('sso_key')) {
    var k = hp.get('sso_key');
    if (k && k !== 'none') {
      localStorage.setItem(K, k);
      var nm = hp.get('sso_name');
      if (nm) localStorage.setItem(N, nm);
      note('key-received', 'signed in from your hfdatalibrary session' + (nm ? ' as ' + nm : ' (no name returned)'));
    } else {
      note('no-session', 'hfdatalibrary reported no signed-in session for this browser');
    }
    sessionStorage.setItem(C, String(Date.now()));
    // Tell page scripts (account.html) this page load IS the check's return trip,
    // so they don't immediately bounce again.
    window.__edl_ssoJustChecked = true;
    history.replaceState({}, document.title, location.pathname + location.search);
    onReady(updateUI); onReady(showNote);
    return;
  }

  // 2) The key is already stored locally — nothing to fetch, so skip the bounce.
  //    hasKey(), NOT signedIn(): a family session is not a key, and treating it as one
  //    skips step 6 and leaves the visitor without the credential this step exists to get.
  if (hasKey()) {
    note('have-key', 'a key is already stored in this browser');
    if (hp.has('sso_recheck')) history.replaceState({}, document.title, location.pathname + location.search);
    onReady(updateUI); onReady(showNote);
    return;
  }

  // 3) Forced re-check: #sso_recheck (set by HF's "Back to Econ" link after you
  //    signed in there), or arriving from another family site — the HF session
  //    may have been created AFTER our silent check this session.
  var fam = /^https:\/\/(www\.)?(hfdatalibrary|elkassabgidata)\.com(\/|$)/;
  if (hp.has('sso_recheck') || (document.referrer && fam.test(document.referrer))) {
    sessionStorage.removeItem(C);
  }

  // 3b) A "no session" answer GOES STALE, so stop treating it as final for the whole
  //     browser session. This is the bug the owner hit, and it looks exactly like the SSO
  //     being broken:
  //       open a browser → visit Econ (no session yet) → we ask HF, get "none", and set the
  //       flag → go to hfdatalibrary and sign in → come back to Econ → the flag is still set,
  //       so we never ask again and the visitor stays signed out for the rest of the session.
  //     Step 3 above re-arms on a referrer from a family site, but that only helps when the
  //     visitor arrives by LINK. Type the address, use a bookmark, or switch to a tab that
  //     was already open, and there is no referrer — which is what actually happens.
  //
  //     A negative answer is only true until the visitor signs in somewhere else, so it is
  //     worth remembering for a short while and no longer. A POSITIVE answer needs no timer:
  //     once a key is stored, step 2 short-circuits and we never reach here at all.
  //
  //     The flag still does its real job — preventing a redirect loop — because it is written
  //     BEFORE the bounce and re-checked here: the worst case is one silent round trip every
  //     RECHECK_MS, not a loop.
  //     THE WINDOW MUST BE SHORTER THAN A SIGN-IN, NOT LONGER. The first version of this
  //     used five minutes, which was useless: the visitor goes to hfdatalibrary, signs in,
  //     and is back inside a minute, so the flag was still fresh and nothing re-checked.
  //     The window only has to outlast a page load, because its single job is to stop an
  //     immediate re-bounce on the return trip. Twenty seconds does that with room to spare
  //     and is far shorter than any real sign-in.
  //
  //     The loop backstop is the TRY COUNTER, not the clock. Even if something pathological
  //     made every check return "none" instantly, this can bounce at most MAX_TRIES times per
  //     browser session and then stops for good. The clock decides how RESPONSIVE the
  //     re-check is; the counter decides whether it can ever run away.
  var RECHECK_MS = 20 * 1000;
  var MAX_TRIES = 5;
  var TRIES = 'edl_sso_tries';
  var tries = parseInt(sessionStorage.getItem(TRIES) || '0', 10) || 0;
  var stamp = parseInt(sessionStorage.getItem(C) || '0', 10);
  // '1' is the legacy value older builds wrote; treat it as "checked, time unknown" and let
  // it expire at once rather than stranding a visitor across a deploy.
  if (tries < MAX_TRIES && (stamp === 1 || (stamp && (Date.now() - stamp) > RECHECK_MS))) {
    sessionStorage.removeItem(C);
  }

  // 4) Already checked this browser session and found no HF session — don't loop.
  if (sessionStorage.getItem(C)) {
    var age = Math.round((Date.now() - (parseInt(sessionStorage.getItem(C), 10) || Date.now())) / 1000);
    note(tries >= MAX_TRIES ? 'capped' : 'skipped',
         tries >= MAX_TRIES
           ? 'stopped after ' + tries + ' checks this browser session'
           : 'last check was ' + age + 's ago; re-checks after ' + (RECHECK_MS / 1000) + 's');
    onReady(showNote);
    return;
  }

  // 5) Only the production origins may auto-bounce: the SSO endpoint 403s any
  //    other return origin (e.g. *.pages.dev deployment previews), which would
  //    strand the visitor on the auth server's error page.
  if (!/^(www\.)?econdatalibrary\.com$/.test(location.hostname)) {
    sessionStorage.setItem(C, String(Date.now()));
    return;
  }

  // 6) No key, and the last check has gone stale → ask again.
  //    Both writes happen BEFORE the navigation, so a check that never returns still counts.
  //    That ordering is the whole loop guarantee: at most MAX_TRIES round trips per browser
  //    session no matter what the server does or how fast the page reloads.
  sessionStorage.setItem(TRIES, String(tries + 1));
  sessionStorage.setItem(C, String(Date.now()));
  bounce();
})();
