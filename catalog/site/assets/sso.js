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

  // ekd_rt counts for the NAV as well, and only for the nav.
  //
  // edl_family is written in exactly two places: account.html (on the SDK's login event) and
  // auth/callback.html (on a silent resume). Sign in through the pop-up on ANY other page —
  // download.html, most obviously — and neither runs, so the SDK holds a perfectly good session
  // while the nav still says "Sign in". Ahmed reported precisely that: "it showed as signed in
  // but the 'sign-in' button still showed sign-in, but I was able to download something." The
  // download worked because download.html asks the SDK directly; only the nav was lying.
  //
  // The standing warning against reading the SDK's storage is about a token that outlives the
  // session it names. That risk is real and bounded here: ekd_rt is removed by the SDK on logout
  // and whenever a refresh comes back 401, so the worst case is a nav pill that reads a name for
  // one page view after a server-side revocation, then corrects itself. Weigh that against the
  // status quo — telling every pop-up user they are signed out — and it is not close.
  //
  // hasKey() is deliberately NOT touched. It guards the bounce that FETCHES the api_key, and
  // widening it is what broke downloads before.
  function signedIn() { return !!(localStorage.getItem(K) || localStorage.getItem(F) || localStorage.getItem('ekd_rt')); }

  // Fill in the display name AFTER the page is up, not before it appears.
  //
  // auth/callback.html deliberately does not fetch the name: it is visible for exactly as long
  // as the work in front of its redirect takes, and a second round trip there is a second round
  // trip the user watches — the reason hf -> econ flashed an interstitial and econ -> hf did
  // not. So the callback stores the token and edl_family (which is what the nav needs
  // synchronously to stop saying "Sign in") and leaves the name to here, where a slow response
  // costs nothing visible: the pill simply reads "Account" until it resolves, then becomes
  // "Ahmed".
  //
  // Reading ekd_at directly is safe FOR A FETCH even though §hasKey warns against reading the
  // SDK's storage: that warning is about using a bare token as a signed-in TEST, where a stale
  // value reports true for a session the server already refused. Here a stale token simply
  // 401s, we set nothing, and the nav keeps its fallback — a wrong answer is impossible.
  function backfillName() {
    try {
      // Same widening as signedIn(): a pop-up sign-in on a page that is not account.html leaves
      // edl_family unset but ekd_rt present, and that visitor needs their name filled in too —
      // otherwise the pill they just earned reads "Account" forever.
      if (!(localStorage.getItem(F) || localStorage.getItem('ekd_rt'))) return;
      if (localStorage.getItem(N)) return;                                // already known
      var raw = localStorage.getItem('ekd_at');
      if (!raw) return;
      var at = JSON.parse(raw);
      if (!at || !at.t || !at.e || Date.now() >= at.e) return;           // expired → let the SDK refresh it elsewhere
      fetch('https://api.hfdatalibrary.com/v1/auth/me', { headers: { 'Authorization': 'Bearer ' + at.t } })
        .then(function (r) { return r.ok ? r.json() : null; })
        .then(function (u) {
          if (u && u.name) { localStorage.setItem(N, u.name); updateUI(); }
          if (u && u.email) localStorage.setItem('edl_email', u.email);
        })
        .catch(function () { /* cosmetic — the session is valid either way */ });
    } catch (e) {}
  }

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
      // hf-canonical avatar chip (family uniformity, Ahmed 2026-08-20): gold
      // initial circle + first name + caret on a navy pill. Built with DOM
      // nodes, never innerHTML — the name is user-typed profile data. CSS is
      // injected here once so all 244 static pages get it from this one file.
      if (!document.getElementById('ekd-chip-css')) {
        var st = document.createElement('style'); st.id = 'ekd-chip-css';
        st.textContent = '.nav a.signin.chip{background:var(--navy-light,#243044)!important;' +
          'color:#fff!important;display:inline-flex;align-items:center;gap:.5rem;' +
          'padding:.3rem .7rem .3rem .35rem!important;font-weight:600}' +
          '.nav a.signin.chip:hover{background:#2c3a52!important}' +
          '.nav a.signin.chip .init{background:var(--gold,#d4a843);color:var(--navy,#1a2332);' +
          'width:26px;height:26px;border-radius:50%;display:inline-flex;align-items:center;' +
          'justify-content:center;font-weight:800;font-size:.85rem}' +
          '.nav a.signin.chip .caret{font-size:.6rem;opacity:.7}';
        (document.head || document.documentElement).appendChild(st);
      }
      a.classList.add('chip');
      a.textContent = '';
      var init = document.createElement('span'); init.className = 'init';
      init.textContent = (first || 'A').charAt(0).toUpperCase();
      a.appendChild(init);
      a.appendChild(document.createTextNode(first || 'Account'));
      var caret = document.createElement('span'); caret.className = 'caret';
      caret.textContent = '▼';
      a.appendChild(caret);
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
    onReady(updateUI); onReady(backfillName); onReady(showNote);
    return;
  }

  // 1b) §SILENT-FAMILY-RESUME — ask the ACCOUNT SERVER, not just HF.
  //
  //     Steps 3–6 below ask exactly one question: "does this browser have an hfd_session
  //     cookie on api.hfdatalibrary.com?" That is the right question for someone who signed
  //     in to hfdatalibrary with a password, and the wrong one for someone who signed in
  //     through the ElkassabgiData pop-up — that flow sets ekd_session on
  //     accounts.elkassabgidata.com and never creates an hfd_session at all. So /v1/auth/sso
  //     truthfully answers "no session", econ shows "Sign in", and the visitor who signed in
  //     one tab ago is told they are a stranger. This is why econ → hf worked and hf → econ
  //     did not: hf now asks the IdP directly and econ still only asked HF.
  //
  //     A TOP-LEVEL navigation is required, not an iframe: ekd_session is SameSite=Lax, which
  //     a top-level GET carries and a framed request does not, and an iframe would also be
  //     third-party — so Safari, Firefox and Chrome-incognito would strip the cookie and
  //     report a signed-in visitor as signed out.
  //
  //     LOOP SAFETY: auth/callback.html writes ekd_silent_done BEFORE it can fail — on
  //     login_required, on a state mismatch, on a failed exchange — and this refuses to start
  //     when that flag is present. One attempt per browser session, then never again.
  //
  //     Placed BEFORE step 2 but AFTER step 1 on purpose. Before step 2, because a family
  //     visitor has no key and would otherwise fall through to the HF-only question that
  //     cannot see their session. After step 1, so the return trip from /v1/auth/sso is
  //     handled first and this never fires on a page load that is already completing a check.
  //     A "no session" ANSWER GOES STALE — the same defect this file already fixed once, for
  //     its own flag, twenty lines further down (§3b, RECHECK_MS/MAX_TRIES). I rebuilt it:
  //     ekd_silent_done was a bare sessionStorage flag with no expiry and no re-arm, so
  //         log out of econ -> log out of hf -> log back in to hf -> return to econ
  //     found the flag still set from the signed-out visit, never asked again, and showed
  //     "Sign in" to someone who had just signed in. Reported exactly that way.
  //
  //     Re-armed on the two signals that mean the answer may have changed:
  //       * arriving from a family site (that IS the "I just signed in over there" case), and
  //       * age, as a backstop for a bookmark, a typed address or an already-open tab, where
  //         there is no referrer at all.
  //     Bounded by tries so it can never run away: at most RESUME_MAX_TRIES round trips per
  //     browser session no matter what the server says. Clock decides responsiveness, counter
  //     decides whether it can loop — the same division §3b settled on.
  var RESUME_RECHECK_MS = 10 * 60 * 1000;
  var RESUME_MAX_TRIES = 3;
  var RESUME_TRIES_K = 'ekd_silent_tries';
  try {
    var famRef = /^https:\/\/(www\.)?(hfdatalibrary|elkassabgidata|ipdatalibrary)\.com(\/|$)/;
    var doneAt = parseInt(sessionStorage.getItem('ekd_silent_done') || '0', 10) || 0;
    var rTries = parseInt(sessionStorage.getItem(RESUME_TRIES_K) || '0', 10) || 0;
    // '1' is what the first build wrote — treat it as "checked, time unknown" and let it expire
    // at once rather than stranding this visitor until they close the browser.
    if (doneAt && rTries < RESUME_MAX_TRIES &&
        ((document.referrer && famRef.test(document.referrer)) || doneAt === 1 || (Date.now() - doneAt) > RESUME_RECHECK_MS)) {
      sessionStorage.removeItem('ekd_silent_done');
    }
  } catch (e) {}

  // An explicit sign-out this browser session means "stay out" — never auto-resume over it.
  if (!hasKey() && !localStorage.getItem(F)
      && !sessionStorage.getItem('ekd_signed_out')
      && !localStorage.getItem('ekd_signed_out')   // sessionStorage is per-TAB: without the
      // durable copy, signing out in one tab left every other tab - and any tab opened later -
      // free to resume, which signed the visitor straight back in whenever server-side
      // revocation had not landed. That is exactly the case this marker exists to cover.
      && !sessionStorage.getItem('ekd_silent_done')
      && /^(www\.)?econdatalibrary\.com$/.test(location.hostname)) {
    try { sessionStorage.setItem(RESUME_TRIES_K, String((parseInt(sessionStorage.getItem(RESUME_TRIES_K) || '0', 10) || 0) + 1)); } catch (e) {}
    try {
      var _ua = navigator.userAgent || '';
      // Never bounce a crawler. Googlebot renders JavaScript, so without this it would follow
      // the redirect off econdatalibrary.com to a noindex auth host on the first view of every
      // page it crawls — and unlike a human, a bot is never signed in, so it would pay it on
      // every page of every crawl. That is an SEO wound inflicted by a sign-in convenience.
      var _bot = navigator.webdriver
        || /bot|crawl|spider|slurp|bingpreview|duckduckbot|baiduspider|yandex|facebookexternalhit|slackbot|discordbot|telegrambot|whatsapp|applebot|petalbot|semrush|ahrefs|mj12bot|dotbot|lighthouse|headless/i.test(_ua);
      if (!_bot && window.isSecureContext !== false && window.crypto && crypto.subtle && crypto.getRandomValues) {
        var _b64 = function (b) { var s = ''; for (var i = 0; i < b.length; i++) s += String.fromCharCode(b[i]); return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, ''); };
        var _rand = function () { return _b64(crypto.getRandomValues(new Uint8Array(32))); };
        var _ver = _rand(), _st = _rand();
        crypto.subtle.digest('SHA-256', new TextEncoder().encode(_ver)).then(function (d) {
          sessionStorage.setItem('ekd_silent_v', _ver);
          sessionStorage.setItem('ekd_silent_s', _st);
          sessionStorage.setItem('ekd_silent_r', location.pathname + location.search + location.hash);
          var u = 'https://accounts.elkassabgidata.com/authorize?response_type=code&prompt=none'
            + '&client_id=' + encodeURIComponent(location.origin)
            + '&redirect_uri=' + encodeURIComponent(location.origin + '/auth/callback')
            + '&state=' + encodeURIComponent(_st)
            + '&code_challenge=' + encodeURIComponent(_b64(new Uint8Array(d)))
            + '&code_challenge_method=S256';
          // replace(), not assign(): the bounce must not become a history entry, or Back from
          // the restored page lands the visitor straight back in the redirect.
          location.replace(u);
        }).catch(function () {});
        return;   // navigating away — do not run the HF-only steps on this page load
      }
    } catch (e) { /* storage or crypto unavailable → fall through to the existing flow */ }
  }

  // 2) The key is already stored locally — nothing to fetch, so skip the bounce.
  //    hasKey(), NOT signedIn(): a family session is not a key, and treating it as one
  //    skips step 6 and leaves the visitor without the credential this step exists to get.
  if (hasKey()) {
    note('have-key', 'a key is already stored in this browser');
    if (hp.has('sso_recheck')) history.replaceState({}, document.title, location.pathname + location.search);
    onReady(updateUI); onReady(backfillName); onReady(showNote);
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
