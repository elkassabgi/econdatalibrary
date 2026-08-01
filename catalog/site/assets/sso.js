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
  var K = 'edl_key', N = 'edl_name', C = 'edl_sso_checked';

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

  function signedIn() { return !!localStorage.getItem(K); }

  function updateUI() {
    if (!signedIn()) return;
    var a = document.querySelector('.nav a.signin')
         || document.querySelector('.nav a[href="account.html"]')
         || document.querySelector('.nav .signin');
    if (a) {
      // Show the first name. /v1/auth/sso already returns it as sso_name and step 1 above
      // already stores it in edl_name — it was only ever used for a tooltip nobody hovers,
      // so a signed-in visitor saw a generic word barely distinguishable from the "Sign in"
      // it replaced. Falls back to "Account" when no name was stored, and truncates so a
      // long name cannot push the nav links off a narrow screen. textContent, never
      // innerHTML: this value is a profile field the user types.
      var nm = (localStorage.getItem(N) || '').trim();
      var first = nm ? nm.split(/\s+/)[0] : '';
      if (first.length > 14) first = first.slice(0, 13) + '\u2026';
      a.textContent = first || 'Account';
      if (nm) a.title = 'Signed in as ' + nm;
    }
    if (document.body) document.body.setAttribute('data-signed-in', '1');
  }
  function onReady(fn) {
    if (document.readyState !== 'loading') fn();
    else document.addEventListener('DOMContentLoaded', fn);
  }
  function bounce() {
    location.replace(API + '/v1/auth/sso?return=' + encodeURIComponent(location.origin + location.pathname));
  }

  var hp = new URLSearchParams((location.hash || '').replace(/^#/, ''));

  // 1) Returning from the SSO redirect?
  if (hp.has('sso_key')) {
    var k = hp.get('sso_key');
    if (k && k !== 'none') {
      localStorage.setItem(K, k);
      var nm = hp.get('sso_name');
      if (nm) localStorage.setItem(N, nm);
    }
    sessionStorage.setItem(C, '1');
    // Tell page scripts (account.html) this page load IS the check's return trip,
    // so they don't immediately bounce again.
    window.__edl_ssoJustChecked = true;
    history.replaceState({}, document.title, location.pathname + location.search);
    onReady(updateUI);
    return;
  }

  // 2) Already signed in on econ (key stored locally).
  if (signedIn()) {
    if (hp.has('sso_recheck')) history.replaceState({}, document.title, location.pathname + location.search);
    onReady(updateUI);
    return;
  }

  // 3) Forced re-check: #sso_recheck (set by HF's "Back to Econ" link after you
  //    signed in there), or arriving from another family site — the HF session
  //    may have been created AFTER our silent check this session.
  var fam = /^https:\/\/(www\.)?(hfdatalibrary|elkassabgidata)\.com(\/|$)/;
  if (hp.has('sso_recheck') || (document.referrer && fam.test(document.referrer))) {
    sessionStorage.removeItem(C);
  }

  // 4) Already checked this browser session and found no HF session — don't loop.
  if (sessionStorage.getItem(C)) return;

  // 5) Only the production origins may auto-bounce: the SSO endpoint 403s any
  //    other return origin (e.g. *.pages.dev deployment previews), which would
  //    strand the visitor on the auth server's error page.
  if (!/^(www\.)?econdatalibrary\.com$/.test(location.hostname)) {
    sessionStorage.setItem(C, '1');
    return;
  }

  // 6) Not signed in, not yet checked → one silent SSO check for this session.
  sessionStorage.setItem(C, '1');
  bounce();
})();
