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
(function () {
  var API = 'https://api.hfdatalibrary.com';
  var K = 'edl_key', N = 'edl_name', C = 'edl_sso_checked';

  function signedIn() { return !!localStorage.getItem(K); }

  function updateUI() {
    if (!signedIn()) return;
    var a = document.querySelector('.nav a.signin')
         || document.querySelector('.nav a[href="account.html"]')
         || document.querySelector('.nav .signin');
    if (a) {
      a.textContent = 'Account';
      var nm = localStorage.getItem(N);
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

  // 5) Not signed in, not yet checked → one silent SSO check for this session.
  sessionStorage.setItem(C, '1');
  bounce();
})();
