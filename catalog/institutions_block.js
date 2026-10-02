// Institutions Represented - ONE block, the same bytes on hfdatalibrary.com, econdatalibrary.com and
// ipdatalibrary.com. A test in each repository checks this file's hash; change all three together.
//
// The list comes from ONE feed, already cleaned, named, ordered and cut:
//   https://api.hfdatalibrary.com/v1/public-stats  ->  institutions: [{institution, users, featured, icon}]
// The rules live in the hfdatalibrary repository, api/src/institutions.js. This code sorts nothing,
// filters nothing and renames nothing: a page that kept its own list is how the three sites came to
// show three different lists (found 2026-10-02).
//
// Names are typed by users, so every one is escaped. This file has no backslash on purpose: two of the
// three sites embed it in a Python string.
var EKD_FAMILY_STATS = 'https://api.hfdatalibrary.com/v1/public-stats';

function ekdEsc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
    return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
  });
}

function ekdToggleInstitutions() {
  var more = document.getElementById('inst-more');
  var sign = document.getElementById('inst-sign');
  var toggle = document.getElementById('inst-toggle');
  if (!more || !sign) return;
  var open = more.style.display !== 'none';
  more.style.display = open ? 'none' : 'block';
  sign.textContent = open ? '+' : String.fromCharCode(8722);
  if (toggle) toggle.setAttribute('aria-expanded', open ? 'false' : 'true');
}

// Returns '' when the feed has no usable row (also for a feed older than this block, whose rows carry no
// "featured" flag): the caller then shows the "not available" line, never a half-cleaned list.
function ekdInstitutionsHtml(list) {
  var rows = (Array.isArray(list) ? list : []).filter(function (i) {
    return i && typeof i.institution === 'string' && i.institution && typeof i.featured === 'boolean';
  });
  if (!rows.length) return '';
  function icon(v) {
    var inner = '';
    if (typeof v === 'string' && v) {
      var url = v.indexOf('https://') === 0 ? v
        : 'https://www.google.com/s2/favicons?sz=32&domain=' + encodeURIComponent(v);
      inner = '<img src="' + ekdEsc(url) + '" width="20" height="20" alt="" loading="lazy" ' +
        'style="vertical-align:middle;border-radius:3px;object-fit:contain" ' +
        'onerror="this.style.display=&quot;none&quot;">';
    }
    return '<span class="inst-ic" style="display:inline-block;width:20px;margin-right:8px;text-align:center;flex-shrink:0">' + inner + '</span>';
  }
  function row(i) {
    return '<div class="inst-row" style="display:flex;align-items:center;padding:0.28rem 0">' +
      icon(i.icon) + '<span class="inst-name">' + ekdEsc(i.institution) + '</span></div>';
  }
  var top = rows.filter(function (i) { return i.featured; });
  var rest = rows.filter(function (i) { return !i.featured; });
  var html = '<div class="inst-list">' + top.map(row).join('');
  if (rest.length) {
    html += '<div id="inst-toggle" class="inst-more" role="button" tabindex="0" style="cursor:pointer" ' +
      'aria-expanded="false" aria-controls="inst-more" ' +
      'onclick="ekdToggleInstitutions()" ' +
      'onkeydown="if(event.key===&quot;Enter&quot;||event.key===&quot; &quot;){event.preventDefault();ekdToggleInstitutions();}">' +
      '<span id="inst-sign" class="inst-sign">+</span> ' +
      '<span>Other institutions (' + rest.length + ')</span></div>' +
      '<div id="inst-more" style="display:none">' + rest.map(row).join('') + '</div>';
  }
  return html + '</div>';
}

async function ekdLoadInstitutions(elementId) {
  var el = document.getElementById(elementId);
  if (!el) return;
  var html = '';
  try {
    var r = await fetch(EKD_FAMILY_STATS);
    if (r.ok) html = ekdInstitutionsHtml((await r.json()).institutions);
  } catch (e) { html = ''; }
  el.innerHTML = html || '<p class="inst-empty" style="opacity:0.7">The list is not available right now.</p>';
}
