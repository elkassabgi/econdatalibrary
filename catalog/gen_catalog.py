"""Generate the interactive Data Catalog page from catalog.json (self-contained HTML)."""
import json, os

HERE = os.path.dirname(os.path.abspath(__file__))
cat = json.load(open(os.path.join(HERE, "catalog.json"), encoding="utf-8"))

# Fold in live findings (stale/moved URLs, maintenance notes, which adapters are built).
fnd_path = os.path.join(HERE, "_findings.json")
if os.path.exists(fnd_path):
    fnd = json.load(open(fnd_path, encoding="utf-8"))
    built = set(fnd.get("adapter_built", []))
    findings = fnd.get("findings", {})
    for s in cat["sources"]:
        s["adapter_built"] = s["id"] in built
        f = findings.get(s["id"], {})
        if f.get("url_status"):
            s["url_status"] = f["url_status"]
        if f.get("maintenance"):
            s["maintenance"] = f["maintenance"]
        if f.get("blocker") and not s.get("blockers"):
            s["blockers"] = f["blocker"]

DATA = json.dumps(cat, ensure_ascii=False).replace("</", "<\\/")

TPL = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Data Catalog - econdatalibrary</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@500;700&display=swap" rel="stylesheet">
<style>
:root{--navy:#1a2332;--navy-light:#243044;--blue:#2563eb;--blue-pale:#eff6ff;--gold:#d4a843;--gold-deep:#8a6d27;
--g50:#f9fafb;--g100:#f3f4f6;--g200:#e5e7eb;--g300:#d1d5db;--g500:#6b7280;--g600:#4b5563;--g700:#374151;--g800:#1f2937;
--green:#047857;--red:#b91c1c;--amber:#92600a;--serif:Georgia,serif;--sans:"Inter",system-ui,sans-serif;--mono:"JetBrains Mono",monospace}
*{box-sizing:border-box;margin:0;padding:0}body{font-family:var(--sans);color:var(--g800);background:#fff;line-height:1.6}
.nav{background:var(--navy);color:#fff;padding:0 1.5rem;height:60px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:50}
.brand{font-family:var(--serif);font-weight:700;font-size:1.2rem}.brand .d{color:var(--gold)}
.nav a{color:rgba(255,255,255,.8);text-decoration:none;font-size:.9rem;margin-left:1rem}
.hero{background:linear-gradient(135deg,var(--navy),var(--navy-light));color:#fff;padding:2.5rem 1.5rem;text-align:center}
.hero h1{font-family:var(--serif);font-size:2rem}.hero h1 .d{color:var(--gold)}
.hero p{color:rgba(255,255,255,.75);max-width:680px;margin:.5rem auto 0}
.hero .stat{display:inline-block;margin:1rem .9rem 0;font-family:var(--mono)}.hero .stat b{color:var(--gold);font-size:1.4rem;display:block}
.hero .stat span{font-size:.75rem;color:rgba(255,255,255,.6);text-transform:uppercase;letter-spacing:.05em}
.wrap{max-width:1200px;margin:0 auto;padding:1.5rem}
.controls{display:flex;gap:.75rem;flex-wrap:wrap;margin-bottom:1rem;position:sticky;top:60px;background:#fff;padding:.75rem 0;z-index:10}
.controls input,.controls select{padding:.55rem .8rem;border:1px solid var(--g300);border-radius:8px;font-size:.9rem;font-family:var(--sans)}
.controls input{flex:1;min-width:220px}
.count{color:var(--g500);font-size:.85rem;align-self:center}
table{width:100%;border-collapse:collapse;font-size:.88rem}
thead th{text-align:left;padding:.6rem .7rem;border-bottom:2px solid var(--g300);color:var(--g700);font-weight:600;cursor:pointer;white-space:nowrap}
tbody td{padding:.55rem .7rem;border-bottom:1px solid var(--g200);vertical-align:top}
tbody tr{cursor:pointer}tbody tr:hover{background:var(--g50)}
.mono{font-family:var(--mono);font-size:.82rem}
.pill{display:inline-block;font-size:.72rem;font-weight:600;padding:.12rem .5rem;border-radius:999px;white-space:nowrap}
.pill.live{background:#ecfdf5;color:var(--green)}.pill.unrun{background:var(--g100);color:var(--g500)}
.pill.blocked{background:#fef2f2;color:var(--red)}.pill.partial{background:#fffbeb;color:var(--amber)}
.s-strat{font-size:.75rem;color:var(--g500)}
.obs{font-family:var(--mono);font-weight:600;color:var(--navy)}
#scrim{display:none;position:fixed;inset:0;background:rgba(15,20,30,.55);z-index:100}
#panel{position:fixed;top:0;right:0;bottom:0;width:min(680px,94vw);background:#fff;z-index:101;overflow-y:auto;box-shadow:-8px 0 30px rgba(0,0,0,.25);transform:translateX(100%);transition:transform .2s}
#panel.open{transform:none}
.pd{padding:1.75rem 2rem;position:relative}
.pd h2{font-family:var(--serif);color:var(--navy);font-size:1.5rem}
.pd .pid{font-family:var(--mono);color:var(--gold-deep);font-size:.85rem;margin-bottom:.25rem}
.pd .close{position:absolute;top:1rem;right:1.25rem;font-size:1.6rem;color:var(--g500);cursor:pointer;background:none;border:none}
.pd .desc{white-space:pre-wrap;background:var(--g50);border:1px solid var(--g200);border-radius:8px;padding:1rem;font-size:.85rem;color:var(--g700);margin:1rem 0;max-height:340px;overflow:auto}
.kv{display:grid;grid-template-columns:165px 1fr;gap:.4rem .9rem;font-size:.88rem;margin-top:1rem}
.kv dt{color:var(--g500);font-weight:600}.kv dd{color:var(--g800);word-break:break-word}
.sec-h{font-family:var(--serif);color:var(--navy);font-size:1.05rem;margin:1.5rem 0 .3rem;border-bottom:1px solid var(--g200);padding-bottom:.3rem}
.note{font-size:.85rem;color:var(--g600);background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:.6rem .8rem;margin-top:.6rem}
.foot{color:var(--g500);font-size:.8rem;text-align:center;padding:2rem 1rem}
</style></head><body>
<div class="nav"><div class="brand">econ<span class="d">datalibrary</span> &middot; Data Catalog</div>
<div><a href="#">Overview</a><a href="#">API</a><a href="#">Cite</a></div></div>
<div class="hero"><h1>The Data <span class="d">Catalog</span></h1>
<p>Every source we host, in detail - provider, license, coverage, how it updates, and how fresh it is. Built for researchers who need to know exactly what they are citing.</p>
<div><div class="stat"><b id="h-src">0</b><span>sources</span></div>
<div class="stat"><b id="h-obs">0</b><span>measured obs*</span></div>
<div class="stat"><b id="h-live">0</b><span>auto-updating</span></div></div>
<p style="font-size:.72rem;opacity:.6;margin-top:.8rem">*measured-to-date; full recount in progress as first-pass ingestion completes</p></div>
<div class="wrap">
<div class="controls">
<input id="q" placeholder="Search source, provider, description..." oninput="render()">
<select id="fStrat" onchange="render()"><option value="">All update strategies</option></select>
<select id="fStat" onchange="render()"><option value="">All statuses</option><option>live</option><option>unrun</option><option>blocked</option><option>partial</option></select>
<select id="fCad" onchange="render()"><option value="">All cadences</option></select>
<span class="count" id="count"></span>
</div>
<table><thead><tr>
<th onclick="sortBy('id')">Source</th><th onclick="sortBy('name')">Name</th>
<th onclick="sortBy('measured_obs')">Measured obs</th><th onclick="sortBy('cadence')">Cadence</th>
<th onclick="sortBy('strategy')">Update</th><th>Status</th><th onclick="sortBy('last_obs')">Newest data</th>
</tr></thead><tbody id="rows"></tbody></table>
<p class="foot">Generated __GEN__ &middot; __COUNT__ sources &middot; figures measured from Parquet footers, not estimated.</p>
</div>
<div id="scrim" onclick="closeP()"></div>
<div id="panel"><div class="pd" id="pd"></div></div>
<script>
const CAT=__DATA__;
const S=CAT.sources;
let sortKey="id",sortDir=1;
function statusOf(r){var b=(r.blockers||"");if(/requires|missing|403|waf|nxdomain|eherkenning/i.test(b)&&/key|403|waf|eherkenning|nxdomain/i.test(b))return "blocked";if(r.state_status==="partial")return "partial";if(r.state_status==="ok"||r.last_obs)return "live";return "unrun";}
function fmtObs(n){if(n==null)return '<span style="color:#9ca3af">&mdash;</span>';if(n>=1e9)return (n/1e9).toFixed(1)+'B';if(n>=1e6)return (n/1e6).toFixed(1)+'M';if(n>=1e3)return (n/1e3).toFixed(0)+'K';return ''+n;}
function uniq(k){return [...new Set(S.map(r=>r[k]).filter(Boolean))].sort();}
function init(){
 document.getElementById('h-src').textContent=CAT.count;
 var tot=S.reduce((a,r)=>a+(r.measured_obs||0),0);
 document.getElementById('h-obs').textContent=tot>=1e9?(tot/1e9).toFixed(1)+'B':(tot/1e6).toFixed(0)+'M';
 document.getElementById('h-live').textContent=S.filter(r=>statusOf(r)==='live').length;
 var fs=document.getElementById('fStrat');uniq('strategy').forEach(v=>fs.add(new Option(v.replace(/_/g,' '),v)));
 var fc=document.getElementById('fCad');uniq('cadence').forEach(v=>fc.add(new Option(v,v)));
 render();
}
function sortBy(k){sortDir=(sortKey===k)?-sortDir:1;sortKey=k;render();}
function render(){
 const q=document.getElementById('q').value.toLowerCase();
 const st=document.getElementById('fStrat').value,sta=document.getElementById('fStat').value,cad=document.getElementById('fCad').value;
 let rows=S.filter(r=>{
  if(st&&r.strategy!==st)return false;if(cad&&r.cadence!==cad)return false;
  if(sta&&statusOf(r)!==sta)return false;
  if(q){const h=(r.id+' '+r.name+' '+(r.description||'')).toLowerCase();if(!h.includes(q))return false;}
  return true;});
 rows.sort((a,b)=>{let x=a[sortKey],y=b[sortKey];if(x==null)x=(sortKey==='measured_obs')?-1:'';if(y==null)y=(sortKey==='measured_obs')?-1:'';return (x>y?1:x<y?-1:0)*sortDir;});
 document.getElementById('count').textContent=rows.length+' of '+S.length;
 const tb=document.getElementById('rows');tb.innerHTML='';
 rows.forEach(r=>{const tr=document.createElement('tr');tr.onclick=()=>openP(r.id);
  const stat=statusOf(r);
  tr.innerHTML='<td class="mono">'+r.id+'</td><td>'+r.name+'</td>'+
   '<td class="obs">'+fmtObs(r.measured_obs)+'</td><td>'+(r.cadence||'')+'</td>'+
   '<td class="s-strat">'+(r.strategy||'').replace(/_/g,' ')+'</td>'+
   '<td><span class="pill '+stat+'">'+stat+'</span></td>'+
   '<td class="mono">'+(r.last_obs||'&mdash;')+'</td>';
  tb.appendChild(tr);});
}
function esc(s){return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;');}
function row(dt,dd){return dd?'<dt>'+dt+'</dt><dd>'+esc(''+dd)+'</dd>':'';}
function openP(id){const r=S.find(x=>x.id===id);if(!r)return;
 const stat=statusOf(r);
 let h='<button class="close" onclick="closeP()">&times;</button>';
 h+='<div class="pid">'+r.id+'  &middot;  <span class="pill '+stat+'">'+stat+'</span></div><h2>'+esc(r.name)+'</h2>';
 if(r.description)h+='<div class="desc">'+esc(r.description)+'</div>';
 h+='<div class="sec-h">Coverage</div><dl class="kv">'+
   row('Measured obs', r.measured_obs!=null?r.measured_obs.toLocaleString()+' (measured)':'pending full recount')+
   row('Newest observation', r.last_obs)+row('Storage layout', r.storage_layout)+'</dl>';
 h+='<div class="sec-h">Licensing &amp; provenance</div><dl class="kv">'+
   row('License / terms', r.license_note)+row('Ingest script', (r.scripts||[]).join(', '))+'</dl>';
 h+='<div class="sec-h">How it updates</div><dl class="kv">'+
   row('Strategy', (r.strategy||'').replace(/_/g,' '))+row('Cadence', r.cadence)+
   row('Change detection', r.vintage_signal)+row('Access method', r.access)+
   row('API key', (r.key_env && !/^none/i.test(r.key_env))?r.key_env:'none required')+
   row('Adapter', r.adapter_built?'built (auto-updating)':'pending')+
   row('Last successful run', r.last_success)+'</dl>';
 if(r.blockers && !/^(none|no\b|n\/a)/i.test(r.blockers.trim()))h+='<div class="note"><b>Caveats / access:</b> '+esc(r.blockers)+'</div>';
 if(r.url_status)h+='<div class="note"><b>Source URL status:</b> '+esc(r.url_status)+'</div>';
 if(r.maintenance)h+='<div class="note"><b>Maintenance:</b> '+esc(r.maintenance)+'</div>';
 document.getElementById('pd').innerHTML=h;
 document.getElementById('scrim').style.display='block';document.getElementById('panel').classList.add('open');
}
function closeP(){document.getElementById('scrim').style.display='none';document.getElementById('panel').classList.remove('open');}
document.addEventListener('keydown',e=>{if(e.key==='Escape')closeP();});
init();
</script></body></html>"""

out = TPL.replace("__DATA__", DATA).replace("__GEN__", cat["generated"]).replace("__COUNT__", str(cat["count"]))
open(os.path.join(HERE, "catalog.html"), "w", encoding="utf-8").write(out)
print("wrote catalog/catalog.html", round(os.path.getsize(os.path.join(HERE, "catalog.html")) / 1024), "KB")
