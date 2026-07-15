"""Assemble DATABASE_LICENSES_VERBATIM.md from the workflow result (88 provider
findings, each with an adversarial verdict), one entry per database."""
import json, os, sys

OUT = sys.argv[1]
DEST = "D:/research/econfindatalibrary/DATABASE_LICENSES_VERBATIM.md"
TODAY = "2026-07-14"

results = json.load(open(OUT, encoding="utf-8"))["result"]

def final_class(r):
    f, v = r.get("finding") or {}, r.get("verdict") or {}
    cls = f.get("classification", "unclear_not_found")
    # adversarial correction wins
    if v.get("verdict") == "DISPUTED" and v.get("corrected_classification"):
        cls = v["corrected_classification"]
    return cls

# Written permissions already on file (override the PUBLIC terms the audit read).
# Source of truth: REDISTRIBUTION_COMPLIANCE.md + REDISTRIBUTION_EMAIL_TRAIL.md.
GRANTS = {
    "kof_globalization": "GRANTED in writing (Prof. Sturm, KOF/ETH Zurich): NC academic re-host; cite 'KOF, ETH Zurich' + link back.",
    "comtrade": "GRANTED in writing (UN Comtrade): 'you can proceed'; holdings must stay <=100,000 records; cite 'UN Comtrade' + link.",
    "whr": "GRANTED in writing (Gallup/WHR, REDACTED) but SCOPED to the Figure 2.1 summary ONLY; currently re-gated pending trim to that scope.",
    "gpi": "GRANTED in writing (IEP): CC BY-NC-SA 4.0 (non-commercial + ShareAlike).",
    "gti": "GRANTED in writing (IEP): CC BY-NC-SA 4.0 (non-commercial + ShareAlike).",
    "ppi": "GRANTED in writing (IEP): CC BY-NC-SA 4.0 (non-commercial + ShareAlike).",
    "etr": "GRANTED in writing (IEP): CC BY-NC-SA 4.0 (non-commercial + ShareAlike).",
}

def tier(r):
    f, v = r.get("finding") or {}, r.get("verdict") or {}
    granted = [s for s in (r.get("sources") or []) if s in GRANTS]
    if granted:
        if any(s == "whr" for s in granted):
            return "CLEARED by WRITTEN PERMISSION (scoped/conditional)"
        return "CLEARED by WRITTEN PERMISSION"
    cls = final_class(r)
    vd = v.get("verdict", "UNVERIFIABLE")
    if cls in ("prohibited", "permission_required"):
        return "RESTRICTED (keep gated)"
    if vd in ("DISPUTED", "UNVERIFIABLE") or cls == "unclear_not_found" or f.get("fetch_status") in ("not_found", "inaccessible"):
        return "NEEDS HUMAN REVIEW"
    if cls == "noncommercial_only":
        return "CLEARED - non-commercial only"
    if cls in ("redistributable_open", "redistributable_attribution"):
        return "CLEARED - re-host OK" + (" (attribution)" if cls == "redistributable_attribution" else "")
    return "NEEDS HUMAN REVIEW"

# expand to per-database
rows = []
for r in results:
    for sid in (r.get("sources") or []):
        rows.append((sid, r))
rows.sort(key=lambda t: t[0])

from collections import Counter
tier_counts = Counter(tier(r) for _, r in rows)
verdict_counts = Counter((r.get("verdict") or {}).get("verdict", "?") for _, r in rows)
class_counts = Counter(final_class(r) for _, r in rows)

def q(s):
    return (s or "").strip()

lines = []
A = lines.append
A(f"# Database licenses — verbatim redistribution audit\n")
A(f"**Generated {TODAY}** by the `econ-license-verbatim-audit` workflow (run wf_9ff754f5-37d): for every database, an agent fetched the provider's OFFICIAL terms, quoted the redistribution clause VERBATIM with the source URL, and classified it; a second, independent adversarial agent re-fetched the URL, confirmed the quote is word-for-word, and tried to refute any over-permissive reading. 88 providers, {len(rows)} databases, 176 agents, 0 errors.\n")
A("**This is the single source of truth. Do NOT re-derive it from scratch** — read it, and only re-run the workflow to fill gaps or refresh. Decision rule (asymmetric caution): a database is only *cleared to re-host* when the terms **explicitly permit redistribution/re-dissemination** by a third party AND the adversarial verifier CONFIRMED it. Anything restricted, ambiguous, unreachable, or DISPUTED stays gated / flagged for human review.\n")
A("---\n")
A("## Summary\n")
A("**Decision tiers (per database):**\n")
for t, n in sorted(tier_counts.items(), key=lambda x: -x[1]):
    A(f"- **{t}** — {n}")
A("\n**Adversarial verdicts:** " + ", ".join(f"{k}={v}" for k, v in verdict_counts.most_common()))
A("\n**Classifications:** " + ", ".join(f"{k}={v}" for k, v in class_counts.most_common()) + "\n")
present_grants = sorted({s for _, r in rows for s in (r.get("sources") or []) if s in GRANTS})
if present_grants:
    A("### Written permissions on file (override the public terms below)\n")
    A("The public terms the audit read may say 'permission required' for these, but we already hold written permission (see `REDISTRIBUTION_COMPLIANCE.md` / `REDISTRIBUTION_EMAIL_TRAIL.md`):\n")
    for s in present_grants:
        A(f"- `{s}` — {GRANTS[s]}")
    A("")

# needs-attention list first (RESTRICTED + REVIEW), by provider
attention = [r for r in results if tier(r).startswith("RESTRICTED") or tier(r) == "NEEDS HUMAN REVIEW"]
if attention:
    A("### ⚠️ Needs attention (restricted or unresolved) — review before serving\n")
    A("| Provider | Databases | Final classification | Verdict | Why |")
    A("|---|---|---|---|---|")
    for r in attention:
        f, v = r.get("finding") or {}, r.get("verdict") or {}
        why = (v.get("contradicting_clause") or v.get("notes") or f.get("reasoning") or "")[:120].replace("|", "/").replace("\n", " ")
        A(f"| {r['provider'][:40]} | {len(r.get('sources',[]))} | {final_class(r)} | {v.get('verdict','?')} | {why} |")
    A("")

A("---\n")
A("## Per-database index\n")
A("| Database | Provider | Final classification | Verdict | Tier |")
A("|---|---|---|---|---|")
for sid, r in rows:
    v = r.get("verdict") or {}
    A(f"| `{sid}` | {r['provider'][:34]} | {final_class(r)} | {v.get('verdict','?')} | {tier(r)} |")
A("")

A("---\n")
A("## Per-provider detail (verbatim terms)\n")
for r in sorted(results, key=lambda x: x["provider"].lower()):
    f, v = r.get("finding") or {}, r.get("verdict") or {}
    A(f"### {f.get('provider') or r['provider']}\n")
    A(f"- **Databases ({len(r.get('sources',[]))}):** " + ", ".join(f"`{s}`" for s in r.get("sources", [])))
    A(f"- **Official terms URL:** {f.get('official_terms_url','(none)')}")
    A(f"- **License:** {f.get('license_name','(unstated)')}")
    A(f"- **Classification:** {f.get('classification','?')}"
      + (f"  →  **corrected to `{v.get('corrected_classification')}`** by adversarial review" if v.get('verdict')=='DISPUTED' and v.get('corrected_classification') else ""))
    A(f"- **Commercial OK:** {f.get('commercial_ok')} · **Attribution required:** {f.get('attribution_required')} · **ShareAlike:** {f.get('sharealike')} · **Fetch:** {f.get('fetch_status')}")
    A(f"- **Adversarial verdict:** **{v.get('verdict','?')}** (quote verbatim: {v.get('quote_verified_verbatim')}, classification agrees: {v.get('classification_agrees')})")
    A(f"- **Decision tier:** {tier(r)}\n")
    A("**Verbatim quote:**")
    for qq in [f.get("verbatim_quote")] + (f.get("additional_quotes") or []):
        if q(qq):
            A("> " + q(qq).replace("\n", "\n> "))
    if v.get("contradicting_clause"):
        A(f"\n**Adversary's contradicting clause:** {v['contradicting_clause']}")
    if v.get("notes"):
        A(f"\n*Verifier notes:* {q(v['notes'])}")
    if f.get("reasoning"):
        A(f"\n*Researcher reasoning:* {q(f['reasoning'])}")
    A("\n---\n")

open(DEST, "w", encoding="utf-8", newline="\n").write("\n".join(lines))
print("WROTE", DEST, f"({os.path.getsize(DEST):,} bytes)")
print("\n=== TIER COUNTS (per database) ===")
for t, n in sorted(tier_counts.items(), key=lambda x: -x[1]):
    print(f"  {n:4}  {t}")
print("\n=== providers needing attention ===")
for r in attention:
    print(f"  [{(r.get('verdict') or {}).get('verdict','?'):12}] {final_class(r):26} {r['provider'][:44]}  ({len(r.get('sources',[]))} db)")
