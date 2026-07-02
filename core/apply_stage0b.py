"""Apply Stage 0b verified license metadata to the registry — CONSERVATIVELY.

Reads the workflow result JSON and updates the source registry + sidecars.
Legal gate rule (safe-by-default):
  reservable=1 (we may redistribute) ONLY when the license_id is a RECOGNISED
  open class (CC-BY / CC0 / public-domain / OGL / Etalab / ODbL / *-IGO open)
  AND confidence=high AND a verbatim license_evidence quote exists.
  Everything else (custom-terms, all-rights-reserved, NC/ND, *-by-ask,
  unverified, low confidence) -> reservable=0. Under-serving is safe; the gate
  never opens on doubt.
Citation/homepage/terms/attribution are applied for high+medium (not legally
gating); low confidence keeps license_id=NEEDS-REVIEW.

Run: python core/apply_stage0b.py <path-to-wr9w2orz1.output>
"""
from __future__ import annotations
import json, os, re, sys
import catalog as cat

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CLEAN = os.path.join(ROOT, "data", "clean_full")
OUT = sys.argv[1] if len(sys.argv) > 1 else \
    r"D:\temp\claude\D--research-hfdatalibrary\b9dda646-3b45-4dc7-96e1-c9b3efa36387\tasks\wr9w2orz1.output"

OPEN_RE = re.compile(
    r"^(us-public-domain|public-domain|cc0|cc-by-4\.0|cc-by-3\.0(-igo)?|cc-by-2\.5(-igo)?|"
    r"ogl-uk-3\.0|ogl-3\.0|etalab-2\.0|odbl-1\.0|statcan-open|dl-de-by[\w.-]*|kogl[\w-]*|govdata-by)$",
    re.I)
NONOPEN_HINT = re.compile(r"\b(nc|nd|non[- ]?commercial|no[- ]?deriv|all rights reserved|by-ask)\b", re.I)


def is_open(license_id: str, conf: str, evidence: str) -> bool:
    if conf != "high" or not (evidence or "").strip():
        return False
    if NONOPEN_HINT.search(license_id or ""):
        return False
    return bool(OPEN_RE.match((license_id or "").strip().lower()))


def parse_flags(flags: str) -> tuple[int, int, int]:
    """best-effort commercial_ok, attribution_required, no_modify from license_flags."""
    f = (flags or "").lower()
    commercial = 0 if re.search(r"commercial_ok\((no|false|requires|by ?permission|unknown)", f) else (1 if "commercial_ok" in f else 0)
    attrib = 1 if re.search(r"attribution\(?(required|true)", f) else 0
    nomod = 1 if re.search(r"(no_modify|share[_-]?alike|share_alike)\((?:required|true)\)|share[_-]?alike\b", f) else 0
    return commercial, attrib, nomod


def main():
    data = json.load(open(OUT, encoding="utf-8"))
    res = data.get("result", data) if isinstance(data, dict) else data
    results = (res or {}).get("results") or data.get("results") or []
    conn = cat.connect(); cat.init(conn)

    # 1) ensure every referenced license_id exists as a class (conservative flags)
    seen_lic = {r[0] for r in conn.execute("SELECT license_id FROM license")}
    added_lic = 0
    for r in results:
        lid = (r.get("license_id") or "").strip()
        if not lid or lid == "unverified" or lid in seen_lic:
            continue
        commercial, attrib, nomod = parse_flags(r.get("license_flags"))
        reservable = 1 if is_open(lid, r.get("confidence"), r.get("license_evidence")) else 0
        conn.execute("INSERT OR REPLACE INTO license(license_id,name,reservable,commercial_ok,attribution_required,no_modify,url) VALUES(?,?,?,?,?,?,?)",
                     (lid, lid, reservable, commercial, attrib, nomod, r.get("terms_url") or ""))
        seen_lic.add(lid); added_lic += 1

    # 2) apply per-source
    applied = reservable_yes = kept_review = 0
    conf_counts = {}
    for r in results:
        sid = r["id"]; conf = r.get("confidence", "low")
        conf_counts[conf] = conf_counts.get(conf, 0) + 1
        lid = (r.get("license_id") or "").strip()
        open_ok = is_open(lid, conf, r.get("license_evidence"))
        # license_id: apply for high+medium & determinate; else keep NEEDS-REVIEW
        if conf in ("high", "medium") and lid and lid != "unverified":
            use_lic = lid
        else:
            use_lic = "NEEDS-REVIEW"; kept_review += 1
        if open_ok:
            reservable_yes += 1
        conn.execute("UPDATE source SET name=COALESCE(?,name), homepage=?, license_id=?, attribution=?, terms_url=? WHERE source_id=?",
                     (r.get("official_name"), r.get("homepage"), use_lic, r.get("attribution"), r.get("terms_url"), sid))
        applied += 1
        # sidecar
        ddir = os.path.join(CLEAN, sid)
        sc = os.path.join(ddir, "_provider.json")
        if os.path.exists(sc):
            try:
                d = json.load(open(sc, encoding="utf-8"))
            except Exception:
                d = {"schema": "econdl/provider/0.1", "source_id": sid}
            d.update({
                "name": r.get("official_name") or d.get("name"),
                "homepage": r.get("homepage"), "terms_url": r.get("terms_url"),
                "license_id": use_lic, "attribution": r.get("attribution"),
                "citation_template": r.get("citation_template"),
                "license_evidence": r.get("license_evidence"),
                "reservable": bool(open_ok), "confidence": conf,
                "_status": "curated" if use_lic != "NEEDS-REVIEW" else "needs-review",
            })
            tmp = sc + ".tmp"
            json.dump(d, open(tmp, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
            os.replace(tmp, sc)
    conn.commit()

    n_src = conn.execute("SELECT COUNT(*) FROM source").fetchone()[0]
    curated = conn.execute("SELECT COUNT(*) FROM source WHERE license_id!='NEEDS-REVIEW'").fetchone()[0]
    reservable = conn.execute("SELECT COUNT(*) FROM source s JOIN license l ON s.license_id=l.license_id WHERE l.reservable=1").fetchone()[0]
    conn.close()
    print(f"applied 0b to {applied} sources | new license classes: {added_lic}")
    print(f"confidence: {conf_counts}")
    print(f"sources total: {n_src} | curated license: {curated} | NEEDS-REVIEW kept: {n_src-curated}")
    print(f"RESERVABLE (gate open, may redistribute): {reservable}  | NON-reservable (safe-held): {n_src-reservable}")


if __name__ == "__main__":
    main()
