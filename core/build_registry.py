"""Stage 0 — registry foundation. ZERO data risk: writes only the catalog DB and
per-source _provider.json sidecars. NEVER touches observation parquet.

Authoritative inputs (in priority order), NO fabrication:
  1. configs/sources.yaml  -- the curated legal backbone: a `licenses:` registry
     (reservable / commercial_ok / attribution / no_modify) and per-source
     license + attribution + homepage. This is the source of truth.
  2. catalog/catalog.json  -- names/descriptions for the 129 documented sources.
  3. on-disk data/clean_full/<source>/  -- authoritative for COVERAGE (all ~299).
Any source NOT covered by (1) gets license_id='NEEDS-REVIEW' (reservable=0 — do not
redistribute until the verified per-source pass, Stage 0b, confirms its real license).

Run: python core/build_registry.py
"""
from __future__ import annotations
import json, os
import yaml
import catalog as cat   # same dir

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CLEAN = os.path.join(ROOT, "data", "clean_full")
CATJSON = os.path.join(ROOT, "catalog", "catalog.json")
SRCYAML = os.path.join(ROOT, "configs", "sources.yaml")


def _attrib_required(v) -> int:
    # sources.yaml uses attribution: required | requested | false
    return 1 if str(v).strip().lower() == "required" else 0


def load_license_registry(yml: dict) -> list[tuple]:
    """license rows from sources.yaml `licenses:` block + a NEEDS-REVIEW sentinel."""
    rows = []
    for lid, f in (yml.get("licenses") or {}).items():
        f = f or {}
        reservable = 1 if f.get("reservable") is True else 0   # 'per_series' -> 0 (handle at ingest)
        rows.append((lid, lid, reservable,
                     1 if f.get("commercial_ok") else 0,
                     _attrib_required(f.get("attribution")),
                     1 if f.get("no_modify") else 0,
                     f.get("url") or ""))
    # extras the Stage 0b verified pass needs that aren't in sources.yaml yet
    rows.append(("educational-only", "Provided for research/educational use only", 0, 0, 1, 1, ""))
    rows.append(("custom-terms", "Source-specific terms — see terms_url", 0, 0, 1, 0, ""))
    rows.append(("NEEDS-REVIEW", "License not yet verified — do not redistribute until reviewed",
                 0, 0, 1, 0, ""))
    return rows


def discover(yml: dict) -> dict[str, dict]:
    """Union of on-disk source dirs + catalog.json + sources.yaml, merged."""
    srcs: dict[str, dict] = {}
    # coverage = on-disk dirs
    if os.path.isdir(CLEAN):
        for e in os.scandir(CLEAN):
            if e.is_dir() and not e.name.startswith("_"):
                srcs[e.name] = {"id": e.name, "name": e.name.replace("_", " ").title(), "on_disk": True}
    # names/descriptions from catalog.json
    if os.path.exists(CATJSON):
        for s in json.load(open(CATJSON, encoding="utf-8")).get("sources", []):
            d = srcs.setdefault(s["id"], {"id": s["id"]})
            d["name"] = s.get("name") or d.get("name") or s["id"]
            d["description"] = s.get("description")
            d["access"] = s.get("access")
            d["catalogued"] = True
    # AUTHORITATIVE license/attribution/homepage from sources.yaml
    for sid, s in (yml.get("sources") or {}).items():
        if not isinstance(s, dict):
            continue
        d = srcs.setdefault(sid, {"id": sid})
        d["name"] = s.get("name") or d.get("name") or sid
        d["license_id"] = s.get("license")
        d["attribution"] = s.get("attribution")
        d["homepage"] = s.get("homepage")
        d["terms_url"] = s.get("terms_url") or s.get("homepage")
        d["yaml_listed"] = True
    return srcs


def main():
    yml = yaml.safe_load(open(SRCYAML, encoding="utf-8"))
    valid_lic = set((yml.get("licenses") or {}).keys())

    conn = cat.connect()
    cat.init(conn)
    conn.execute("DELETE FROM license")   # idempotent clean re-seed (no casing-duplicate leftovers)
    conn.executemany(
        "INSERT OR REPLACE INTO license(license_id,name,reservable,commercial_ok,attribution_required,no_modify,url) "
        "VALUES(?,?,?,?,?,?,?)", load_license_registry(yml))
    conn.commit()

    srcs = discover(yml)
    lic_counts: dict[str, int] = {}
    sidecars = 0
    for sid, s in sorted(srcs.items()):
        lic = s.get("license_id")
        lic = lic if lic in valid_lic else "NEEDS-REVIEW"
        lic_counts[lic] = lic_counts.get(lic, 0) + 1
        conn.execute(
            "INSERT OR REPLACE INTO source(source_id,name,homepage,license_id,attribution,terms_url) VALUES(?,?,?,?,?,?)",
            (sid, s.get("name") or sid, s.get("homepage"), lic, s.get("attribution"), s.get("terms_url")))
        ddir = os.path.join(CLEAN, sid)
        if os.path.isdir(ddir):
            sidecar = {
                "schema": "econdl/provider/0.1",
                "provider_code": sid.upper(), "source_id": sid,
                "name": s.get("name") or sid, "description": s.get("description"),
                "homepage": s.get("homepage"), "terms_url": s.get("terms_url"),
                "license_id": lic, "attribution": s.get("attribution"),
                "citation_template": None,  # Stage 0b
                "access": s.get("access"),
                "catalogued": s.get("catalogued", False),
                "_status": "curated" if lic != "NEEDS-REVIEW" else "needs-review",
            }
            tmp = os.path.join(ddir, "_provider.json.tmp")
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(sidecar, fh, ensure_ascii=False, indent=2)
            os.replace(tmp, os.path.join(ddir, "_provider.json"))
            sidecars += 1
    conn.commit()

    n_src = conn.execute("SELECT COUNT(*) FROM source").fetchone()[0]
    n_lic = conn.execute("SELECT COUNT(*) FROM license").fetchone()[0]
    needs = conn.execute("SELECT COUNT(*) FROM source WHERE license_id='NEEDS-REVIEW'").fetchone()[0]
    conn.close()
    print(f"licenses seeded (from sources.yaml): {n_lic}")
    print(f"sources registered: {n_src}  | _provider.json sidecars: {sidecars}")
    print(f"curated license: {n_src - needs}  | NEEDS-REVIEW (Stage 0b research): {needs}")
    print("license_id distribution:")
    for lic, n in sorted(lic_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {n:4}  {lic}")


if __name__ == "__main__":
    main()
