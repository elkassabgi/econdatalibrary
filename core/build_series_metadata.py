"""Merge source-level series metadata (configs/series_metadata.yaml) into the catalog.

Bakes description_key / description_processing / citation_short / citation_long into
every series' `metadata` JSON so the /v1 metadata endpoint, the D1 export, bundle
provenance, and the landing pages surface them with NO endpoint changes (they already
read these keys). Producer-FIRST citations: explicit ones from the YAML, else derived
from the registry (source.name + homepage), library credited second.

Re-runnable + authoritative: the four managed keys are overwritten from the YAML/registry
on every run; all OTHER existing metadata keys (description, citation, product_id, table,
…) are left untouched. Run:  python core/build_series_metadata.py
"""
from __future__ import annotations

import json
import os
import sqlite3

import yaml

_THIS = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(_THIS, ".."))
CATALOG = os.path.join(ROOT, "data", "catalog.db")
YAML = os.path.join(ROOT, "configs", "series_metadata.yaml")

_MANAGED = ("description_key", "description_processing", "citation_short", "citation_long")


def _derive_citation(name: str | None, homepage: str | None) -> tuple[str, str]:
    """Producer-first citation from registry facts (no fabrication beyond the name/url)."""
    producer = (name or "").strip() or "Source"
    short = f"{producer}."
    long = producer
    if homepage:
        long += f". Retrieved from {homepage}"
    long += ". Compiled and redistributed by the Elkassabgi Data Library."
    return short, long


def main() -> None:
    cfg = yaml.safe_load(open(YAML, encoding="utf-8")) or {}
    src_meta = cfg.get("sources", {}) or {}
    default_proc = (cfg.get("_defaults", {}) or {}).get("description_processing")

    conn = sqlite3.connect(CATALOG)
    conn.row_factory = sqlite3.Row
    # registry facts for citation fallback (dict, not sqlite3.Row, so .get works)
    sources = {r["source_id"]: dict(r) for r in conn.execute("SELECT source_id, name, homepage FROM source")}

    rows = conn.execute("SELECT series_id, source_id, metadata FROM series").fetchall()
    updated = 0
    per_source = {}
    for r in rows:
        sid, src = r["series_id"], r["source_id"]
        try:
            meta = json.loads(r["metadata"]) if r["metadata"] else {}
        except (ValueError, TypeError):
            meta = {}
        y = src_meta.get(src, {})

        # producer-first citation: explicit YAML wins, else derive from registry.
        if y.get("citation_short") or y.get("citation_long"):
            cs = y.get("citation_short")
            cl = y.get("citation_long")
            if not (cs and cl):
                dshort, dlong = _derive_citation((sources.get(src) or {}).get("name"),
                                                 (sources.get(src) or {}).get("homepage"))
                cs, cl = cs or dshort, cl or dlong
        else:
            cs, cl = _derive_citation((sources.get(src) or {}).get("name"),
                                      (sources.get(src) or {}).get("homepage"))
        meta["citation_short"] = cs
        meta["citation_long"] = cl

        # caveats: only where the source declares them (never invent).
        if y.get("description_key"):
            meta["description_key"] = list(y["description_key"])
        else:
            meta.pop("description_key", None)

        proc = y.get("description_processing") or default_proc
        if proc:
            meta["description_processing"] = proc

        conn.execute("UPDATE series SET metadata=? WHERE series_id=?",
                     (json.dumps(meta, ensure_ascii=False), sid))
        updated += 1
        per_source[src] = per_source.get(src, 0) + 1

    conn.commit()
    conn.close()
    print(f"updated {updated} series across {len(per_source)} sources")
    have_key = sum(1 for s in src_meta.values() if s.get("description_key"))
    print(f"sources with description_key caveats: {have_key}")
    for s in sorted(per_source):
        flag = " [description_key]" if src_meta.get(s, {}).get("description_key") else ""
        print(f"  {s:18} {per_source[s]:6}{flag}")


if __name__ == "__main__":
    main()
