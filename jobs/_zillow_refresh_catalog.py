#!/usr/bin/env python3
"""Compare the LIVE Zillow research page catalog vs the saved file list and
rewrite data/_zillow_files.json from the LIVE page so we crawl the current catalog."""
import json
import os

ROOT = r"D:/research/econfindatalibrary"
LIVE = os.path.join(ROOT, "data", "_zillow_page_live.html")
OUTJSON = os.path.join(ROOT, "data", "_zillow_files.json")


def extract_data_object(t: str) -> str:
    start = t.find("var data")
    brace = t.find("{", start)
    i = brace
    depth = 0
    instr = False
    esc = False
    while i < len(t):
        c = t[i]
        if instr:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                instr = False
        else:
            if c == '"':
                instr = True
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    return t[brace:i + 1]
        i += 1
    raise RuntimeError("no balanced object found")


def main():
    live = open(LIVE, encoding="utf-8", errors="replace").read()
    data = json.loads(extract_data_object(live))

    live_urls = set()
    for s, types in data.items():
        for tl, geos in types.items():
            for gl, url in geos.items():
                live_urls.add(url.split("?")[0])

    saved = json.load(open(OUTJSON, encoding="utf-8")) if os.path.exists(OUTJSON) else []
    saved_urls = set(r["url"] for r in saved)

    print("SETS in live:", len(data))
    print("LIVE unique urls:", len(live_urls))
    print("SAVED unique urls:", len(saved_urls))
    print("in LIVE not SAVED:", len(live_urls - saved_urls))
    for u in sorted(live_urls - saved_urls):
        print("  +", u)
    print("in SAVED not LIVE:", len(saved_urls - live_urls))
    for u in sorted(saved_urls - live_urls):
        print("  -", u)

    seen = {}
    for s, types in data.items():
        for tl, geos in types.items():
            for gl, url in geos.items():
                u = url.split("?")[0]
                if u not in seen:
                    seen[u] = {"set": s, "type": tl, "geo": gl, "url": u}
    json.dump(list(seen.values()), open(OUTJSON, "w", encoding="utf-8"), indent=1)
    print("rewrote _zillow_files.json with", len(seen), "records from LIVE page")


if __name__ == "__main__":
    main()
