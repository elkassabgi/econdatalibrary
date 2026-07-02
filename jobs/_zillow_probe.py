#!/usr/bin/env python3
"""HEAD/GET-probe every enumerated Zillow CSV URL to find dead links and shapes
BEFORE the full crawl. Writes data/_zillow_probe.json."""
import json
import os
import time
import urllib.request

ROOT = r"D:/research/econfindatalibrary"
FILES = os.path.join(ROOT, "data", "_zillow_files.json")
OUT = os.path.join(ROOT, "data", "_zillow_probe.json")
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"


def probe(url):
    """Range-GET the first 2KB to read the header line + confirm reachability."""
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Range": "bytes=0-2047"})
    last = None
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                code = resp.getcode()
                clen = resp.headers.get("Content-Range") or resp.headers.get("Content-Length")
                chunk = resp.read()
                txt = chunk.decode("utf-8", "replace")
                header = txt.split("\n", 1)[0]
                cols = header.split(",")
                idcols = []
                for c in cols:
                    cs = c.strip()
                    if len(cs) >= 8 and cs[:4].isdigit() and cs[4] == "-":
                        break
                    idcols.append(cs)
                return {"ok": True, "code": code, "content_range": clen,
                        "n_cols_in_chunk": len(cols), "n_id_cols": len(idcols),
                        "id_cols": idcols[:12]}
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return {"ok": False, "code": 404, "error": "404 Not Found"}
            last = f"HTTP {e.code}"
        except Exception as e:  # noqa: BLE001
            last = f"{type(e).__name__}: {e}"
        time.sleep(1.5 * (attempt + 1))
    return {"ok": False, "code": None, "error": last}


def main():
    files = json.load(open(FILES, encoding="utf-8"))
    out = []
    n_ok = n_dead = 0
    for i, r in enumerate(files):
        res = probe(r["url"])
        rec = dict(r)
        rec.update(res)
        out.append(rec)
        if res["ok"]:
            n_ok += 1
        else:
            n_dead += 1
            print(f"  DEAD [{res.get('code')}] {r['url']}", flush=True)
        if (i + 1) % 25 == 0:
            print(f"  probed {i+1}/{len(files)}  ok={n_ok} dead={n_dead}", flush=True)
        time.sleep(0.2)
    json.dump(out, open(OUT, "w", encoding="utf-8"), indent=1)
    print(f"DONE probe: {n_ok} ok, {n_dead} dead, written {OUT}", flush=True)

    # distinct id-col signatures among the live files
    from collections import Counter
    sig = Counter(tuple(r.get("id_cols", [])) for r in out if r.get("ok"))
    print("\nDistinct ID-col signatures (live files):")
    for k, v in sig.most_common():
        print(f"  n={v:>3}  {list(k)}")


if __name__ == "__main__":
    main()
