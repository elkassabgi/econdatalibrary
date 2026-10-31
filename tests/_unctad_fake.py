"""A fake UNCTADstat API and a pinned clock for the windowed unctad tests (tests/test_unctad_window_cap.py and
its child process). Nothing here can reach the network: requests.get/post are replaced in
jobs.ingest_unctad_ds, and any URL the fake does not know answers 404.

The dataset: key dims Cat (3 codes), Economy (4), Partner (3), Flow (2), annual Years, two measures (like the
real US.NonPlasticSubstsTradeByPartner store: M0100 and M2200). The Facts API refuses any request over CAP
cells with the real 400 "maximal size" text, so the chunked walk splits exactly as it does upstream.

THE CLOCK: jobs.ingest_unctad_ds.time and _unctad._time are replaced by one FakeClock. A POST "takes"
`post_s` seconds by advancing it, and every sleep advances it, so the window arithmetic is exact and the
tests take no wall time.

Child mode (one pass in its own process, so a planted kill is a real os._exit):
    python tests/_unctad_fake.py ROOT UP KILL      KILL: after_merge | after_owed_save:N | none
"""
from __future__ import annotations

import hashlib
import io
import itertools
import json
import os
import re
import sys

CAP = 40
DIMS = [("Categories", "Cat", ["C1", "C2", "C3"]), ("Economies", "Economy", ["E1", "E2", "E3", "E4"]),
        ("Partners", "Partner", ["P1", "P2", "P3"]), ("Flows", "Flow", ["01", "02"])]
MEASURES = ["0100", "2200"]
DS = "US.NonPlasticSubstsTradeByPartner"
SID = "unctad_nonplasticsubststradebypartner"


def release(up: str) -> dict:
    """base: 10009, years 2000-2009. add: 10010, year 2010 too. rev: 10010, 2005 values +1, no new row.
    reissue: 10009 (same version), lastUpdated moved, 2005 values +1. regrain: 10010, a 5th key dim."""
    years = list(range(2000, 2010)) + ([2010] if up == "add" else [])
    ver = "10009" if up in ("base", "reissue") else "10010"
    lu = {"base": "2026-08-05T13:55:36", "reissue": "2026-10-20T00:00:00", "add": "2026-10-21T00:00:00",
          "rev": "2026-10-22T00:00:00"}.get(up, "2026-10-23T00:00:00")
    dims = DIMS + ([("Units", "Unit", ["U1"])] if up == "regrain" else [])
    return {"years": years, "version": ver, "lastUpdated": lu, "rev": up in ("rev", "reissue"), "dims": dims}


def value(combo, year, m, rel) -> float:
    v = float(100 + int(hashlib.md5("|".join(list(combo) + [str(year), m]).encode()).hexdigest()[:6], 16) % 1000)
    return v + 1.0 if rel["rev"] and year == 2005 else v


def truth(up: str) -> dict:
    """{series_key: sorted [(iso date, value)]} a complete pull of `up` holds."""
    import datetime as dt
    rel = release(up)
    out = {}
    for m in MEASURES:
        for combo in itertools.product(*[c for _n, _f, c in rel["dims"]]):
            out[".".join(list(combo) + [f"M{m}"])] = sorted(
                (dt.date(y, 12, 31).isoformat(), value(combo, y, m, rel)) for y in rel["years"])
    return out


def meta(rel) -> dict:
    d = rel["dims"]
    return {"name": DS, "version": rel["version"], "lastUpdated": rel["lastUpdated"],
            "defaults": {"rowAxe": [{"name": n, "field": f} for n, f, _ in d[:2]],
                         "colAxe": [{"name": "Years", "field": "Year", "isTime": True, "codetype": "number"}],
                         "pageAxe": [{"name": n, "field": f} for n, f, _ in d[2:]],
                         "observations": [{"measures": [{"code": m, "magnitude": 1}]} for m in MEASURES]}}


class FakeClock:
    def __init__(self, t0: float = 1_793_000_000.0):
        self.t = float(t0)

    def time(self):
        return self.t

    def monotonic(self):
        return self.t

    def sleep(self, s):
        self.t += max(0.0, float(s))

    def strftime(self, *a):
        import time as _t
        return _t.strftime(*a)

    def __getattr__(self, n):
        import time as _t
        return getattr(_t, n)


class _Resp:
    def __init__(self, status, text="", js=None):
        self.status_code, self.text, self._js, self.encoding = status, text, js, "utf-8"

    def json(self):
        return self._js

    def raise_for_status(self):
        import requests
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Client Error: fake")

    def iter_content(self, chunk_size=1):
        yield self.text.encode()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeApi:
    """state: up (release), post_s (seconds a POST takes on the clock), status (forced POST status or None).
    log: every request, in order: ("meta"|"dim"|"cap"|"leaf", detail)."""

    def __init__(self, clock: FakeClock, up="base", post_s=0.0, status=None):
        import requests
        self.clock, self.up, self.post_s, self.status = clock, up, post_s, status
        self.log = []
        self.RequestException, self.HTTPError = requests.RequestException, requests.HTTPError

    def get(self, url, headers=None, timeout=None, **kw):
        rel = release(self.up)
        if "/reportMetadata/" in url:
            self.log.append(("meta", url))
            return _Resp(200, js=meta(rel))
        m = re.search(r"/(\w+)\?\$orderby", url)
        dim = m.group(1) if m else ""
        self.log.append(("dim", dim))
        if dim == "Years":
            return _Resp(200, js={"value": [{"Code": y} for y in rel["years"]]})
        for n, _f, codes in rel["dims"]:
            if n == dim:
                return _Resp(200, js={"value": [{"Code": c} for c in codes]})
        return _Resp(404, "unknown")

    def post(self, url, headers=None, files=None, stream=False, timeout=None, **kw):
        rel = release(self.up)
        form = {k: v[1] for k, v in (files or {}).items()}
        mcode = re.search(r"M(\d+)/Value", form["$select"]).group(1)
        restr = {f: [x.strip().strip("'") for x in vals.split(",")]
                 for f, vals in re.findall(r"(\w+)(?:/Code)? in \(([^)]*)\)", form.get("$filter", ""))}
        years = [int(y) for y in restr.get("Year", rel["years"])]
        sets = [restr.get(f, codes) for _n, f, codes in rel["dims"]]
        est = len(years)
        for s in sets:
            est *= len(s)
        self.clock.sleep(self.post_s)
        if self.status is not None:
            self.log.append(("status", self.status))
            return _Resp(self.status, "forced")
        if est > CAP:
            self.log.append(("cap", form.get("$filter", "")))
            return _Resp(400, f"The request exceed the maximal size. Maximal size : {CAP}. Estimated size : {est}.")
        self.log.append(("leaf", (mcode, form.get("$filter", ""))))
        out = io.StringIO()
        out.write(",".join([f"{f}_Code" for _n, f, _c in rel["dims"]] + ["Year", f"M{mcode}_Value"]) + "\n")
        for combo in itertools.product(*sets):
            for y in years:
                out.write(",".join(list(combo) + [str(y), repr(value(combo, y, mcode, rel))]) + "\n")
        return _Resp(200, out.getvalue())

    def leaves(self):
        return [d for k, d in self.log if k == "leaf"]


def install(j, U, root: str, api: FakeApi, clock: FakeClock, setattr_=setattr):
    """Point the job and the shared fetcher at the fakes. `setattr_` is monkeypatch.setattr in a test."""
    os.environ["UNCTAD_CLIENT_ID"], os.environ["UNCTAD_API_KEY"] = "fake-id", "fake-key"
    setattr_(j, "requests", api)
    setattr_(j, "time", clock)
    if hasattr(U, "_time"):          # absent on main, so the same tests can be run against main's tree
        setattr_(U, "_time", clock)
    setattr_(j, "_spill_dir", lambda ds: os.path.join(root, "spill", j.source_id_for(ds)))
    setattr_(U.config, "source_dir", lambda s: os.path.join(root, "clean_full", s))
    setattr_(U.config, "STATE_DB", os.path.join(root, "state.db"))


def store_of(root: str) -> dict:
    import pyarrow.parquet as pq
    p = os.path.join(root, "clean_full", SID, f"{SID}.parquet")
    if not os.path.exists(p):
        return {}
    t = pq.read_table(p)
    out = {}
    for k, d, v in zip(t.column("series_key").to_pylist(), t.column("obs_date").to_pylist(),
                       t.column("value").to_pylist()):
        out.setdefault(k, []).append((d.isoformat(), v))
    return {k: sorted(v) for k, v in out.items()}


if __name__ == "__main__":
    root, up, kill = sys.argv[1], sys.argv[2], sys.argv[3]
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, repo)
    os.environ.pop("AQUEDUCT_BACKEND", None)
    import jobs.ingest_unctad_ds as J
    from updater import blob, merge
    from updater.strategies.fetchers import _unctad as UU
    from updater.strategies.fetchers import unctad_nonplasticsubststradebypartner as NP
    clk = FakeClock()
    install(J, UU, root, FakeApi(clk, up=up), clk)
    if kill == "after_merge":
        real = merge.merge_and_write

        def _m(*a, **kw):
            r = real(*a, **kw)
            print("PLANTED KILL after merge_and_write", flush=True)
            os._exit(137)
        merge.merge_and_write = _m
    elif kill.startswith("after_owed_save:"):
        n, real_w = int(kill.split(":")[1]), blob.write_bytes_atomic
        cnt = [0]

        def _w(path, data, *a, **kw):
            r = real_w(path, data, *a, **kw)
            if str(path).endswith(UU.OWED_FILE):
                cnt[0] += 1
                if cnt[0] >= n:
                    print(f"PLANTED KILL after owed-mark save #{cnt[0]}", flush=True)
                    os._exit(137)
            return r
        blob.write_bytes_atomic = _w
    res = NP.update(None, None)
    print(json.dumps({"status": res.status, "changed": None if res.changed_keys is None
                      else sorted(res.changed_keys), "error": res.error}))
