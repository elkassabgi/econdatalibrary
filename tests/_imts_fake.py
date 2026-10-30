"""A fake api.imf.org for imf_imts_direct's tests (tests/test_imts_window_cap.py and its child process).

The series carry the REAL store's identity attributes - COUNTERPART_COUNTRY, COUNTRY, FREQUENCY,
INDICATOR, METHODOLOGY (the sidecar's key_dims) - plus one publication attribute (SCALE) the
ingester drops by name. Frequencies A/Q/M/S and a day form, data from 1948 (the real store starts
1948-12-31) and forecasts to Y+1. The data endpoint filters by the YEAR of each period, which is how
SDMX startPeriod/endPeriod behave for these forms. urlopen raises for any URL it does not know, so
nothing can reach the network.
"""
from __future__ import annotations

import hashlib
import io
import urllib.error
import urllib.parse

BASE = "https://api.imf.org/external/sdmx/2.1"
DATA = BASE + "/data/IMF.STA,IMTS/all"
DIMS = ["COUNTERPART_COUNTRY", "COUNTRY", "FREQUENCY", "INDICATOR", "METHODOLOGY"]
LAST_YEAR = 2027                                  # Y+1 for the pinned 2026 clock


def _periods(freq):
    if freq == "A":
        return [str(y) for y in range(1948, LAST_YEAR + 1)]
    if freq == "Q":
        return [f"{y}-Q{q}" for y in range(1955, 2027) for q in (1, 2, 3, 4)] + ["2027-Q1"]
    if freq == "M":
        return [f"{y}-M{m:02d}" for y in range(1957, 2027) for m in range(1, 13)
                if (y, m) <= (2026, 4)]
    if freq == "S":
        return [f"{y}-S{s}" for y in range(1958, 2027) for s in (1, 2)]
    if freq == "D":
        return ["1959-12-31", "1960-01-01", "2019-12-31", "2020-01-01", "2024-12-31", "2025-01-01",
                "2026-03-31"]
    raise ValueError(freq)


SERIES = [("ABW", "USA", "A"), ("CHN", "USA", "Q"), ("ABW", "FRA", "M"), ("CHN", "FRA", "S"),
          ("ABW", "DEU", "D")]


def value(cp, c, f, p, up):
    v = 100.0 + int(hashlib.md5(f"{cp}|{c}|{f}|{p}".encode()).hexdigest()[:4], 16) % 1000
    if p in ("1949", "2005-Q2", "2026-M04") and up in ("rev", "rev2"):
        v += 1.0 if up == "rev" else 2.0
    return v


def _year(p):
    return int(p[:4])


def body(up="base", start=None, end=None, drop_attr_years=None, leak=False):
    """SDMX StructureSpecific body. up: base | add (one more month) | rev | rev2 | empty.
    drop_attr_years=(lo, hi, attr[, extra]): series in a document whose window starts in [lo, hi]
    lack attr (and carry `extra`). leak=True: the day-form series also answers with the periods one
    year either side of the window (a publisher that does not honour the bounds exactly)."""
    if up == "empty":
        return (b'<?xml version="1.0"?><m:StructureSpecificData xmlns:m="urn:m"><m:DataSet>'
                b'</m:DataSet></m:StructureSpecificData>')
    parts = ['<?xml version="1.0"?><m:StructureSpecificData xmlns:m="urn:m"><m:DataSet>']
    for cp, c, f in SERIES:
        ps = list(_periods(f))
        if up == "add" and f == "M":
            ps.append("2026-M05")
        obs = [f'<Obs TIME_PERIOD="{p}" OBS_VALUE="{value(cp, c, f, p, up):.1f}"/>' for p in ps
               if start is None or int(start) <= _year(p) <= int(end)
               or (leak and f == "D" and int(start) - 1 <= _year(p) <= int(end) + 1)]
        if not obs:
            continue
        attrs = {"COUNTERPART_COUNTRY": cp, "COUNTRY": c, "FREQUENCY": f, "INDICATOR": "MG_CIF_USD",
                 "METHODOLOGY": "IMTS", "SCALE": "0"}
        if drop_attr_years and start is not None and drop_attr_years[0] <= int(start) <= drop_attr_years[1]:
            attrs.pop(drop_attr_years[2], None)
        if drop_attr_years and len(drop_attr_years) == 4 and start is not None \
                and drop_attr_years[0] <= int(start) <= drop_attr_years[1]:
            attrs[drop_attr_years[3]] = "X"
        a = " ".join(f'{k}="{v}"' for k, v in attrs.items())
        parts.append(f"<Series {a}>" + "".join(obs) + "</Series>")
    parts.append("</m:DataSet></m:StructureSpecificData>")
    return "".join(parts).encode()


DATAFLOW = (b'<?xml version="1.0"?><s:Structure xmlns:s="urn:s"><s:Dataflows>'
            b'<s:Dataflow id="IMTS" agencyID="IMF.STA" version="1.0.0"><s:Name>x</s:Name></s:Dataflow>'
            b'</s:Dataflows></s:Structure>')


class Clock:
    """The module's `time`: time() and sleep(); sleep advances it."""

    def __init__(self, t):
        self.t = float(t)
        self.slept = []

    def time(self):
        return self.t

    def sleep(self, s):
        self.slept.append(s)
        self.t += s

    def monotonic(self):
        return self.t


class _Resp:
    def __init__(self, data, chunks, per_chunk, clock):
        n = max(1, chunks)
        self.pieces = [data[i * len(data) // n:(i + 1) * len(data) // n] for i in range(n)]
        self.per_chunk, self.clock = per_chunk, clock

    def _tick(self):
        if self.clock is not None:
            self.clock.t += self.per_chunk

    def read1(self, n=-1):
        if not self.pieces:
            return b""
        self._tick()
        return self.pieces.pop(0)

    def read(self, n=-1):
        if n is not None and n >= 0:
            return self.read1(n)
        out = b""
        while self.pieces:
            out += self.read1()
        return out

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class Imf:
    """State of the fake: what it serves and what it was asked."""

    def __init__(self, clock=None):
        self.clock = clock
        self.up = "base"
        self.requests = []                         # (url, timeout)
        self.per_request = 0.0                     # clock seconds a data request costs
        self.chunks = 1
        self.status = {}                           # startPeriod -> list of (code, retry_after)
        self.drop_attr = None
        self.leak = False

    def urlopen(self, req, timeout=None, *a, **k):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        self.requests.append((url, timeout))
        if url == BASE + "/dataflow":
            return _Resp(DATAFLOW, 1, 0.0, None)
        if url == DATA:
            return _Resp(body(self.up), self.chunks, self.per_request / self.chunks, self.clock)
        if url.startswith(DATA + "?"):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
            s, e = q["startPeriod"][0], q["endPeriod"][0]
            pending = self.status.get(s)
            if pending:
                code, ra = pending.pop(0)
                hdrs = {"Retry-After": str(ra)} if ra is not None else {}
                raise urllib.error.HTTPError(url, code, "planted", hdrs, io.BytesIO(b""))
            return _Resp(body(self.up, s, e, self.drop_attr, self.leak), self.chunks,
                         self.per_request / self.chunks, self.clock)
        raise RuntimeError(f"fake IMF: unexpected URL {url!r} (no network in tests)")

    def data_requests(self):
        return [u for u, _ in self.requests if u.startswith(DATA)]
