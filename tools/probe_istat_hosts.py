"""Is ISTAT's SDMX estate unreachable on a SCHEDULE? Measure it instead of inferring it.

WHY THIS EXISTS
===============
`updater/strategies/fetchers/istat.py` aborts a whole run when every HOSTS base fails a
10-second TCP connect probe (`_tcp_reachable`, istat.py:150-165, threshold `_TCP_PROBE_TIMEOUT`).
The abort is deliberate and correct — it stops istat paying a dead publisher's timeout on 2,483
flows — but it costs the source its slot for that pass.

Read from `state.db` `runs` on 2026-09-17, istat's 19 recorded runs classify like this by the
UTC hour the run STARTED:

    22Z   5 runs   4 host-dead aborts + 1 transient_fail (the Aug redirect outage)
    23Z   3 runs   3 host-dead aborts
    00Z   6 runs   2 host-dead, 2 real sweeps, 2 transient_fail (outage era)
    01Z   1 run    1 real sweep
    10Z/11Z/13Z   4 runs   3 host-dead, 1 clean

Every run that began at 22Z or 23Z failed; the runs that swept began at 00Z-01Z. A same-pass
control (`bls`, a different publisher on the same local runner) shows NO clustering — it ran
clean at 23Z — so our egress is not the explanation.

That is 8 runs in the suspect window. It is a HYPOTHESIS, not a finding, and n=8 over a
self-selected sample (the scheduler chose those hours, not us) cannot carry a schedule change.
This tool takes the measurement the hypothesis actually needs: probe the hosts on a fixed
cadence across the whole day, so the sample is chosen by the clock rather than by the runner.

WHAT IT MEASURES
================
Exactly what the fetcher measures — `socket.create_connection((host, 443), timeout=10)` — so a
row here means the same thing as `_tcp_reachable` returning False. Nothing else: a TCP connect
says the socket opens, NOT that the SDMX API serves data. A host can pass this and still 302
to its own root, which is what the 2026-08 outage did.

CONTROLS, BECAUSE A PROBE THAT CANNOT FAIL MEASURES NOTHING
===========================================================
Two, and both are written into every sample:

  POSITIVE  www.istat.it:443 — ISTAT's public site, measured up throughout the SDMX outage
            (runbook istat.md, and again 2026-09-17T06:08Z at HTTP 200). If this goes down at
            the same time as the SDMX hosts, the cause is more likely our egress than ISTAT's
            maintenance, and the sample says so.
  NEGATIVE  127.0.0.1:9 (discard) — must REFUSE. If it ever reports reachable, the probe's
            notion of "reachable" is broken and the sample is stamped VOID. Without this, a
            probe that returns True unconditionally would print a reassuring all-up table.

`--summarise` REFUSES to give a verdict when any sample is VOID, when the positive control was
down for more than 10% of samples, or when the run covered fewer than 12 distinct hours. Being
unable to measure counts as a failure, never as a pass.

USAGE
=====
    python tools/probe_istat_hosts.py --hours 21 --every 10   # sample, appends to the log
    python tools/probe_istat_hosts.py --summarise             # read the log back

The log is append-only and fsync'd per sample, so a killed run keeps every sample it took
(R1047: a figure quoted from an in-memory counter died with the job that held it).
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import os
import socket
import sys
import time

# Mirrors istat.py's HOSTS and _TCP_PROBE_TIMEOUT. If those move, move these and say so.
TARGETS = [
    ("sdmx", "sdmx.istat.it", 443, "subject"),
    ("esplora", "esploradati.istat.it", 443, "subject"),
    ("www", "www.istat.it", 443, "control_positive"),
    ("nullport", "127.0.0.1", 9, "control_negative"),
]
CONNECT_TIMEOUT = 10          # == istat.py _TCP_PROBE_TIMEOUT
LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs",
                   "istat_host_probe.tsv")
HEADER = "ts_utc\tlabel\thost\tport\trole\treachable\tconnect_s\terror\n"


def probe(host: str, port: int):
    t0 = time.time()
    try:
        socket.create_connection((host, port), timeout=CONNECT_TIMEOUT).close()
        return True, time.time() - t0, ""
    except OSError as e:
        return False, time.time() - t0, f"{type(e).__name__}:{str(e)[:60]}"


def sample(fh) -> dict:
    """One sweep of every target. Returns {label: reachable}."""
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    got = {}
    for label, host, port, role in TARGETS:
        ok, secs, err = probe(host, port)
        got[label] = ok
        fh.write(f"{now}\t{label}\t{host}\t{port}\t{role}\t{int(ok)}\t{secs:.3f}\t{err}\n")
    fh.flush()
    os.fsync(fh.fileno())
    return got


def run(hours: float, every_min: float) -> int:
    path = os.path.abspath(LOG)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    new = not os.path.exists(path) or os.path.getsize(path) == 0
    deadline = time.time() + hours * 3600.0
    n = 0
    with open(path, "a", encoding="utf-8", newline="") as fh:
        if new:
            fh.write(HEADER)
        while time.time() < deadline:
            got = sample(fh)
            n += 1
            flag = " VOID(negative control reachable)" if got.get("nullport") else ""
            print(f"[{n:4}] {dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')} "
                  f"sdmx={int(got.get('sdmx', False))} esplora={int(got.get('esplora', False))} "
                  f"www={int(got.get('www', False))}{flag}", flush=True)
            time.sleep(max(1.0, every_min * 60.0))
    print(f"done: {n} sample(s) appended to {path}")
    return 0


def summarise() -> int:
    path = os.path.abspath(LOG)
    if not os.path.exists(path):
        print(f"no log at {path} — nothing measured yet")
        return 2
    rows = []
    with open(path, encoding="utf-8") as fh:
        head = fh.readline()
        if not head.startswith("ts_utc"):
            print("log is missing its header; refusing to parse")
            return 2
        for line in fh:
            p = line.rstrip("\n").split("\t")
            if len(p) >= 8:
                rows.append(p)
    if not rows:
        print("log has a header and no samples")
        return 2

    by_ts = collections.defaultdict(dict)
    for ts, label, _h, _p, _role, reach, _secs, _err in rows:
        by_ts[ts][label] = reach == "1"
    samples = sorted(by_ts)

    void = [t for t in samples if by_ts[t].get("nullport")]
    pos_down = [t for t in samples if not by_ts[t].get("www", False)]
    hours_seen = sorted({int(t[11:13]) for t in samples})

    print(f"samples: {len(samples)}   span: {samples[0]} .. {samples[-1]}")
    print(f"distinct UTC hours covered: {len(hours_seen)}  {hours_seen}")
    print(f"VOID samples (negative control reachable): {len(void)}")
    print(f"positive control (www.istat.it) DOWN in {len(pos_down)} sample(s) "
          f"= {100.0 * len(pos_down) / len(samples):.1f}%")

    tab = collections.defaultdict(lambda: collections.Counter())
    for t in samples:
        h = int(t[11:13])
        tab[h]["n"] += 1
        for lbl in ("sdmx", "esplora"):
            if not by_ts[t].get(lbl, False):
                tab[h][lbl + "_down"] += 1
        if not by_ts[t].get("sdmx", False) and not by_ts[t].get("esplora", False):
            tab[h]["both_down"] += 1
    print("\n hour   n   sdmx_down  esplora_down  BOTH_down   <- BOTH_down is what aborts a run")
    for h in sorted(tab):
        c = tab[h]
        print(f"  {h:02d}Z  {c['n']:3}   {c['sdmx_down']:9}  {c['esplora_down']:12}  "
              f"{c['both_down']:9}")

    print()
    if void:
        print("NO VERDICT: the negative control was reachable in "
              f"{len(void)} sample(s), so 'reachable' cannot be trusted here.")
        return 1
    if len(pos_down) > 0.10 * len(samples):
        print("NO VERDICT: the positive control was down in more than 10% of samples — "
              "this looks like our egress, not ISTAT's estate.")
        return 1
    if len(hours_seen) < 12:
        print(f"NO VERDICT YET: only {len(hours_seen)} distinct hours covered; a schedule "
              "claim needs at least 12. Keep sampling.")
        return 1
    worst = [h for h in tab if tab[h]["both_down"]]
    if not worst:
        print("VERDICT: both hosts were never simultaneously unreachable in this sample. "
              "The maintenance-window hypothesis is REFUTED for the hours covered; istat's "
              "host-dead aborts need another explanation.")
    else:
        print("VERDICT: both hosts were simultaneously unreachable in hour(s) "
              + ", ".join(f"{h:02d}Z" for h in sorted(worst))
              + ". That is when an istat run aborts. Compare against the run-start hours "
                "before moving its schedule.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hours", type=float, default=21.0,
                    help="how long to keep sampling (default 21, enough for a full day cycle)")
    ap.add_argument("--every", type=float, default=10.0,
                    help="minutes between samples (default 10)")
    ap.add_argument("--summarise", action="store_true",
                    help="read the log back and cross-tab by UTC hour; give no verdict "
                         "when the controls say the measurement cannot be trusted")
    a = ap.parse_args()
    if a.summarise:
        return summarise()
    return run(a.hours, a.every)


if __name__ == "__main__":
    sys.exit(main())
