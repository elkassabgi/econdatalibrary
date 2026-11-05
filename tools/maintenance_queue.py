"""Read and triage the maintenance queue (data/_maintenance/queue.jsonl).

The queue is an append-only event log: jobs/watch_notices.py appends items, and every status change
here is a NEW line `{"id", "event": "status", "status", "note", "utc"}` - nothing is edited in place,
so the history of a decision survives. An item's state is its first line plus every later event
for its id, in file order; a repeated item line (a re-queue after a crash) folds into the first.

Nothing in the queue is public. Publishing a confirmed publisher change is a separate, reviewed
edit of catalog/source_changes.yaml.

    python tools/maintenance_queue.py list [--status new] [--component notices]
    python tools/maintenance_queue.py show <id>
    python tools/maintenance_queue.py triage <id> relevant|irrelevant|duplicate --note "..."
    python tools/maintenance_queue.py stats
    python tools/maintenance_queue.py coverage        # publishers with no feed yet
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

QUEUE = os.path.join(ROOT, "data", "_maintenance", "queue.jsonl")
REGISTRY = os.path.join(ROOT, "updater", "notices.yaml")
TRIAGE = ("relevant", "irrelevant", "duplicate")
STATUSES = ("new",) + TRIAGE + ("confirmed", "published", "internal_only")

CHECKLIST = """when relevant:
  1. which of OUR sources does it affect (the item's sources + the fetcher's endpoint)?
  2. does that fetcher still work today? dry-run that source.
  3. action: none / fetcher change (own PR + review) / discontinued.yaml (needs its own measured
     probe - the notice is a citation, not a measurement) / re-key.
  4. public entry in catalog/source_changes.yaml: yes or no."""


def load(path: str = QUEUE) -> dict:
    """{id: item} with `status`, `history` folded from the event lines. Bad lines raise: a queue
    that cannot be read must not look empty."""
    items: dict = {}
    if not os.path.exists(path):
        return items
    with open(path, encoding="utf-8") as f:
        for n, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                d = json.loads(line)
            except ValueError as e:
                raise SystemExit(f"{path}:{n}: not JSON ({e}) - fix the line; refusing to guess") from None
            iid = d.get("id")
            if not iid:
                raise SystemExit(f"{path}:{n}: line has no id")
            if d.get("event") == "status":
                if iid not in items:
                    raise SystemExit(f"{path}:{n}: status event for unknown id {iid}")
                items[iid]["status"] = d["status"]
                items[iid]["history"].append(d)
            elif iid not in items:
                d = dict(d)
                d["history"] = []
                items[iid] = d
    return items


def set_status(iid: str, status: str, note: str, path: str = QUEUE, now=None) -> dict:
    if status not in STATUSES:
        raise SystemExit(f"unknown status {status!r}; one of {', '.join(STATUSES)}")
    if iid not in load(path):
        raise SystemExit(f"no item {iid} in {path}")
    ev = {"id": iid, "event": "status", "status": status, "note": note,
          "utc": (now or dt.datetime.now(dt.timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")}
    with open(path, "a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(ev, sort_keys=True, ensure_ascii=False) + "\n")
    return ev


def stats(items: dict) -> dict:
    out: dict = {}
    for d in items.values():
        k = (d.get("component", "?"), d.get("status", "?"))
        out[k] = out.get(k, 0) + 1
    return out


def coverage(registry: dict) -> list:
    """[(publisher_id, note)] of publishers with no feed: the gaps, shown rather than hidden."""
    return [(p["publisher_id"], p.get("notes", "")) for p in registry.get("publishers") or []
            if not p.get("feeds")]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--queue", default=QUEUE)
    sub = ap.add_subparsers(dest="cmd", required=True)
    ls = sub.add_parser("list")
    ls.add_argument("--status")
    ls.add_argument("--component")
    sh = sub.add_parser("show")
    sh.add_argument("id")
    tr = sub.add_parser("triage")
    tr.add_argument("id")
    tr.add_argument("verdict", choices=TRIAGE)
    tr.add_argument("--note", required=True)
    sub.add_parser("stats")
    sub.add_parser("coverage")
    a = ap.parse_args(argv)

    if a.cmd == "coverage":
        import yaml                                              # noqa: PLC0415
        with open(REGISTRY, encoding="utf-8") as f:
            gaps = coverage(yaml.safe_load(f) or {})
        print(f"{len(gaps)} publisher(s) with no feed:")
        for pid, note in gaps:
            print(f"  {pid:<12} {note}")
        return 0
    items = load(a.queue)
    if a.cmd == "list":
        for d in items.values():
            if a.status and d.get("status") != a.status:
                continue
            if a.component and d.get("component") != a.component:
                continue
            print(f"{d['id']}  {d.get('status', '?'):<12} {d.get('publisher_id', ''):<10} "
                  f"{d.get('score', '-')!s:>3}  {d.get('title', '')[:80]}")
    elif a.cmd == "show":
        d = items.get(a.id)
        if not d:
            print(f"no item {a.id}")
            return 1
        print(json.dumps(d, indent=1, ensure_ascii=False))
        print(CHECKLIST)
    elif a.cmd == "triage":
        print(json.dumps(set_status(a.id, a.verdict, a.note, a.queue)))
    elif a.cmd == "stats":
        for (comp, st), n in sorted(stats(items).items()):
            print(f"  {comp:<10} {st:<12} {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
