# Publisher notices: seeing a source change before a fetch breaks on it

Part of the standing maintenance system. Until now every publisher change reached us after a
fetch broke (for example the IMF HPDD to HPD rename, and 13 World Bank ESG indicator ids that were
re-coded). This component reads the publishers' own announcements instead.

## Parts

| part | file | state |
|---|---|---|
| feed registry | `updater/notices.yaml` | publishers and the sources they speak for; feeds are added only after someone has opened them (`verified_by`) |
| words that matter | `jobs/notice_terms.yaml` | strong (ending, moving) and medium (changing) terms, several languages |
| watcher | `jobs/watch_notices.py` | fetches due feeds (RSS, Atom, or the links of an HTML news page), scores new items, appends to the queue |
| queue | `data/_maintenance/queue.jsonl` (desktop, not committed) | append-only event log; nothing in it is public |
| triage | `tools/maintenance_queue.py` | `list`, `show`, `triage`, `stats`, `coverage` |
| tests | `tests/test_watch_notices.py` | every rule has a planted positive and negative case |

Not built yet: the desktop schedule (a `-IfDue` wrapper in the guard loop), a section in the
daily digest, and the public "Source changes" page (`catalog/source_changes.yaml` rendered by
`catalog/gen_site.py`). Each one is its own reviewed change.

## Rules the code keeps

- **Not a data run.** `notices.yaml` is separate from `registry.yaml`, so it never changes
  `EXPECTED_SOURCE_COUNT` and can never stop the updater.
- **Polite.** A feed is fetched only when its `poll_days` have passed, with `If-None-Match` and
  `If-Modified-Since`. One request at a time, at least 5 s between two requests to one host, at
  most 200 requests a run. A failure is never retried in a loop; it waits for the next run.
- **A removed feed is news.** Three failures in a row raise one `feed_broken` item.
- **History is not news.** On a feed's first poll, every item is marked seen, and only items
  dated in the last 90 days can be queued.
- **No article text is kept.** Title (cut to 160 characters), link and date only.
- **Private until reviewed.** A queue item becomes a public entry only through a reviewed edit
  of `catalog/source_changes.yaml`. A notice is a citation, not a measurement: adding a source
  to `updater/discontinued.yaml` still needs its own probe.
- **Gaps show.** A publisher with `feeds: []` carries a dated note, and
  `python tools/maintenance_queue.py coverage` lists every such publisher.
