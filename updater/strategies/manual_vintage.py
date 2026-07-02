"""S6 — manual_vintage.

For sources that publish DISCRETE VERSIONED RELEASES with no incremental API: a new
file simply appears per vintage (PWT-, Maddison-, Barro-Lee-style editions; the
registry's fraser_efw / gii / social_progress / spi). There is no `since=` delta to
build — between editions the data is frozen, and on a new edition the WHOLE release is
republished (often with back-revisions across the entire panel).

Strategy (matches the Strategy contract; thin, like overwrite_if_changed):

  detect_change(unit, state) -> str | None
      Probe the published vintage TOKEN via the source fetcher's current_vintage()
      (an edition id / DOI version / file SHA / Last-Modified — whatever changes iff
      a new edition shipped). Return the token ONLY when it differs from the stored
      upstream_vintage (or on first-ever run). When it equals the stored one -> None
      (no_change, skip the expensive re-fetch).

  run(unit, since) -> Result
      Fetch + parse the WHOLE release and publish via merge.merge_and_write
      (mode='merge', dedup on the STABLE (series_key, obs_date), new wins on
      revision, never-shrink, atomic). Stamp the resolved vintage onto the Result as
      new_vintage so the orchestrator persists it as upstream_vintage; the fetcher
      additionally records the vintage_date in the published release itself.

THREE things distinguish this from overwrite_if_changed, all deliberate:

  1. The vintage token is the SOLE discriminator. overwrite_if_changed FORCES a
     re-fetch when its cheap probe returns None ("can't tell -> fetch anyway, the
     merge dedups it"). A manual_vintage source has no live delta to lean on and its
     probe is often a dead/poll-only URL; forcing a re-fetch every tick of a release
     we already hold would either re-download a static edition pointlessly or hammer a
     known-dead URL and launder the failure. So on a NORMAL poll an undeterminable
     vintage means "no NEW vintage" -> None (clean no_change). We only fetch when we
     have a genuinely new token, on first run (nothing stored yet), or under --force.

  2. Honest status is preserved end to end. detect_change never fetches, so a probe
     transient surfaces as TransientError (orchestrator: transient_fail, retry) — it
     is NOT laundered into no_change. The fetcher's update() obeys the Tally/finalize
     contract (structural break -> DefinitiveError/partial, transient -> partial,
     net-new>0 -> ok, else no_change), and merge_and_write never shrinks/empties good
     data. A partial/failed run does NOT advance upstream_vintage (orchestrator), so a
     half-fetched edition is retried rather than silently marked current.

  3. The emitted series_key MUST be vintage-STABLE. Because dedup is on
     (series_key, obs_date), a re-fetch of the SAME edition has to collapse to 0 new
     rows. The vintage identity therefore lives in the token (-> upstream_vintage) and
     in a vintage_date COLUMN, never in series_key and never in the dedup key — that is
     what makes the strategy non-duplicating across re-runs of one edition.
"""
from __future__ import annotations

from . import register
from .base import Strategy, Unit, Result
from .fetchers import get as get_fetcher


@register("manual_vintage")
class ManualVintage(Strategy):
    def detect_change(self, unit: Unit, unit_state: dict | None) -> str | None:
        f = get_fetcher(unit.source_id)
        # current_vintage() raises TransientError on a probe network/5xx failure; let it
        # propagate so the orchestrator records transient_fail (never launder to no_change).
        cur = f.current_vintage(unit) if hasattr(f, "current_vintage") else None
        self._cur = cur
        stored = (unit_state or {}).get("upstream_vintage")

        if stored is None:
            # First-ever run for this unit: there is no held edition, so fetch whatever
            # is currently published (cur may be None for a poll-only source whose probe
            # can't name the edition — the fetch itself then decides ok/partial honestly).
            return cur or "first-run"

        if cur is None:
            # We already hold an edition and the probe can't determine the current one
            # (poll-only / dead URL / header stripped). Treat as NO NEW vintage rather
            # than re-pulling a static release or hammering a dead URL every tick. A real
            # new edition is detected the moment the probe yields a token again; --force
            # still bypasses this in the orchestrator if a human wants to re-pull.
            return None

        # A determinable token that matches what we hold -> nothing new. A different
        # token -> a NEW edition shipped; fetch + overwrite_if_changed the whole release.
        return None if cur == stored else cur

    def run(self, unit: Unit, since: str | None) -> Result:
        r = get_fetcher(unit.source_id).update(unit, since)
        # Carry the probed vintage token through to the Result so the orchestrator
        # persists it as upstream_vintage (the fetcher may also set its own, which wins).
        if r.new_vintage is None and getattr(self, "_cur", None):
            r.new_vintage = self._cur
        return r
