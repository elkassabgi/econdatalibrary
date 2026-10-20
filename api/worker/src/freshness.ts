// ---------------------------------------------------------------------------
// src/freshness.ts  --  the public freshness vocabulary: "late", "paused", "retired".
//
// Owner decisions, Ahmed, 2026-10-20. The rule is core/freshness_vocab.py (the dev shim runs it, and
// tests/test_freshness_vocab.py runs both on the same cases); the classes it needs that are not in a
// state row - the health gate's clock, the paused / rotating / lane / discontinued sets and the run
// mechanisms - are generated into src/freshnessClasses.ts. The same code runs on the cloud worker
// (D1) and on the self-hosted origin (its copy of the same tables, core.sync_state_d1.emit_sql).
//
//   late     the source keeps running but its last full success is older than 2 x cadence
//            (base.CADENCE_DAYS x health.SLA_TOLERANCE, `>` as health's RED-SLA). Source grain: the
//            /v1/sources status and every unit row of the source whose stored status is ok, no_change,
//            partial or transient_fail. NOT late: a `rotating: true` source whose latest pass is a pure
//            budget deferral (health._deferral_only) and a `served_by: lane` source whose reporter says
//            the lane is current - each only while that pass ran within the same window.
//   paused   registry `paused:` (never a state row: nothing to delete on un-pause). No next update.
//   retired  not discontinued, not paused, and no run mechanism names it (RUN_BY). No next update.
// Precedence: discontinued > paused > retired > late > what state holds. Field shapes do not change:
// these are new values of the existing `status` string fields.
// ---------------------------------------------------------------------------

import {
  DEFERRAL_RE, DISCONTINUED, LANE, LANE_CURRENT_RE, LATE_CADENCE_DAYS, NON_FAILURE_NOTES, PAUSED,
  ROTATING, RUN_BY, SLA_TOLERANCE,
} from "./freshnessClasses.ts";

export const LATE = "late";
export const PAUSED_STATUS = "paused";
export const RETIRED = "retired";
const DISCONTINUED_STATUS = "discontinued";
const LATE_FROM_SOURCE = new Set(["ok", "no_change"]);
const LATE_FROM_UNIT = new Set(["ok", "no_change", "partial", "transient_fail"]);
const UNIT_OK = new Set(["ok", "no_change"]);

/** What the rule reads of the source_state row. */
export interface SourceFresh {
  status: string | null;
  last_success_utc: string | null;
  cadence: string | null;
}
/** What the rule reads of each unit_state row. */
export interface UnitFresh {
  status: string | null;
  last_error?: string | null;
  last_attempt_utc?: string | null;
  last_success_utc?: string | null;
}

/** The classes, injectable so tests can plant their own; production uses the generated ones. */
export interface FreshnessClasses {
  slaTolerance: number;
  cadenceDays: Readonly<Record<string, number>>;
  paused: Readonly<Record<string, string>>;
  runBy: ReadonlySet<string>;
  discontinued: ReadonlySet<string>;
  rotating: ReadonlySet<string>;
  lane: ReadonlySet<string>;
  laneCurrent: RegExp;
  deferral: RegExp;
  nonFailureNotes: readonly string[];
}
export const CLASSES: FreshnessClasses = {
  slaTolerance: SLA_TOLERANCE, cadenceDays: LATE_CADENCE_DAYS, paused: PAUSED, runBy: RUN_BY,
  discontinued: DISCONTINUED, rotating: ROTATING, lane: LANE, laneCurrent: LANE_CURRENT_RE,
  deferral: DEFERRAL_RE, nonFailureNotes: NON_FAILURE_NOTES,
};

function ts(v: string | null | undefined): number | null {
  if (!v) return null;
  const t = Date.parse(v);
  return Number.isNaN(t) ? null : t;
}

/** True when `v` is more than `days` before `now`; null when absent or unreadable. */
function over(v: string | null | undefined, days: number, now: number): boolean | null {
  const t = ts(v);
  if (t === null) return null;
  return (now - t) / 86_400_000 > days;
}

/** health._deferral_only for ONE partial unit's note: drop the non-failure tails segment by segment, and
 *  the remainder must be finalize()'s anchored deferral sentence. */
function cleanDeferral(note: string, c: FreshnessClasses): boolean {
  const rest = note.split("; ").filter((seg) => !c.nonFailureNotes.some((p) => seg.startsWith(p)))
    .join("; ").trim();
  return c.deferral.test(rest);
}

/** The declared source's LATEST pass says it is current, and it ran within the window. */
function latestPassCurrent(kind: "rotating" | "lane", units: UnitFresh[], windowDays: number,
                           now: number, c: FreshnessClasses): boolean {
  if (units.length === 0) return false;
  for (const u of units) {
    const st = u.status ?? "";
    if (UNIT_OK.has(st)) continue;
    if (st !== "partial") return false;
    const note = String(u.last_error ?? "");
    if (kind === "lane" ? !c.laneCurrent.test(note) : !cleanDeferral(note, c)) return false;
  }
  let newest: number | null = null;
  for (const u of units) {
    const t = ts(u.last_attempt_utc);
    if (t !== null && (newest === null || t > newest)) newest = t;
  }
  if (newest === null) return false;
  return (now - newest) / 86_400_000 <= windowDays;
}

/** The new status for a whole source, or null to keep what is stored. */
export function sourceLabel(sid: string, src: SourceFresh | null, units: UnitFresh[], now: number,
                            c: FreshnessClasses = CLASSES): string | null {
  const sStatus = src?.status ?? null;
  if (sStatus === DISCONTINUED_STATUS || c.discontinued.has(sid)) return null;
  if (Object.prototype.hasOwnProperty.call(c.paused, sid)) return PAUSED_STATUS;
  if (!c.runBy.has(sid)) return RETIRED;
  if (sStatus === null || !LATE_FROM_SOURCE.has(sStatus)) return null;
  const period = src?.cadence ? c.cadenceDays[src.cadence] : undefined;
  if (period === undefined) return null;
  const windowDays = period * c.slaTolerance;
  // The newest success anyone recorded for it: a source whose source_state is stamped by its own
  // refresher (sec_edgar, R1193) can hold an older unit row, and that row is not the source's truth.
  let newest: string | null = null;
  for (const v of [src?.last_success_utc, ...units.map((u) => u.last_success_utc)]) {
    const t = ts(v);
    if (t !== null && (newest === null || t > (ts(newest) as number))) newest = v as string;
  }
  if (newest === null || over(newest, windowDays, now) !== true) return null;
  if (c.rotating.has(sid) && latestPassCurrent("rotating", units, windowDays, now, c)) return null;
  if (c.lane.has(sid) && latestPassCurrent("lane", units, windowDays, now, c)) return null;
  return LATE;
}

/** What /v1/last-updates prints for one unit of a source whose sourceLabel() is `label`. */
export function unitStatus(stored: string | null, label: string | null): string | null {
  if (label === PAUSED_STATUS || label === RETIRED) return label;
  if (label === LATE && stored !== null && LATE_FROM_UNIT.has(stored)) return LATE;
  return stored;
}

/** paused and retired promise no next update. */
export function promisesNextUpdate(label: string | null): boolean {
  return label !== PAUSED_STATUS && label !== RETIRED;
}

/** The sources whose /v1/sources label needs their unit rows (the declared ones), for one PK-range query. */
export function needsUnits(sid: string, c: FreshnessClasses = CLASSES): boolean {
  return c.rotating.has(sid) || c.lane.has(sid);
}
