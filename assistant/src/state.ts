// ---------------------------------------------------------------------------
// src/state.ts — AssistantState Durable Object: atomic rate-limits + budget.
//
// A single DO instance (idFromName("main")) serializes all gate/charge ops, so
// the monthly budget cap can't be overshot by concurrent bursts (the classic
// read-then-write race a plain D1 counter would have). Rate-limit windows are
// fixed-window counters keyed per subject (per-IP for anon, per-user for
// registered), with multiple windows (hour + day) checked together.
// ---------------------------------------------------------------------------

import { DurableObject } from "cloudflare:workers";
import type { Env } from "./types";

interface RlSpec { key: string; max: number; windowS: number; }
interface Window { count: number; start: number; } // start = epoch ms

export class AssistantState extends DurableObject<Env> {
  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    if (url.pathname === "/gate") return this.gate(req);
    if (url.pathname === "/charge") return this.charge(req);
    if (url.pathname === "/budget") return this.budget();
    return new Response("not found", { status: 404 });
  }

  // Check every rate-limit window AND the monthly budget, then RESERVE an
  // estimated cost so the cap holds under concurrency (the paid LLM call runs
  // outside the DO, between gate and charge; without a reservation, N requests
  // arriving while spent<cap would all be admitted). Reconciled to actual in
  // charge(). Increment the rate windows only if ALL pass. Atomic because the
  // DO runs one request at a time with no interleaving I/O here.
  private async gate(req: Request): Promise<Response> {
    const { rlKeys, capUsd, reserveUsd } = (await req.json()) as {
      rlKeys: RlSpec[]; capUsd: number; reserveUsd: number;
    };
    const now = Date.now();
    const reserve = Number.isFinite(reserveUsd) && reserveUsd > 0 ? reserveUsd : 0;

    // 1) budget first (includes already-reserved in-flight spend)
    const monthKey = this.monthKey(now);
    const spent = (await this.ctx.storage.get<number>(monthKey)) ?? 0;
    if (spent >= capUsd) {
      return this.json({ ok: false, reason: "budget", spentUsd: spent });
    }

    // 2) evaluate all rate windows without mutating yet
    const loaded: { spec: RlSpec; win: Window }[] = [];
    for (const spec of rlKeys) {
      const win = (await this.ctx.storage.get<Window>("rl:" + spec.key)) ?? { count: 0, start: now };
      const expired = now - win.start >= spec.windowS * 1000;
      const cur = expired ? { count: 0, start: now } : win;
      if (cur.count >= spec.max) {
        const retryAfter = Math.max(1, Math.ceil((cur.start + spec.windowS * 1000 - now) / 1000));
        return this.json({ ok: false, reason: "rate", retryAfter, limit: spec.max, windowS: spec.windowS });
      }
      loaded.push({ spec, win: cur });
    }

    // 3) all passed -> reserve estimated spend + increment every window
    await this.ctx.storage.put(monthKey, spent + reserve);
    for (const { spec, win } of loaded) {
      await this.ctx.storage.put("rl:" + spec.key, { count: win.count + 1, start: win.start });
    }
    return this.json({ ok: true, spentUsd: spent + reserve });
  }

  // Reconcile the reservation: delta = actualCost - reservedEstimate (signed;
  // negative on the common case where actual < the conservative estimate, or
  // when a request errored and its reservation is being partly refunded).
  private async charge(req: Request): Promise<Response> {
    const { delta } = (await req.json()) as { delta: number };
    const now = Date.now();
    const monthKey = this.monthKey(now);
    const spent = (await this.ctx.storage.get<number>(monthKey)) ?? 0;
    const next = Math.max(0, spent + (Number.isFinite(delta) ? delta : 0));
    await this.ctx.storage.put(monthKey, next);
    return this.json({ spentUsd: next });
  }

  private async budget(): Promise<Response> {
    const now = Date.now();
    const spent = (await this.ctx.storage.get<number>(this.monthKey(now))) ?? 0;
    return this.json({ month: this.monthLabel(now), spentUsd: spent });
  }

  private monthKey(now: number): string {
    return "budget:" + this.monthLabel(now);
  }
  private monthLabel(now: number): string {
    const d = new Date(now);
    return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}`;
  }
  private json(o: unknown): Response {
    return new Response(JSON.stringify(o), { headers: { "content-type": "application/json" } });
  }
}
