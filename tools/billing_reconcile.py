"""Reconcile the billing meter against a REAL invoice. Run it after touching the pricing.

WHY THIS EXISTS. `billing_guard.py` has been wrong twice in one week, both times in the
cheap direction and both times while Ahmed was worried about the bill: R502 priced D1 from
row zero and ignored the 25B included allowance, and R505 printed R2 operations (~$31/mo)
four lines above a total that omitted them, turning $75 into "$24". Both were caught by
reading the code, which is the weakest instrument available. The invoice is the strongest,
and there is now one on file.

WHAT IT DOES. Measures a CLOSED billing period from the same GraphQL datasets the guard
uses, prices it with the guard's own `units()`, and prints the delta against the invoice.
It imports billing_guard rather than restating its arithmetic, so the pricing under test is
the pricing that ships — a reconciliation that reimplements the formula only proves the copy
agrees with itself.

GROUND TRUTH: invoice IN-74622130, Jul 9..Aug 8 2026, $154.96 pre-tax / $165.19 with tax.

    py -3.14 tools/billing_reconcile.py                  # against the invoice on file
    py -3.14 tools/billing_reconcile.py 2026-08-09 2026-09-08   # any closed period

Exit 1 if the reconstruction misses the invoice by more than TOLERANCE_PCT, so a pricing
regression fails loudly instead of shipping a plausible number.
"""
import datetime as dt
import sys

sys.path.insert(0, __file__.rsplit("\\", 1)[0].rsplit("/", 1)[0])
import billing_guard as bg                                          # noqa: E402

# The invoice on file. Keep the raw line items: a total alone cannot tell you WHICH term
# regressed, and every past error here was one term, not a uniform drift.
INVOICE = {
    "id": "IN-74622130",
    "start": "2026-07-09", "end": "2026-08-08",
    "subtotal": 154.96, "tax": 10.23, "total": 165.19,
    "lines": {"base": 5.00, "d1_writes": 33.00, "r2_class_a": 94.50, "r2_storage": 22.46},
}
TOLERANCE_PCT = 2.0     # the review reconstructed this invoice to 0.11%; 2% is generous


def _series(tok, acct, start, end, field, query, limit):
    d = bg._graphql(tok, query, {"acct": acct, "start": start, "end": end})
    return None if d is None else bg._rows(d, field, limit)


def measure(start: str, end: str) -> dict:
    """Every billable quantity over [start, end] inclusive. None values mean UNMEASURED and
    are never silently read as zero — that conflation is the whole R505 failure."""
    tok, acct = bg._load_env_token(), bg._account_id()
    if not tok or not acct:
        sys.exit("CF_ANALYTICS_TOKEN / account id missing — cannot reconcile.")
    span = (dt.date.fromisoformat(end) - dt.date.fromisoformat(start)).days + 1
    if span > 32:
        # The API rejects wider spans. Saying so beats a fail-open returning a cheap answer.
        sys.exit(f"window is {span} days; the analytics API caps a query at 32.")
    m = {"days": span}

    rows = _series(tok, acct, start, end, "d1AnalyticsAdaptiveGroups", """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    d1AnalyticsAdaptiveGroups(limit: 500, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { date } sum { rowsRead rowsWritten } } } } }""", 500)
    if rows is not None:
        m["d1_days"] = len({r["dimensions"]["date"] for r in rows})
        m["d1_reads"] = sum(r["sum"]["rowsRead"] for r in rows)
        m["d1_writes"] = sum(r["sum"]["rowsWritten"] for r in rows)

    rows = _series(tok, acct, start, end, "r2OperationsAdaptiveGroups", """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    r2OperationsAdaptiveGroups(limit: 2000, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { date actionType } sum { requests } } } } }""", 2000)
    if rows is not None:
        m["r2_a"] = sum(r["sum"]["requests"] for r in rows
                        if r["dimensions"]["actionType"] not in bg._R2_CLASS_B)
        m["r2_b"] = sum(r["sum"]["requests"] for r in rows
                        if r["dimensions"]["actionType"] in bg._R2_CLASS_B)

    rows = _series(tok, acct, start, end, "workersInvocationsAdaptive", """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    workersInvocationsAdaptive(limit: 500, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { date } sum { requests } } } } }""", 500)
    if rows is not None:
        m["workers"] = sum(r["sum"]["requests"] for r in rows)

    # Storage is a LEVEL, so it is averaged, and it must be grouped by bucket AND storage
    # class: this dataset offers only `max`, so grouping by date alone silently returns the
    # largest single bucket — 758 vs 1,486 GB-months over this very invoice, 49% low.
    rows = _series(tok, acct, start, end, "r2StorageAdaptiveGroups", """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    r2StorageAdaptiveGroups(limit: 3000, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { date bucketName storageClass } max { payloadSize metadataSize } } } } }""",
                   3000)
    if rows is not None:
        per = {}
        for r in rows:
            per[r["dimensions"]["date"]] = per.get(r["dimensions"]["date"], 0) + \
                (r["max"]["payloadSize"] or 0) + (r["max"]["metadataSize"] or 0)
        if per:
            m["r2_gb_mean"] = sum(per.values()) / len(per) / 1e9
            m["r2_gb_days"] = len(per)

    rows = _series(tok, acct, start, end, "d1StorageAdaptiveGroups", """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    d1StorageAdaptiveGroups(limit: 500, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { date } max { databaseSizeBytes } } } } }""", 500)
    if rows is not None:
        per = {}
        for r in rows:
            per[r["dimensions"]["date"]] = max(per.get(r["dimensions"]["date"], 0),
                                               r["max"]["databaseSizeBytes"] or 0)
        if per:
            m["d1_gb_mean"] = sum(per.values()) / len(per) / 1e9
    return m


def price(m: dict) -> dict:
    """Guard's own `units()` on every term. A term that was not measured is priced None,
    not 0.0 — so a missing meter shows up as a hole rather than as a discount."""
    def _u(key, included, rate):
        return None if m.get(key) is None else bg.units(m[key], included, rate)
    out = {
        "base": 5.00,
        "d1_reads": _u("d1_reads", bg.D1_READS_INCLUDED, 0.001),
        "d1_writes": _u("d1_writes", bg.D1_WRITES_INCLUDED, 1.00),
        "r2_class_a": _u("r2_a", bg.R2_CLASS_A_INCLUDED, 4.50),
        "r2_class_b": _u("r2_b", bg.R2_CLASS_B_INCLUDED, 0.36),
        "workers": _u("workers", bg.WORKERS_INCLUDED, 0.30),
        # GB-MONTHS (730 hours), not a GB mean, and no 10 GB deduction: deducting it moves
        # the fit away from this invoice, so on this plan it is evidently not applied.
        "r2_storage": (None if m.get("r2_gb_mean") is None
                       else bg.gb_months(m["r2_gb_mean"], m["days"]) * 0.015),
        "d1_storage": (None if m.get("d1_gb_mean") is None
                       else max(0.0, bg.gb_months(m["d1_gb_mean"], m["days"]) - 5.0) * 0.75),
    }
    out["SUBTOTAL"] = sum(v for v in out.values() if v is not None)
    out["TOTAL_WITH_TAX"] = out["SUBTOTAL"] * bg.TAX_UPLIFT
    out["_holes"] = [k for k, v in out.items() if v is None]
    return out


def main() -> int:
    start = sys.argv[1] if len(sys.argv) > 2 else INVOICE["start"]
    end = sys.argv[2] if len(sys.argv) > 2 else INVOICE["end"]
    against_invoice = (start, end) == (INVOICE["start"], INVOICE["end"])
    print(f"Reconciling {start} .. {end}"
          + (f" against invoice {INVOICE['id']}" if against_invoice else " (no invoice)"))
    m = measure(start, end)
    p = price(m)

    print("\nMEASURED")
    for k in ("d1_days", "d1_reads", "d1_writes", "r2_a", "r2_b", "workers"):
        if m.get(k) is not None:
            print(f"  {k:<12} {m[k]:>18,}")
    for k in ("r2_gb_mean", "d1_gb_mean"):
        if m.get(k) is not None:
            print(f"  {k:<12} {m[k]:>18,.1f} GB")
    if m.get("d1_days") not in (None, m["days"]):
        # Say it, do not swallow it: a short window makes every total below a floor.
        print(f"  !! D1 returned {m['d1_days']} of {m['days']} days — totals are FLOORS")

    print("\nPRICED (guard's own units())")
    for k in ("base", "d1_reads", "d1_writes", "r2_class_a", "r2_class_b",
              "workers", "r2_storage", "d1_storage"):
        v = p[k]
        inv = INVOICE["lines"].get(k) if against_invoice else None
        cmp_txt = ""
        if inv is not None and v is not None:
            cmp_txt = f"   invoice ${inv:>7,.2f}   delta ${v - inv:+,.2f}"
        print(f"  {k:<12} " + ("UNMEASURED" if v is None else f"${v:>8,.2f}") + cmp_txt)
    print(f"  {'SUBTOTAL':<12} ${p['SUBTOTAL']:>8,.2f}")
    print(f"  {'WITH TAX':<12} ${p['TOTAL_WITH_TAX']:>8,.2f}")

    if p["_holes"]:
        print("\nUNMEASURED TERMS: " + ", ".join(p["_holes"])
              + " — the subtotal above is a FLOOR, not a reconstruction.")
        return 1
    if not against_invoice:
        return 0
    err = (p["SUBTOTAL"] - INVOICE["subtotal"]) / INVOICE["subtotal"] * 100
    terr = (p["TOTAL_WITH_TAX"] - INVOICE["total"]) / INVOICE["total"] * 100
    print(f"\nSUBTOTAL  ${p['SUBTOTAL']:,.2f} vs invoice ${INVOICE['subtotal']:,.2f}"
          f"  ({err:+.2f}%)")
    print(f"WITH TAX  ${p['TOTAL_WITH_TAX']:,.2f} vs invoice ${INVOICE['total']:,.2f}"
          f"  ({terr:+.2f}%)")
    if abs(err) > TOLERANCE_PCT:
        print(f"\nFAIL: off by more than {TOLERANCE_PCT}%. The pricing model has regressed, "
              f"or the invoice constants are stale. Do not ship a number from this tool "
              f"until this reconciles.")
        return 1
    print(f"\nPASS: within {TOLERANCE_PCT}% of a real invoice, per line and in total.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
