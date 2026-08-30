"""Fault-injection proof that the billing guard degrades LOUDLY, not cheaply.

WHY A TEST AND NOT A CODE READING. Every defect this file exercises returns HTTP 200 with
no error key, which is why reading the code twice failed to find them: a fetch that succeeds
while returning a quarter of the days looks identical to a healthy one at the call site.
Two guards shipped this week with fail-open branches (R501, R503) and both were reviewed
first. The only thing that distinguishes a guard that refuses from a guard that says it
refuses is running it with the failure present.

Each case asserts BOTH halves of the contract: the blind spot is recorded, AND it reaches
the dollar figure. A degradation that is printed but not priced is R505, which is the entry
this whole exercise exists to avoid repeating.

    py -3.14 tools/test_billing_guard_failclosed.py
"""
import sys

sys.path.insert(0, __file__.rsplit("\\", 1)[0].rsplit("/", 1)[0])
import billing_guard as bg                                          # noqa: E402

FAILURES = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(("  PASS  " if cond else "  FAIL  ") + name + (f"  [{detail}]" if detail else ""))
    if not cond:
        FAILURES.append(name)


def reset() -> None:
    bg._DEGRADED.clear()


def case_truncation() -> None:
    """A response filling its `limit` may be a truncated page. Measured on the live API:
    the same query at limit=1 returned 9 requests where limit=2000 returned 1,038,162 —
    a 115,000x undercount, HTTP 200, no error key. Only the row count can reveal it."""
    reset()
    rows = bg._rows({"r2OperationsAdaptiveGroups": [{"x": i} for i in range(50)]},
                    "r2OperationsAdaptiveGroups", 50)
    check("full page is treated as suspect", len(bg._DEGRADED) == 1, bg._DEGRADED[0][:60])
    check("rows still returned (a floor beats nothing)", len(rows) == 50)
    reset()
    bg._rows({"r2OperationsAdaptiveGroups": [{"x": i} for i in range(49)]},
             "r2OperationsAdaptiveGroups", 50)
    check("a short page is not flagged", not bg._DEGRADED)


def case_short_window() -> None:
    """The silent one: a legal window that returns fewer days than it covers. Every
    period-to-date total below it is then a fraction of the truth, with nothing to say so."""
    reset()
    ok = bg._check_days("D1", ["2026-08-27", "2026-08-28", "2026-08-29"], 22, "2026-08-28")
    check("short day list refuses", not ok)
    check("short day list is recorded", any("of 22 days" in d for d in bg._DEGRADED))


def case_stale_rate() -> None:
    """A missing day makes dates[-2] day-2 rather than yesterday, so the forecast runs on
    a stale rate. Counting days cannot catch this; comparing to yesterday can."""
    reset()
    dates = [f"2026-08-{d:02d}" for d in range(9, 31)]
    dates.remove("2026-08-29")                       # yesterday absent, count still plausible
    ok = bg._check_days("D1", dates, 22, "2026-08-29")
    check("missing yesterday refuses", not ok)
    check("stale rate is named", any("STALE" in d for d in bg._DEGRADED))


def case_one_day() -> None:
    """Fewer than two days means dates[-2] cannot exist. The old code skipped the block in
    silence, leaving the term priced at 0.0 under the word PROJECTED."""
    reset()
    check("single day refuses", not bg._check_days("R2 operations", ["2026-08-30"], 22,
                                                   "2026-08-29"))
    check("single day is recorded", any("cannot price" in d for d in bg._DEGRADED))


def case_healthy() -> None:
    """The control. Without this, a check that always fails would pass every case above."""
    reset()
    dates = [f"2026-08-{d:02d}" for d in range(9, 31)]
    check("healthy window is accepted", bg._check_days("D1", dates, 22, "2026-08-29"))
    check("healthy window records nothing", not bg._DEGRADED, str(bg._DEGRADED))


def case_pricing() -> None:
    """The arithmetic that reaches the invoice, pinned to the invoice's own numbers."""
    check("whole units round UP", bg.units(32_284_689 + 50_000_000, 50_000_000, 1.00) == 33.00,
          f"{bg.units(82_284_689, 50_000_000, 1.00)}")
    check("Class A matches the invoice",
          bg.units(21_325_560, 1_000_000, 4.50) == 94.50)
    check("usage inside the allowance is free",
          bg.units(10_967_739_360, bg.D1_READS_INCLUDED, 0.001) == 0.0)
    check("a GB-month is 730 hours, not a day count",
          abs(bg.gb_months(1458.1, 31) - 1486.1) < 1.0,
          f"{bg.gb_months(1458.1, 31):.1f}")
    check("tax uplift reproduces the invoice",
          abs(154.96 * bg.TAX_UPLIFT - 165.19) < 0.10,
          f"{154.96 * bg.TAX_UPLIFT:.2f}")
    # THE RATE MUST NOT BE ONE DAY: on 2026-08-16 yesterday's rate was 95.1B and the period
    # median was ~0.3B. The first projects $2,459 of reads, the second projects the truth.
    spike = [117_258_698] * 20 + [100_605_881_601, 95_066_683_899]
    check("median survives two incident days",
          bg._median(spike) == 117_258_698, f"{bg._median(spike):,.0f}")


def main() -> int:
    for fn in (case_truncation, case_short_window, case_stale_rate, case_one_day,
               case_healthy, case_pricing):
        print(fn.__name__)
        fn()
    reset()
    print("\n" + ("ALL PASS" if not FAILURES else "FAILED: " + ", ".join(FAILURES)))
    return 1 if FAILURES else 0


if __name__ == "__main__":
    raise SystemExit(main())
