"""How expensive is ONE request, per site? The scaling question the totals hide.

WHY THIS MATTERS. Measured 2026-09-03: econdatalibrary takes 4.8% of the account's worker
requests and its catalogue accounts for 98.1% of all D1 rows read. Even after the 2026-08-15 fix
that removed a per-visitor COUNT(*) over 12.3M rows, D1 reads sit near half the included
allowance on a median day. If econ's traffic grew toward hf's, the question is not "what does it
cost now" but "what does one more visitor cost".

That is rows-read per request, and it is the number that decides whether growth is affordable.
Totals cannot answer it: a site can be cheap because it is expensive per visit and has few
visitors, which is the worst position to grow from.

THE HONEST LIMIT, stated up front. D1 reads are attributed per DATABASE and requests per SCRIPT,
and the mapping is not one-to-one - econdl-api reads econ-catalog, econ-catalog-climate AND
hfdatalibrary-db (the shared login), and hfdatalibrary-api reads hfdatalibrary-db. So this can
attribute a database to the script that owns it, but a cross-read lands on the owner. Every
figure below is therefore an approximation whose direction is stated, not a clean per-site cost.

Window: the days AFTER the 2026-08-15 serving fix, because the two spike days would otherwise
dominate and describe a bug rather than the system. Read-only, no D1 statements.
"""
import datetime as dt
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from tools import billing_guard as bg  # noqa: E402

FIX_DAY = "2026-08-16"          # first full day after the per-visitor COUNT(*) fix

# database_id -> (name, the script that owns it), from api/worker/wrangler.toml:38,49,58
DBS = {
    "1a6d0755-ecef-46d0-a478-46cad1cf064c": ("econ-catalog", "econdl-api"),
    "e34114f2-c0be-43d9-bcb5-798a3952414c": ("econ-catalog-climate", "econdl-api"),
    "a396506e-e78a-4978-bc38-883056f98810": ("hfdatalibrary-db", "hfdatalibrary-api"),
}

Q_D1 = """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    d1AnalyticsAdaptiveGroups(limit: 5000, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { databaseId } sum { rowsRead } } } } }"""

Q_W = """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    workersInvocationsAdaptive(limit: 2000, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { scriptName } sum { requests } } } } }"""


def main():
    tok, acct = bg._load_env_token(), bg._account_id()
    if not tok or not acct:
        print("no analytics token / account id")
        return 2
    end = dt.datetime.now(dt.timezone.utc).date()
    print(f"window {FIX_DAY} .. {end}  (after the 2026-08-15 serving fix)\n")

    d = bg._graphql(tok, Q_D1, {"acct": acct, "start": FIX_DAY, "end": end.isoformat()})
    w = bg._graphql(tok, Q_W, {"acct": acct, "start": FIX_DAY, "end": end.isoformat()})
    if d is None or w is None:
        print("a query returned nothing")
        return 2

    reads = defaultdict(int)
    for r in bg._rows(d, "d1AnalyticsAdaptiveGroups", 5000):
        reads[r["dimensions"]["databaseId"]] += r["sum"]["rowsRead"]
    reqs = defaultdict(int)
    for r in bg._rows(w, "workersInvocationsAdaptive", 2000):
        reqs[r["dimensions"].get("scriptName") or "(unnamed)"] += r["sum"]["requests"]

    by_script = defaultdict(int)
    print(f"{'database':<24}{'rows read':>18}{'attributed to':>22}")
    for did, total in sorted(reads.items(), key=lambda kv: -kv[1]):
        name, owner = DBS.get(did, (did[:22], "(unmapped)"))
        by_script[owner] += total
        print(f"{name:<24}{total:>18,}{owner:>22}")

    print(f"\n{'site':<24}{'requests':>14}{'rows read':>18}{'rows per request':>19}")
    for script in sorted(set(by_script) | set(reqs), key=lambda s: -reqs.get(s, 0)):
        rq, rd = reqs.get(script, 0), by_script.get(script, 0)
        per = (rd / rq) if rq else float("nan")
        print(f"{script:<24}{rq:>14,}{rd:>18,}{per:>19,.0f}")

    tot_reads = sum(reads.values())
    days = (end - dt.date.fromisoformat(FIX_DAY)).days or 1
    print(f"\ntotal {tot_reads:,} rows over {days} days = {tot_reads/days:,.0f}/day")
    print(f"a 31-day period at that rate is {tot_reads/days*31/1e9:.1f} B against "
          f"{bg.D1_READS_INCLUDED/1e9:.0f} B included "
          f"({tot_reads/days*31/bg.D1_READS_INCLUDED*100:.0f}% used)")

    econ_per = by_script.get("econdl-api", 0) / max(reqs.get("econdl-api", 0), 1)
    hf_per = by_script.get("hfdatalibrary-api", 0) / max(reqs.get("hfdatalibrary-api", 0), 1)
    if hf_per:
        print(f"\nONE econ request reads about {econ_per/hf_per:,.0f}x what one hf request does.")
        head = bg.D1_READS_INCLUDED - tot_reads / days * 31
        if econ_per > 0 and head > 0:
            print(f"At that rate the remaining monthly headroom is about "
                  f"{head/econ_per/31:,.0f} extra econ requests a DAY before reads start "
                  f"costing money.")
    print("\nCROSS-READS ARE NOT SEPARATED: econdl-api also reads hfdatalibrary-db for login,")
    print("so hf's figure carries some econ traffic and econ's is if anything understated.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
