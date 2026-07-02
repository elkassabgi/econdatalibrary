import requests, json, time
from concurrent.futures import ThreadPoolExecutor, as_completed
UA = 'Econ-Fin Data Library admin@hfdatalibrary.com'
sess = requests.Session(); sess.headers.update({'User-Agent': UA})
API = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service'

# Master candidate endpoint list: union of everything discovered, plus probes for
# the datasets in the 54-slug list that aren't yet covered. I include many plausible
# names so live-probing tells us which exist (anti-undercount).
candidates = set(json.load(open('D:/research/econfindatalibrary/data/_treasury_dd_paths.json')))

# current DTS (9)
candidates |= {
 'v1/accounting/dts/operating_cash_balance',
 'v1/accounting/dts/deposits_withdrawals_operating_cash',
 'v1/accounting/dts/public_debt_transactions',
 'v1/accounting/dts/adjustment_public_debt_transactions_cash_basis',
 'v1/accounting/dts/debt_subject_to_limit',
 'v1/accounting/dts/inter_agency_tax_transfers',
 'v1/accounting/dts/income_tax_refunds_issued',
 'v1/accounting/dts/federal_tax_deposits',
 'v1/accounting/dts/short_term_cash_investments',
}
# new TOP combined endpoint + auctions + securities txns + buybacks + frn + tips + ibonds
candidates |= {
 'v1/accounting/od/top_treasury_offset_program',       # guess
 'v1/debt/top/treasury_offset_program',                # guess
 'v1/accounting/od/auctions_query',                    # securities auctions
 'v1/accounting/od/securities_q',                      # guess
 'v1/accounting/od/electronic_securities_transactions',
 'v1/accounting/od/buybacks_operations',               # buybacks
 'v1/accounting/od/buybacks_security_details',
 'v1/accounting/od/frn_daily_indexes',
 'v1/accounting/od/tips_cpi_data',
 'v1/accounting/od/i_bonds_interest_rates',
 'v1/accounting/od/ibonds_interest_rates',
 'v1/accounting/od/qtcb_historical_rates',
 'v1/accounting/od/fed_credit_similar_maturity_rates',
 'v1/accounting/od/treasury_managed_accounts',
 'v1/accounting/od/managed_accounts',
 'v2/accounting/od/utf_account_balances',
 'v1/accounting/od/utf_account_balances',
 'v1/accounting/od/utf_federal_activity_statement',
 'v1/accounting/od/utf_transaction_subtotals',
 'v2/accounting/od/utf_qtr_yields',
 'v1/accounting/od/unemployment_trust_fund_yields',
 'v1/accounting/od/gold_reserve',
 'v1/accounting/od/receipts_by_department',
 'v1/accounting/mts/mts_receipts_by_department',
 'v1/accounting/od/monthly_treasury_disbursements',
 'v1/accounting/od/disbursements',
 'v1/accounting/od/treasury_bulletin',
 'v1/accounting/od/hist_debt_outstanding',
 'v1/debt/historical/hist_debt_outstanding',
 'v2/accounting/od/hist_debt_outstanding',
 'v1/accounting/od/dgas',  # daily government account series
 'v1/accounting/od/daily_government_account_series',
 'v1/accounting/od/fbp_distribution_transaction',
 'v1/accounting/od/fbp_interest_uninvested',
 'v1/accounting/od/fbp_summary_general_ledger',
 'v1/accounting/od/fip_interest_cost_by_fund',
 'v1/accounting/od/federal_investments_principal',
 'v1/accounting/od/federal_investments_statement',
 'v1/accounting/od/slgs_daily_rate_table',
 'v1/accounting/od/slgs_program_stats',
 'v1/accounting/od/savings_bonds_issues_redemptions',
 'v1/accounting/od/treasury_certified_rates_annual',
 'v1/accounting/od/treasury_certified_rates_monthly',
 'v1/accounting/od/treasury_certified_rates_quarterly',
 'v1/accounting/od/treasury_certified_rates_semiannual',
 'v1/accounting/od/us_government_financial_report',
 'v2/accounting/od/financial_report',
 'v1/accounting/od/unemployment_trust_funds_report',
 'v1/accounting/od/upcoming_auctions',
 'v1/accounting/od/record_setting_auction',
 'v1/accounting/od/qualified_tax',
 'v1/reference/fiscal_data/announcements',
}

def probe(p):
    for attempt in range(4):
        try:
            r = sess.get(f'{API}/{p}', params={'page[size]': '1'}, timeout=40)
            if r.status_code == 200:
                j = r.json(); m = j.get('meta', {})
                cols = list((j.get('data') or [{}])[0].keys())
                return p, {'ok': True, 'total': m.get('total-count'), 'ncols': len(cols)}
            if r.status_code == 404:
                return p, {'ok': False, 'status': 404}
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(2**attempt, 8)); continue
            return p, {'ok': False, 'status': r.status_code}
        except Exception as e:
            time.sleep(min(2**attempt, 8)); err = str(e)[:60]
    return p, {'ok': False, 'status': 'err', 'err': err}

res = {}
with ThreadPoolExecutor(max_workers=6) as ex:
    for f in as_completed({ex.submit(probe, p): p for p in candidates}):
        p, info = f.result(); res[p] = info

ok = {p: i for p, i in res.items() if i.get('ok')}
print('=== NEWLY confirmed (not in the original 68) ===')
orig68 = set(json.load(open('D:/research/econfindatalibrary/data/_treasury_endpoint_verify.json')).keys())
for p in sorted(ok):
    flag = '' if p in orig68 else '  <== NEW'
    if flag:
        print(f'  {p:60} total={ok[p]["total"]:>12,}{flag}')
print(f'\nTotal OK endpoints now: {len(ok)}')
json.dump(res, open('D:/research/econfindatalibrary/data/_treasury_discover2.json','w'), indent=1)
