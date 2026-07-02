import requests, re, json
UA = 'Econ-Fin Data Library admin@hfdatalibrary.com'
h = {'User-Agent': UA}
API = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service'
sess = requests.Session(); sess.headers.update(h)

# 1) Probe current DTS endpoint naming. The DTS moved to operating-cash-balance style names.
# Known current DTS endpoints (post-restructure): dts/* with descriptive names.
dts_candidates = [
    'v1/accounting/dts/operating_cash_balance',
    'v1/accounting/dts/deposits_withdrawals_operating_cash',
    'v1/accounting/dts/public_debt_transactions',
    'v1/accounting/dts/adjustment_public_debt_transactions_cash_basis',
    'v1/accounting/dts/debt_subject_to_limit',
    'v1/accounting/dts/inter_agency_tax_transfers',
    'v1/accounting/dts/income_tax_refunds_issued',
    'v1/accounting/dts/federal_tax_deposits',
    'v1/accounting/dts/short_term_cash_investments',
    'v1/accounting/dts/dts_table_1',
    'v1/accounting/dts/dts_table_2',
]
def probe(p):
    try:
        r = sess.get(f'{API}/{p}', params={'page[size]':'1'}, timeout=40)
        if r.status_code == 200:
            j = r.json(); m = j.get('meta', {})
            return p, ('OK', m.get('total-count'), len((j.get('data') or [{}])[0]))
        return p, ('HTTP', r.status_code, None)
    except Exception as e:
        return p, ('ERR', str(e)[:60], None)

print('=== DTS candidate probes ===')
for p in dts_candidates:
    print(' ', p, probe(p)[1])

# 2) Probe current TROR subcategory naming (the 4 stale ones)
tror_candidates = [
    'v2/debt/tror/collected_outstanding_recv_by_agency',
    'v2/debt/tror/delinquent_debt_by_agency',
    'v2/debt/tror/written_off_delinquent_debt_by_agency',
    'v2/debt/tror/collections_delinquent_debt_by_agency',
    'v2/debt/tror/debt_collected_third_party',
]
print('\n=== TROR candidate probes ===')
for p in tror_candidates:
    print(' ', p, probe(p)[1])
