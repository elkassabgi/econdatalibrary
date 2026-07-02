import requests, re, json, time
UA = 'Econ-Fin Data Library admin@hfdatalibrary.com'
sess = requests.Session(); sess.headers.update({'User-Agent': UA})
base = 'https://fiscaldata.treasury.gov'
API = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service'

slugs = json.load(open('D:/research/econfindatalibrary/data/_treasury_authoritative.json'))['slugs'] \
        if False else None
slugs = [
 'average-interest-rates-treasury-securities','daily-government-account-series','daily-treasury-statement',
 'debt-to-the-penny','delinquent-debt-referral-compliance','electronic-securities-transactions',
 'fbp-distribution-transaction-data','fbp-interest-on-uninvested-funds','fbp-summary-general-ledger-balances-report',
 'fed-credit-similar-maturity-rates','federal-investments-program-principal-outstanding',
 'federal-investments-program-statement-of-account','fip-interest-cost-by-fund','frn-daily-indexes',
 'gift-contributions-reduce-debt-held-by-public','historical-debt-outstanding','i-bonds-interest-rates',
 'interest-expense-debt-outstanding','judgment-fund-report-to-congress','monthly-statement-public-debt',
 'monthly-treasury-disbursements','monthly-treasury-statement','qtcb-historical-interest-rates',
 'receipts-by-department','record-setting-auction-data','redemption-tables','revenue-collections-management',
 'savings-bond-value-files','savings-bonds-issues-redemptions-maturities-by-series','savings-bonds-securities',
 'schedules-federal-debt','schedules-federal-debt-daily','slgs-daily-rate-table','slgs-securities',
 'slgs-securities-program-stats','ssa-title-xii-advance-activities','status-report-government-gold-reserve',
 'tips-cpi-data','top-treasury-offset-program','treasury-bulletin','treasury-bulletin-trust-funds',
 'treasury-certified-interest-rates-annual','treasury-certified-interest-rates-monthly',
 'treasury-certified-interest-rates-quarterly','treasury-certified-interest-rates-semiannual',
 'treasury-managed-accounts','treasury-report-on-receivables','treasury-reporting-rates-exchange',
 'treasury-securities-auctions-data','treasury-securities-buybacks','u-s-government-financial-report',
 'unemployment-trust-fund-yields','unemployment-trust-funds-report-selection','upcoming-auctions',
]

# For each slug, fetch the dataset HTML page and the gatsby page-data; extract any
# v?/.../... endpoint base path that appears (the page embeds the live API URL).
slug_eps = {}
for s in slugs:
    found = set()
    for u in [f'{base}/datasets/{s}/', f'{base}/page-data/datasets/{s}/page-data.json']:
        try:
            r = sess.get(u, timeout=40)
            if r.status_code != 200:
                continue
            for m in re.findall(r'(v\d/[a-z_]+/[a-z0-9_]+/[a-z0-9_]+)', r.text):
                found.add(m)
            # also single-level deeper (e.g. v2/debt/tror)
            for m in re.findall(r'api/fiscal_service/(v\d/[a-z0-9_/]+)', r.text):
                found.add(m.split('?')[0].rstrip('/'))
        except Exception as e:
            pass
        time.sleep(0.1)
    slug_eps[s] = sorted(found)

print('=== slug -> endpoint paths embedded in page ===')
for s, eps in slug_eps.items():
    print(f'{s:50} {eps}')
json.dump(slug_eps, open('D:/research/econfindatalibrary/data/_treasury_slug_eps.json','w'), indent=1)
