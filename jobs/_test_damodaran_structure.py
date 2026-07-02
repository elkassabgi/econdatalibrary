import requests, xlrd, openpyxl, io
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
BASE = "https://pages.stern.nyu.edu/~adamodar/pc/datasets"

# Check wacc.xls rows 10-20 to find actual header
r = requests.get(f"{BASE}/wacc.xls", headers=UA, timeout=30)
wb = xlrd.open_workbook(file_contents=r.content)
ws = wb.sheet_by_name("Industry Averages")
print(f"wacc.xls 'Industry Averages': {ws.nrows}r x {ws.ncols}c")
for ri in range(10, min(25, ws.nrows)):
    row_vals = []
    for ci in range(min(6, ws.ncols)):
        v = ws.cell_value(ri, ci)
        if v or v == 0:
            row_vals.append(repr(v)[:25])
    if row_vals:
        print(f"  R{ri}: {row_vals}")

# Exact apostrophe check in Sovereign Ratings sheet name
r2 = requests.get(f"{BASE}/ctrypremApr26.xlsx", headers=UA, timeout=30)
wb2 = openpyxl.load_workbook(io.BytesIO(r2.content), read_only=True, data_only=True)
for sn in wb2.sheetnames:
    if "overeign" in sn or "oody" in sn:
        ords = [f"{c}={ord(c)}" for c in sn]
        print(f"\nExact: {ords}")
        # Also check what my config string would be
        my_name = "Sovereign Ratings (Moody's,S&P)"
        my_ords = [f"{c}={ord(c)}" for c in my_name]
        print(f"Config: {my_ords}")
        print(f"Match: {sn == my_name}")
        print(f"Lower match: {sn.lower() == my_name.lower()}")
