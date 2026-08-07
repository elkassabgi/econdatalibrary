import pyarrow.parquet as pq
import os, glob

root = r'D:/research/econfindatalibrary/data/clean_full'

new_datasets = [
    'imf_mfs', 'imf_gfsmab', 'imf_bop', 'imf_gfsssuc', 'imf_apdreo',
    'imf_cpi', 'imf_irfcl', 'imf_bopagg', 'imf_cofer', 'imf_commodity',
    'imf_fas', 'imf_fdi', 'imf_fiscaldecentralization', 'imf_fm',
    'imf_fsire', 'imf_gender_budgeting', 'imf_gender_equality',
    'imf_hpdd', 'imf_ifs', 'imf_mcdreo', 'imf_namain_idc_n',
    'imf_pctot', 'imf_pgcs', 'imf_pgi', 'imf_psbsfad',
    'imf_unsdg_imf_inputs', 'imf_whdreo', 'imf_world', 'imf_fsi',
    'imf_dot', 'imf_weo', 'imf_afrreo',
]

print(f'{"Dataset":<35} {"Obs":>15}')
print('-'*52)
total_new = 0
for ds in sorted(new_datasets):
    d = os.path.join(root, ds)
    n = 0
    for f in glob.glob(d + '/*.parquet'):
        try: n += pq.read_metadata(f).num_rows
        except: pass
    total_new += n
    if n > 0:
        print(f'{ds:<35} {n:>15,}')

print(f'\n{"TOTAL (IMF datasets)":<35} {total_new:>15,}')