# summarize.py PROF... — batch-1 dim_customer window-exchange cost from DBSPNET_PROFILE reports.
# Skippable = the ExchangeOps feeding the 22 (customer_id, x_grp) windows: every ExchangeOp after the
# chain's first WindowAggregate (the root PARTITION BY customer_id exchange stays).
import re, sys, statistics as st
W = 12
rows = []
for path in sys.argv[1:]:
    txt = open(path).read().split('===== DBSPNET BATCH PROFILE =====')[1]  # batch 1
    step = float(re.search(r'engine step\s+(\d+) ms', txt).group(1))
    wall = float(re.search(r'batch wall (\d+) ms', txt).group(1))
    alloc = float(re.search(r'allocated ([\d.]+) GiB', txt).group(1))
    view = txt.split("-- all operators of view 'dim_customer'")[1].split('\n-- ')[0]
    ops = [(int(m[1]), m[2], float(m[3]), float(m[4])) for m in
           re.finditer(r'\[\s*(\d+)\] (\w+)\s+([\d.]+) ms\s+alloc\s+([\d.]+) MiB', view)]
    first_win = next(i for i, o in enumerate(ops) if o[1] == 'WindowAggregate')
    skip = {o[0] for o in ops[first_win:] if o[1] == 'ExchangeOp'}
    allx = {o[0] for o in ops if 'Exchange' in o[1]}
    wait = {int(m[1]): float(m[2]) for m in re.finditer(
        r'\[\s*(\d+)\] \w*Exchange\w*\s+wait\s+(\d+) ms .*dim_customer', txt)}
    ms = {o[0]: o[2] for o in ops}; mib = {o[0]: o[3] for o in ops}
    rows.append(dict(
        step=step, wall=wall, alloc=alloc, n_skip=len(skip), n_x=len(allx),
        skip_cpu=sum(ms[i] for i in skip), skip_wait=sum(wait.get(i, 0) for i in skip),
        skip_mib=sum(mib[i] for i in skip),
        allx_cpu=sum(ms[i] for i in allx),
        win_cpu=sum(o[2] for o in ops if o[1] == 'WindowAggregate'),
        view_cpu=sum(o[2] for o in ops), view_mib=sum(o[3] for o in ops)))
for r in rows:
    r['skip_wall'] = r['skip_cpu'] / W                     # per-worker mean time in those exchanges
    r['skip_pct'] = r['skip_wall'] / r['step'] * 100
    r['allx_pct'] = r['allx_cpu'] / W / r['step'] * 100
    r['win_pct'] = r['win_cpu'] / W / r['step'] * 100
    r['view_pct'] = r['view_cpu'] / W / r['step'] * 100
keys = ['step', 'skip_cpu', 'skip_wait', 'skip_wall', 'skip_pct', 'allx_cpu', 'allx_pct',
        'win_cpu', 'win_pct', 'view_cpu', 'view_pct', 'skip_mib', 'view_mib', 'alloc']
print('run  ' + ' '.join(f'{k:>9}' for k in keys))
for p, r in zip(sys.argv[1:], rows):
    print(f'{p.split("/")[-1][:4]:4} ' + ' '.join(f'{r[k]:9.1f}' for k in keys))
print('med  ' + ' '.join(f'{st.median(r[k] for r in rows):9.1f}' for k in keys))
print(f'skippable exchanges = {rows[0]["n_skip"]} of {rows[0]["n_x"]} in dim_customer')
