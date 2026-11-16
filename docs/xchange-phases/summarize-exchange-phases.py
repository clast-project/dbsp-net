#!/usr/bin/env python3
"""Median exchange phase decomposition of ivm-bench batch 1 over several DBSPNET_PROFILE runs.

usage: summarize-exchange-phases.py W run1.prof run2.prof ...
Reads the first (batch 1) report of each file: engine step, allocation, GC pause, the
'exchange phases' kind table and per-site table (ProgramRunner.AppendExchangePhases).
Phase ms are summed over workers; '% step' divides by W and by the run's engine step wall.
"""
import re, statistics as st, sys
from collections import defaultdict

PH = ['map', 'split', 'wait', 'key', 'add', 'build']
W = int(sys.argv[1])
runs = []
for path in sys.argv[2:]:
    text = open(path).read().split('===== END BATCH PROFILE =====')[0]
    r = {'path': path}
    r['step'] = float(re.search(r'engine step\s+(\d+) ms', text).group(1))
    r['alloc'] = float(re.search(r'allocated ([\d.]+) GiB', text).group(1)) * 1024
    r['pause'] = float(re.search(r'GC pause (\d+) ms', text).group(1))
    sec = text.split('-- exchange phases')[1]
    cell = r' *(-?\d+)/ *(-?\d+) *'
    kinds = {}
    for m in re.finditer(r'^  (\w+Op)\s+(\d+)\s+(-?\d+)\s+(\d+)\s+(\d+) \|' + r'\|'.join([cell] * 6), sec, re.M):
        g = m.groups()
        kinds[g[0]] = dict(n=int(g[1]), own=float(g[2]), alloc=float(g[3]), gc=float(g[4]),
                           **{p: (float(g[5 + 2 * i]), float(g[6 + 2 * i])) for i, p in enumerate(PH)})
    sites = {}
    for m in re.finditer(r'^  \[\s*(\d+)\] (\w+)\s+own\s+(-?\d+) alloc\s+(\d+) gc\s+(\d+) rows\s+(\d+) \|' + r'\|'.join([cell] * 6) + r' (\S+)', sec, re.M):
        g = m.groups()
        sites[int(g[0])] = dict(kind=g[1], own=float(g[2]), alloc=float(g[3]), gc=float(g[4]), rows=int(g[5]), view=g[-1],
                                **{p: (float(g[6 + 2 * i]), float(g[7 + 2 * i])) for i, p in enumerate(PH)})
    r['kinds'], r['sites'] = kinds, sites
    runs.append(r)

def med(xs): return st.median(xs)
step = med([r['step'] for r in runs]); alloc = med([r['alloc'] for r in runs])
print(f"runs {len(runs)}  engine step median {step:.0f} ms (range {min(r['step'] for r in runs):.0f}-{max(r['step'] for r in runs):.0f})"
      f"  alloc {alloc / 1024:.2f} GiB  GC pause median {med([r['pause'] for r in runs]):.0f} ms")
def pct_step(ms_sum, r_steps): return med([m / W / s * 100 for m, s in zip(ms_sum, r_steps)])
steps = [r['step'] for r in runs]

print("\n## by kind (median; ms summed over workers ex-GC; % step = ms/W/step)")
print(f"| kind | n | own ms | own % step | gc-in-op ms | alloc MiB | alloc % | " + " | ".join(f"{p} ms" for p in PH) + " |")
print("|---|--:|--:|--:|--:|--:|--:|" + "--:|" * len(PH))
tot = defaultdict(list)
for k in runs[0]['kinds']:
    rows = [r['kinds'][k] for r in runs]
    own = [x['own'] for x in rows]
    print(f"| `{k}` | {rows[0]['n']} | {med(own):,.0f} | {pct_step(own, steps):.1f}% | {med([x['gc'] for x in rows]):,.0f} | "
          f"{med([x['alloc'] for x in rows]):,.0f} | {med([x['alloc'] / r['alloc'] * 100 for x, r in zip(rows, runs)]):.1f}% | "
          + " | ".join(f"{med([x[p][0] for x in rows]):,.0f}" for p in PH) + " |")

print("\n## by phase, all exchanges (median)")
print("| phase | ms ex-GC | % step | alloc MiB | % alloc |")
print("|---|--:|--:|--:|--:|")
for p in PH:
    ms = [sum(x[p][0] for x in r['kinds'].values()) for r in runs]
    mib = [sum(x[p][1] for x in r['kinds'].values()) for r in runs]
    print(f"| {p} | {med(ms):,.0f} | {pct_step(ms, steps):.1f}% | {med(mib):,.0f} | {med([m / r['alloc'] * 100 for m, r in zip(mib, runs)]):.1f}% |")
own = [sum(x['own'] for x in r['kinds'].values()) for r in runs]
mib = [sum(x['alloc'] for x in r['kinds'].values()) for r in runs]
gc = [sum(x['gc'] for x in r['kinds'].values()) for r in runs]
print(f"| **own (all but wait)** | {med(own):,.0f} | {pct_step(own, steps):.1f}% | {med(mib):,.0f} | {med([m / r['alloc'] * 100 for m, r in zip(mib, runs)]):.1f}% |")
print(f"| GC pause inside exchanges | {med(gc):,.0f} | {pct_step(gc, steps):.1f}% | | |")

print("\n## top sites by median own ex-GC (and by alloc)")
ids = set().union(*[r['sites'] for r in runs])
site = []
for i in ids:
    rows = [r['sites'][i] for r in runs if i in r['sites']]
    o = [x['own'] for x in rows]
    site.append((i, rows[0], med(o), min(o), max(o), med([x['gc'] for x in rows]), med([x['alloc'] for x in rows]), rows))
print("| op | kind | view | rows | own ms (min-max) | own % step | gc-in-op ms | alloc MiB | ns/row ex-GC | " + " | ".join(PH) + " |")
print("|--:|---|---|--:|--:|--:|--:|--:|--:|" + "--:|" * len(PH))
top = sorted(site, key=lambda s: -s[2])[:12]
for s in sorted(site, key=lambda s: -s[6])[:12]:
    if s not in top: top.append(s)
for i, x, o, lo, hi, g, a, rows in top:
    print(f"| {i} | `{x['kind']}` | `{x['view']}` | {x['rows']:,} | {o:,.0f} ({lo:,.0f}-{hi:,.0f}) | {o / W / step * 100:.1f}% | {g:,.0f} | {a:,.0f} | "
          f"{o * 1e6 / max(1, x['rows']):,.0f} | " + " | ".join(f"{med([y[p][0] for y in rows]):,.0f}/{med([y[p][1] for y in rows]):,.0f}" for p in PH) + " |")
