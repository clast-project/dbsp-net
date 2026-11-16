#!/usr/bin/env python3
"""Exchange phase totals grouped by the operator right after the exchange (its consumer, usually),
over the detail (DBSPNET_PROFILE_XPHASE=1) runs: usage by-consumer.py W step_ms run.prof ..."""
import re, statistics as st, sys
from collections import defaultdict
PH = ['map', 'split', 'wait', 'key', 'add', 'build']
cell = r' *(-?\d+)/ *(-?\d+) *'
agg = defaultdict(lambda: defaultdict(list))
for path in sys.argv[1:]:
    text = open(path).read().split('===== END BATCH PROFILE =====')[0]
    names = {int(m.group(1)): m.group(2) for m in re.finditer(r'^  \[\s*(\d+)\] (\w+)\s+[\d.]+ ms \(', text, re.M)}
    sec = text.split('-- exchange phases')[1]
    tot = defaultdict(lambda: [0.0] * 13)
    for m in re.finditer(r'^  \[\s*(\d+)\] (\w+)\s+own\s+(-?\d+) alloc\s+(\d+) gc\s+(\d+) rows\s+(\d+) \|' + r'\|'.join([cell] * 6), sec, re.M):
        g = m.groups(); i = int(g[0])
        nxt = i + 1
        while 'Exchange' in names.get(nxt, ''): nxt += 1
        c = names.get(nxt, '?')
        t = tot[c]; t[0] += 1
        for k in range(12): t[1 + k] += float(g[6 + k])
    for c, t in tot.items():
        for k in range(13): agg[c][k].append(t[k])
print('| consumer | n | ' + ' | '.join(f'{p} ms/MiB' for p in PH) + ' |')
print('|---|--:|' + '--:|' * 6)
for c, d in sorted(agg.items(), key=lambda kv: -sum(st.median(kv[1][k]) for k in (1, 3, 7, 9, 11))):
    print(f'| {c} | {st.median(d[0]):.0f} | ' + ' | '.join(f'{st.median(d[1 + 2 * i]):,.0f}/{st.median(d[2 + 2 * i]):,.0f}' for i in range(6)) + ' |')
