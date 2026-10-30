#!/usr/bin/env python3
# Attribute Server-GC-thread samples from macOS `sample <pid> <secs> 2 -file out.txt` to GC phases
# (design-row-representation §27.6). Usage: python3 -I scripts/gc-phases.py out.txt
import re, sys, collections
# Attribute every Server-GC-thread sample (self time of each call-tree node) to a GC phase.
lines = open(sys.argv[1]).read().split('\n')
line_re = re.compile(r'^(\s*[+!:| ]*?)(\d+) (.+?)\s+\(in ')
cat = collections.Counter()
per_thread = []
in_gc = False
stack = []  # (indent, count, frame, childsum)
def flush(upto):
    while stack and stack[-1][0] >= upto:
        ind, cnt, frame, childsum, path = stack.pop()
        self_ = cnt - childsum
        if self_ > 0:
            cat[classify(path)] += self_
def classify(path):
    p = ' > '.join(path)
    if 'garbage_collect' not in p:
        if 'SuspendRuntime' in p or 'SuspendEE' in p: return 'suspend (outside gc1)'
        return 'idle (waiting for a GC)'
    if 'join' in path[-1] or 'Wait' in path[-1] or 'cvwait' in path[-1] or 'psynch' in path[-1] or 'spin' in path[-1].lower() or 'YieldProcessor' in p.split(' > ')[-1]:
        ph = phase(p)
        return f'{ph}: join/wait'
    ph = phase(p)
    if 'mark_through_cards_for_uoh' in p: sub = 'cards LOH'
    elif 'mark_through_cards_for_segments' in p: sub = 'cards SOH'
    elif 'relocate_survivors' in p: sub = 'survivors'
    elif 'compact_in_brick' in p or 'compact_plug' in p: sub = 'copy'
    elif 'ScanStackRoots' in p or 'GcScanRoots' in p: sub = 'stack roots'
    else: sub = 'other'
    return f'{ph}: {sub}'
def phase(p):
    for name in ['relocate_phase', 'compact_phase', 'mark_phase', 'sweep_phase', 'plan_phase']:
        if name in p: return name.replace('_phase','')
    if 'SuspendRuntime' in p or 'SuspendEE' in p: return 'suspend'
    if 'RestartEE' in p or 'RestartRuntime' in p: return 'restart'
    return 'gc other'
for ln in lines:
    if re.match(r'^\s+\d+ Thread_', ln):
        flush(-1)
        in_gc = '.NET Server GC' in ln
        stack = []
        continue
    if not in_gc: continue
    m = line_re.match(ln)
    if not m: continue
    indent = len(m.group(1)); cnt = int(m.group(2)); frame = m.group(3)
    flush(indent)
    if stack:
        stack[-1][3] += cnt
    path = (stack[-1][4] if stack else []) + [frame]
    stack.append([indent, cnt, frame, 0, path])
flush(-1)
tot = sum(cat.values())
busy = sum(v for k, v in cat.items() if not k.startswith('idle'))
print(f'GC-thread samples {tot}, in a GC {busy}')
for k, v in sorted(cat.items(), key=lambda kv: -kv[1]):
    if k.startswith('idle'): continue
    print(f'{100*v/busy:5.1f}%  {k}')
