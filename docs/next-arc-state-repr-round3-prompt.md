# Starting prompt — state-representation arc, round 3

> Kickoff prompt for the session after rounds 1–2 of the state-representation
> arc (`docs/design-state-representation.md`). Paste the block below into a new
> session. Use Opus.

---

**State-representation arc, round 3: measure where we now stand against Feldera, then go after what still decays.**

Read `docs/design-state-representation.md` first, all of it. The arc's goal is to
stop DbspNet's throughput from decaying as operator state grows; Feldera's holds
flat. Rounds 1–2 (2026-10-28/29) ran parallel agent experiments on `exp/state-*`
branches and shipped four changes to main:

- **A** compact partitioned TOP-K (`b0d25ad`, with `IFingerprintAlias` so old snapshots restore): q18 ~2×, q19 ~1.6×
- **B** compact COUNT(DISTINCT) state (`c53e782`): q15/q16 +35–38%
- **G** trace-free linear aggregate (`a55b0e4`): q15 ~2.8×, q16 ~8×, both now flat
- **B2** hybrid MIN/MAX (`8311be3`): q4 +15%, flat, −34% state

Not shipped, still on their branches: C (FlatHash trace), D (sorted LSM trace;
both shrink join state 35–48% but are 9–15% slower), E (string interning), H
(ingest string arena: −85% objects, no throughput gain, retention risk). The
`IIndexedTrace` seam (`exp/state-trace-seam`, `DBSP_TRACE_REPR`) is also unmerged.
The lesson so far: **throughput follows work per row**, not bytes and not object
count.

**Instruments and rules (learned the hard way):**
- `decay [events] [batch] [segments] [stream|pregen] [q..]` gives per-segment ev/s,
  in-step GC pause, and live bytes after a full GC.
- Live bytes and `w1profile` B/ev are deterministic, so agents can measure them
  in parallel. Wall-clock is valid only when serialized on an idle machine.
- **Always use `NEXMARK_GEN=feldera`** (a Feldera-faithful generator). The legacy
  generator under-represents strings and hot keys, so every pre-10-28 Feldera
  ratio was on lighter data.
- **Wrap every unattended timing run in `caffeinate -i`.** macOS idle sleep
  produced fake stalls in round 2.
- Give each agent its own scratch directory. A shared one clobbered a baseline
  in round 1.
- Toolchain: `DOTNET_ROOT=$HOME/.dotnet`, `DOTNET_GCDynamicAdaptationMode=0`.
  Feldera needs `RUSTUP_TOOLCHAIN=1.93.1`.

**Round 3, in order:**

1. **Head-to-head vs Feldera on faithful data.** Make `scripts/compare-nexmark.sh`
   use `NEXMARK_GEN=feldera` on our side. Run 1M and 10M events, serialized,
   under caffeinate. Compare against the 2026-08-31 M4 table in
   `docs/benchmarks-comparison.md`. Compare decay curves, not just ratios: which
   of our queries still lose throughput between 1M and 10M, and does Feldera's?
2. **Re-take the Phase 0 decay table** (§1.2) on main with
   `NEXMARK_GEN=feldera`, so the remaining decayers are ranked on today's code.
3. **W>1.** None of the shipped changes has been measured on the parallel path.
   - Check that A, G and B2 are actually selected when W>1 (typed parallel
     compile).
   - Measure them at W=4/8/14 against Feldera's multi-core numbers.
4. **ivm-bench.** It runs the structural program path, where G does not apply,
   so check what A, B and B2 do there. See the memory notes on ivm-bench.
5. Loose ends, by size of prize:
   - G falls back to the old operator for a GROUP BY containing SUM over a
     nullable column.
   - A's plan duplicates partition/order columns, costing about 24 B/row.
   - Decide whether C or D is worth keeping as an opt-in memory mode, or retire
     the branches.
   - Clean up `.claude/worktrees/agent-*` and the stale `exp/state-*` branches
     once decided.

Use parallel agents where the work is independent, using only the deterministic
metrics while they run. Do the serialized timing yourself. Before launching
agents, propose the plan and the agent count.
