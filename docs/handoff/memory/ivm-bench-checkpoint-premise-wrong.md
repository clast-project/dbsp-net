---
name: ivm-bench-checkpoint-premise-wrong
description: "We ran ivm-bench with our per-batch checkpoint on \"for honesty\" — Feldera writes no checkpoint at all during those runs"
metadata: 
  node_type: memory
  type: project
  originSessionId: 76b7e6c9-eb62-4996-8dd2-e02c5fd00969
  modified: 2026-08-31T02:18:29.089Z
---

`design-incremental-persistence.md` §0 claimed ivm-bench measures Feldera "with persistence inside the
batch window (`transaction_mode: always`)". **Wrong**, verified on both sides 2026-08-30:

- `transaction_mode: always` is a per-source **Delta connector** option, not durability. No
  `fault_tolerance` key exists anywhere in ivm-bench → `checkpoint_interval()` is `None`.
- A checkpoint cannot start mid-transaction (`controller.rs:9480`); Feldera has a FIXME about exactly
  that.
- Spilled files carry `DeleteOnDrop{keep:false}`; only `Spine::save` flips `keep`. No checkpoint ⇒
  **recoverable Feldera state at end of an ivm-bench batch is zero**.

Their batch-window I/O is spill I/O with no recovery value; ours is snapshot I/O buying a recovery
point. Turning ours on cost ~18.7 s/batch the other side never paid.

Also wrong: `feldera_client.py:185-189` attributes the ~47k-operator commit walk to persisting state.
It is the **DAG being evaluated** (`schedule.rs:219-226`) — under `transaction_mode: always` ingest
does almost nothing and commit does all view computation. Maps to our *step*, not our checkpoint.

**Why:** it inflated our ivm-bench numbers and it was the stated urgency behind Track A / A2.

**How to apply:** turn the per-batch checkpoint off for ivm-bench runs and fix that comment before
quoting any batch timing. Re-price A2 before building it — see [[feldera-source-comparison]],
[[ivm-bench-arc]]. New axis found: Feldera accumulates then evaluates ONE coalesced delta; we evaluate
the sequence, so part of the 3.5× batch-1 gap may be algorithmic.
