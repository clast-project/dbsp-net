---
name: engineered-wood-path-in-schema
description: "engineered-wood parquet writer omitted the required path_in_schema field by default → Arrow/DuckDB/pyarrow reject with 'TProtocolException: Invalid data'; fixed by flipping OmitPathInSchema default to false"
metadata:
  type: project
  originSessionId: d04e1240-137c-49b7-b437-e6290184b5c5
---

**ROOT CAUSE of "Couldn't deserialize thrift: TProtocolException: Invalid data"**
when DuckDB/pyarrow read DbspNet's Delta/parquet output (the [[ivm-bench-arc]] output-
validation blocker). NOT a footer-encoding bug, NOT the cmettler fork's VariantType change.

**The bug:** `EngineeredWood.Parquet/Parquet/ParquetWriteOptions.cs` had
`public bool OmitPathInSchema { get; init; } = true;` — a **bad default** that omits
`ColumnMetaData.path_in_schema` (thrift **field 3, a required list<string>**). The footer is
still *structurally* valid thrift-compact (a missing field is legal), so generic walkers accept
it — but Arrow/DuckDB/pyarrow use thrift-generated readers that enforce required-field
validation, so a missing path_in_schema → bare `INVALID_DATA` → "Invalid data". Present in BOTH
Curt's original (09ce9c1) AND the cmettler fork. DbspNet writes via `DeltaTable.WriteAsync` with
no options → inherited the default. engineered-wood's OWN tests already knew "ParquetSharp
requires path_in_schema" and set `OmitPathInSchema=false` for every external cross-validation —
yet the default stayed true.

**The fix AS SHIPPED (dbsp-net main `d854971`, pushed; ivm-bench `dbspnet-engine` `472568e`
bumps DBSPNET_COMMIT→d854971):** chosen approach was the **DbspNet-side override**, NOT flipping
engineered-wood's default (avoids an engineered-wood push/pin dance). `DeltaOutputConnector` now
passes `DeltaTableOptions { ParquetWriteOptions = ParquetWriteOptions.Default with {
OmitPathInSchema = false } }` to `OpenOrCreateAsync` (DeltaTable threads `_options.ParquetWriteOptions`
to every `new ParquetFileWriter(...)`). Works against the ALREADY-PINNED engineered-wood (both
`DeltaTableOptions.ParquetWriteOptions` and `ParquetWriteOptions.OmitPathInSchema` exist there;
we only override the value). ALSO **reverted the submodule off the cmettler fork back to
CurtHagenlocher/engineered-wood @ 09ce9c1** (fork changes were unrelated) — `.gitmodules` URL +
gitlink both reverted; `git submodule sync` re-added `origin`=CurtHagenlocher. **SUPERSEDED
2026-07-18:** submodule switched BACK to the cmettler fork @ 13fead6 (dbsp-net main `b99d66a`) to
A/B-test the [[ivm-bench-validation-findings]] #5 snappy read error — see that memory for the plan
(cherry-pick the real fix into the original + revert if it works). So the current pin is the FORK,
not the original, until that test resolves. Verified end-to-end:
the connector's output (via real `DeltaOutputConnector` against original eng-wood) reads cleanly in
BOTH pyarrow AND DuckDB read_parquet, `path_in_schema` present on every column chunk; 6 connector
round-trip tests green. **Fixing engineered-wood's `OmitPathInSchema` default to false upstream is
still a good follow-up** (a bad default), but deferred — the override makes DbspNet correct
regardless. (An earlier throwaway proof flipped the default in the fork checkout + verified; that
edit was discarded on revert.)

**Diagnosis method (reusable):** the footer passed two generic thrift-compact walkers + a
64-bit-varint-strict walker (all consumed exactly to top STOP). fastparquet (independent pure-
python thrift) parsed the whole footer then died at `column.meta_data.path_in_schema is None` —
that pinpointed it. Confirmed by decoding each ColumnMetaData's field set (all 10 chunks:
[1,2,4,5,6,7,9,12], field 3 absent). PROVED it was the sole blocker by generically re-encoding the
real output file's footer with path_in_schema=[colname] injected (data pages untouched, offsets
absolute) → pyarrow then read all 11057 rows correctly. Scripts in the session scratchpad.

**REMAINING (user-driven, as of 2026-07-17):** code is committed+pushed. User must rebuild the
dbspnet image (`docker compose build dbspnet` / the harness's build step picks up DBSPNET_COMMIT=
d854971), re-run the SF=3 harness with `PRESERVE_RESULTS=1`, then run `compare_outputs.py`
(src/.scripts/) — which should now read DbspNet's output via pyarrow (and DuckDB) and produce the
DbspNet-vs-Feldera EXCEPT-ALL diff. compare_outputs.py could even be simplified back to DuckDB
`delta_scan` now that footers are spec-compliant, but the pyarrow+_delta_log-replay path already
works. NOTE the earlier worry that Curt's original engineered-wood might lack the fork's writer
fixes (all-null pages / null-struct alignment) is UNVERIFIED against our gold schemas — if the
re-run shows data diffs or a write failure, that's the place to look (cherry-pick specific fork
fixes to Curt's engineered-wood properly).
Related: [[ivm-bench-arc]], [[connector-framework]]
