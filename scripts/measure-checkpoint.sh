#!/usr/bin/env bash
# Checkpoint + restore wall time on ivm-bench SF=3, serial vs parallel
# (docs/design-durable-state.md §1b). Runs IvmMultiBatch with IVM_SNAPSHOT_DIR set: three
# batches, a checkpoint after each (checkpoint_ms), then a restore of the last one into a fresh
# program (restore_ms) with a view-row-count check. Arms alternate, one run at a time.
#
#   scripts/measure-checkpoint.sh                 # 5 runs each of W=1 and W=12
#   RUNS=3 ARMS="12 12:nofsync" scripts/measure-checkpoint.sh
#
# An arm is a worker count, optionally ":nofsync" (DBSPNET_SNAPSHOT_FSYNC=0) to price the fsyncs
# and/or ":gcbatch" (DBSPNET_CHECKPOINT_GC=batch: no background GC during checkpoint/restore).
# Needs the SF=3 data under ~/src/ivm-bench/mount/raw/3/delta, including staging-multi/
# (see the ivm-bench-local-datagen notes), and ~10 GiB free for the snapshots.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
DATA="${IVM_DATA_ROOT:-$HOME/src/ivm-bench/mount/raw/3/delta}"
SPEC="${IVM_SPEC:-$HOME/src/ivm-bench/mount/ivm_spec_sf3.json}"
WORK="${WORK:-$HOME/ivm-checkpoint-measure}"
RUNS="${RUNS:-5}"
ARMS="${ARMS:-1 12}"
DOTNET="${DOTNET:-$HOME/.dotnet/dotnet}"

[[ -d "$DATA/staging-multi" ]] || { echo "missing $DATA/staging-multi" >&2; exit 1; }
[[ -f "$SPEC" ]] || { echo "missing spec $SPEC" >&2; exit 1; }
if git -C "$REPO" ls-files | grep -q ' [0-9]\.'; then
  echo "iCloud conflict copies are tracked; fix before measuring" >&2; exit 1
fi

mkdir -p "$WORK"
RESULTS="$WORK/results-$(date +%Y%m%d-%H%M%S).txt"
echo "# $(git -C "$REPO" rev-parse --short HEAD) $(date)" | tee "$RESULTS"

"$DOTNET" build "$REPO/tests/DbspNet.Tests" -c Release 2>&1 | grep -E " error |Build succeeded" | sort -u

for run in $(seq 1 "$RUNS"); do
  for arm in $ARMS; do
    workers="${arm%%:*}"
    fsync=1
    [[ "$arm" == *:nofsync* ]] && fsync=0
    gcmode=default
    [[ "$arm" == *:gcbatch* ]] && gcmode=batch
    echo "## run $run arm $arm" | tee -a "$RESULTS"
    DOTNET_gcServer=1 \
    DBSPNET_SNAPSHOT_FSYNC="$fsync" \
    DBSPNET_CHECKPOINT_GC="$gcmode" \
    IVM_DATA_ROOT="$DATA" \
    IVM_SPEC="$SPEC" \
    IVM_STAGING_ROOT="$DATA/staging-multi" \
    IVM_OUTPUT_MODE=incremental \
    IVM_OUT_ROOT="$WORK/out" \
    IVM_SNAPSHOT_DIR="$WORK/snap" \
    IVM_WORKERS="$workers" \
      "$DOTNET" test "$REPO/tests/DbspNet.Tests" -c Release --no-build \
        --filter 'FullyQualifiedName~IvmMultiBatch.RunThreeBatches_Local' \
        --logger 'console;verbosity=detailed' 2>&1 \
      | grep -E '^\s*(RESULT|RESTORE)|Failed|error' | sed 's/^ *//' | tee -a "$RESULTS"
  done
done

rm -rf "$WORK/snap" "$WORK/out"
echo "results: $RESULTS"
