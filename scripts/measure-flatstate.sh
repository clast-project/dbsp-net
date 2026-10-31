#!/usr/bin/env bash
# The flat-state probe matrix (docs/design-flat-state-probe.md): Nexmark q20 with the algorithm
# fixed and the memory model varied. One process per run, one run at a time, arms alternating
# inside each round so drift on the box lands on every arm alike.
#
#   scripts/measure-flatstate.sh                       # 5 rounds of the full matrix
#   RUNS=1 SIZES=1000000 scripts/measure-flatstate.sh  # a quick pass
#
# Arms: engine (q20 through SQL, typed path), managed (objects + Dictionary), poh (flat pages on
# the pinned-object heap), native (flat pages in NativeMemory). The flat and managed arms also
# checkpoint and restore their state after the run (native adds a memory-mapped restore).
# No Docker. Needs ~10 GiB free under $WORK for the checkpoints.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
WORK="${WORK:-$HOME/flatstate-measure}"
RUNS="${RUNS:-5}"
ARMS="${ARMS:-engine managed poh native}"
WORKERS="${WORKERS:-1 8}"
SIZES="${SIZES:-1000000 10000000}"
RETRACT="${RETRACT:-0 1}"
DOTNET="${DOTNET:-$HOME/.dotnet/dotnet}"
export DOTNET_ROOT="${DOTNET_ROOT:-$HOME/.dotnet}"

if git -C "$REPO" ls-files | grep -q ' [0-9]\.'; then
  echo "iCloud conflict copies are tracked; fix before measuring" >&2; exit 1
fi

mkdir -p "$WORK/ckpt"
RESULTS="$WORK/results-$(date +%Y%m%d-%H%M%S).txt"
echo "# $(git -C "$REPO" rev-parse --short HEAD) $(date) DOTNET_gcServer=1 DOTNET_GCDynamicAdaptationMode=0" | tee "$RESULTS"
"$DOTNET" build "$REPO/src/DbspNet.Benchmarks" -c Release 2>&1 | grep -E " error |Build succeeded" | sort -u
BIN="$REPO/src/DbspNet.Benchmarks/bin/Release/net10.0/DbspNet.Benchmarks"

for run in $(seq 1 "$RUNS"); do
  for size in $SIZES; do
    for retract in $RETRACT; do
      for w in $WORKERS; do
        for arm in $ARMS; do
          ckpt="$WORK/ckpt"
          [[ "$arm" == engine ]] && ckpt=""
          line=$(DOTNET_gcServer=1 DOTNET_GCDynamicAdaptationMode=0 \
            "$BIN" flatstate "$arm" "$w" "$size" "$retract" 10000 5 $ckpt 2>&1 | grep -E '^RESULT|Exception' || true)
          echo "run=$run $line" | tee -a "$RESULTS"
        done
      done
    done
  done
done

echo "results: $RESULTS"
