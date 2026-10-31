#!/usr/bin/env bash
# VARCHAR ingest decoding on ivm-bench SF=3 (docs/design-connectors.md, G5): Transcode (today)
# vs Copy (a byte[] per cell) vs Arena (one copy per column block). Runs IvmMultiBatch per arm,
# alternating, one run at a time, and records per batch the drain time and allocation, plus the
# live heap after batch 1's drain (IVM_MEASURE_AT=drain, so output views don't pollute it).
#
#   scripts/measure-ingest-strings.sh                    # 5 rounds, W=12
#   RUNS=1 ARMS="copy" scripts/measure-ingest-strings.sh
#
# Needs the SF=3 data under ~/src/ivm-bench/mount/raw/3/delta incl. staging-multi/. No Docker.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
DATA="${IVM_DATA_ROOT:-$HOME/src/ivm-bench/mount/raw/3/delta}"
SPEC="${IVM_SPEC:-$HOME/src/ivm-bench/mount/ivm_spec_sf3.json}"
WORK="${WORK:-$HOME/ingest-strings-measure}"
RUNS="${RUNS:-5}"
ARMS="${ARMS:-transcode copy arena}"
WORKERS="${WORKERS:-12}"
DOTNET="${DOTNET:-$HOME/.dotnet/dotnet}"

[[ -d "$DATA/staging-multi" ]] || { echo "missing $DATA/staging-multi" >&2; exit 1; }
[[ -f "$SPEC" ]] || { echo "missing spec $SPEC" >&2; exit 1; }
if git -C "$REPO" ls-files | grep -q ' [0-9]\.'; then
  echo "iCloud conflict copies are tracked; fix before measuring" >&2; exit 1
fi

mkdir -p "$WORK"
RESULTS="$WORK/results-$(date +%Y%m%d-%H%M%S).txt"
echo "# $(git -C "$REPO" rev-parse --short HEAD) $(date) W=$WORKERS" | tee "$RESULTS"
"$DOTNET" build "$REPO/tests/DbspNet.Tests" -c Release 2>&1 | grep -E " error |Build succeeded" | sort -u

for run in $(seq 1 "$RUNS"); do
  for arm in $ARMS; do
    echo "## run $run arm $arm" | tee -a "$RESULTS"
    DOTNET_gcServer=1 \
    DBSPNET_INGEST_STRINGS="$arm" \
    IVM_MEASURE_AT=drain \
    IVM_MEASURE_LIVE=1 \
    IVM_DATA_ROOT="$DATA" \
    IVM_SPEC="$SPEC" \
    IVM_STAGING_ROOT="$DATA/staging-multi" \
    IVM_OUTPUT_MODE=incremental \
    IVM_OUT_ROOT="$WORK/out" \
    IVM_WORKERS="$WORKERS" \
      "$DOTNET" test "$REPO/tests/DbspNet.Tests" -c Release --no-build \
        --filter 'FullyQualifiedName~IvmMultiBatch.RunThreeBatches_Local' \
        --logger 'console;verbosity=detailed' 2>&1 \
      | grep -E '^\s*(RESULT|LIVE)|Failed|error' | sed 's/^ *//' | tee -a "$RESULTS"
  done
done

rm -rf "$WORK/out"
echo "results: $RESULTS"
