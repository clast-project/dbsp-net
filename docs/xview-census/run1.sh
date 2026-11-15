#!/bin/bash
# run1.sh ARM NAME — one IvmMultiBatch run (SF=3, W=12, as the ivm-bench server: incremental output,
# broadcast <=1000, size input order, server GC, background GC off). Output -> ~/tpcdi-ab/runs/NAME.txt
ARM=$1; NAME=$2
export DOTNET_ROOT=$HOME/.dotnet PATH=$HOME/.dotnet:$PATH
export DOTNET_gcServer=1 DOTNET_gcConcurrent=0
export IVM_DATA_ROOT=$HOME/src/ivm-bench/mount/raw/3/delta
export IVM_STAGING_ROOT=$HOME/src/ivm-bench/mount/raw/3/delta/staging-multi
export IVM_SPEC=$HOME/tpcdi-ab/spec-main.json
export IVM_OUTPUT_MODE=incremental IVM_WORKERS=12 IVM_BCAST_MAXROWS=1000
export IVM_OUT_ROOT=$HOME/tpcdi-ab/out
mkdir -p ~/tpcdi-ab/runs
dotnet test ~/tpcdi-ab/bin-$ARM/DbspNet.Tests.dll --filter "FullyQualifiedName~IvmMultiBatch.RunThreeBatches_Local" \
  --logger "console;verbosity=detailed" > ~/tpcdi-ab/runs/$NAME.txt 2>&1
echo "exit $?" >> ~/tpcdi-ab/runs/$NAME.txt
