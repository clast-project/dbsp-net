#!/bin/bash
# xphase.sh ARM NAME [DETAIL] — prof1.sh with the exchange phase table; DETAIL=1 also times the fused map
# and the gather key per row (DBSPNET_PROFILE_XPHASE). Profile -> ~/tpcdi-ab/runs/NAME.prof
ARM=$1; NAME=$2
export DOTNET_GCDynamicAdaptationMode=0
export DBSPNET_PROFILE=1 DBSPNET_PROFILE_TOP=100000
export DBSPNET_PROFILE_XPHASE=${3:-0}
export DBSPNET_PROFILE_FILE=$HOME/tpcdi-ab/runs/$NAME.prof
rm -f $DBSPNET_PROFILE_FILE
exec ~/tpcdi-ab/run1.sh $ARM $NAME
