#!/bin/bash
# prof1.sh ARM NAME — run1.sh plus DBSPNET_PROFILE (per-op times, every dim_customer op) and DATAS off.
# Profile -> ~/tpcdi-ab/runs/NAME.prof (appends one report per batch)
ARM=$1; NAME=$2
export DOTNET_GCDynamicAdaptationMode=0
export DBSPNET_PROFILE=1 DBSPNET_PROFILE_TOP=100000 DBSPNET_PROFILE_VIEW=dim_customer
export DBSPNET_PROFILE_FILE=$HOME/tpcdi-ab/runs/$NAME.prof
rm -f $DBSPNET_PROFILE_FILE
exec ~/tpcdi-ab/run1.sh $ARM $NAME
