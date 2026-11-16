#!/bin/bash
for spec in "x1 0" "x2 0" "x3 1" "x4 0" "x5 0" "x6 1" "x7 0" "x8 0"; do
  set -- $spec; ~/tpcdi-ab/xphase.sh b84c7ae $1 $2; sleep 10
done
echo DONE > ~/tpcdi-ab/runs/xloop.done
