#!/bin/sh
# rate.sh N [VAR=value...]: hangs of the 4-domain stress probe over N fresh
# processes. Logs of hung runs, and their event logs, go to ./hangs.
exe=${STRESS_EXE:-_build/default/affect/repro/caravan/stress.exe}
n=$1; shift
f=0; i=0
mkdir -p hangs
while [ $i -lt $n ]; do
  i=$((i+1))
  out=hangs/run.$$.$i
  env ENGINE=old DBG_OUT=$out.dbg "$@" timeout -s KILL 30 $exe 1 4 > $out 2>&1
  if [ $? -ne 0 ]; then f=$((f+1)); else rm -f $out; fi
done
echo "$*: $f/$n hangs"
