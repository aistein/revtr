#! /bin/bash

#wartsdir="collections/7_23_2019-survey-measurements/ping"
wartsdir="collections/7_23_2019-survey-measurements/rr"

for wartpath in $(ls -1 $wartsdir/*.warts); do
    wart=$(basename -- $wartpath)
    echo "processing warts file $wart..."
    sc_warts2json "$wartsdir/$wart" > "$wartsdir/json/${wart%.warts}.json"
done
