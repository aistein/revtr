#!/bin/bash
dnet=$1
vp=$2
measurements=($(grep "$dnet"  12_17_2018/reachables/test/vp_measurements/asn/"$vp".csv | sort --field-separator=',' --key=3))
echo "${measurements[0]}" | awk 'BEGIN { FS = "," } ; { print $3 }'
