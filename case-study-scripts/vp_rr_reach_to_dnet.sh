#!/bin/bash
dnet=$1
vp=$2
dnet_type=$3
grep "$dnet" 12_17_2018/reachables/train/vp_measurements/"$dnet_type"/"$vp".csv | wc -l
