#! /bin/bash

if [ $# -lt 2 ]; then
    echo "usage: ./load_all_pings.sh <csv-path> <rr/ping>"
    exit 1
fi
csv_path=$1
mode=$2

for csvfile in $(ls -1 "$csv_path"); do

    echo "loading probes from csv file $csvfile into database..."
    ./load_csv_pings_into_database.py "${csv_path}" "${csvfile}" "${mode}"&> logs/"${csvfile%.csv}-${mode}_loading.log"

done # end for

echo "done."
