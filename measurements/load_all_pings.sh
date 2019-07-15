#! /bin/bash

csv_path="/data/workspace/measurements/collections/6_25_2019-survey-measurements/csv"
pids=()
num_processes=0
max_processes=10

# function to wait for a bunch of jobs to finish
#synchronization_barrier () {
#    echo "waiting for jobs to finish..."
#    arr=("$@")
#    for pid in "${arr[@]}"; do
#        wait "$pid"
#    done
#}

for csvfile in $(ls -1 "$csv_path"); do

    # deploy job
    echo "loading probes from csv file $csvfile into database..."
    #./load_csv_pings_into_database.py "${csvfile}" &> logs/"${csvfile%.csv}_loading.log" & pids+=("$!")
    ./load_csv_pings_into_database.py "${csvfile}" &> logs/"${csvfile%.csv}_loading.log"

    ## wait for _max_ jobs to complete before deploying more
    #num_processes=$(($num_processes+1))
    #if [ $num_processes == $max_processes ]; then
    #    synchronization_barrier "${pids[@]}"
    #    num_processes=0
    #fi

done # end for

# wait for remaining jobs to finish
#synchronization_barrier "${pids[@]}"

echo "done."
