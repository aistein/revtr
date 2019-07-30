#! /bin/bash

survey_path="/data/workspace/measurements/survey/internet_address_survey_reprobing_it84w-20190130/data"
csv_path="/data/workspace/measurements/survey/internet_address_survey_reprobing_it84w-20190130/data/csv"
txt_path="/data/workspace/measurements/survey/internet_address_survey_reprobing_it84w-20190130/data/txt"
pids=()
num_processes=0
max_processes=14

# function to wait for a bunch of jobs to finish
synchronization_barrier () {
    arr=("$@")
    for pid in "${arr[@]}"; do
        wait "$pid"
    done
}

#for bzfile in $(ls -1 "$survey_path"); do
for i in $(seq 1 306); do

    # get filename
    if [ $i -lt 10 ]; then
        bzfile="reprobing.pinger-w2.it84w.00${i}.bz2"
    elif [ $i -lt 100 ]; then
        bzfile="reprobing.pinger-w2.it84w.0${i}.bz2"
    else
        bzfile="reprobing.pinger-w2.it84w.${i}.bz2"
    fi

    # deploy job
    echo "loading binary file $bzfile into database..."
    #print_datafile -f -j "${survey_path}/${bzfile}" | ./probes_txt_to_csv.py &> ${csv_path}/${bzfile%.bz2}.csv & pids+=("$!")
    print_datafile -f -j "${survey_path}/${bzfile}" | ./probes_bz2-stream_to_txt.py &> ${txt_path}/${bzfile%.bz2}.txt & pids+=("$!")

    # wait for _max_ jobs to complete before deploying more
    num_processes=$(($num_processes+1))
    if [ $num_processes == $max_processes ]; then
        synchronization_barrier "${pids[@]}"
        num_processes=0
    fi

done # end for

echo "done."
