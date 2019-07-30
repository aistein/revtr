#! /bin/bash

# For running scamper commands in parallel on planetlab nodes.

# ml_scamper.sh first sets up the nodes with a working directory "alex" for
# input and output files.  Next, it sends the list of destinations to each node
# and shuffles them.  Finally, it runs scamper, and waits for the results.

# Args:
# 1 file containing list of destinations
# 2 output directory

if [ $# -ne 2 ]; then
   echo Usage: ml_scamper.sh dst_file out_dir
   echo $# "arguments given"
   exit 1
fi

# RR-enabled probe command
rr_probe_command="ping -R -c 1"

# RR-disabled probe command
ping_probe_command="ping -c 1"

rate_limit=20
local_dsts=$1
out_dir=$2

location=$(pwd)

# update planetlab vps
sliceinfo_dir="$location/sliceinfo/`date +%Y%m%d`"
if [ ! -d $sliceinfo_dir ]; then
   mkdir -p $sliceinfo_dir
   $location/get_ml_slice_info.py $sliceinfo_dir
   echo Getting active nodes...
   $location/get_ml_active_nodes.sh $sliceinfo_dir/uw_geoloc4_all_hostnames.txt \
      $sliceinfo_dir
   echo done
   echo Choosing site candidates...
   $location/choose_candidates.py $sliceinfo_dir/uw_geoloc4_hostnames_site_ids.txt \
      $sliceinfo_dir/uw_geoloc4_active_nodes.txt > \
      $sliceinfo_dir/candidate_nodes.txt
   echo done
fi


# list of all nodes/ directory for remote destinations/ filename for remote
# output
nodes="${sliceinfo_dir}/candidate_nodes.txt"
#nodes="$location/dummy_nodes.txt"
remote_dsts="/home/uw_geoloc4/alex/in/dsts.txt"
remote_rr_warts="out/rr/\${HOSTNAME}.warts"
remote_ping_warts="out/ping/\${HOSTNAME}.warts"

# create my directory on each node
setup_command="rm -rf alex && mkdir -p alex/in && mkdir -p alex/out/rr && mkdir -p alex/out/ping"
echo Initializing nodes...
parallel-ssh -h $nodes -l uw_geoloc4 -x "-p 806" -x "-i /home/ubuntu/.ssh/planetlab_id_rsa" $setup_command

# shuffle the destinations on each node
shuffle_command="shuf $remote_dsts > tmp && mv tmp $remote_dsts"
printf "\nSending destinations...\n"
parallel-rsync -x "-avz -e \"ssh -p 806 -i /home/ubuntu/.ssh/planetlab_id_rsa\"" -h $nodes -l uw_geoloc4 -t 1800 \
   $local_dsts $remote_dsts
printf "\nShuffling destinations...\n"
parallel-ssh -h $nodes -l uw_geoloc4 -x "-p 806" -x "-i /home/ubuntu/.ssh/planetlab_id_rsa" -t 300 $shuffle_command

# run scamper on all nodes
scamper_command="cd alex;\
   sudo nohup /home/uw_geoloc4/plvp/scamper -c \"$ping_probe_command\"\
   -f in/dsts.txt -p $rate_limit -O warts -o $remote_ping_warts & \
   sudo nohup /home/uw_geoloc4/plvp/scamper -c \"$rr_probe_command\"\
   -f in/dsts.txt -p $rate_limit -O warts -o $remote_rr_warts &"

printf "\nExecuting scamper...\n"
parallel-ssh -h $nodes -l uw_geoloc4 -x "-p 806" -x "-i /home/ubuntu/.ssh/planetlab_id_rsa" $scamper_command

# calculate how long this should take and sleep for 90 seconds longer thanthat
dst_count=`wc -l $local_dsts | sed "s/ .*//g"`
sleep_time=`expr $dst_count / $rate_limit + 10` # + 90`
printf "\nSleeping for $sleep_time seconds...\n"
sleep $sleep_time

# Collect results
# Since we can't send the PSSH_HOST env. variable to pl nodes, we need some
# other way of knowing where the files come from.  For this, we will just cat
# the results and save the output to a directory.  Each file will be saved under
# the name of its node. Doesn't work.... cat fails sometimes. Maybe pipe breaks?
# cat_command="cat /home/uw_geoloc4/alex/out/warts.out"
# parallel-ssh -h $nodes -l uw_geoloc4 -x "-p 806" -t 0 -o $out_dir $cat_command

# instead, fetch sequentially, using rsync.  Go over everything twice just in
# case
printf "\nCollecting results...\n"

mkdir -p $out_dir/rr
mkdir -p $out_dir/ping
for i in {1..2}; do
    for n in $(cat $nodes); do
        rsync -avz -e "ssh -p 806 -i /home/ubuntu/.ssh/planetlab_id_rsa"\
            uw_geoloc4@$n:alex/$remote_rr_warts $out_dir/rr
        rsync -avz -e "ssh -p 806 -i /home/ubuntu/.ssh/planetlab_id_rsa"\
            uw_geoloc4@$n:alex/$remote_ping_warts $out_dir/ping
    done
done

# double rsync better work, because otherwise we're nuking everything
printf "\nCleaning Up\n"
cleanup_command="rm -r alex"
parallel-ssh -h $nodes -l uw_geoloc4 -x "-p 806" -x "-i /home/ubuntu/.ssh/planetlab_id_rsa" $cleanup_command
