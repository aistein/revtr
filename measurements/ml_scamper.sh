#! /bin/bash

# For running scamper commands in parallel on planetlab nodes.

# ml_scamper.sh first sets up the nodes with a working directory "alex" for
# input and output files.  Next, it sends the list of destinations to each node
# and shuffles them.  Finally, it runs scamper, and waits for the results.

# Args:
# 1 probe command to use with scamper in quotes (e.g. "ping -R -c 1")
# 2 scamper rate limit 
# 3 file containing list of nodes
# 4 file containing list of destinations
# 5 time (in seconds) that we must sleep before collecting results
# 6 output directory

if [ $# -ne 4 ]; then
   echo Usage: ml_scamper.sh scamper_command rate_limit dst_file\
   out_dir
   echo $# "arguments given"
   exit 1
fi

if [ -z "${username}" ] || [ -z "${password}" ]; then
    echo "error: planetlab username and password have not been exported."
    exit 1
fi

probe_command=$1
rate_limit=$2
local_dsts=$3
out_dir=$4

location=$(pwd)

# update planetlab vps
sliceinfo_dir="$location/sliceinfo/`date +%Y%m%d`"
if [ ! -d $sliceinfo_dir ]; then
   mkdir -p $sliceinfo_dir
   $location/get_ml_slice_info.py $username $password $sliceinfo_dir
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
#nodes="${sliceinfo_dir}/candidate_nodes.txt"
nodes="$location/dummy_nodes.txt"
remote_dsts="/home/uw_geoloc4/alex/in/dsts.txt"
remote_warts="out/\${HOSTNAME}.warts"

# create my directory on each node
setup_command="rm -rf alex && mkdir -p alex/in && mkdir -p alex/out"
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
   sudo nohup /home/uw_geoloc4/plvp/scamper -c \"$probe_command\"\
   -f in/dsts.txt -p $rate_limit -O warts -o $remote_warts &"
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

mkdir -p $out_dir
for i in {1..2}; do
    for n in $(cat $nodes); do
        rsync -avz -e "ssh -p 806 -i /home/ubuntu/.ssh/planetlab_id_rsa"\
            uw_geoloc4@$n:alex/$remote_warts $out_dir
    done
done

# double rsync better work, because otherwise we're nuking everything
printf "\nCleaning Up\n"
cleanup_command="rm -r alex"
parallel-ssh -h $nodes -l uw_geoloc4 -x "-p 806" -x "-i /home/ubuntu/.ssh/planetlab_id_rsa" $cleanup_command