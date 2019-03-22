#! /bin/bash

## Generates active planetlab nodes from a list of possible nodes

if [ "$#" -lt 2 ]; then
   echo "Usage: $0 node_list_file out_dir"
   exit 1
fi

node_file=$1
out_dir="$2"
mkdir -p $out_dir/tmp

parallel-ssh -h $node_file -l "uw_geoloc4" -x "-p 806" -x "-i ~/.ssh/planetlab_id_rsa" -X "-o StrictHostKeyChecking no" -t 3 -o $out_dir/tmp "ls" 

out_file="uw_geoloc4_active_nodes.txt"
cd $out_dir/tmp
touch $out_file
for f in *; do
   if grep "plvp" $f; then
      echo $f >> $out_file
   fi
done >/dev/null

# cleanup
mv $out_file ..
cd ..
rm -r tmp
