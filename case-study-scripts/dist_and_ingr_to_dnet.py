#! /usr/bin/python3

# what's it do?
# - Find the ingress into the target dnet of the given probe, and the distance to that ingress 
# - Output goes to stdout

import pyasn
import sys
import os
import csv
import numpy as np
from collections import defaultdict

try:
    target_dnet = sys.argv[1]
    probe_file = sys.argv[2]
    ipasn_file = sys.argv[4]
    if sys.argv[3] == 'asn':
        dnet_type = 0
    if sys.argv[3] == 'bgp':
        dnet_type = 1
except (IndexError, ValueError):
    exit('Usage: dist_and_ingr_to_dnet.py <target_dnet> <rr_probe_file>.csv [dnet_type: asn/bgp] <ipasn_file>.dat')

ipasn = pyasn.pyasn(ipasn_file)
vp_name = probe_file.strip().split('/')[-1]

ingrs = defaultdict(list)
out_file = "dists_to_" + target_dnet + "_from_" + ".".join(vp_name.split('.')[:-2])
with open(probe_file, 'r') as f, open(out_file, 'w') as g:
    for row in csv.reader(f):
        if row[0] == target_dnet:
            dnet = int(row[0])
            dest = row[1]
            hops = row[2:]
            hops_as_dnet = [ipasn.lookup(hop)[dnet_type] for hop in hops]
            if dnet in hops_as_dnet:
                dist = hops_as_dnet.index(dnet) + 1
                ingr = hops[dist-1]
                ingrs[ingr].append(dist)
                #print('{},{}'.format(ingr, dist))
                g.write('{},{},{},{}\n'.format( dest, ingr, dist, ",".join(hops)))
            else:
                dist = 10
                ingr = '-'
                #print('{},{}'.format(ingr, dist))

print("Ingresses into {} from VP {}:".format(target_dnet, vp_name))
[print("ingr {}, avg_dist {}, num_meas {}".format(ingr, np.mean(dists), len(dists)))
        for ingr,dists in ingrs.items()]
