#! /usr/local/bin/python3

import os
import sys
import csv
from collections import defaultdict
import pickle

if len(sys.argv) != 4:
    exit('Usage: rank_vps_destination_cover_single_dnet.py <dnet> <dsts_by_dnet_file> <train_dir>')

target_dnet = sys.argv[1]
dsts_by_dnet_file = sys.argv[2]
train_dir  = sys.argv[3]

with open(dsts_by_dnet_file, 'rb') as f:
    dsts_by_dnet = pickle.load(f)

dsts_by_vp_by_dnet = defaultdict(lambda: defaultdict(set))
for vp_csv in os.listdir(train_dir):
    vp = vp_csv.replace('.csv', '')
    with open(os.path.join(train_dir, vp_csv), 'r') as f:
        for row in csv.reader(f):
            dnet = row[0]
            dst  = row[1]
            hops = row[2:]
            if dst in hops:
                dsts_by_vp_by_dnet[dnet][vp].add(dst)

remaining_dsts = dsts_by_dnet[target_dnet]
vp_ranking = []
while(len(remaining_dsts) > 0):

    max_coverage = 0
    chosen_vp = ''
    chosen_dsts = set()
    for vp, covered_dsts in dsts_by_vp_by_dnet[target_dnet].copy().items():

        this_coverage = len(covered_dsts & remaining_dsts)
        print("vp {} - coverage {}".format(vp, this_coverage))
        if this_coverage == 0:
            del dsts_by_vp_by_dnet[target_dnet][vp]
            continue

        if this_coverage > max_coverage:
            chosen_vp = vp
            chosen_dsts = covered_dsts
            max_coverage = this_coverage

    print("Choosing VP {} - {} additional destinations covered".format(chosen_vp, len(chosen_dsts)))
    vp_ranking.append(chosen_vp)
    remaining_dsts -= chosen_dsts

    del dsts_by_vp_by_dnet[target_dnet][chosen_vp]

print('{},{}'.format(dnet, ','.join(vp_ranking)))
