#!/usr/local/bin/python3

"""
Do best VPs traverse same logical ingresses as returned by ingr-ranking...
- in their test measurements to the target destination?
- in all their training measurements to the network containing the target?
"""
import os
import sys
import csv
from collections import defaultdict

try:
    target_ip = sys.argv[1]
    dnet_type = sys.argv[2]
    target_dnet = sys.argv[3]
    source_dir = sys.argv[4]
except:
    exit("usage: ./grade_ingress_ranking.py <target-ip> <target-dnet> <source_dir>")

best_vps_file = os.path.join(source_dir,"best_vps-{}".format(dnet_type))
ips_by_ingr_file = os.path.join(source_dir,"logical_ingr_ips-{}".format(dnet_type))
test_measurements = os.path.join(source_dir, "test_measurements-{}".format(dnet_type))
train_measurements = os.path.join(source_dir, "train_measurements-{}".format(dnet_type))

ips_by_ingr = []
with open(ips_by_ingr_file, 'r') as igf:
    r = csv.reader(igf, delimiter=',')
    for ingr in r:
        print("Ingr {} has {} IPs".format(ingr[0], len(ingr[1:])))
        ips_by_ingr.append(set(ingr[1:]))

ideal_vps = []
with open(best_vps_file, 'r') as bvf:
    r = csv.reader(bvf, delimiter=',')
    for row in r:
         ideal_vps.append(row[1])

test_meas_by_ideal_vp = defaultdict(list)
with open(test_measurements, 'r') as tmf:
    r = csv.reader(tmf, delimiter=',')
    for row in r:
        vp = row[0].split('/')[-1].split(':')[0]
        if vp in ideal_vps:
            test_meas_by_ideal_vp[vp].append(row[3:]) 

print("Checking if Best VPs all Traverse Logical Ingresses returned by ranking for {} in testing".format(
    target_ip))
dist_by_vp_test = defaultdict(list)
for vp, measurements in test_meas_by_ideal_vp.items():
    print("\tVP {} has {} measurement(s)".format(vp, len(measurements)))
    for meas in measurements:
        found_ingr = False
        for i, hop in enumerate(meas):
            for ingr in ips_by_ingr:
                if hop in ingr:
                    found_ingr = True
                    dist_by_vp_test[vp].append(i)
                    break
        if not found_ingr:
            print("\tVP {} has a miss!".format(vp))
            print("\t\t{}".format(meas))

train_meas_by_ideal_vp = defaultdict(list)
with open(train_measurements, 'r') as tmf:
    r = csv.reader(tmf, delimiter=',')
    for row in r:
        vp = row[0].split('/')[-1].split(':')[0]
        if vp in ideal_vps:
            train_meas_by_ideal_vp[vp].append(row[3:]) 

print("Checking if Best VPs Traverse L-Ingrs returned for dnet {} during training".format(
    target_dnet))
dist_by_vp_train = defaultdict(list)
for vp, measurements in train_meas_by_ideal_vp.items():
    print("\tVP {} has {} measurement(s)".format(vp, len(measurements)))
    for meas in measurements:
        found_ingr = False
        for i, hop in enumerate(meas):
            for ingr in ips_by_ingr:
                if hop in ingr:
                    found_ingr = True
                    dist_by_vp_train[vp].append(i)
                    break
        if not found_ingr:
            print("\tVP {} has a miss!".format(vp))
            print("\t\t{}".format(meas))

for vp, dist in dist_by_vp_test.items():
    if vp not in dist_by_vp_train or dist not in dist_by_vp_train[vp]:
        print("VP {} has differing distances to {} between test and train sets.".format(
            vp, target_dnet))
