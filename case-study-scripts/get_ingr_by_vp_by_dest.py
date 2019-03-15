#!/usr/local/bin/python3

# What's this do?
# - for a given destination and dnet_type, comb over all measurements to that dest in the original
#   set of VP measurements, and find the ingress for each meas into that destination's containing dnet.

import pyasn
import sys
import csv
import os

try:
    target_ip = sys.argv[1]
    dnet_type = sys.argv[2]
    vp_dir = sys.argv[3]
    ipasn_file = sys.argv[4]
except IndexError:
    print("usage: ./get_ingr_by_vp_by_dest.py <target ip> <dnet type> <vp dir> <ipasn database file>")
    exit(1)

ipasn = pyasn.pyasn(ipasn_file)

asn, bgp = ipasn.lookup(target_ip)
if dnet_type == "bgp":
    target_dnet = bgp
if dnet_type == "asn":
    target_dnet = asn

def ingr_first_inside(ping, target_dnet):
    for hop in ping:
        hop_asn, hop_bgp = ipasn.lookup(hop)
        if dnet_type == "bgp":
            hop_dnet = hop_bgp
        if dnet_type == "asn":
            hop_dnet = hop_asn
        if hop_dnet == target_dnet:
            return hop
    assert False, 'only call ingr_first_inside with reachable pings'

print("target dest: {}".format(target_ip))
print("target dnet: {}".format(target_dnet))

unique_ingrs = {}
for vpfile in os.listdir(vp_dir):
    with open(vp_dir+'/'+vpfile, 'r') as f:
        measurements = csv.reader(f, delimiter=',')
        print("vp: {}".format(vpfile))
        for ping in measurements:
            # first col is dest
            if ping[0] != target_ip:
                continue
            if target_ip in ping[1:]:
                ingr = ingr_first_inside(ping[1:], target_dnet)
                unique_ingrs[ingr] = True
                print("\tingr: {}, dist: {}".format(ingr, ping.index(ingr)))
                print("\t\tping: {}".format(ping[1:]))

print("There are {} ingrs into dnet {} associated with dest {}.".format(
        len(unique_ingrs), target_dnet, target_ip))
print("Unique Ingrs: ")
[print(ingr) for ingr in unique_ingrs.keys()]
