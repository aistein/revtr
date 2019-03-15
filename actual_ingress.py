#!/usr/local/bin/python3

import sys
from collections import defaultdict

try:
    dnet = sys.argv[1]
    filename = sys.argv[2]
except IndexError:
    print("usage: ./actual_ingress.py <destination network> <original measurements file>")
    exit(1)

def inside_dnet(addr):
    n_octets_to_match = int(dnet.split("/")[1]) // 8
    n_bits_to_match = int(dnet.split("/")[1]) % 8
    dnet_octets = dnet.split(".")
    addr_octets = addr.split(".")
    for i in range(n_octets_to_match):
        if dnet_octets[i] != addr_octets[i]:
            return False
    mask = 0xff & (0xff<<(8-n_bits_to_match))
    dnet_octet_masked = int(dnet_octets[n_octets_to_match]) & mask
    addr_octet_masked = int(addr_octets[n_octets_to_match]) & mask
    if dnet_octet_masked != addr_octet_masked:
        return False
    return True

def get_overlap(set_a, set_b):
    numerator = len(set_a.intersection(set_b))
    denominator = min(len(set_a), len(set_b))
    return numerator / denominator

print("Dest-Dnet: {}".format(dnet))

vps_by_ingress = defaultdict(set)
vps_by_external_ingress = defaultdict(set)
dist_by_vp_by_ingress = defaultdict(dict)
freq_by_vp_by_ingress = defaultdict(dict)
vp_in_dnet = 0
n_meas = 0
with open(filename, 'r') as f:
    for line in f:
        fields = line.strip().split(",")
        dest = fields[0].split(":")[1]
        vp = fields[0].split(":")[0]
        for hops, addr in enumerate(fields[1:]):
            n_meas += 1
            if inside_dnet(addr):
                vps_by_ingress[addr].add(vp)
                if vp in dist_by_vp_by_ingress[addr]:
                    dist_by_vp_by_ingress[addr][vp] = min(dist_by_vp_by_ingress[addr][vp], hops)
                    freq_by_vp_by_ingress[addr][vp] += 1
                else:
                    dist_by_vp_by_ingress[addr][vp] = hops
                    freq_by_vp_by_ingress[addr][vp] = 1
                try:
                    vps_by_external_ingress[fields[hops-1]].add(vp)
                except IndexError:
                    vp_in_dnet += 1


# Analysis for "Inside" Ingress Definition

avg_overlap = 0.0
for i, (ingr_i, vp_set_i) in enumerate(sorted(vps_by_ingress.items())):
    for j, (ingr_j, vp_set_j) in enumerate(sorted(vps_by_ingress.items())):
        if i < j: # only top half of "matrix"
            avg_overlap += get_overlap(vp_set_i, vp_set_j)
avg_overlap = avg_overlap / ((len(vps_by_ingress)**2 - len(vps_by_ingress)) / 2)

print("Number of Measurements to DNET {}: {}".format(dnet, n_meas))

print("Number of Unique Ingresses(first inside): {}".format(len(vps_by_ingress)))
print("Number of Unique VPs Traversing Ingress(first inside):")
[print("{:<16} : {}".format(ingress, len(vplist))) for ingress, vplist in vps_by_ingress.items()]
print("Average Overlap: {}".format(avg_overlap))

print("Minimum 15 (Distances/Frequencies) to Ingress(first inside):")
for ingr in vps_by_ingress.keys():
    sorted_by_dist = sorted(dist_by_vp_by_ingress[ingr].items(), key=lambda kv : kv[1])
    freq_by_vp = freq_by_vp_by_ingress[ingr]
    distfreq_list = [str(dist)+"/"+str(freq_by_vp[vp]) for vp,dist in sorted_by_dist]
    print("{:<16} : {}".format(ingr, ", ".join(distfreq_list[:15])))




# Analysis for "Outside" Ingress Definition

avg_overlap = 0.0
for i, (ingr_i, vp_set_i) in enumerate(sorted(vps_by_external_ingress.items())):
    for j, (ingr_j, vp_set_j) in enumerate(sorted(vps_by_external_ingress.items())):
        if i < j: # only top half of "matrix"
            avg_overlap += get_overlap(vp_set_i, vp_set_j)
avg_overlap = avg_overlap / ((len(vps_by_external_ingress)**2 - len(vps_by_external_ingress)) / 2)
print("Number of VPs 1 hop away: {}".format(vp_in_dnet))
print("Number of Unique Ingresses(first before): {}".format(len(vps_by_external_ingress)))
print("Number of Unique VPs Traversing Ingress(first before):")
[print("{:<16} : {}".format(ingress, len(vplist))) for ingress, vplist in vps_by_external_ingress.items()]
print("Average Overlap: {}".format(avg_overlap))
