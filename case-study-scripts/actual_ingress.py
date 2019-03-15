#!/usr/local/bin/python3

import sys
from collections import OrderedDict, defaultdict

try:
    dnet = sys.argv[1]
    filename = sys.argv[2]
except IndexError:
    print("usage: ./actual_ingress.py <destination network> <original measurements file>")
    exit(1)

# transposition of function from original emulation study - inline comments -
def aggregate_ingresses(dists_by_vp_by_ingr, overlap_threshold=0.75):
    ingrs_by_vp = defaultdict(set)
    for ingr, dists_by_vp in dists_by_vp_by_ingr.items():
        for vp in dists_by_vp.keys():
            ingrs_by_vp[vp].add(ingr) # - destorys frequency information -

    sorted_ingrs = OrderedDict((sorted(ingrs_by_vp.items(), key=lambda x : -len(x[1]))))
    vps_by_ingrt = {}
    while len(sorted_ingrs) > 0:
        vp, agg_ingrs = sorted_ingrs.popitem(last=False)
        vps_this_round = [vp]
        ingrs_added = True
        start = 1
        while ingrs_added:
            ingrs_added = False
            for vp, ingrs in sorted_ingrs.items():
                if len(ingrs & agg_ingrs) >= overlap_threshold * len(ingrs):
                    vps_this_round.append(vp)
                    agg_ingrs |= ingrs
                    ingrs_added = True
            for vp in vps_this_round[start:]:
                del sorted_ingrs[vp]
            start = len(vps_this_round)

        vps_by_ingrt[tuple(agg_ingrs)] =\
                sorted(vps_this_round, key=lambda x :
                min([min(dists_by_vp_by_ingr[ingr][x])\
                if x in dists_by_vp_by_ingr[ingr] else 10
                for ingr in agg_ingrs])) # - destroys path information -

    return vps_by_ingrt



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
dists_by_vp_by_ingress = defaultdict(dict)
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
                    dists_by_vp_by_ingress[addr][vp].append(hops)
                    dist_by_vp_by_ingress[addr][vp] = min(dist_by_vp_by_ingress[addr][vp], hops)
                    freq_by_vp_by_ingress[addr][vp] += 1
                else:
                    dists_by_vp_by_ingress[addr][vp] = [hops]
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

print("Logical Ingresses via IngressCover Algorithm:")
vps_by_logical_ingrt = aggregate_ingresses(dists_by_vp_by_ingress)
print("Number of Logical Ingresses: {}".format(len(vps_by_logical_ingrt)))
[print(logical_ingress) for logical_ingress in vps_by_logical_ingrt.keys()]

print("IngressCover VP Rankings:")
[print(vps[0]) for vps in vps_by_logical_ingrt.values()]
#[print(dist_by_vp_by_ingress[ingr][

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
