#!/usr/local/bin/python3

import pyasn
import csv
import sys
import os
from collections import defaultdict

try:
    dnet_type = sys.argv[1]
    vpdir = sys.argv[2] + '/' + dnet_type
    ipasnfile = sys.argv[3]
except IndexError:
    print("usage: ./first_outside.py <dnet_type> <path_to_annotated_vp_dir> <ipasnfile> <opt: target dnet>")
    exit(1)

try:
    target_dnet = sys.argv[4]
except IndexError:
    target_dnet = None

ipasn = pyasn.pyasn(ipasnfile)

# return ip-address of first hop before the target dnet from this probe
def get_first_inside(target, hops):
    for i, hop in enumerate(hops):
        hop_asn, hop_prefix = ipasn.lookup(hop)
        if dnet_type == "asn":
            if str(hop_asn) == target:
                try:
                    return hops[i-1]
                except:
                    continue
        if dnet_type == "bgp":
            if str(hop_prefix) == target:
                try:
                    return hops[i-1]
                except:
                    continue
    return ""


ingress_by_dnet_by_vp = {}

for vpcsv in os.listdir(vpdir):
    with open(vpdir + '/' + vpcsv, 'r') as vpfile:

        vp = ".".join(vpcsv.strip().split('.')[:-1]) # name of VP
        if vp not in ingress_by_dnet_by_vp.keys():
            ingress_by_dnet_by_vp[vp] = defaultdict(list)

        probe_reader = csv.reader(vpfile, delimiter=',')
        for probe in probe_reader:
            first_inside = None
            dnet = probe[0] 
            if (target_dnet != None):
                if (target_dnet != dnet):
                    continue
                else:
                    hops = probe[3:]
                    first_inside = get_first_inside(dnet, hops)
            else:
                hops = probe[3:]
                first_inside = get_first_insid(dnet, hops)
            if first_inside != None:
                ingress_by_dnet_by_vp[vp][dnet].append(first_inside)

for vp, ingress_by_dnet in ingress_by_dnet_by_vp.items():
    print("VP: {}".format(vp))
    for dnet, ingresses in ingress_by_dnet.items():
        print("\tDNET: {}".format(dnet))
        for ingress in ingresses:
            print("\t\t{}".format(ingress))
