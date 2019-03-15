#!/usr/local/bin/python3

import pyasn
import pprint
import sys
import csv
from collections import defaultdict

def ddprint(ddict):
    for k, v in sorted(ddict.items()):
        print("\t{}:\t{}".format(k, v))

try:
    target_ip = sys.argv[1]
    reachables_file = sys.argv[2]
    ipasnfile = sys.argv[3]
except IndexError:
    print("usage: ./parse_reachables.py <target ipv4> <reachables file>.csv <ipasn database file>.dat")
    exit(1)

ipasn = pyasn.pyasn(ipasnfile)
target_asn, target_bgp = ipasn.lookup(target_ip)

hops_to_bgp = defaultdict(int)
doesnt_reach_bgp = 0
hops_to_asn = defaultdict(int)
doesnt_reach_asn = 0
with open(reachables_file, 'r') as f:
    r = csv.reader(f, delimiter=',')
    for ping in r:
        lookups = map(lambda ip: ipasn.lookup(ip), ping)
        asn_found = False
        bgp_found = False
        for i, (asn,bgp) in enumerate(lookups):
            if not asn_found and asn == target_asn:
                hops_to_asn[i+1] += 1
                asn_found = True
            if not bgp_found and bgp == target_bgp:
                hops_to_bgp[i+1] += 1
                bgp_found = True
        if not asn_found:
            doesnt_reach_asn += 1
        if not bgp_found:
            doesnt_reach_bgp += 1

print("hops_to_asn:")
ddprint(hops_to_asn)
print("doesnt_reach_asn: ", doesnt_reach_asn) 
print("hops_to_bgp:")
ddprint(hops_to_bgp)
print("doesnt_reach_bgp: ", doesnt_reach_bgp) 

