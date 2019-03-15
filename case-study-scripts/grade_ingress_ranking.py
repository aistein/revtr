#!/usr/local/bin/python3

"""
Do best VPs traverse same logical ingresses as returned by ingr-ranking...
- in their test measurements to the target destination?
- in all their training measurements to the network containing the target?
"""

import sys
import csv

try:
    target_ip = sys.argv[1]
    target_dnet = sys.argv[2]
    ips_by_ingr_file = sys.argv[3]
    best_vps_file = sys.argv[4]
except:
    exit("usage: ./grade_ingress_ranking.py <target-ip> <target-dnet> <ips_by_ingr_file> <best_vps_file>")

ips_by_ingr = []
with open(ips_by_ingr_file, 'r') as igf:
    r = csv.reader(igf, delimiter=',')
    for ingr in r:
        print("Ingr {} has {} IPs".format(ingr[0], len(ingr[1:])))
        ips_by_ingr.append(set(ingr[1:]))


