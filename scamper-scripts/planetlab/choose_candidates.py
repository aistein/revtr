#! /usr/bin/python

# input: 1: list of pl hostnames and sites
#        2: list of pl active hostnames (tested ssh)

# prints to stdout a candidate (randomly chosen) host for each site

from collections import defaultdict
import sys
import random


site_map = {}
with open(sys.argv[1], "r") as f:
    for line in f:
        host, site = line.split()
        site_map[host] = site

all_active_hosts_by_site = defaultdict(list)
with open(sys.argv[2], "r") as f:
    for line in f:
        host = line.strip()
        site = site_map[host]
        all_active_hosts_by_site[site].append(host)

for hosts in all_active_hosts_by_site.values():
    print(hosts[0])
