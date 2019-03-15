#! /usr/bin/python

import sys
from collections import defaultdict

if len(sys.argv) < 3:
    print("usage: find_equivalent_hostnames.py host_file1 host_file2 site_map")
    exit()

hf1 = sys.argv[1]
hf2 = sys.argv[2]
smf = sys.argv[3]
equivalent_hostnames = defaultdict(list)

with open(hf1, "r") as f, open(hf2, "r") as g, open(smf, "r") as h:
    hset1 = set([l.strip() for l in f.readlines()])
    hset2 = set([l.strip() for l in g.readlines()])
    sites_by_id = dict([ (l.strip().split()) for l in h.readlines()])

for h in hset1:
    try:
        sid = sites_by_id[h]
        equivalent_hostnames[sid].append(h)
    except KeyError:
        pass

for h in hset2:
    try:
        sid = sites_by_id[h]
        equivalent_hostnames[sid].append(h)
    except KeyError:
        pass

print("%s\t%s" % (hf1, hf2))
for sid, hosts in equivalent_hostnames.iteritems():
    if len(hosts) == 2:
        print("%s\t%s" % (hosts[0], hosts[1]))
