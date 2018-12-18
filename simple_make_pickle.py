import os
import sys
import csv
import pyasn
import pickle
from collections import defaultdict


probe_dir = sys.argv[1]
ipasn_file = sys.argv[2]
out_dir = sys.argv[3]

bgpdb = pyasn.pyasn(ipasn_file)

asn_dir = os.path.join(out_dir, 'asn')
bgp_dir = os.path.join(out_dir, 'bgp')
s24_dir = os.path.join(out_dir, 's24')

os.makedirs(out_dir, exist_ok=True)
os.makedirs(asn_dir, exist_ok=True)
os.makedirs(bgp_dir, exist_ok=True)
os.makedirs(s24_dir, exist_ok=True)

for vp_csv in os.listdir(probe_dir):
    print('processing {}...'.format(vp_csv))

    dest_by_dnet_asn = defaultdict(set)
    dest_by_dnet_bgp = defaultdict(set)
    dest_by_dnet_s24 = defaultdict(set)

    vp = vp_csv.replace('.csv', '.pickle')
    with open(os.path.join(probe_dir, vp_csv), 'r') as f:
        for row in csv.reader(f):
            dest = row[0]
            hops = row[1:]

            try:
                asn, prfx = bgpdb.lookup(dest)
            except:
                continue

            d = dest.split('.')
            d[-1] = '0/24'
            s24 = '.'.join(d)

            if asn:
                dest_by_dnet_asn[asn].add(dest)
            if prfx:
                dest_by_dnet_bgp[prfx].add(dest)
            dest_by_dnet_s24[s24].add(dest)

    with open(os.path.join(asn_dir, vp), 'wb+') as f:
        pickle.dump(dict(dest_by_dnet_asn), f)
    with open(os.path.join(bgp_dir, vp), 'wb+') as f:
        pickle.dump(dict(dest_by_dnet_bgp), f)
    with open(os.path.join(s24_dir, vp), 'wb+') as f:
        pickle.dump(dict(dest_by_dnet_s24), f)
