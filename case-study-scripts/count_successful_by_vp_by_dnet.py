#!/usr/local/bin/python3

import pyasn
import json
import csv
import sys

from collections import defaultdict
from collections import namedtuple
DnetEntry = namedtuple('DnetEntry',['asn','bgp','s24'])

try:
    vpcsv = sys.argv[1]
    target_dnet = sys.argv[2]
    target_dnet_type = sys.argv[3]
    if target_dnet_type not in ['asn','bgp','s24','all']:
        raise ValueError()
    ipasnfile = sys.argv[4]
except:
    exit("usage: ./count_successful_by_vp_by_dnet.py <vp-csv-file> <dnet> <dnet-type> <ipasn-file>")

ipasn = pyasn.pyasn(ipasnfile)
def s24_of(target_ip):
    octets = target_ip.split('.')
    return ".".join(octets[:3]) + ".0/24"

meas_cnt_by_asn = defaultdict(int)
meas_cnt_by_bgp = defaultdict(int)
meas_cnt_by_s24 = defaultdict(int)
with open(vpcsv, 'r') as f:
    measurements = csv.reader(f, delimiter=',')
    for m in measurements:
        lookup = DnetEntry(*(ipasn.lookup(m[0])), s24_of(m[0]))
        if target_dnet == 'all' or str(eval('lookup.{}'.format(target_dnet_type))) == target_dnet:
            meas_cnt_by_asn[lookup.asn] += 1
            meas_cnt_by_bgp[lookup.bgp] += 1
            meas_cnt_by_s24[lookup.s24] += 1

by_value = lambda tup : tup[1]
meas_cnt_by_asn_sorted = dict(sorted(meas_cnt_by_asn.items(), key=by_value))
meas_cnt_by_bgp_sorted = dict(sorted(meas_cnt_by_bgp.items(), key=by_value))
meas_cnt_by_s24_sorted = dict(sorted(meas_cnt_by_s24.items(), key=by_value))
print(json.dumps({**meas_cnt_by_asn_sorted, **meas_cnt_by_bgp_sorted, **meas_cnt_by_s24_sorted}, indent=2))
