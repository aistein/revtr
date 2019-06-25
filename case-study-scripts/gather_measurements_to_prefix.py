#! /usr/local/bin/python3 -u

import os, sys
import csv
import json
import pyasn
from collections import defaultdict

from collections import namedtuple
DnetEntry = namedtuple('DnetEntry',['asn','bgp','s24'])

#vp_dir = '/data/workspace/data/test/test/vp_measurements/annotated/all'
vp_dir = '/data/workspace/data/4_19_2019/train/vp_measurements/annotated/all'
ipasnfile = '/data/workspace/bgpdumps/ipasn20181203.dat'
#target_prefix = '209.58.192.0/19'
#target_prefix = '88.176.0.0/12'
target_prefix = '180.69.0.0/16'

ipasn = pyasn.pyasn(ipasnfile)

def s24_of(target_ip):
    octets = target_ip.split('.')
    return ".".join(octets[:3]) + ".0/24"

def dnets_of(target_ip):
    try:
        return DnetEntry(*(ipasn.lookup(target_ip)), s24_of(target_ip))
    except:
        return DnetEntry('','','')

measurements_to_target_prefix_by_vp = defaultdict(list)
for vpcsv in os.listdir(vp_dir):
    vpname = vpcsv.replace('.csv','')
    print("opening file {}...".format(os.path.join(vp_dir, vpcsv)))
    with open(os.path.join(vp_dir, vpcsv), 'r') as f:
        next(f)
        r = csv.reader(f, delimiter=',')
        for measurement in r:
            #if dnets_of(measurement[0]).bgp == target_prefix:
            #    measurements_to_target_prefix_by_vp[vpcsv].append(measurement)
            #    print(measurement)
            if measurement[1] == target_prefix:
                measurements_to_target_prefix_by_vp[vpname].append(measurement)
                print(measurement)


with open('./gather_measurements_to_prefix.json', 'w') as f:
    json.dump(measurements_to_target_prefix_by_vp, f)
