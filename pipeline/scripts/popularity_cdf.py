#! /usr/local/bin/python3

"""
Generate a CDF of how commonly a VP uses its most popular ingress into a
bgp-prefix.  

In one pass over the raw VP data, for each <VP, prefix> build a list of 
<ingress, count> tuples.  At the end, do another pass to output for each
<VP, prefix> the (highest_count/ total_count)
"""

import sys, os
import csv
import pyasn
import multiprocessing as mp
from collections import namedtuple
from collections import defaultdict
from lookup_dnet import dnets_of, DnetEntry

VpPrefix = namedtuple('VpPrefix', ['vp','prefix'])
IngrCnt = namedtuple('IngrCnt', ['ingress', 'count'])

try:
    vp_dir = sys.argv[1]
    output_dir = sys.argv[2]
    ipasnfile = sys.argv[3]
    ipasn = pyasn.pyasn(ipasnfile)
except:
    exit("usage: ./popularity_cdf.py <raw-probe-dir> <output-dir> <bgp-dump-file>")

# given a ping probe, return (six) ingresses for all definitions:
# - first, by 'asn / bgp / s24' of the destination
# - then, by 'first-inside' or 'first-outside' for each dnet_type above
def ingr_lookup(probe):
    dest, hops = probe[0], probe[1:]
    fi = {'asn':None, 'bgp':None, 's24':None}
    fo = {'asn':None, 'bgp':None, 's24':None}
    dest_dentry = dnets_of(dest, ipasn)
    #print('dest_dentry: {}'.format(dest_dentry))
    hop_dentries = [dnets_of(hop, ipasn) for hop in hops]
    #print('hop_dentries: {}'.format(hop_dentries))
    prev_hop = None
    type_found = {'asn':False, 'bgp':False, 's24':False}
    for i, hop in enumerate(hops):
        if all( found for found in type_found.values() ):
            break
        hop_dentry = hop_dentries[i]
        for dnet_type in ['asn', 'bgp', 's24']:
            if not type_found[dnet_type]:
                dest_dnet = eval('dest_dentry.{}'.format(dnet_type))
                hop_dnet = eval('hop_dentry.{}'.format(dnet_type))
                if dest_dnet == hop_dnet:
                    fi[dnet_type] = hop
                    fo[dnet_type] = prev_hop
                    type_found[dnet_type] = True
        prev_hop = hop

    return fi, fo

# tdef = 'asn/bgp/s24' ingress def
# ldef = 'first inside/outside' ingress def
# vpt = 'VP:prefix' tuple
count_by_ingr_by_tdef_by_ldef_by_vpt = defaultdict( #vpt
        lambda: defaultdict( # ldef
        lambda: defaultdict( # tdef
        lambda: defaultdict( int ))))# ingr<-->int
for vpcsv in os.listdir(vp_dir):
    vpname = vpcsv.replace('.csv','')
    with open(os.path.join(vp_dir, vpcsv), 'r') as f:
        r = csv.reader(f, delimiter=',')
        for probe in r:
            vpt = (vpname, dnets_of(probe[0], ipasn).bgp)
            fi, fo = ingr_lookup(probe)
            print('vpt: {}'.format(vpt))
            print('hops: {}'.format(probe[1:]))
            print('ingrs-fi: {}'.format(fi))
            print('ingrs-fo: {}\n'.format(fo))
            for tdef in ['asn','bgp','s24']:
                ingrfi, ingrfo = fi[tdef], fo[tdef]
                count_by_ingr_by_tdef_by_ldef_by_vpt[vpt]['first-inside'][tdef][ingrfi] += 1
                count_by_ingr_by_tdef_by_ldef_by_vpt[vpt]['first-outside'][tdef][ingrfo] += 1

popularity_by_tdef_by_ldef_by_vpt = defaultdict( # vpt
        lambda: defaultdict( # ldef
        lambda: defaultdict( float ))) # tdef <--> popularity (score)
for vpt, count_by_ingr_by_tdef_by_ldef in count_by_ingr_by_tdef_by_ldef_by_vpt.items():
    for ldef in ['first-inside','first-outside']:
        for tdef in ['asn','bgp','s24']:
            max_ingr, max_count, total_count = '', 0, 0
            for ingr, count in count_by_ingr_by_tdef_by_ldef[ldef][tdef].items():
                if count > max_count:
                    max_ingr, max_count = ingr, count
                total_count += count
            print("popularity[{}][{}][{}] = {}".format(vpt, ldef, tdef, max_count / total_count))
            popularity_by_tdef_by_ldef_by_vpt[vpt][ldef][tdef] = max_count / total_count
