#!/usr/local/bin/python3

import pyasn
import sys

from collections import namedtuple
DnetEntry = namedtuple('DnetEntry',['asn','bgp','s24'])

def s24_of(target_ip):
    octets = target_ip.split('.')
    return ".".join(octets[:3]) + ".0/24"

def dnets_of(target_ip, ipasn):
    return DnetEntry(*(ipasn.lookup(target_ip)), s24_of(target_ip))

if __name__ == '__main__':
    try:
        target_ip = sys.argv[1]
        dnet_type = sys.argv[2]
        ipasnfile = sys.argv[3]
    except IndexError:
        print("usage: ./lookup_dnet.py <ipv4 address> <dnet type> <ipasn database file>")
        exit(1)

    ipasn = pyasn.pyasn(ipasnfile)
    lookup = dnets_of(target_ip, ipasn)
    
    if dnet_type == 'asn':
        print(lookup.asn)
    if dnet_type == 'bgp':
        print(lookup.bgp)
    if dnet_type == 's24':
        print(lookup.s24)
