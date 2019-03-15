#!/usr/local/bin/python3

import pyasn
import sys

try:
    target_ip = sys.argv[1]
    dnet_type = sys.argv[2]
    ipasnfile = sys.argv[3]
except IndexError:
    print("usage: ./lookup_dnet.py <ipv4 address> <dnet type> <ipasn database file>")
    exit(1)

ipasn = pyasn.pyasn(ipasnfile)

def s24_of(target_ip):
    octets = target_ip.split('.')
    return ".".join(octets[:3]) + ".0/24"

if dnet_type == 'asn':
    print(ipasn.lookup(target_ip)[0])
if dnet_type == 'bgp':
    print(ipasn.lookup(target_ip)[1])
if dnet_type == 's24':
    print(s24_of(target_ip))
