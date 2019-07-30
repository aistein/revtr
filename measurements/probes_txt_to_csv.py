#! /usr/bin/python3 -u

# take output of print_datafile as streaming input of probe-records
# for each probe record, check if ICMP echo was successful and if so, write to csv file for that probe
# also, do asn/bgp-prefix lookup on destination so that this information is accessible later on

import sys, os
import time
import socket, pyasn

from collections import namedtuple
DnetEntry = namedtuple('DnetEntry',['asn','bgp','s24'])

def s24_of(ip):
    octets = ip.split('.')
    return ".".join(octets[:3]) + ".0/24"

def dnets_of(ip, ipasn):
    try:
        return DnetEntry(*(ipasn.lookup(ip)), s24_of(ip))
    except:
        return DnetEntry('','', s24_of(ip))

def isValidIP(ip):
    return len(ip.split('.')) == 4

def ipv4_index_of( addr ):
    try:
        as_bytes = socket.inet_aton(addr)
    except OSError as e:
        sys.exit("on trying socket.inet_aton({}), received error {}".format(addr, e))
    return int.from_bytes(as_bytes, byteorder='big', signed=False)

def ipv4_address_of( index ):
    as_bytes = index.to_bytes(4, byteorder='big', signed=False)
    return socket.inet_ntoa(as_bytes)

ipasnfile = "../bgpdumps/2019-01-30.dat"
ipasn = pyasn.pyasn(ipasnfile)

#targetfile = "./new_target_slash24s.it84w.txt"
targetfile = "./target_slash24s.it84w.txt"
with open(targetfile, 'r') as tf:
    target_networks = set()
    for slash_24 in tf:
        target_networks.add(slash_24.strip())

#txtdir = "survey/internet_address_survey_reprobing_it84w-20190130/data/txt"
txtdir = "tmp"
csvdir = "survey/internet_address_survey_reprobing_it84w-20190130/data/csv"

unique_probe_addrs = set()

# first, gather all unique destinations into a set
for txtfile in os.listdir(txtdir):
    with open(os.path.join(txtdir,txtfile), 'r') as f:
        for line in f:
            probe_addr = line.strip()
            if isValidIP( probe_addr ) and (s24_of(probe_addr) in target_networks):
                unique_probe_addrs.add(probe_addr)
    print("current number of unique destinations {}".format(len(unique_probe_addrs)))

print("discerned {} unique probe addresses".format(len(unique_probe_addrs)))

# second, do lookups for bgp-prefix and asn of each unique IP
print("doing lookups...")
entries = []
for probe_addr in unique_probe_addrs:
    probe_index = ipv4_index_of(probe_addr)

    # get networks as strings and convert to numbers
    as_number_str, bgp_prefix_str, slash_24_str = dnets_of( probe_addr, ipasn)
    as_number = int(as_number_str) if as_number_str else -1
    bgp_prefix = ipv4_index_of(bgp_prefix_str.split('/')[0]) if bgp_prefix_str else -1
    bgp_prefix_length = int(bgp_prefix_str.split('/')[1]) if bgp_prefix_str else -1
    slash_24 = ipv4_index_of(slash_24_str.split('/')[0])

    entries.append("{},{},{},{},{}".format(probe_index, as_number, bgp_prefix, bgp_prefix_length, slash_24))

# finally, write the entries out to csv
print("writing to csv...")
with open(os.path.join(csvdir,'destinations_and_networks.csv'), 'w') as f:
    for entry in entries:
        f.write("{}\n".format(entry))

print("done.")
