#! /usr/bin/python3 -u

import os, re, sys
import json, socket, pyasn
from collections import Counter
from json import JSONDecoder, JSONDecodeError

#jsondir = "collections/6_25_2019-survey-measurements/json"
jsondir = "./test"
asndb = pyasn.pyasn("../bgpdumps/2019-04-02.dat")

blacklist = [
        ]

NOT_WHITESPACE = re.compile(r'[^\s]')

def ipv4_index_of( addr ):
    as_bytes = socket.inet_aton(addr)
    return int.from_bytes(as_bytes, byteorder='big', signed=False)

def decode_stacked_json(document, pos=0, decoder=JSONDecoder()):
    while True:
        match = NOT_WHITESPACE.search(document, pos)
        if not match:
            return

        pos = match.start()
        try:
            obj, pos = decoder.raw_decode(document, pos)
        except JSONDecodeError as e:
            print("error decoding json: {}".format(e))
        yield obj

ping_responsive_probes = set()
#ping_responsive_prefixes = set()
#all_prefixes = set()

for jsonfile in os.listdir(jsondir):

    vp_name = '.'.join(jsonfile.split('.')[:-1])
    print("processing json for VP {}...".format(vp_name))

    with open(os.path.join(jsondir,jsonfile), 'r') as jf:
        document = jf.read()

    if vp_name in blacklist: # skip VPs we've already loaded
        continue

    total_pings, responsive_pings = 0, 0
    prefixes_this_vp = {}
    for record in decode_stacked_json(document):

        if record['type'] == 'ping':

            total_pings += 1
            dst_prefix = asndb.lookup(record['dst'])
            src_index, dst_index = ipv4_index_of(record['src']), ipv4_index_of(record['dst'])

            if dst_prefix not in prefixes_this_vp:
                prefixes_this_vp[ dst_prefix ] = False

            if record['statistics']['replies'] > 0:
                responsive_pings += 1
                ping_responsive_probes.add( (src_index, dst_index) )
                prefixes_this_vp[ dst_prefix ] = True

    #ping_responsive_prefixes.update( set( [prefix for prefix, responsive in prefixes_this_vp.items() if responsive] ) )
    #all_prefixes.update( set( [prefix for prefix in prefixes_this_vp] ) )

    responsive_prefixes = Counter(prefixes_this_vp.values())[True]
    total_prefixes = len(prefixes_this_vp)
    print("VP {} had {} out of {} responsive pings, {} out of {} responsive prefixes.".format(vp_name, responsive_pings, total_pings, responsive_prefixes, total_prefixes))

#print("Prefix-Responsiveness: {} out of {} prefixes had at least one probe that was ping-responsive".format(len(ping_responsive_prefixes), len(all_prefixes)))

print("done.")
