#! /usr/bin/python3 -u

import os, re, sys
import json, socket
from json import JSONDecoder, JSONDecodeError

jsondir = "collections/6_25_2019-survey-measurements/json"
csvdir = "collections/6_25_2019-survey-measurements/csv"

blacklist = [
        #'planetlab-2.calpoly-netlab.net.csv'
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



for jsonfile in os.listdir(jsondir):

    vp_name = '.'.join(jsonfile.split('.')[:-1])
    print("processing json for VP {}...".format(vp_name))

    with open(os.path.join(jsondir,jsonfile), 'r') as jf:
        document = jf.read()

    if vp_name in blacklist: # skip VPs we've already loaded
        continue

    args_list, ping_cnt = [], 0
    for record in decode_stacked_json(document):
        if record['type'] == 'ping':

            ping_cnt += 1

            src_index, dst_index = ipv4_index_of(record['src']), ipv4_index_of(record['dst'])
            rr_slots, rr_responsive, rr_reachable  = [], 'N', 'N'
            rtt, hop_dist = 0.0, 10
            ping_responsive = 'N'

            if record['statistics']['replies'] > 0:
                
                ping_responsive = 'Y'
                if 'RR' in record['responses'][0]: 
                    rr_slots = [ipv4_index_of(hop_addr) for hop_addr in record['responses'][0]['RR']]
                    rr_responsive = 'Y'
                    rr_reachable = 'Y' if dst_index in rr_slots else 'N'
                    if rr_reachable == 'Y':
                       hop_dist = rr_slots.index(dst_index) + 1 

                if 'rtt' in record['responses'][0]:
                    rtt = record['responses'][0]['rtt']

            rr_slots_str = "\\N"
            if rr_slots:
                rr_slots_str = '|'.join(list(map(str, rr_slots)))
            
            argstr = '\\N,{},{},{},{},{},{},{},{},{}'.format(
                    vp_name,
                    src_index,
                    dst_index,
                    rr_slots_str,
                    rr_responsive,
                    rr_reachable,
                    rtt,
                    hop_dist,
                    ping_responsive)
            #argstr = ','.join([str(arg) for arg in args])
            #print("{}".format(argstr))
            args_list.append(argstr)
            
    csvfile = vp_name + '.csv' 
    print("writing VP {} RR-pings to CSV...".format(vp_name))
    with open(os.path.join(csvdir,csvfile), 'w') as cf:
        for argstr in args_list:
            cf.write("{}\n".format(argstr))
    print("done writing to csv.")

print("done.")
