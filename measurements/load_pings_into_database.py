#! /usr/bin/python3 -u

import os, re, sys, json
import pymysql.cursors
from json import JSONDecoder, JSONDecodeError

jsondir = "collections/6_24_2019-healthcheck/json"

NOT_WHITESPACE = re.compile(r'[^\s]')

def decode_stacked_json(document, pos=0, decoder=JSONDecoder()):
    while True:
        match = NOT_WHITESPACE.search(document, pos)
        if not match:
            return

        pos = match.start()
        try:
            obj, pos = decoder.raw_decode(document, pos)
        except JSONDecodeError:
            print("error decoding json...")
        yield obj

connection = pymysql.connect(host='localhost',
                             user='ubuntu',
                             password='',
                             db='MEASUREMENTS',
                             cursorclass=pymysql.cursors.DictCursor)

try:

    with connection.cursor() as cursor:

        sql = "INSERT IGNORE INTO `pings_tbl` " +\
            "(`dest_id`,`vp_id`,`rr_responsive`,`rr_reachable`," +\
            "`src_addr`,`dst_addr`,`rr_slots`,`rtt`,`hop_dist`) VALUES (" +\
            "(SELECT `dest_id` FROM `destinations_tbl` WHERE `dest_addr`=%s)," +\
            "(SELECT `vp_id` FROM `vantage_points_tbl` WHERE `vp_name`=%s)," +\
            "%s, %s, %s, %s, %s, %s, %s)"

        for jsonfile in os.listdir(jsondir):
            with open(os.path.join(jsondir,jsonfile), 'r') as jf:
                document = jf.read()

            vp_name = '.'.join(jsonfile.split('.')[:-1])

            for record in decode_stacked_json(document):
                if record['type'] == 'ping':

                    src_addr, dst_addr = record['src'], record['dst']
                    rr_slots, rr_responsive, rr_reachable  = [], 'N', 'N'
                    rtt, hop_dist = 0.0, 10

                    if record['statistics']['replies'] > 0:

                        if 'RR' in record['responses'][0]: 
                            rr_slots = record['responses'][0]['RR']
                            rr_responsive = 'Y'
                            rr_reachable = 'Y' if dst_addr in rr_slots else 'N'
                            if rr_reachable == 'Y':
                               hop_dist = rr_slots.index(dst_addr) + 1 

                        if 'rtt' in record['responses'][0]:
                            rtt = record['responses'][0]['rtt']
                    
                    print("loading ping {}->{}".format(vp_name, dst_addr))
                    args = (dst_addr, vp_name,
                            rr_responsive, rr_reachable,
                            src_addr, dst_addr,
                            ','.join(rr_slots),
                            rtt, hop_dist)

                    
                    # retry loop to handle deadlocks
                    while True:
                        try:
                            cursor.execute(sql, (*args,))
                        except pymysql.err.OperationalError:
                            continue
                        break

            print("committing...")
            connection.commit()

finally:
    print("done.")
    connection.close()
