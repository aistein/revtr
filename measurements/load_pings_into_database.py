#! /usr/bin/python3 -u

import os, re, sys, json
import pymysql.cursors
from json import JSONDecoder, JSONDecodeError

jsondir = "collections/6_25_2019-survey-measurements/json"

blacklist = [
            'mlab1.arn02.measurement-lab.org', 
            'mlab1.atl03.measurement-lab.org', 
            'mlab1.bcn01.measurement-lab.org', 
            'mlab1.bru02.measurement-lab.org', 
            'mlab1.bru04.measurement-lab.org', 
            'mlab1.dfw06.measurement-lab.org', 
            'mlab1.lga03.measurement-lab.org', 
            'mlab1.lhr05.measurement-lab.org', 
            'mlab1.mad03.measurement-lab.org', 
            'mlab1.ord03.measurement-lab.org', 
            'mlab1.prg02.measurement-lab.org', 
            'mlab1.sea02.measurement-lab.org', 
            'mlab1.trn01.measurement-lab.org', 
            'mlab1.ywg01.measurement-lab.org', 
            'mlab1.yyz02.measurement-lab.org', 
            'node1.planetlab.mathcs.emory.edu',
            'pl2.cs.montana.edu',
            'planetlab01.cs.washington.edu',
            'planetlab-2.calpoly-netlab.net',
            'mlab2.akl01.measurement-lab.org',
            'planetlab2.ie.cuhk.edu.hk',
            'mlab1.mia04.measurement-lab.org',
            'mlab1.sea06.measurement-lab.org',
            'mlab1.ord04.measurement-lab.org',
            'mlab1.mia06.measurement-lab.org',
            'mlab1.lga05.measurement-lab.org',
            'mlab1.iad03.measurement-lab.org',
            'mlab1.lis02.measurement-lab.org',
            'mlab1.mil02.measurement-lab.org',
            'mlab1.nuq03.measurement-lab.org',
            'mlab1.dfw08.measurement-lab.org',
            'planetlab4.mini.pw.edu.pl',
            'mlab2.bom02.measurement-lab.org',
            'mlab1.bru03.measurement-lab.org',
            'mlab1.ams03.measurement-lab.org',
            'mlab1.ord06.measurement-lab.org',
            'plink.cs.uwaterloo.ca',
            'mlab2.lax06.measurement-lab.org',
            'mlab1.arn03.measurement-lab.org',
            'mlab3.syd02.measurement-lab.org',
            'mlab1.lhr04.measurement-lab.org',
            'mlab1.yqm01.measurement-lab.org',
            'mlab1.sea04.measurement-lab.org',
            'mlab1.tgd01.measurement-lab.org',
            'planetlab2.citadel.edu',
            'mlab1.mia05.measurement-lab.org',
            'mlab1.vie01.measurement-lab.org',
            'planetlab3.cs.uoregon.edu',
            'planetlab1.pop-mg.rnp.br',
            'mlab1.lax04.measurement-lab.org',
            'mlab1.den04.measurement-lab.org',
            'mlab1.prg04.measurement-lab.org',
            'mlab1.nuq06.measurement-lab.org'
        ]

NOT_WHITESPACE = re.compile(r'[^\s]')

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

        args_list, args_cnt = [], 0
        for jsonfile in os.listdir(jsondir):
            with open(os.path.join(jsondir,jsonfile), 'r') as jf:
                document = jf.read()

            vp_name = '.'.join(jsonfile.split('.')[:-1])
            if vp_name in blacklist: # skip VPs we've already loaded
                continue
            print("processing json for VP {}...".format(vp_name))

            ping_cnt = 0
            for record in decode_stacked_json(document):
                if record['type'] == 'ping':

                    ping_cnt += 1

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
                    
                    #print("loading ping {}->{}".format(vp_name, dst_addr))
                    args = (dst_addr, vp_name,
                            rr_responsive, rr_reachable,
                            src_addr, dst_addr,
                            ','.join(rr_slots),
                            rtt, hop_dist)
                    args_list.append(args)
                    
                    # retry loop to handle deadlocks
                    if args_cnt == 1000:
                        while True:
                            try:
                                cursor.executemany(sql, (*args,))
                            except pymysql.err.OperationalError:
                                continue
                            break

            print("committing {} pings from VP {}...".format(ping_cnt, vp_name))
            connection.commit()

finally:
    print("done.")
    connection.close()
