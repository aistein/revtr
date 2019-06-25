#! /usr/bin/python3 -u

# take output of print_datafile as streaming input of probe-records
# for each probe record, check if ICMP echo was successful and if so, insert into MySQL database

import sys
import time
import pyasn
import pymysql.cursors

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

def echoSucceeded(icmp_reply_type_and_code):
    return (int(icmp_reply_type_and_code, 0) & 0x0B00) == 0

ipasnfile = "../bgpdumps/2019-01-30.dat"
ipasn = pyasn.pyasn(ipasnfile)

targetfile = "./new_target_slash24s.it84w.txt"
with open(targetfile, 'r') as tf:
    target_networks = set()
    for slash_24 in tf:
        target_networks.add(slash_24.strip())

connection = pymysql.connect(host='localhost',
                             user='ubuntu',
                             password='',
                             db='MEASUREMENTS',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    unique_probe_addrs, probe_cnt, total_cnt = set(), 0, 0

    with connection.cursor() as cursor:

        #sql = "INSERT IGNORE INTO `destinations_tbl` (`dest_addr`, `as_number`, `bgp_prefix`, `slash_24`) VALUES (%s, %s, %s, %s)"
        sql = "INSERT INTO `destinations_tbl` (`dest_addr`, `as_number`, `bgp_prefix`, `slash_24`) VALUES (%s, %s, %s, %s) " +\
              "ON DUPLICATE KEY UPDATE `dest_addr`=%s, `as_number`=%s, `bgp_prefix`=%s, `slash_24`=%s"

        for line in sys.stdin:
            if line[0] == '0': # valid probe entry
                fields = line.split()
                typeandcode, probe_addr = fields[0], fields[4]
                as_number, bgp_prefix, slash_24 = dnets_of( probe_addr, ipasn)

                if echoSucceeded( typeandcode ) and isValidIP( probe_addr ) and (slash_24 in target_networks) and (probe_addr not in unique_probe_addrs):
                    #print("loading probe_addr {}".format(probe_addr))
                    args = (probe_addr, as_number, bgp_prefix, slash_24)
                    
                    # retry loop to handle deadlocks
                    while True:
                        try:
                            cursor.execute(sql, (*args, *args))
                        except pymysql.err.OperationalError:
                            continue
                        break

                    unique_probe_addrs.add(probe_addr)
                    probe_cnt, total_cnt = probe_cnt + 1, total_cnt + 1

                    if probe_cnt == 100: # make a commit every 100 probes cleared
                        #print("committing 100 probes...")
                        connection.commit()
                        probe_cnt = 0

                    if (total_cnt % 1000) == 0:
                        print("{} probes committed so far...".format(total_cnt))

    #print("committing remaining probes...")
    connection.commit()

finally:
    print("done.")
    connection.close()
