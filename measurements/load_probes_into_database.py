#! /usr/bin/python3 -u

# take output of print_datafile as streaming input of probe-records
# for each probe record, check if ICMP echo was successful and if so, insert into MySQL database
# also, do asn/bgp-prefix lookup on destination so that this information is accessible later on

import sys
import time
import socket, pyasn
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

def ipv4_index_of( addr ):
    try:
        as_bytes = socket.inet_aton(addr)
    except OSError as e:
        sys.exit("on trying socket.inet_aton({}), received error {}".format(addr, e))
    return int.from_bytes(as_bytes, byteorder='big', signed=False)

def ipv4_address_of( index ):
    as_bytes = index.to_bytes(4, byteorder='big', signed=False)
    return socket.inet_ntoa(as_bytes)

def echoSucceeded(icmp_reply_type_and_code):
    # 0x03-- indicates "destination unreachable"
    # 0x08-- indicates "no reply"
    return (int(icmp_reply_type_and_code, 0) & 0x0B00) == 0

ipasnfile = "../bgpdumps/2019-01-30.dat"
ipasn = pyasn.pyasn(ipasnfile)

targetfile = "./new_target_slash24s.it84w.txt"
with open(targetfile, 'r') as tf:
    target_networks = set()
    for slash_24 in tf:
        target_networks.add(slash_24.strip())

connection = pymysql.connect(host='rtr-offline-aurora-db-inst.cdutp9pdmnzk.us-east-2.rds.amazonaws.com',
                             user='as5281',
                             password='datapants',
                             db='measurements',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    unique_probe_addrs, probe_cnt, total_cnt = set(), 0, 0

    with connection.cursor() as cursor:

        #sql = "INSERT INTO `destinations_tbl` (`dst_index`, `as_number`, `bgp_prefix`, `bgp_prefix_length`, `slash_24`) VALUES (%s, %s, %s, %s, %s) " +\
        #      "ON DUPLICATE KEY UPDATE `dst_index`=%s, `as_number`=%s, `bgp_prefix`=%s, `bgp_prefix_length`=%s, `slash_24`=%s"
        sql = "INSERT INTO `destinations_tbl` (`dst_index`, `as_number`, `bgp_prefix`, `bgp_prefix_length`, `slash_24`) VALUES (%s, %s, %s, %s, %s)"

        print("connection established, loading pings...")
        multiple_rows_args = []

        for line in sys.stdin:
            if line[0] == '0': # valid probe entry

                fields = line.split()
                typeandcode, probe_addr = fields[0], fields[4]
                if not isValidIP( probe_addr ):
                    continue

                probe_index = ipv4_index_of(probe_addr)

                #print("processing probe towards {}".format(probe_addr))

                # get networks as strings and convert to numbers
                as_number_str, bgp_prefix_str, slash_24_str = dnets_of( probe_addr, ipasn)
                #print("asn {}, bgp {}, s24 {}".format(as_number_str, bgp_prefix_str, slash_24_str))
                as_number = int(as_number_str) if as_number_str else -1
                bgp_prefix = ipv4_index_of(bgp_prefix_str.split('/')[0]) if bgp_prefix_str else -1
                bgp_prefix_length = int(bgp_prefix_str.split('/')[1]) if bgp_prefix_str else -1
                slash_24 = ipv4_index_of(slash_24_str.split('/')[0])

                if echoSucceeded( typeandcode ) and (slash_24_str in target_networks) and (probe_addr not in unique_probe_addrs):
                    #print("loading probe_addr {} in prefix {}/{}".format(probe_addr, bgp_prefix, bgp_prefix_length))
                    args = (probe_index, as_number, bgp_prefix, bgp_prefix_length, slash_24)
                    print(args)
                    multiple_rows_args.append( args )
                    
                    ## retry loop to handle deadlocks
                    #while True:
                    #    try:
                    #        cursor.execute(sql, (*args, *args))
                    #    except pymysql.err.OperationalError:
                    #        continue
                    #    break

                    unique_probe_addrs.add(probe_addr)
                    probe_cnt, total_cnt = probe_cnt + 1, total_cnt + 1

                    #if probe_cnt == 100: # make a commit every 100 probes cleared
                    if probe_cnt == 1000: # make a commit every 1000 probes cleared
                        # retry loop to handle deadlocks
                        while True:
                            try:
                                cursor.executemany(sql, multiple_rows_args)
                            except pymysql.err.OperationalError:
                                continue
                            break
                        connection.commit()
                        probe_cnt, multiple_rows_args = 0, []

                    if (total_cnt % 1000) == 0:
                        print("{} probes committed so far...".format(total_cnt))

    print("committing remaining probes...")
    connection.commit()

finally:
    print("done.")
    connection.close()
