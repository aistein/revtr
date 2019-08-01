#! /usr/bin/python3 -u

# take output of print_datafile as streaming input of probe-records
# for each probe record, check if ICMP echo was successful and if so, insert into MySQL database

import sys, os, csv
import socket, pyasn
import pymysql.cursors
from collections import namedtuple

try:
    vpcsvdir = sys.argv[1]
    ipasnfile = sys.argv[2]
except IndexError:
    sys.exit("usage: ./load_vps_into_database.py <vp-csv-directory-path> <bgp-dumpfile>")

connection = pymysql.connect(host='rtr-offline-aurora-db-inst.cdutp9pdmnzk.us-east-2.rds.amazonaws.com',
                             user='as5281',
                             password='datapants',
                             db='measurements',
                             local_infile=True,
                             cursorclass=pymysql.cursors.DictCursor)

DnetEntry = namedtuple('DnetEntry',['asn','bgp','s24'])

def s24_of(ip):
    octets = ip.split('.')
    return ".".join(octets[:3]) + ".0/24"

def dnets_of(ip, ipasn):
    try:
        return DnetEntry(*(ipasn.lookup(ip)), s24_of(ip))
    except:
        return DnetEntry('','', s24_of(ip))

def ipv4_index_of( addr ):
    try:
        as_bytes = socket.inet_aton(addr)
    except OSError as e:
        sys.exit("on trying socket.inet_aton({}), received error {}".format(addr, e))
    return int.from_bytes(as_bytes, byteorder='big', signed=False)

def ipv4_address_of( index ):
    as_bytes = index.to_bytes(4, byteorder='big', signed=False)
    return socket.inet_ntoa(as_bytes)

ipasn = pyasn.pyasn(ipasnfile)

try:

    with connection.cursor() as cursor:

        sql = "INSERT INTO `vantage_points_tbl` " +\
              "(`as_number`,`bgp_prefix`,`bgp_prefix_length`,`slash_24`,`src_index`,`vpname`,`pop`) " +\
              "VALUES (%s,%s,%s,%s,%s,%s,%s);"

        for vpcsv in os.listdir(vpcsvdir):
            vpname = '.'.join(vpcsv.split('.')[:-1]) # cut off the '.csv' file extension
            pop = '.'.join(vpname.split('.')[1:]) # cut off the machine-specific part of the name
            with open(os.path.join(vpcsvdir,vpcsv), 'r') as f:
                r = csv.reader(f, delimiter=',')
                for line in r: 
                    src_index = int(line[2])
                    print("loading vp {} with index {}".format(vpname, src_index))

                    as_number_str, bgp_prefix_str, slash_24_str = dnets_of( ipv4_address_of( src_index ), ipasn ) 
                    as_number = int(as_number_str) if as_number_str else -1
                    bgp_prefix = ipv4_index_of(bgp_prefix_str.split('/')[0]) if bgp_prefix_str else -1
                    bgp_prefix_length = int(bgp_prefix_str.split('/')[1]) if bgp_prefix_str else -1
                    slash_24 = ipv4_index_of(slash_24_str.split('/')[0])

                    args = (as_number, bgp_prefix, bgp_prefix_length, slash_24, src_index, vpname, pop)
                    print("\t ASN, BGP, BGP-LEN, S24, INDEX, NAME, POP = {}".format( args ))
                    
                    # retry loop to handle deadlocks
                    while True:
                        try:
                            cursor.execute(sql, (*args,))
                        except pymysql.err.OperationalError:
                            continue
                        break
                    
                    # only actually need to inspect a single entry per VP csv file
                    break

    print("committing...")
    connection.commit()

finally:
    print("done.")
    connection.close()
