#! /usr/bin/python3 -u

import sys, os
import socket, pyasn
import pymysql.cursors

from collections import namedtuple

try:
    ipasnfile = sys.argv[1]
    outputdir = sys.argv[2]
except IndexError:
    sys.exit("usage: ./load_destinations_into_database.py <ipasnfile> <output-dir>")

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

ipasn = pyasn.pyasn(ipasnfile)

connection = pymysql.connect(host='rtr-offline-aurora-db-inst.cdutp9pdmnzk.us-east-2.rds.amazonaws.com',
                             user='as5281',
                             password='datapants',
                             db='measurements',
                             local_infile=True,
                             cursorclass=pymysql.cursors.DictCursor)

try:

    destinations = None

    with connection.cursor() as cursor:

        sql = "SELECT DISTINCT dst_index FROM pings_tbl;"

        while True:
            try:
                cursor.execute(sql)
                destinations = set([entry['dst_index'] for entry in cursor.fetchall()])
            except pymysql.err.OperationalError:
                continue
            break

        # write the destinations and metainfo out to CSV format for bulk loading into the DB
        with open(os.path.join(outputdir,'destinations.csv'), 'w') as f:
            entries = []
            for dst_index in destinations:
                # get networks as strings and convert to numbers
                as_number_str, bgp_prefix_str, slash_24_str = dnets_of( ipv4_address_of(dst_index) , ipasn)
                as_number = int(as_number_str) if as_number_str else -1
                bgp_prefix = ipv4_index_of(bgp_prefix_str.split('/')[0]) if bgp_prefix_str else -1
                bgp_prefix_length = int(bgp_prefix_str.split('/')[1]) if bgp_prefix_str else -1
                slash_24 = ipv4_index_of(slash_24_str.split('/')[0])

                entries.append("{},{},{},{},{}".format(dst_index, as_number, bgp_prefix, bgp_prefix_length, slash_24))
            for entry in entries:
                f.write("{}\n".format(entry))

        # now write the CSV into the DB
        full_filepath = os.path.join(outputdir,'destinations.csv')
        sql = "LOAD DATA LOCAL INFILE '{}' ".format(full_filepath) +\
              "INTO TABLE destinations_tbl " +\
              "FIELDS TERMINATED BY ',';"
        while True:
            try:
                cursor.execute(sql)
            except pymysql.err.OperationalError:
                continue
            break
        connection.commit()

finally:
    print("done.")
    connection.close()

