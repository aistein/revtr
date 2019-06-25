#! /usr/bin/python3 -u

# for each /24 in the target-networks list, count how many IPs from that network are in the database

import sys
import pymysql.cursors
from collections import defaultdict, Counter

targetfile = "./target_slash24s.it84w.txt"
with open(targetfile, 'r') as f:
    all_slash_24s = []
    for line in f:
        all_slash_24s.append(line.strip())

connection = pymysql.connect(host='localhost',
                             user='ubuntu',
                             password='',
                             db='MEASUREMENTS',
                             cursorclass=pymysql.cursors.DictCursor)

ips_by_slash_24 = {}
try:
    with connection.cursor() as cursor:
        for slash_24 in all_slash_24s:
            sql = "SELECT `dest_addr` FROM `destinations_tbl` WHERE `slash_24`=%s"
            cursor.execute(sql, (slash_24,))
            value_list = cursor.fetchall()
            #print("{} entries for slash_24 {}".format(len(value_list), slash_24))
            ips_by_slash_24[slash_24] = [""] if not value_list else [item['dest_addr'] for item in value_list]

finally:
    print("done.")
    connection.close()

TOLERANCE = 10
[print(slash_24) for slash_24 in ips_by_slash_24 if len(ips_by_slash_24[slash_24]) < TOLERANCE]
