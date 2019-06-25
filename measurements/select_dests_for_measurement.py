#! /usr/bin/python3 -u

import sys
import random
import pymysql.cursors
from collections import defaultdict

targetfile = "./target_slash24s.it84w.txt"
with open(targetfile, 'r') as tf:
    target_slash_24s = []
    for s24 in tf:
        target_slash_24s.append(s24.strip())

connection = pymysql.connect(host='localhost',
                             user='ubuntu',
                             password='',
                             db='MEASUREMENTS',
                             cursorclass=pymysql.cursors.DictCursor)

ips_by_slash_24 = defaultdict(list)
shuffled_ips = []

try:
    with connection.cursor() as cursor:
        for slash_24 in target_slash_24s:
            sql = "SELECT `dest_addr` FROM `destinations_tbl` WHERE `slash_24`=%s LIMIT 70"
            cursor.execute(sql, (slash_24,))
            values = cursor.fetchall()
            print("selected {} ips for slash_24 {}".format(len(values), slash_24))
            ips_by_slash_24[slash_24] = [] if not values else [value['dest_addr'] for value in values]
            shuffled_ips.extend([ip for ip in ips_by_slash_24[slash_24]])

finally:
    #print("done.")
    connection.close()

random.shuffle(shuffled_ips)

with open('./survey_dsts.txt', 'w') as f:
    for ip in shuffled_ips:
        f.write("{}\n".format(ip))

