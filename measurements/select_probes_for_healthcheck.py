#! /usr/bin/python3 -u

# for each /24 in the target-networks list, pick 1 IP from the database
# the resultant list of IPs will be used in a preliminary scamper round for checking the validity of selected /24s

import sys
import pymysql.cursors

targetfile = "./target_slash24s.it84w.txt"
with open(targetfile, 'r') as tf:
    healthcheck_ip_by_slash_24 = {}
    for slash_24 in tf:
        healthcheck_ip_by_slash_24[slash_24.strip()] = ""

connection = pymysql.connect(host='localhost',
                             user='ubuntu',
                             password='',
                             db='MEASUREMENTS',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:
        for slash_24 in healthcheck_ip_by_slash_24.keys():
            sql = "SELECT `dest_addr` FROM `destinations_tbl` WHERE `slash_24`=%s LIMIT 1"
            cursor.execute(sql, (slash_24,))
            value = cursor.fetchone()
            #print("selected {} for slash_24 {}".format(value, slash_24))
            healthcheck_ip_by_slash_24[slash_24] = "" if not value else value['dest_addr']

finally:
    #print("done.")
    connection.close()


[print(healthcheck_ip_by_slash_24[slash_24]) for slash_24 in healthcheck_ip_by_slash_24]
