#! /usr/bin/python3 -u

import sys
import pymysql.cursors

connection = pymysql.connect(host='localhost',
                             user='ubuntu',
                             password='',
                             db='MEASUREMENTS',
                             cursorclass=pymysql.cursors.DictCursor)

try:

    with connection.cursor() as cursor:

        sql = "SELECT `slash_24` FROM `destinations_tbl` WHERE `dest_id` IN ( SELECT `dest_id` FROM `pings_tbl` WHERE `rr_responsive`='N' )"
        cursor.execute(sql)
        bad_slash_24s = [item['slash_24'] for item in cursor.fetchall()]

finally:
    print("done.")
    connection.close()

[print(s24) for s24 in bad_slash_24s]
