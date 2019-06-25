#! /usr/bin/python3 -u

# take output of print_datafile as streaming input of probe-records
# for each probe record, check if ICMP echo was successful and if so, insert into MySQL database

import sys
import pymysql.cursors

vpsfile= "./sliceinfo/20190624/candidate_nodes.txt"
with open(vpsfile, 'r') as vpf:
    vps = set()
    for vp in vpf:
        vps.add(vp.strip())

connection = pymysql.connect(host='localhost',
                             user='ubuntu',
                             password='',
                             db='MEASUREMENTS',
                             cursorclass=pymysql.cursors.DictCursor)

try:

    with connection.cursor() as cursor:

        sql = "INSERT INTO `vantage_points_tbl` (`vp_name`) VALUES (%s) " +\
              "ON DUPLICATE KEY UPDATE `vp_name`=%s"

        for vp in vps:

                    print("loading vp {}".format(vp))
                    args = (vp, vp)
                    
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
