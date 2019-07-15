#! /usr/bin/python3 -u

import sys, os
import csv
import pymysql.cursors

# SS Cursor allows for streaming of large amounts of data returned in queries (generators)
stream_connection = pymysql.connect(host='localhost',
                             user='ubuntu',
                             password='',
                             db='MEASUREMENTS',
                             cursorclass=pymysql.cursors.SSDictCursor)

# Dict Cursor slower but easier to read into memory for smaller queries
dict_connection = pymysql.connect(host='localhost',
                             user='ubuntu',
                             password='',
                             db='MEASUREMENTS',
                             cursorclass=pymysql.cursors.DictCursor)

dump_dir = "/data/workspace/data/survey_vps"

try:

    # first, get the list of VPs from the database
    with dict_connection.cursor() as cursor:
        sql = "SELECT vp_name FROM vantage_points_tbl"
        cursor.execute(sql)
        vantage_points = [item['vp_name'] for item in cursor.fetchall()]

    with stream_connection.cursor() as cursor:
        sql = "SELECT `destinations_tbl`.`dest_addr`, `destinations_tbl`.`as_number`, `destinations_tbl`.`bgp_prefix`, `destinations_tbl`.`slash_24`," +\
                "`pings_tbl`.`hop_dist`, `pings_tbl`.`rr_slots`, `vantage_points_tbl`.`vp_name` " +\
              "FROM `pings_tbl` JOIN `destinations_tbl` ON `pings_tbl`.`dest_id` = `destinations_tbl`.`dest_id` " +\
              "JOIN `vantage_points_tbl` ON `pings_tbl`.`vp_id` = `vantage_points_tbl`.`vp_id` " +\
              "WHERE `vantage_points_tbl`.`vp_name`=%s AND `pings_tbl`.`rr_responsive`='Y'"

        for vp in vantage_points:

            cursor.execute(sql, (vp,))
            filename = vp + '.csv'

            num_rows = 0
            with open( os.path.join( dump_dir, filename ), 'w' ) as f:
                for row in cursor.fetchall_unbuffered():
                    f.write("{},{}\n".format(row['dest_addr'], row['rr_slots']))
                    num_rows += 1
            
            print("Completed writing {} rows to CSV for VP {}.".format(num_rows, vp))

finally:
    print("done.")
    dict_connection.close()
    stream_connection.close()
