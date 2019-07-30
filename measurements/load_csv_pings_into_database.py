#! /usr/bin/python3 -u

import os, sys
import pymysql.cursors

try:
    csvdir = sys.argv[1]
    csvfile = sys.argv[2]
    rr_enabled = True if sys.argv[3] == 'rr' else False
except IndexError:
    sys.exit("usage: ./load_csv_pings_into_database.py <csv-dir> <csv-file-name>.csv <rr/ping>")

blacklist = [
        ]


connection = pymysql.connect(host='rtr-offline-aurora-db-inst.cdutp9pdmnzk.us-east-2.rds.amazonaws.com',
                             user='as5281',
                             password='datapants',
                             db='measurements',
                             local_infile=True,
                             cursorclass=pymysql.cursors.DictCursor)

try:

    with connection.cursor() as cursor:

        full_filepath = os.path.join(csvdir, csvfile)
        table = 'rr_pings_tbl' if rr_enabled else 'pings_tbl'
        sql = "LOAD DATA LOCAL INFILE '{}' ".format(full_filepath) +\
              "INTO TABLE {} ".format(table) +\
              "FIELDS TERMINATED BY ',' " +\
              "SET ping_id = NULL;"

        vp_name = '.'.join(csvfile.split('.')[:-1])
        print("bulk-loading csv for VP {}...".format(vp_name))

        while True:
            try:
                cursor.execute(sql)
            except pymysql.err.OperationalError:
                continue
            break

        print("committing pings from VP {}...".format(vp_name))
        connection.commit()

finally:
    print("done.")
    connection.close()
