#!/usr/local/bin/python3

"""
For use with the run_case_study automated script.

Takes in destination IP and an annotated measurements file.
Spits out names of VPs < 9 hops away from destination.
"""

import sys
from collections import defaultdict

try:
    target_ip = sys.argv[1]
    test_meas_file = sys.argv[2]
except IndexError:
    print("usage: ./vps_in_dest_range.py <target ipv4> <test measurements file>")
    exit(1)

with open(test_meas_file, 'r') as f:
    for entry in f:
        articles = entry.split(",")
        vp, dist = articles[0].split(":")[0].split("/")[-1], int(articles[2])
        if dist < 9:
            print("{},{}".format(dist,vp))
