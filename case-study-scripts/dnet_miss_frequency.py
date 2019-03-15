#!/usr/local/bin/python3

import sys
import csv
from collections import defaultdict

try:
    missing_file = sys.argv[1]
except:
    print("usage: ./dnet_miss_frequency.py <filename>")
    exit(1)

frequency_by_dnet = defaultdict(int)
with open(missing_file, 'r') as f:
    r = csv.reader(f, delimiter=',')
    for line in r:
        dnet = line[1]
        frequency_by_dnet[dnet] += 1

for dnet, frequency in sorted(frequency_by_dnet.items(), key=lambda x : x[1], reverse=True):
    print("dnet: {}, frequency: {}".format(dnet, frequency))
