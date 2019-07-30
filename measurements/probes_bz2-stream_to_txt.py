#! /usr/bin/python3 -u

# take output of print_datafile as streaming input of probe-records
# for each probe record, check if ICMP echo was successful and if so, write to destination IP to txt file

import sys

def isValidIP(ip):
    return len(ip.split('.')) == 4

def echoSucceeded(icmp_reply_type_and_code):
    # 0x03-- indicates "destination unreachable"
    # 0x08-- indicates "no reply"
    return (int(icmp_reply_type_and_code, 0) & 0x0B00) == 0

for line in sys.stdin:
    if line[0] == '0': # valid probe entry
        fields = line.split()
        typeandcode, probe_addr = fields[0], fields[4]
        if echoSucceeded( typeandcode ) and isValidIP( probe_addr ):
            print("{}".format(probe_addr))
