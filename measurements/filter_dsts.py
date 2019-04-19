#! /usr/bin/python3 -u 
# usage: ./filter_dests.py < path/to/dest/file.txt > /path/to/filtered/dest/file.txt

import sys

def is_bad_ipv4(ip):
    try:
        # Private IP address ranges: 10.0.0.0/8, 172.16.0.0/16, 192.168.0.0/16
        oct0, oct1, oct2, oct3 = [int(octet) for octet in ip.split('.')]
        if oct0 == 10 or (oct0 == 172 and oct1 == 16) or (oct0 == 192 and oct1 == 168):
            return True
    except ValueError:
        # if the IPv4 address has a bad format, just mark it 'bad' and continue
        return True
    return False

for rawip in sys.stdin:
    if not is_bad_ipv4(rawip.strip()):
        sys.stdout.write(rawip.strip() + '\n')
