#!/usr/local/bin/python3

# revtr_split_data.py
# -------------------
# usage:
# - ./revtr_split_data.py
#
# function:
# - create the following file hierarchy for test and training data
# --train-----vp_measurements (1/3 of input data)
#                    |
#                <vp_name>.csv
#                dnet, dst, [hops path]
# --test------vp_measurements (2/3 of input data)
#                    |
#                <vp_name>.csv
#                dnet, dst, [hops path]
#
# note:
# - the input JSON file is expected to have the following format
#    {
#       <bgp-routable-prefix>:
#           (collection of destination IP's)
#    }
# - "training" split will contain 1/3rd of 'unique dest-IPs' per bgp-routable-prefix
# - "test" split will contain 2/3rds of 'unique dest-IPs' per bgp-routable-prefix
# - In other words, "training" and "test" are guaranteed NOT to have the same
#   destination IPs, but will still properly split measurements from the same prefix. 

import sys
import os
import yaml
import json
import time
import csv
import threading
from collections import defaultdict

#==========================================================
# Configuration Parsing (YAML)
#==========================================================

try:
    configfile = open("./config.yml", 'r')
    configurations = yaml.load(configfile)
    configfile.close()
except (FileNotFoundError,IndexError):
    raise ValueError('error: config.yml either not found or incorrect.')


# flag indicating if the measurement files need to be downloaded or if they are stored locally
download = configurations['download']

# directory where VP files will be stored
vpdir = configurations['vpdir']

# directory where all output data should be stored
datadir = configurations['datadir']

# groupby indicates the which of the methods (prefix,asn,s24) we care about
groupby = configurations['groupby']

# sources / outputs
traindir = datadir + "/train/vp_measurements"
testdir = datadir + "/test/vp_measurements"
mapfile = datadir + "/mappings/dests_by_"+groupby+".json"

#==========================================================
# Core Data Splitting Functions
#==========================================================

# ddict
# dictionary that will hold decision to place in train/test for all destinations
# items look like {... , dest-ip: ("train"/"test", dnet-prefix)}
ddict = defaultdict(tuple)

# _tag
# given 'ip' tell me if 'ip' has been marked for "test" or for "train"
# returns the corresponding dset entry if it exists, else None
def _tag(ip):
    if ip in ddict.keys():
        return ddict[ip]
    else:
        return None

# _filterbydest
# given 'vpfile'.csv, return pings from that file for which dest is in ddict
# returns 3-tuple (tag, dest-ip, [ping-hops]), where tag = ("train/test", dnet-prefix)
def _filterbydest(vpfile):
    # print("Filtering VP: {}".format(vpfile))
    with open(vpdir + '/' + vpfile, 'r') as csvfile:
        datareader = csv.reader(csvfile)
        yield from map(lambda ping: (_tag(ping[0]), ping[0], ping[1:]),
            filter(lambda ping: _tag(ping[0]) != None, datareader))

# _writetask
# Given 'vpfile'.csv, create two new CSV files containing "train" and "test" data splits.
# The split is based on tags assigned to each destination IP address, to be found in ddict
def _writetask(vpfile):
    print("Thread executing write task for VP: {}...\n".format(vpfile))
    trainfile = open(traindir + '/' + groupby + '/' + vpfile, 'w')
    testfile = open(testdir + '/' + groupby + '/' + vpfile, 'w')
    for ((target, dnet), dest, hops) in _filterbydest(vpfile):
        if dnet == str(None):
            continue
        if target == "train":
            # print("TRAIN <-- {},{},{}".format(dnet, dest, ','.join(hops)))
            trainfile.write("{},{},{}\n".format(dnet, dest, ','.join(hops)))
        if target == "test":
            # print("TEST <-- {},{},{}".format(dnet, dest, ','.join(hops)))
            testfile.write("{},{},{}\n".format(dnet, dest, ','.join(hops)))
    trainfile.close()
    testfile.close()
    print("Thread completed write task for VP: {}\n".format(vpfile))

# WriteSet ...
# Spin off a _writetask thread for the given 'vpfile'
# Returns thread handle
def WriteSet(vpfile):
    t = threading.Thread(target=_writetask, args=(vpfile,), name=vpfile)
    t.start()
    return t

#==========================================================
# File/Dir Processing Functions
#==========================================================

# SetupDirs ...
# Ensures that the directories we need are correctly set up
def SetupDirs():
    if not os.path.exists(datadir+'/test/vp_measurements/'+groupby):
        os.makedirs(datadir+'/test/vp_measurements/'+groupby)
    if not os.path.exists(datadir+'/train/vp_measurements/'+groupby):
        os.makedirs(datadir+'/train/vp_measurements/'+groupby)

#==========================================================
# Main
#==========================================================

def main():
    start = time.time()

    # setup the needed directory structure
    SetupDirs()

    # Split the annotated, filtered measurement data into two sets
    num_prefixes = 0
    with open(mapfile, 'r') as jsonfile:
        json_data = json.load(jsonfile)
        for dnet, dests in json_data.items():
            # print("Analyzing BGP-routable prefix {}\n".format(dnet))
            if len(dests) > 1:
                index = 0
                num_prefixes = num_prefixes + 1
                for dest in dests:
                    # ~1/3rd of data goes in the training set
                    if index % 3 == 0:
                        ddict[dest] = ("train", dnet)
                    # ~2/3rds of data goes in the test set
                    else:
                        ddict[dest] = ("test", dnet)
                    # increment the index
                    index = index + 1

    print("Input contained {} unique, viable destination networks.\n".format(num_prefixes))
    print("Identified {} unique destination IPs to split.\n".format(len(ddict.keys())))

    # for each VP, create trianing and test CSV files
    # -- putting this in a separate loop avoids opening each vpfile several times
    # -- each VP will be processed in parallel with 'threading'
    threads = set()
    for vpfile in os.listdir(vpdir):
        print("Writing set for VP: {}\n".format(vpfile))
        threads.add(WriteSet(vpfile))

    # wait for all threads to complete
    for t in threads:
        print("Waiting for thread {} to complete...\n".format(t.name))
        t.join()
        print("Thread {} to completed.\n".format(t.name))
    
    end = time.time()
    print("time elapsed: {}".format(end-start)) 

if __name__ == "__main__":
    main()
