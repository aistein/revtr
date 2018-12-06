#!/usr/bin/python

# revtr_map_dests_to_dnet.py
# - parse all VP CSV files, one-by-one from the online source
# - filter out paths > 9 hops
# - write out a JSON file that maps each destination to its BGP-prefix
# - PURPOSE: the above file will be used to split data into test and training sets
# usage:
# - ./revtr_map_dests_to_dnet.py
# note:
# - be sure to update the BGP datadump before running this script
# - to do so, run ./scripts/refresh

# for data parsing
import pyasn
import threading
import pickle
import yaml
import json
import time
import os
import sys
import csv
from collections import defaultdict

# for file download, etc.
import re, pycurl
from bs4 import BeautifulSoup

#==========================================================
# Configuration Parsing (YAML)
#==========================================================

try:
    credfile = open("./credentials.yml", 'r')
    configfile = open("./config.yml", 'r')
    configurations = {**yaml.load(credfile), **yaml.load(configfile)}
    credfile.close()
    configfile.close()
except (FileNotFoundError,IndexError):
    raise ValueError('error: config.yml or credentials.yml files either not found or incorrect.')

# flag indicating if the measurement files need to be downloaded or if they are stored locally
download = configurations['download']

# rooturl where measurement files will be downloaded from
rooturl = configurations['rooturl']

# bgpdumpfile containing DB to be used for IP<-->Prefix mapping
bgpdumpfile = configurations['bgpdump']

# directory where VP files will be stored
vpdir = configurations['vpdir']

# directory where all intermediate and output data should be stored
datadir = configurations['datadir']

# the number of VPs we'd like to process
numvps = configurations['numvps']

# the number of CPUs available on the running machine
numcpus = configurations['numcpus']

# username and password
userpwd = configurations['username']+":"+configurations['password']

#==========================================================
# BGP Processing Functions
#==========================================================

# asndb
# the BGP-dump DB must be global
# - it will be accessed concurrently by multiple threads   
asndb = pyasn.pyasn(bgpdumpfile)

# _numhops
# returns either the number of hops this ping takes to reach dst, or 10 if never
def _numhops(ping):
    dest = ping[0]
    dist = 0
    for hop in ping[1:]:
        dist = dist + 1
        if hop == dest:
            break
    return dist

# _lookuptask
# Query the BGP-dump database with IP, and store value in destinfo[] dict.
# Additionally, append the IP's corresponding /24 CIDR prefix to the dict.
# No locking is needed! Python's core data structures are all thread-safe
# https://docs.python.org/3/glossary.html#term-global-interpreter-lock
def _lookuptask(ip, destinfo):
    try:
        s24 = '.'.join(ip.split('.')[:3]) + ".0/24"
        destinfo[ip] = asndb.lookup(ip) + (s24,)
    except:
        return

# _prefixinfo
# spin off a _lookuptask(ip) thread
# returns tuple(thread-handle, ip-address)
def _prefixinfo(ip, destinfo):
    t = threading.Thread(target=_lookuptask(ip,destinfo), name=ip+".prefixer")
    t.start()
    return (t, ip)

# FilterAndCount ...
# Remove pings from the input CSV file for which the destination is never reached
# Yields ip-address
# Adapted from StackOverflow post: https://stackoverflow.com/a/17444799/3341596
def FilterAndCount(vp):
    if not download or os.path.isfile(vpdir + '/' + vp + ".csv"):
        filename = vpdir + '/' + vp + ".csv"
    else:
        filename = cget(vp+'.csv')
    with open(filename, "r") as csvfile:
        datareader = csv.reader(csvfile)
        # filter and count
        yield from map(lambda ping: ping[0],
            filter(lambda ping: _numhops(ping) <= 9, datareader))

    # these files are large, don't want to keep them around!
    # TODO: eventually uncomment and remove the files... need them now for data-splitting
    # os.remove(filename)
    return

# MapPrefixes ...
# Take filtered output and return the destination IP address; 
# Collect corresponding prefix info in the background
def MapPrefixes(vp,destinfo):
    dests = FilterAndCount(vp)
    # the below line will yeild a 2-tuple (thread_handle, IP address)
    yield from map(lambda dest: _prefixinfo(dest,destinfo), dests)
    return

# GroupTask ...
# Group the (filtered) set of destinations by the configured method
# Note: this is the first function that forces an actual read! (doesn't "yeild")
def GroupTask(vp):

    # if we've already got a pickle file containing the mappings, skip!
    if os.path.isfile(datadir + '/tmp/' + vp + ".pkl"):
        return

    # destinfo holds the mapping information for each destination
    destinfo = {}

    # dest_by_*
    # dictionaries to hold groupings for each grouping method
    dest_by_prefix = defaultdict(set)
    dest_by_asn = defaultdict(set)
    dest_by_s24 = defaultdict(set)

    # fill the grouping dicts
    for entry in MapPrefixes(vp, destinfo):
        # parse apart the returned entry
        thread = entry[0]    # thread handle
        dest = entry[1]      # destination ip-address

        # wait for the generated (updating destinfo) thread to finish
        thread.join()

        # gather the output
        if dest in destinfo:
            dinfo = destinfo[dest]
            asn = str(dinfo[0])     # ASN
            prefix = str(dinfo[1])  # BGP-routable prefix
            s24 = str(dinfo[2])     # /24 prefix
        else:
            continue

        # DB lookup returned None, don't process
        if prefix == str(None) or asn == str(None):
            continue

        # Only add dest if the lookup method worked (/24 always does)
        if prefix != str(None):
            dest_by_prefix[prefix].add(dest)
        if asn != str(None):
            dest_by_asn[asn].add(dest)
        dest_by_s24[s24].add(dest)
        
        print("From VP:{}, mapped dest:{} to prefix {}, asn {}, s24 {}.".format(
            vp, dest, prefix, asn, s24))

    # store the groupings in intermediate pickle file
    # - stored as 3 pickle objects -> will have to be read in loop
    filename = datadir + '/tmp/' + vp + ".pkl"
    with open(filename, 'wb') as pklfile:
        pickle.dump(dest_by_prefix, pklfile)
        pickle.dump(dest_by_asn, pklfile)
        pickle.dump(dest_by_s24, pklfile)

    return

#==========================================================
# File/Dir Processing Functions
#==========================================================

# cget
# the python wget package has no options, so using curl instead
if download:
    def cget(extension):
        if extension == "":
            filename = vpdir + '/vplist'
        else:
            filename = vpdir + '/' + extension
        with open(filename, 'wb') as f:
            url = rooturl + extension
            c = pycurl.Curl()
            c.setopt(c.URL, url)
            c.setopt(c.USERPWD, userpwd)
            c.setopt(c.WRITEDATA, f)
            c.perform()
            c.close()
        return filename

# SetupDirs ...
# Ensures that the directories we need are correctly set up
def SetupDirs():
    if not os.path.exists(datadir+'/mappings'):
        os.makedirs(datadir+'/mappings')
    if not os.path.exists(datadir+'/vps') and download:
        os.makedirs(datadir+'/vps')
    if not os.path.exists(datadir+'/tmp'):
        os.makedirs(datadir+'/tmp')

# DumpMappings ...
# Read in the intermediate mapping files, dump their contents
# to the central JSON files, and delete them.
def DumpMappings():

    # dicts to load pickles into
    dest_by_prefix = defaultdict(set)
    dest_by_asn = defaultdict(set)
    dest_by_s24 = defaultdict(set)

    # global files to dump dicts out to
    prefixfile = open(datadir+'/mappings/dests_by_prefix.json', 'w')
    asnfile = open(datadir+'/mappings/dests_by_asn.json', 'w')
    s24file = open(datadir+'/mappings/dests_by_s24.json', 'w')

    # read pickles, append into dicts
    for vpkl in os.listdir(datadir+'/tmp'):
        with open(datadir+'/tmp/'+vpkl, 'rb') as pklfile:
            # first by bgp-prefix
            for prefix, dests in pickle.load(pklfile).items():
                # print("DumpMappings: prefix {} gets dests {}\n".format(
                #     prefix, list(dests)))
                if prefix in dest_by_prefix:
                    dest_by_prefix[prefix].union(dests)
                else:
                    dest_by_prefix[prefix] = dests
            # then by asn
            for asn, dests in pickle.load(pklfile).items():
                # print("DumpMappings: asn {} gets dests {}\n".format(
                #     asn, list(dests)))
                if asn in dest_by_asn:
                    dest_by_asn[asn].union(dests)
                else:
                    dest_by_asn[asn] = dests
            # finally by /24
            for s24, dests in pickle.load(pklfile).items():
                # print("DumpMappings: s24 {} gets dests {}\n".format(
                #     s24, list(dests)))
                if s24 in dest_by_s24:
                    dest_by_s24[s24].union(dests)
                else:
                    dest_by_s24[s24] = dests

    # dump pickles out to json  
    json.dump({ k:list(v) for k, v in dest_by_prefix.items() }, prefixfile)
    json.dump({ k:list(v) for k, v in dest_by_asn.items() }, asnfile)
    json.dump({ k:list(v) for k, v in dest_by_s24.items() }, s24file)  
    
    # close the json files
    prefixfile.close()
    asnfile.close()
    s24file.close()

    # delete the intermediate pickle files
    # TODO: uncomment for space savings!
    # for pklfile in os.listdir(datadir+'/tmp'):
    #     os.remove(datadir+'/tmp/'+pklfile)

#==========================================================
# Main
#==========================================================

def main():
    start = time.time()

    # setup the needed directory structure
    SetupDirs()

    # get and process the list of vp-csv files
    vplist = set()
    if not download: # operating on measurement server
        for vpfile in os.listdir(vpdir):
            vplist.add(os.path.splitext(vpfile)[0])
    else: # remote system; must download measurements
        vplisthtml = cget("")
        soup = BeautifulSoup(open(vplisthtml,'r'),'html.parser')
        num_to_read = numvps
        for hit in soup.find_all('a'):
            match = re.match(r'^.*csv', hit['href'])
            if match and num_to_read > 0:
                # splitext removes the ".csv" postfix
                vplist.add(os.path.splitext(match[0])[0])
                num_to_read = num_to_read - 1
        # done extracting, remove the vp-html-file
        os.remove(vplisthtml)

    # deploy 'in parallel' one _grouptask per VP
    # - only doing 10 at a time so as not to overwhelm RAM
    max_threads = numcpus
    thread_cnt = 0
    threads = set()
    for vp in vplist:
        t = threading.Thread(target=GroupTask, args=(vp,), name=vp+'.grouper')
        t.start()
        threads.add(t)
        thread_cnt = thread_cnt + 1
        print("Deployed group task for " + vp + "...")

        # once numcpus threads are deployed, wait for them to finish before moving on
        if thread_cnt == max_threads:
            thread_cnt = 0
            for t in threads:
                print("Waiting for thread {} to complete...\n".format(t.name))
                t.join()
                threads.remove(t)
                print("Thread {} to completed.\n".format(t.name))


    # wait for all remaining _grouptask threads to complete
    for t in threads:
        print("Waiting for thread {} to complete...\n".format(t.name))
        t.join()
        print("Thread {} to completed.\n".format(t.name))

    # dump mappings into json files
    DumpMappings()

    end = time.time()
    print("time elapsed: {}".format(end-start)) 
    
if __name__ == "__main__":
    main()
