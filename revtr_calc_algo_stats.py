#! /usr/local/bin/python3
import os
import sys
import csv
import pyasn
import pickle
import yaml
import json
from ipaddress import ip_network
from collections import defaultdict

#==========================================================
# Configuration Parsing (YAML)
#==========================================================

try:
    configfile = open("./revtr.calc_algo_stats.config.yml", 'r')
    configurations = yaml.load(configfile)
    configfile.close()
except (FileNotFoundError,IndexError):
    raise ValueError('error: revtr.calc_algo_stats.config.yml file either not found or incorrect.')

print("Configurations:\n=======================")

# results directory
results_dir = configurations['results_dir']
print("Results Directory: {}".format(results_dir))

# rankings directory
rankings_dir = configurations['rankings_dir']
print("Rankings Directory: {}".format(rankings_dir))

# for set-cover, number of VPs to consider
K = configurations['K']
print("Number of Probes for Set Cover: {}".format(K))

# type of destination network (one of bgp, s24, asn)
dnet_type = configurations['dnet_type']
print("DNET Type: {}".format(dnet_type))

# probe directory
probe_dir = configurations['probe_dir']
if dnet_type == 'bgp':
    probe_dir = os.path.join(probe_dir, 'prefix')
if dnet_type == 'asn':
    probe_dir = os.path.join(probe_dir, 'asn')
if dnet_type == 's24':
    probe_dir = os.path.join(probe_dir, 's24')
print("Probe Directory: {}".format(probe_dir))

# have the rankings been refreshed? if so, any saved pickles will need to be rewritten
refresh = configurations['refresh']
print("Using New Rankings? {}".format(refresh))

# (optional) location of bgpdump file (could be None)
ipasn_file = configurations['ipasn_file']
print("BGP Dump File: {}".format(ipasn_file))

bgpdb = None

def setupdirs():
    if not os.path.exists(os.path.join(results_dir, "tmp")):
        os.makedirs(os.path.join(results_dir, "tmp"))
    if not os.path.exists(os.path.join(results_dir, dnet_type, "tmp")):
        os.makedirs(os.path.join(results_dir, dnet_type, "tmp"))

def slash_24_of(ip_str):
    dots = ip_str.strip().split('.')
    dots[-1] = '0/24'
    return '.'.join(dots)

def bgp_prfx_of(ip_str):
    _, prfx = bgpdb.lookup(ip_str)
    return prfx

def asn_of(ip_str):
    asn, _ = bgpdb.lookup(ip_str)
    return str(asn)

if __name__ == '__main__':

    setupdirs()

    bgpdb = pyasn.pyasn(ipasn_file)
    subdir = ''
    if dnet_type not in ['bgp', 'asn', 's24']:
        exit('Illegal dnet type: \'{}\''.format(dnet_type))
    if dnet_type == 's24':
        dnet_lookup = slash_24_of
        subdir = 's24'
    if dnet_type == 'asn':
        dnet_lookup = asn_of
        subdir = 'asn'
    if dnet_type == 'bgp':
        dnet_lookup = bgp_prfx_of
        subdir = 'bgp'

    # dictify dist-by-dest
    vp_dist_by_dest = defaultdict(list)
    dbd_pickle = os.path.join(results_dir, "tmp", "dist_by_dest.pkl")
    if os.path.isfile(dbd_pickle) and not refresh:
        with open(dbd_pickle, 'rb') as dbdpkl:
            vp_dist_by_dest = pickle.load(dbdpkl)
    else:
        for csv_name in os.listdir(probe_dir):
            vp = csv_name.replace('.csv', '')

            with open(os.path.join(probe_dir, csv_name), 'r') as f:
                for row in csv.reader(f):
                    dnet = row[0]
                    dest = row[1]
                    dist = row[2:].index(dest) + 1 if dest in row[2:] else -1

                    vp_dist_by_dest[dest].append((vp, dist))

        dbdpkl = open(dbd_pickle, 'wb')
        pickle.dump(vp_dist_by_dest, dbdpkl)
        dbdpkl.close()

        # with open(dist_by_dest_file, 'r') as f:
            # vp_dist_by_dest = json.load(f)

    # listify set cover rankings
    set_cover_rankings = []
    set_cover_pickle = os.path.join(results_dir, dnet_type, "tmp", "set_cover_rankings.pkl")
    if os.path.isfile(set_cover_pickle) and not refresh:
        with open(set_cover_pickle, 'rb') as scpkl:
            set_cover_rankings = pickle.load(scpkl)
    else:
        with open(os.path.join(rankings_dir, "set_cover_rankings.csv"), 'r') as f:
            datareader = csv.reader(f)
            next(datareader)
            for i, line in enumerate(csv.reader(f)):
                if i == K:
                    break
                set_cover_rankings.append(os.path.splitext(line[0])[0])

        scpkl = open(set_cover_pickle, 'wb')
        pickle.dump(set_cover_rankings, scpkl)
        scpkl.close()
        
    print("--DEBUG-- sc: {}".format(len(set_cover_rankings)))

    # setify ingress cover rankings
    ingr_rankings_by_dnet = {}
    ingress_cover_pickle = os.path.join(results_dir, dnet_type, "tmp", "ingress_cover_rankings.pkl")
    if os.path.isfile(ingress_cover_pickle) and not refresh:
        with open(ingress_cover_pickle, 'rb') as icpkl:
            ingress_cover_ranking = pickle.load(icpkl)
    else:
        icpckl = open(ingress_cover_pickle, 'wb')
        with open(os.path.join(rankings_dir, "ingress_cover", subdir, 'rankings_by_dnet.csv'), 'r') as f:
            for row in csv.reader(f):
                ingr_rankings_by_dnet[row[0]] = row[1:]

        icpkl = open(ingress_cover_pickle, 'wb')
        pickle.dump(ingr_rankings_by_dnet, icpkl)
        icpkl.close()

    print("--DEBUG-- ingr: {}".format(len(ingr_rankings_by_dnet)))

    # setify destination cover rankings
    dst_rankings_by_dnet = {}
    destination_cover_pickle = os.path.join(results_dir, dnet_type, "tmp", "destination_cover_rankings.pkl")
    if os.path.isfile(destination_cover_pickle) and not refresh:
        with open(destination_cover_pickle, 'rb') as dcpkl:
            dst_rankings_by_dnet = pickle.load(dcpkl)
    else:
        with open(os.path.join(rankings_dir, "destination_cover", subdir, 'rankings_by_dnet.csv'), 'r') as f:
            for row in csv.reader(f):
                dst_rankings_by_dnet[row[0]] = row[1:]

        dcpkl = open(destination_cover_pickle, 'wb')
        pickle.dump(dst_rankings_by_dnet, dcpkl)
        dcpkl.close()

    print("--DEBUG-- dst: {}".format(len(dst_rankings_by_dnet)))

    # redirect stdout to write to a file
    with open(os.path.join(results_dir, dnet_type, dnet_type + "_fucking_results.goddamn"), 'w') as sys.stdout:
        print('# <destination>,<dnet>,<optimal_vp>,<optimal_dist>,<set_vp>,<set_dist>,<set_pings>,<ingr_vp>,<ingr_dist>,<inr_pings>,<dest_vp>,<dest_dist>,<dest_pings>')
        bad = 0
        for dest, vp_dist in vp_dist_by_dest.items():
            opt = [elem for elem in sorted(vp_dist, key=lambda x: int(x[1]) if int(x[1]) > 0 else 10)][0]
            dnet = dnet_lookup(dest)
            sys.stdout.write('{},{},{},{},'.format(dest, dnet, opt[0], opt[1]))

            dists = {vp: dist for vp, dist in vp_dist}
            found = False
            for i, vp in enumerate(set_cover_rankings):
                if vp in dists and dists[vp] > -1:
                        sys.stdout.write('{},{},{},'.format(vp, dists[vp], i + 1))
                        found = True
                        break
            if not found:
                sys.stdout.write(',,,')

            found = False
            if dnet in ingr_rankings_by_dnet:
                for i, vp in enumerate(ingr_rankings_by_dnet[dnet]):
                    if vp in dists and dists[vp] > -1:
                        sys.stdout.write('{},{},{},'.format(vp, dists[vp], i + 1))
                        found = True
                        break
            if not found:
                sys.stdout.write(',,,')

            found = False
            if dnet in dst_rankings_by_dnet:
                for i, vp in enumerate(dst_rankings_by_dnet[dnet]):
                    if vp in dists and dists[vp] > -1:
                        sys.stdout.write('{},{},{}'.format(vp, dists[vp], i + 1))
                        found = True
                        break
            if not found:
                sys.stdout.write(',,,')

            sys.stdout.write('\n')
