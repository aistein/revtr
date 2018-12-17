#!/usr/local/bin/python3
import os
import csv
import yaml
import pickle
import numpy as np
import matplotlib as plt
from collections import defaultdict

#==========================================================
# Configuration Parsing (YAML)
#==========================================================

try:
    configfile = open("./revtr.parse_stats.config.yml", 'r')
    configurations = yaml.load(configfile)
    configfile.close()
except (FileNotFoundError,IndexError):
    exit('error: revtr.parse_stats.config.yml file either not found or incorrect.')

# output directory
output_dir = configurations['output_dir']

# dnet type
dnet_type = configurations['dnet_type']

# results directory & filename
results_dir = configurations['results_dir']
results_file = os.path.join(results_dir, dnet_type, dnet_type + '_fucking_results.goddamn')


#==========================================================
# Functions
#==========================================================

def makecdf(data):
    num_bins = 20
    counts, bin_edges = np.histogram (data, bins=num_bins, normed=True)
    cdf = np.cumsum (counts)
    plt.plot (bin_edges[1:], cdf/cdf[-1])
    return

def parse_line(line):
    dest = line[0]
    dnet = line[1]
    entry = { 
                'opt_vp': line[2],
                'opt_dist': -1 if line[3] == '' else int(line[3]),
                'set_vp': line[4],
                'set_dist': -1 if line[5] == '' else int(line[5]),
                'set_pings': -1 if line[6] == '' else int(line[6]),
                'ingr_vp': line[7],
                'ingr_dist': -1 if line[8] == '' else int(line[8]),
                'ingr_pings': -1 if line[9] == '' else int(line[9]),
                'dest_vp': line[10],
                'dest_dist': -1 if line[11] == '' else int(line[11]),
                'dest_pings': -1 if line[12] == '' else int(line[12])
            }
    return dnet, dest, entry


def results_by_dnet(rfile):
    with open(rfile, 'r') as csvfile:
        datareader = csv.reader(csvfile, delimiter=',')
        next(datareader)
        yield from map(parse_line, datareader)

def filter_in_range(metrics):
    yield from filter(lambda metric: metric[2]['opt_dist'] > -1, metrics)

def optprinter(dn, ds, mx):
    print("[IN-RANGE]\tDNET: {}, Dest: {}, Opt-VP: {}, Opt-Dist: {}".format(dn, ds, mx['opt_vp'], mx['opt_dist']))

#==========================================================
# Main
#==========================================================

def main():

    # Questions: (Produce a CDF for each question)
    
    # 1. For each dnet-type, how many dnets were in range of any VP?
    for dnet, dest, entry in filter_in_range(results_by_dnet(results_file)):
        optprinter(dnet, dest, entry)
    
    # 2. Of in-range dnets, what ranking methods (if any) produced a successful VP
    
    # 3. If a ranking method produced a successful VP, was it the optimal distance away? If not, how far off?
    
    # 4. If a ranking method produced a successful VP, how many pings did it take?

if __name__ == "__main__":
    main()
