#!/usr/local/bin/python3
import os
import sys
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

# results directory & filename
results_dir = configurations['results_dir']


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

    print("[     | opt # reach | sc # reach | dc # reach | ic # reach | M(sc-opt) | M(dc-opt) | M(ic-opt) ]")
    print("[=====|=============|============|============|============|===========|===========|===========]")
    for dnet_type in ('bgp', 'asn', 's24'):

        print("[ {} |".format(dnet_type), end='', flush=True)
        
        # 1. For each dnet-type, how many dnets were in range of any VP?
        results_file = os.path.join(results_dir, dnet_type, dnet_type + '_fucking_results.goddamn')
        op_in_range, sc_in_range, dc_in_range, ic_in_range = 0, 0, 0, 0
        sc_cum_delta, dc_cum_delta, ic_cum_delta = 0, 0, 0
        n_opt, n_sc, n_dc, n_ic = 0, 0, 0, 0
        for dnet, dest, entry in filter_in_range(results_by_dnet(results_file)):
            # optprinter(dnet, dest, entry)
            if entry['opt_dist'] == -1:
                continue
            n_opt += 1
            if entry['set_dist'] > -1:
                n_sc += 1
                sc_in_range += 1
                sc_cum_delta += entry['set_dist'] - entry['opt_dist'] 
            if entry['dest_dist'] > -1:
                n_dc += 1
                dc_in_range += 1
                dc_cum_delta += entry['dest_dist'] - entry['opt_dist'] 
            if entry['ingr_dist'] > -1:
                n_ic += 1
                ic_in_range += 1
                ic_cum_delta += entry['ingr_dist'] - entry['opt_dist'] 
        
        sc_avg_delta, dc_avg_delta, ic_avg_delta = -1, -1, -1
        if n_sc > 0:
            sc_avg_delta = sc_cum_delta / n_sc
        if n_dc > 0:
            dc_avg_delta = dc_cum_delta / n_dc
        if n_ic > 0:
            ic_avg_delta = ic_cum_delta / n_ic

        print(" {: >11} | {: >10} | {: >10} | {: >10} | {: >9.5f} | {: >9.5f} | {: >9.5f} ]".format(
            op_in_range, sc_in_range, dc_in_range, ic_in_range, sc_avg_delta, dc_avg_delta, ic_avg_delta))
        
        # 2. Of in-range dnets, what ranking methods (if any) produced a successful VP
        
        # 3. If a ranking method produced a successful VP, was it the optimal distance away? If not, how far off?
        
        # 4. If a ranking method produced a successful VP, how many pings did it take?

if __name__ == "__main__":
    main()
