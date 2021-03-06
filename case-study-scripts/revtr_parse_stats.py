#!/usr/local/bin/python3
import os
import sys
import csv
import yaml
import pickle
import numpy as np
import matplotlib.pyplot as plt
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
stats_dir = configurations['stats_dir']

# type of dnet plot to make
plot_type = configurations['plot_type']

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

    print("[     | opt # reach |  sc # reach  |  dc # reach  |  ic # reach  | M(sc-opt) | M(dc-opt) | M(ic-opt) ]")
    print("[=====|=============|==============|==============|==============|===========|===========|===========]")

    stacked_bar_graph_stats = np.zeros(shape=(3,4,11), dtype=int)

    for type_index, dnet_type in enumerate(('bgp', 'asn', 's24')):

        print("[ {} |".format(dnet_type), end='', flush=True)
        
        # 1. For each dnet-type, how many dnets were in range of any VP?

        #results_file = os.path.join(stats_dir, dnet_type, dnet_type + '_fucking_results.goddamn')
        results_file = os.path.join(stats_dir, dnet_type + '_res.csv')
        op_in_range, sc_in_range, dc_in_range, ic_in_range = 0, 0, 0, 0
        sc_cum_delta, dc_cum_delta, ic_cum_delta = 0, 0, 0
        sc_cum_pings, dc_cum_pings, ic_cum_pings = 0, 0, 0
        n_opt, n_sc, n_dc, n_ic = 0, 0, 0, 0
        n_scp, n_dcp, n_icp = 0, 0, 0

        for dnet, dest, entry in results_by_dnet(results_file):
            #optprinter(dnet,dest,entry)
            # opt stats
            n_opt += 1
            op_in_range += 1
            stacked_bar_graph_stats[type_index][0][entry['opt_dist']] += 1
            # set-cover stats
            if entry['set_pings'] > -1:
                n_scp += 1
                sc_cum_pings += entry['set_pings']
            if entry['set_dist'] > -1:
                n_sc += 1
                sc_in_range += 1
                sc_cum_delta += entry['set_dist'] - entry['opt_dist'] 
                stacked_bar_graph_stats[type_index][1][entry['set_dist']] += 1
            else:
                stacked_bar_graph_stats[type_index][1][-1] += 1
            # destination-cover stats
            if entry['dest_pings'] > -1:
                n_dcp += 1
                dc_cum_pings += entry['dest_pings']
            if entry['dest_dist'] > -1:
                n_dc += 1
                dc_in_range += 1
                dc_cum_delta += entry['dest_dist'] - entry['opt_dist'] 
                stacked_bar_graph_stats[type_index][2][entry['dest_dist']] += 1
            else:
                stacked_bar_graph_stats[type_index][2][-1] += 1
            # ingress-cover stats
            if entry['ingr_pings'] > -1:
                n_icp += 1
                ic_cum_pings += entry['ingr_pings']
            if entry['ingr_dist'] > -1:
                n_ic += 1
                ic_in_range += 1
                ic_cum_delta += entry['ingr_dist'] - entry['opt_dist'] 
                stacked_bar_graph_stats[type_index][3][entry['ingr_dist']] += 1
            else:
                stacked_bar_graph_stats[type_index][3][-1] += 1
        
        sc_avg_delta, dc_avg_delta, ic_avg_delta = -1, -1, -1
        if n_sc > 0:
            sc_avg_delta = sc_cum_delta / n_sc
        if n_dc > 0:
            dc_avg_delta = dc_cum_delta / n_dc
        if n_ic > 0:
            ic_avg_delta = ic_cum_delta / n_ic

        p_sc, p_dc, p_ic = sc_in_range / op_in_range, dc_in_range / op_in_range, ic_in_range / op_in_range

        # table
        print(" {: >11} | {: >6}({:0.2f}) | {: >6}({:0.2f}) | {: >6}({:0.2f}) | {: >9.5f} | {: >9.5f} | {: >9.5f} ]".format(
            op_in_range, sc_in_range, p_sc, dc_in_range, p_dc, ic_in_range, p_ic, sc_avg_delta, dc_avg_delta, ic_avg_delta))

        # record the number of pings for each method
        sc_avg_pings, dc_avg_pings, ic_avg_pings = 0, 0, 0
        #sc_avg_pings, dc_avg_pings, ic_avg_pings = sc_cum_pings / n_opt, dc_cum_pings / n_opt, ic_cum_pings / n_opt
        if n_scp > 0:
            sc_avg_pings = sc_cum_pings / n_scp
        if n_dcp > 0:
            dc_avg_pings = dc_cum_pings / n_dcp
        if n_icp > 0:
            ic_avg_pings = ic_cum_pings / n_icp
        print("[{}] SC-Avg-Pings: {}, DC-Avg-Pings: {}, IC-Avg-Pings: {}".format(
            dnet_type, sc_avg_pings, dc_avg_pings, ic_avg_pings))
    
    # stacked bar plot
    ind = np.arange(1,11)
    width = 0.15
    if plot_type == 'bgp':
        opt = plt.bar(ind-3*width/2, stacked_bar_graph_stats[0][0][1:11], width=width, color='r', align='center')
        ingr = plt.bar(ind-width/2, stacked_bar_graph_stats[0][3][1:11], width=width, color='y', align='center')
        topk = plt.bar(ind+width/2, stacked_bar_graph_stats[0][1][1:11], width=width, color='g', align='center')
        dest = plt.bar(ind+3*width/2, stacked_bar_graph_stats[0][2][1:11], width=width, color='b', align='center')
    if plot_type == 'asn':
        opt = plt.bar(ind-3*width/2, stacked_bar_graph_stats[1][0][1:11], width=width, color='r', align='center')
        ingr = plt.bar(ind-width/2, stacked_bar_graph_stats[1][3][1:11], width=width, color='y', align='center')
        topk = plt.bar(ind+width/2, stacked_bar_graph_stats[1][1][1:11], width=width, color='g', align='center')
        dest = plt.bar(ind+3*width/2, stacked_bar_graph_stats[1][2][1:11], width=width, color='b', align='center')
    if plot_type == 's24':
        opt = plt.bar(ind-3*width/2, stacked_bar_graph_stats[2][0][1:11], width=width, color='r', align='center')
        ingr = plt.bar(ind-width/2, stacked_bar_graph_stats[2][3][1:11], width=width, color='y', align='center')
        topk = plt.bar(ind+width/2, stacked_bar_graph_stats[2][1][1:11], width=width, color='g', align='center')
        dest = plt.bar(ind+3*width/2, stacked_bar_graph_stats[2][2][1:11], width=width, color='b', align='center')
    plt.ylabel('# Destinations RR-Reachable')
    plt.xlabel('# Hops')
    plt.title('Ranked VP Distance from Destination ('+plot_type+')')
    plt.xticks(ind, ('1','2','3','4','5','6','7','8','9','>9'))
    plt.yticks(np.arange(0,60000,10000))
    plt.legend((opt, topk, dest, ingr), ('Optimal VP', 'Top K', 'Destination Cover', 'Ingress Cover'))
    plt.savefig('ranked_vp_distance_from_destination-'+plot_type+'.png')
        

if __name__ == "__main__":
    main()
