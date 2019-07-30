#! /usr/bin/python3 -u
import os
import sys
import csv
import pickle
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict

# output directory
output_dir = 'data/4_19_2019/stats'
one_per_pfx_output_dir = 'data/6_25_2019/stats'

# results directory & filename
stats_dir = 'data/4_19_2019/test_results'
one_per_pfx_stats_dir = 'data/6_25_2019/test_results'

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
                'ingrfi_vp': line[7],
                'ingrfi_dist': -1 if line[8] == '' else int(line[8]),
                'ingrfi_pings': -1 if line[9] == '' else int(line[9]),
                'ingrfo_vp': line[10],
                'ingrfo_dist': -1 if line[11] == '' else int(line[11]),
                'ingrfo_pings': -1 if line[12] == '' else int(line[12]),
                'dest_vp': line[13],
                'dest_dist': -1 if line[14] == '' else int(line[14]),
                'dest_pings': -1 if line[15] == '' else int(line[15])
            }
    return dnet, dest, entry

IGNORE_BLACKLIST = True
BLACKLIST = ['planetlab1.dtc.umn.edu', 'mlab1.yqm01.measurement-lab.org']
def filter_blacklist(metrics):
    def no_blacklisted_vps(metric):
        entry = metric[2]
        entry_vps = [entry['opt_vp'], entry['set_vp'], entry['ingrfi_vp'], entry['ingrfo_vp'], entry['dest_vp']]
        return True if IGNORE_BLACKLIST else not any([blk_ip in entry_vps for blk_ip in BLACKLIST])
    yield from filter(no_blacklisted_vps, metrics)

def results_by_dnet(rfile):
    with open(rfile, 'r') as csvfile:
        datareader = csv.reader(csvfile, delimiter=',')
        next(datareader)
        yield from filter_blacklist(map(parse_line, datareader))

def filter_in_range(metrics):
    yield from filter(lambda metric: metric[2]['opt_dist'] > -1, metrics)

def optprinter(dn, ds, mx):
    print("[IN-RANGE]\tDNET: {}, Dest: {}, Opt-VP: {}, Opt-Dist: {}".format(dn, ds, mx['opt_vp'], mx['opt_dist']))

#==========================================================
# Main
#==========================================================

def main():

    print("[     | opt # reach |  sc # reach  |  dc # reach  |  if # reach  |  io # reach  | M(sc-opt) | M(dc-opt) | M(if-opt) | M(io-opt) ]")
    print("[=====|=============|==============|==============|==============|==============|===========|===========|===========|===========]")

    multi_bar_graph_stats = np.zeros(shape=(3,6,11), dtype=int)

    for type_index, dnet_type in enumerate(('bgp', 'asn', 's24')):

        print("[ {} |".format(dnet_type), end='', flush=True)
        
        # 1. For each dnet-type, how many dnets were in range of any VP?

        results_file = os.path.join(stats_dir, dnet_type + '_res.csv')
        op_in_range, sc_in_range, dc_in_range, icf_in_range, ico_in_range = 0, 0, 0, 0, 0
        sc_cum_delta, dc_cum_delta, icf_cum_delta, ico_cum_delta = 0, 0, 0, 0
        sc_cum_pings, dc_cum_pings, icf_cum_pings, ico_cum_pings = 0, 0, 0, 0
        n_opt, n_sc, n_dc, n_icf, n_ico = 0, 0, 0, 0, 0
        n_scp, n_dcp, n_icfp, n_icop = 0, 0, 0, 0

        # (*). Get data from "one_per_prefix" results for an additional bar to the plot
        opp_results_file = os.path.join(one_per_pfx_stats_dir, dnet_type + '_res.csv')
        opp_in_range, opp_cum_delta, opp_cum_pings, n_opp, n_oppp = 0, 0, 0, 0, 0
        opp_results_by_dest = {}
        for _, dest, entry in results_by_dnet(opp_results_file):
            opp_results_by_dest[dest] = entry

        for dnet, dest, entry in results_by_dnet(results_file):
            #optprinter(dnet,dest,entry)
            # opt stats
            n_opt += 1
            op_in_range += 1
            multi_bar_graph_stats[type_index][0][entry['opt_dist']] += 1
            # set-cover stats
            if entry['set_pings'] > -1:
                n_scp += 1
                sc_cum_pings += entry['set_pings']
            if entry['set_dist'] > -1:
                n_sc += 1
                sc_in_range += 1
                sc_cum_delta += entry['set_dist'] - entry['opt_dist'] 
                multi_bar_graph_stats[type_index][1][entry['set_dist']] += 1
            else:
                multi_bar_graph_stats[type_index][1][-1] += 1
            # destination-cover stats
            if entry['dest_pings'] > -1:
                n_dcp += 1
                dc_cum_pings += entry['dest_pings']
            if entry['dest_dist'] > -1:
                n_dc += 1
                dc_in_range += 1
                dc_cum_delta += entry['dest_dist'] - entry['opt_dist'] 
                multi_bar_graph_stats[type_index][2][entry['dest_dist']] += 1
            else:
                multi_bar_graph_stats[type_index][2][-1] += 1
            # ingress-cover stats (first-inside)
            if entry['ingrfi_pings'] > -1:
                n_icfp += 1
                icf_cum_pings += entry['ingrfi_pings']
            if entry['ingrfi_dist'] > -1:
                n_icf += 1
                icf_in_range += 1
                icf_cum_delta += entry['ingrfi_dist'] - entry['opt_dist'] 
                multi_bar_graph_stats[type_index][3][entry['ingrfi_dist']] += 1
            else:
                multi_bar_graph_stats[type_index][3][-1] += 1
            # ingress-cover stats (first-outside)
            if entry['ingrfo_pings'] > -1:
                n_icop += 1
                ico_cum_pings += entry['ingrfo_pings']
            if entry['ingrfo_dist'] > -1:
                n_ico += 1
                ico_in_range += 1
                ico_cum_delta += entry['ingrfo_dist'] - entry['opt_dist'] 
                multi_bar_graph_stats[type_index][4][entry['ingrfo_dist']] += 1
            else:
                multi_bar_graph_stats[type_index][4][-1] += 1
            # one-per-prefix, ingress-cover stats (first-inside)
            opp_entry = opp_results_by_dest[dest]
            if opp_entry['ingrfi_pings'] > -1:
                n_oppp += 1
                opp_cum_pings += opp_entry['ingrfi_pings']
            if opp_entry['ingrfi_dist'] > -1:
                n_opp += 1
                opp_in_range += 1
                opp_cum_delta += opp_entry['ingrfi_dist'] - entry['opt_dist'] 
                multi_bar_graph_stats[type_index][5][opp_entry['ingrfi_dist']] += 1
            else:
                multi_bar_graph_stats[type_index][5][-1] += 1
        
        sc_avg_delta, dc_avg_delta, icf_avg_delta, ico_avg_delta = -1, -1, -1, -1
        opp_avg_delta = -1
        if n_sc > 0:
            sc_avg_delta = sc_cum_delta / n_sc
        if n_dc > 0:
            dc_avg_delta = dc_cum_delta / n_dc
        if n_icf > 0:
            icf_avg_delta = icf_cum_delta / n_icf
        if n_ico > 0:
            ico_avg_delta = ico_cum_delta / n_ico
        if n_opp > 0:
            opp_avg_delta = opp_cum_delta / n_opp

        p_sc, p_dc, p_icf, p_ico = sc_in_range / op_in_range, dc_in_range / op_in_range, icf_in_range / op_in_range, ico_in_range / op_in_range
        p_opp = opp_in_range / op_in_range

        # table
        print(" {: >11} | {: >6}({:0.2f}) | {: >6}({:0.2f}) | {: >6}({:0.2f}) | {: >6}({:0.2f}) | {: >9.5f} | {: >9.5f} | {: >9.5f} | {: >9.5f}]".format(
            op_in_range, sc_in_range, p_sc, dc_in_range, p_dc, icf_in_range, p_icf, ico_in_range, p_ico,
            sc_avg_delta, dc_avg_delta, icf_avg_delta, ico_avg_delta))
        print("opp_in_range {}, p_opp {}, p_avg_delta {}".format(opp_in_range, p_opp, opp_avg_delta))

        # record the number of pings for each method
        sc_avg_pings, dc_avg_pings, icf_avg_pings, ico_avg_pings = 0, 0, 0, 0
        opp_avg_pings = 0
        #sc_avg_pings, dc_avg_pings, ic_avg_pings = sc_cum_pings / n_opt, dc_cum_pings / n_opt, ic_cum_pings / n_opt
        if n_scp > 0:
            sc_avg_pings = sc_cum_pings / n_scp
        if n_dcp > 0:
            dc_avg_pings = dc_cum_pings / n_dcp
        if n_icfp > 0:
            icf_avg_pings = icf_cum_pings / n_icfp
        if n_icop > 0:
            ico_avg_pings = ico_cum_pings / n_icop
        if n_oppp > 0:
            opp_avg_pings = opp_cum_pings / n_oppp

        print("[{}] SC-Avg-Pings: {}, DC-Avg-Pings: {}, ICF-Avg-Pings: {}, ICO-Avg-Pings: {}, OPP-Avg-Pings: {}".format(
            dnet_type, sc_avg_pings, dc_avg_pings, icf_avg_pings, ico_avg_pings, opp_avg_pings))
    
    # stacked bar plot
    ind = np.arange(1,11)
    #width = 0.15
    width = 0.13
    for type_index, plot_type in enumerate(('bgp', 'asn', 's24')):
        opt = plt.bar(ind-5*width/2, multi_bar_graph_stats[type_index][0][1:11], width=width, color='r', align='center')
        ingrfi = plt.bar(ind-3*width/2, multi_bar_graph_stats[type_index][3][1:11], width=width, color='y', align='center')
        ingrfo = plt.bar(ind-width/2, multi_bar_graph_stats[type_index][4][1:11], width=width, color='k', align='center')
        topk = plt.bar(ind+width/2, multi_bar_graph_stats[type_index][1][1:11], width=width, color='g', align='center')
        dest = plt.bar(ind+3*width/2, multi_bar_graph_stats[type_index][2][1:11], width=width, color='b', align='center')
        opp = plt.bar(ind+5*width/2, multi_bar_graph_stats[type_index][5][1:11], width=width, color='c', align='center')
        plt.ylabel('# Destinations RR-Reachable')
        plt.xlabel('# Hops')
        # plt.title('Ranked VP Distance from Destination ('+plot_type+')')
        plt.xticks(ind, ('1','2','3','4','5','6','7','8','9','>9'))
        if IGNORE_BLACKLIST:
            plt.yticks(np.arange(0,65000,5000))
        else:
            plt.yticks(np.arange(0,6000,500)) # without blacklisted VPs
        plt.legend((opt, ingrfi, ingrfo, topk, dest, opp), ('Optimal VP', 'Ingress Score-FI', 'Ingress Score-FO', 'Top K', 'Destination Cover', 'One Per Prefix (IS-FI)'))
        plt.savefig('ranked_vp_distance_from_destination-'+plot_type+'.png')
        plt.clf()
        

if __name__ == "__main__":
    main()
