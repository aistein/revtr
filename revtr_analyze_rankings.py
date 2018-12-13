#!/usr/bin/python
import csv
import os
import yaml
import json
import pyasn

# config files are the tits
with open('./config.yml', 'r') as cfgfile:
	configs = yaml.load(cfgfile)

# dictionary of dictionaries indexed by dest-IP
ranks_by_dest = {}
metric_entry = {}

def try_vps(rankfile, dest_ip):
    num_hops = 0
    num_pings = 0
    
    # considering only set_cover_results.csv
    if rankfile == "set_cover_results.csv":
        # read first line of csv
        with open("rankfile", "r") as rf:
            each_vp = csv.reader(rf, delimiter=",")
            # read VP file names from first column
            vp_file = each_vp[0]
            # open VP file name from test directory
            with open ("testdir"+vp_file) as vp:
                # read each line in that VP file
                each_destination = csv.reader(vp, delimiter=",")
                for each_hop in each_destination:
                    # check if dest_ip is pinged by this VP
                    if each_hop[0] == dest_ip:
                        # if yes, see if its reachable within 9 hops
                        for x in range(2, 11):
                            if each_hop[0] == each_hop[x]:
                                num_hops = x-1
                                num_pings += num_hops
                                vp_chosen = vp_file
                                return vp_chosen, num_pings, num_hops
                            else:
                                num_pings += 9
                    else:
                        continue
        return "", num_pings, -1

    # for ingress_map.json files
    elif rankfile == "ingress_map.json":
        # a dict of dicts to read in json
        prefix_object = {}
        # create an asndb
        asndb = pyasn.pyasn('ipasn_20140513.dat')
        # open json file
        with open("rankfile", "r") as rf:
            # load json file
            prefix_object = json.load(rf)
            # find asn of dest_ip
            dest_pyasn = asndb.lookup(dest_ip)   
            
            for asn_prefix in prefix_object:
                if asn_prefix == dest_pyasn[1]:
                    for as_ip in prefix_object[asn_prefix]:
                        if as_ip == dest_ip:
                            for all_vps in prefix_object[asn_prefix][as_ip]:
                                #print(all_vps, prefix_object[asn_prefix][as_ip][all_vps])
                                #TODO: add logic to ping using each VP from this sub-dictionary
                                
                                
                                
    elif rankfile == "rankings_by_dnet.csv":
        with open("rankfile", "r") as rf:
            each_dnet = csv.reader(rf, delimiter=",")
            # read VP file names from first column
            dnet = each_dnet[0]
            #
            for vp_file in each_dnet[1:]
                # open VP file name from test directory
                with open ("testdir"+vp_file) as vp:
                    # find asn of dest_ip
                    dest_pyasn = asndb.lookup(dest_ip) 
                    # read each line in that VP file
                    each_destination = csv.reader(vp, delimiter=",")
                    for each_hop in each_destination:
                        # check if dest_ip is pinged by this VP
                        if each_hop[0] == dest_ip:
                            # if yes, see if its reachable within 9 hops
                            for x in range(2, 11):
                                if each_hop[0] == each_hop[x]:
                                    num_hops = x-1
                                    num_pings += num_hops
                                    vp_chosen = vp_file
                                    return vp_chosen, num_pings, num_hops
                                else:
                                    num_pings += 9
                                    else:
                                        continue
        return "", num_pings, -1
             

        
def get_best_vp(dest_ip):
	# TODO: your code here
	return ""

with open(configs['testdir']) as testcsv:
	pingreader = csv.reader(testcsv)
	# for each test destination
	for ping in pingreader:
		# for each ranking method
		for rankfile in os.walk(configs['rankdir']):
			# extracts destination IP address
			dest_ip = ping[0]
            
			# function tries each VP in the ranking 
			vp_chosen, num_pings, num_hops = try_vps(rankfile, dest_ip)
            
#			# function finds best VP
#			best_vp = get_best_vp(dest_ip)
			
            # construct metric_entry dict, pass to global
			metric_entry = {
				"method": os.path.splitext(rankfile)[0],
				"vp_chosen": vp_chosen,
				"num_pings": num_pings,
				"num_hops": num_hops,
				"best_vp": best_vp
			}
			ranks_by_dest[dest_ip] = metric_entry