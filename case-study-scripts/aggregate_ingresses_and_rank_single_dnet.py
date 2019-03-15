#! /usr/local/bin/python3
import sys
import pickle
import json
from collections import OrderedDict, defaultdict

def aggregate_ingresses(dists_by_vp_by_ingr, overlap_threshold=0.75):
    """
    aggregate dnet ingresses sets whose intersections are above overlap threshold.

    The idea here is that, if two sets of ingress ips have sufficiently high overlap, we
    can consider them the same ingress set.

    Returns dictionary with the following kv pairs:
        keys: tuple of vps that use same aggregated ingress set
        vals: said aggregated ingress set
    """

    # construct set of all dnet ingress ips for each vp
    ingrs_by_vp = defaultdict(set)
    for ingr, dists_by_vp in dists_by_vp_by_ingr.items():
        for vp in dists_by_vp.keys():
            ingrs_by_vp[vp].add(ingr)

    # sort ingress sets by size (large -> small)
    sorted_ingrs = OrderedDict((sorted(ingrs_by_vp.items(),
            key=lambda x : -len(x[1]))))

    # aggregate ingresses using overlap threshold
    vps_by_ingrt = {} # aggregated ingresses by vp tuples
    while len(sorted_ingrs) > 0:

        # pop the largest (vp, ingress set) pair still remaining
        vp, agg_ingrs = sorted_ingrs.popitem(last=False)

        # compare aggregated ingress set, A, to every other ingress set S.
        # If the intersection of A and S is at least overlap_threshold of S, then combine
        # the two into a new aggregate.  Continue until no new sets are added to aggregate.
        # Note that, since ingresses are sorted, A will always be larger than S
        vps_this_round = [vp]
        ingrs_added = True
        start = 1
        while ingrs_added:
            ingrs_added = False
            for vp, ingrs in sorted_ingrs.items():

                # overlap threshold exceeded
                if len(ingrs & agg_ingrs) >= overlap_threshold * len(ingrs):
                    vps_this_round.append(vp)
                    agg_ingrs |= ingrs
                    ingrs_added = True

            # remove aggregated ingresses from sorted list
            for vp in vps_this_round[start:]:
                del sorted_ingrs[vp]
            start = len(vps_this_round) # starting index for next round

        ## note : this is where ranking (sorting) happens by distance (within a logical ingress)
        ## note : "distance" from some VP to a logical ingress is actually min-dist of all measurements from
        ##          ... that VP to the logical ingress
        ## TODO: consier using "mean of all measurements" instead of "min of all measurements"
        #vps_by_ingrt[tuple(agg_ingrs)] =\
        #        sorted(vps_this_round, key=lambda x : 
        #        min([min(dists_by_vp_by_ingr[ingr][x])\
        #        if x in dists_by_vp_by_ingr[ingr] else 10
        #        for ingr in agg_ingrs]))
        by_dist = lambda x:\
                    min( [ min( dists_by_vp_by_ingr[ingr][x] )\
                    if x in dists_by_vp_by_ingr[ingr]\
                    else 10 for ingr in agg_ingrs ] )
        dist_sorted_vps_this_round = sorted(vps_this_round, key=by_dist)

        def min_group(sorted_vps):
            min_group = [] 
            min_dist = by_dist( sorted_vps[0] )
            for vp in sorted_vps: 
                if by_dist( vp ) == min_dist: 
                    min_group.append(vp) 
            return min_group 

        vps_by_ingrt[tuple(agg_ingrs)] = min_group(dist_sorted_vps_this_round)

        # below for debugging only
        logical_ingr_uid = hash(tuple(agg_ingrs))
        print("UID of logical ingress: {}".format(logical_ingr_uid))
        print("VPs this round sorted by distance...")
        for vp in dist_sorted_vps_this_round:
            print("\tVP: {}".format(vp))
            min_dist_to_logical_ingr = float('inf')
            nmeas_to_logical_ingr = 0
            for ingr in agg_ingrs:
                if vp in dists_by_vp_by_ingr[ingr]:
                    nmeas_to_logical_ingr += 1
                    min_dist_to_logical_ingr = min( min_dist_to_logical_ingr, min( dists_by_vp_by_ingr[ingr][vp] ) )
            print("\t\t# Measurements into l-ingr {} = {}".format( logical_ingr_uid, nmeas_to_logical_ingr ))
            print("\t\tMinimum Distance to l-ingr {} = {}".format( logical_ingr_uid, min_dist_to_logical_ingr))

    return vps_by_ingrt

if __name__ == '__main__':

    dists_by_vp_by_ingr_by_dnet = None

    if len(sys.argv) < 3:
        exit('Usage aggreage_ingresses <dnet> <dest-by-ingress-by-dnet json file>')

    target_dnet = sys.argv[1]

    with open(sys.argv[2], 'r') as f:
        dists_by_vp_by_ingr_by_dnet = json.loads(f.read())

    vps_by_ingrt_by_dnet = {}
    #with open(sys.argv[3], 'w+') as f:
    #    vps_by_ingrt = aggregate_ingresses(dists_by_vp_by_ingr_by_dnet[target_dnet])
    #    print(vps_by_ingrt)
    #    ranked_vps = [vps[0] for vps in vps_by_ingrt.values()]
    #    f.write("{},{}\n".format(target_dnet, ','.join(ranked_vps)))
    vps_by_ingrt = aggregate_ingresses(dists_by_vp_by_ingr_by_dnet[target_dnet])
    #print(vps_by_ingrt)
    #ranked_vps = [vps[0] for vps in vps_by_ingrt.values()]
    #print("{},{}\n".format(target_dnet, ','.join(ranked_vps)))
    print("target dnet: {}".format(target_dnet))
    for vps in vps_by_ingrt.values():
        print("min_vp_group: {}".format(",".join(vps)))
