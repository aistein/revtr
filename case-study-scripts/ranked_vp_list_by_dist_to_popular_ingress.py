#! /usr/local/bin/python3 -u

import os, sys
import csv
import json
from collections import defaultdict

#pop_dir = '/data/workspace/data/popularity'
pop_dir = '/data/workspace/data/4_19_2019/train_results/popularity'
#target_prefix = '209.58.192.0/19'
target_prefix = '180.69.0.0/16'

pop_weight, vpd_weight = 0.5, 1.5
assert(pop_weight + vpd_weight == 2.0)

#vps_dist_by_ingr_pop_by_ndef = defaultdict( lambda: defaultdict(list) )
score_by_ingr_by_ndef = defaultdict( lambda: defaultdict(float) )

#breaker = 5

for vpcsv in os.listdir(pop_dir):
    #vpid = vpcsv.split('-')[0]
    vpname = vpcsv.replace('-ingr_counts.csv','')
    print(vpname)
    #print(vpid)
    if 'popularity' in vpcsv:
        continue

    #breaker -= 1
    #if not breaker:
    #    break

    with open(os.path.join(pop_dir, vpcsv), 'r') as f:
        r = csv.reader(f, delimiter=',')
        #max_ingr, max_ingr_cnt = '', 0
        for ingr_entry in r:
            dprefix, nloc, ntype, ingr, cnt = ingr_entry[1:]
            ndef = (nloc, ntype)
            if dprefix == target_prefix and ingr is not None:
                # at this point, "score" means "max number of measurements traversing a fixed ingress for this <vp, prefix> tuple"
                score_by_ingr_by_ndef[ndef][ingr] = max(score_by_ingr_by_ndef[ndef][ingr], float(cnt))
                #if int(cnt) > max_ingr_cnt:
                #    max_ingr, max_ingr_cnt = ingr, int(cnt)
                #    for vpname, measurements_to_target_prefix in measurements_to_target_prefix_by_vp.items():
                #        if vpid not in vpname:
                #            continue
                #        min_dist = 10
                #        for mid, measurement in enumerate(measurements_to_target_prefix):
                #            if ingr in measurement[1:] and measurement[1:].index(ingr) < min_dist:
                #                min_id, min_dist = mid, measurement[1:].index(ingr)
                #        vps_dist_by_ingr_pop_by_ndef[ndef][(int(cnt), ingr)].append((min_dist, vpid))
                #        print("ndef {}, ingr {}, ingr_pop {}, min_dist {}, vpid {}".format(ndef, ingr, max_ingr_cnt, min_dist, vpid))
                #        print("\tmin-meas: {}".format(measurements_to_target_prefix[min_id]))

sum_by_ndef = { ndef:sum([score for score in score_by_ingr_by_ndef[ndef].values()]) for ndef in score_by_ingr_by_ndef }

# "max_by_ndef" is the max raw "score" seen over all <VP, prefix> tuples
max_by_ndef = { ndef:max([score for score in score_by_ingr_by_ndef[ndef].values()]) for ndef in score_by_ingr_by_ndef }

print("sums: {}".format(sum_by_ndef))
print("maxes: {}".format(max_by_ndef))

# normalize the score for each ingress by the max raw score seen over all <VP, prefix> tuples
for ndef in score_by_ingr_by_ndef:
    for ingr in score_by_ingr_by_ndef[ndef]:
        score_by_ingr_by_ndef[ndef][ingr] /= max_by_ndef[ndef]

min_by_ndef_norm = { ndef:min([score for score in score_by_ingr_by_ndef[ndef].values()]) for ndef in score_by_ingr_by_ndef }
print("mins_norm: {}".format(min_by_ndef_norm))





# Now, get the minimum distance from each VP to each ingress for this prefix
with open('./gather_measurements_to_prefix.json', 'r') as f:
    measurements_to_target_prefix_by_vp = json.load(f)

#breaker = 5
vps_dist_by_ingr_by_ndef = defaultdict( lambda: defaultdict(list) )

for vpcsv in os.listdir(pop_dir):
    vpname = vpcsv.replace('-ingr_counts.csv','')
    print(vpname)
    if 'popularity' in vpcsv:
        continue

    #breaker -= 1
    #if not breaker:
    #    break

    with open(os.path.join(pop_dir, vpcsv), 'r') as f:
        r = csv.reader(f, delimiter=',')
        for ingr_entry in r:
            dprefix, nloc, ntype, ingr, cnt = ingr_entry[1:]
            ndef = (nloc, ntype)
            if dprefix == target_prefix and ingr is not None:
                min_dist_to_ingr, min_meas_id = 10, -1
                for meas_id, measurement in enumerate(measurements_to_target_prefix_by_vp[vpname]):
                    if ingr in measurement[4:] and measurement[4:].index(ingr) < min_dist_to_ingr:
                        min_meas_id, min_dist_to_ingr = meas_id, measurement[4:].index(ingr)
                vps_dist_by_ingr_by_ndef[ndef][ingr].append((min_dist_to_ingr, vpname))
                print("ndef {}, ingr {}, vp {}, min_dist {}".format(ndef, ingr, vpname, min_dist_to_ingr))
                print("\tmin-meas: {}".format(measurements_to_target_prefix_by_vp[vpname][meas_id]))

# update the ingress rankings to reflect the minimum_distance from any VP to that ingress
for ndef in vps_dist_by_ingr_by_ndef:
    for ingr, vp_dists in vps_dist_by_ingr_by_ndef[ndef].items():
        min_dist_norm = (10.0 - sorted(vp_dists)[0][0]) / 10.0
        score_by_ingr_by_ndef[ndef][ingr] = 0.5 * (pop_weight * score_by_ingr_by_ndef[ndef][ingr] + vpd_weight * min_dist_norm)

with open('./ingr_rankings.csv', 'w') as f:

    f.write("prefix,nloc,ntype,ingr,ingr_score,vps_ordered_by_min_distance...\n")

    for ndef in score_by_ingr_by_ndef:
        nloc, ntype = ndef
        num_ingrs = 10

        for ingr, score in reversed(sorted(score_by_ingr_by_ndef[ndef].items(), key=lambda t: t[1])):
            f.write("{},{},{},{},{}".format(target_prefix,nloc,ntype,ingr,score))
            num_vps = 10
            for (min_dist, vp) in sorted(vps_dist_by_ingr_by_ndef[ndef][ingr]):
                f.write(",{},{}".format(vp,min_dist))
                num_vps -= 1
                if not num_vps:
                    break

            f.write("\n")

            num_ingrs -= 1
            if not num_ingrs:
                break

        f.write("\n\n")

#with open('./raw_ingr_rankings.csv', 'w') as f:
#    f.write("prefix,nloc,ntype,ingr_by_vp,popularity_by_vp,vp,min_vp_dist_to_ingr\n")
#    for ndef, vps_dist_by_ingr_pop in vps_dist_by_ingr_pop_by_ndef.items():
#        print("Ranked Ingrs for 89.137.0.0/16 by Ingr-Popularity for Ndef = {}:".format(ndef))
#        for (pop, ingr), vps_dist in sorted(vps_dist_by_ingr_pop.items()):
#            print("\tIngr {}, Pop {}:".format(ingr, pop))
#            for dist, vp in sorted(vps_dist):
#                f.write("89.137.0.0\/16,{},{},".format(ndef[0],ndef[1]))
#                f.write("{},{},".format(ingr,pop))
#                f.write("{},{}\n".format(vp,dist))
#                print("\t\tVP {}, Dist-To-Ingr {}".format(vp, dist))
