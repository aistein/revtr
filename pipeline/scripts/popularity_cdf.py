#! /usr/bin/python3 -u

"""
Generate a CDF of how commonly a VP uses its most popular ingress into a
bgp-prefix.  

In one pass over the raw VP data, for each <VP, prefix> build a list of 
<ingress, count> tuples.  At the end, do another pass to output for each
<VP, prefix> the (highest_count/ total_count)
"""

import sys, os
import csv
import pyasn
import multiprocessing as mp
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
from lookup_dnet import dnets_of, DnetEntry

# given a bgp prefix, return the class (small, medium, large) it falls in
def prefix_class(vp, prefix):
    #print("{},{}".format(vp,prefix))
    if prefix != 'None':
        prefix_length = int(prefix.split('/')[1])
        if prefix_length <= 12: # /12 or lower
            return 'large'
        if prefix_length <= 22: # /12 < l <= /22
            return 'medium'
        return 'small' # /22 < l
    return None

# given a ping probe, return (six) ingresses for all definitions:
# - first, by 'asn / bgp / s24' of the destination
# - then, by 'first-inside' or 'first-outside' for each dnet_type above
def ingr_lookup(probe, ipasn):
    dest, hops = probe[0], probe[1:]
    fi = {'asn':None, 'bgp':None, 's24':None}
    fo = {'asn':None, 'bgp':None, 's24':None}
    dest_dentry = dnets_of(dest, ipasn)
    #print('dest_dentry: {}'.format(dest_dentry))
    hop_dentries = [dnets_of(hop, ipasn) for hop in hops]
    #print('hop_dentries: {}'.format(hop_dentries))
    prev_hop = None
    type_found = {'asn':False, 'bgp':False, 's24':False}
    for i, hop in enumerate(hops):
        if all( found for found in type_found.values() ):
            break
        hop_dentry = hop_dentries[i]
        for dnet_type in ['asn', 'bgp', 's24']:
            if not type_found[dnet_type]:
                dest_dnet = eval('dest_dentry.{}'.format(dnet_type))
                hop_dnet = eval('hop_dentry.{}'.format(dnet_type))
                if dest_dnet == hop_dnet:
                    fi[dnet_type] = hop
                    fo[dnet_type] = prev_hop
                    type_found[dnet_type] = True
        prev_hop = hop

    return fi, fo

def producer(vp, probes, q, ipasn):
    for probe in probes:
        vpt = (vpname, dnets_of(probe[0], ipasn).bgp)
        fi, fo = ingr_lookup(probe, ipasn)
        q.put((vpt, fi, fo))

    return

def consumer(vp, q, outdir):
    count_by_ingr_by_tdef_by_ldef_by_vpt = defaultdict( #vpt
            lambda: defaultdict( # ldef
            lambda: defaultdict( # tdef
            lambda: defaultdict( int ))))# ingr<-->int

    while True:
        m = q.get()
        if m == 'kill':
            print('consumer {} quitting...'.format(os.getpid()))
            break
        vpt, fi, fo = m
        for tdef in ['asn','bgp','s24']:
            ingrfi, ingrfo = fi[tdef], fo[tdef]
            count_by_ingr_by_tdef_by_ldef_by_vpt[vpt]['first-inside'][tdef][ingrfi] += 1
            count_by_ingr_by_tdef_by_ldef_by_vpt[vpt]['first-outside'][tdef][ingrfo] += 1

    with open(os.path.join(outdir,vp+'-ingr_counts.csv'), 'w') as f:
        for vpt, count_by_ingr_by_tdef_by_ldef in count_by_ingr_by_tdef_by_ldef_by_vpt.items():
            for ldef, count_by_ingr_by_tdef in count_by_ingr_by_tdef_by_ldef.items():
                for tdef, count_by_ingr in count_by_ingr_by_tdef.items():
                    for ingr, count in count_by_ingr.items():
                        f.write('{},{},{},{},{},{}\n'.format(vpt[0],vpt[1],ldef,tdef,ingr,count))

    return








if __name__ == '__main__':

    try:
        vp_dir = sys.argv[1]
        output_dir = sys.argv[2]
        ipasnfile = sys.argv[3]
        do_mapping = True if sys.argv[4] == 'True' else False
        ipasn = pyasn.pyasn(ipasnfile)
    except:
        exit("usage: ./popularity_cdf.py <raw-probe-dir> <output-dir> <bgp-dump-file> <do-mapping>")

    if do_mapping:
        manager = mp.Manager()
        q = manager.Queue()

        for vpcsv in os.listdir(vp_dir):

            vpname = vpcsv.replace('.csv','')
            print("-I- popularity_cdf: processing vp {}".format(vpname))

            with open(os.path.join(vp_dir, vpcsv), 'r') as f:
                data = [probe for probe in csv.reader(f)]

            num_processes = (2 * 16) // 3
            num_producers = num_processes - 1
            chunksize, leftover = len(data) // num_producers, len(data) % num_producers
            left, to_deploy = leftover, num_producers

            with mp.Pool(processes=num_processes) as pool:
                producers = []
            
                print("Creating Producers...")
                tot = 0
                if leftover:
                    probes = data[0:left]
                    tot += len(probes)
                    p = mp.Process(target=producer, args=(vpname, probes, q, ipasn))
                    producers.append(p)
                    to_deploy -= 1
                for i in range(to_deploy):
                    probes = data[left:left+chunksize]
                    tot += len(probes)
                    p = mp.Process(target=producer, args=(vpname, probes, q, ipasn))
                    producers.append(p)
                    left += chunksize

                print("Creating Consumer...")
                writer = mp.Process(target=consumer, args=(vpname, q, output_dir))

                print("Starting Processes...")
                for p in producers:
                    p.start()
                writer.start()

                print("Expecting {} entries in CSV file".format(6*tot))
                
                print("Wating for Processes to Finish...")
                for p in producers:
                    p.join()
                q.put('kill')
                writer.join()

            print("-I- popularity_cdf: done processing vp {}".format(vpname))

        #for vpcsv in os.listdir(vp_dir):
        #    vpname = vpcsv.replace('.csv','')
        #    with open(os.path.join(vp_dir, vpcsv), 'r') as f:
        #        r = csv.reader(f, delimiter=',')
        #        for probe in r:
        #            vpt = (vpname, dnets_of(probe[0], ipasn).bgp)
        #            fi, fo = ingr_lookup(probe, ipasn)
        #            print('vpt: {}'.format(vpt))
        #            print('hops: {}'.format(probe[1:]))
        #            print('ingrs-fi: {}'.format(fi))
        #            print('ingrs-fo: {}\n'.format(fo))
        #            for tdef in ['asn','bgp','s24']:
        #                ingrfi, ingrfo = fi[tdef], fo[tdef]
        #                count_by_ingr_by_tdef_by_ldef_by_vpt[vpt]['first-inside'][tdef][ingrfi] += 1
        #                count_by_ingr_by_tdef_by_ldef_by_vpt[vpt]['first-outside'][tdef][ingrfo] += 1

    count_by_ingr_by_tdef_by_ldef_by_vpt = defaultdict( #vpt
            lambda: defaultdict( # ldef
            lambda: defaultdict( # tdef
            lambda: defaultdict( int ))))# ingr<-->int
    popularity_by_vpt_by_ldef_by_tdef = defaultdict( # tdef
            lambda: defaultdict( # ldef
            lambda: defaultdict( float ))) # vpt <--> popularity (score)

    if not os.path.exists(os.path.join(output_dir,'popularity_scores.csv')):

        # Load the ingress counts from mapped CSV files
        # - tdef = 'asn/bgp/s24' ingress def
        # - ldef = 'first inside/outside' ingress def
        # - vpt = 'VP:prefix' tuple
        print("-I- Loading mapped data...")
        for vpcsv in os.listdir(output_dir):
            print("\tLoading mapped data for VP {}".format(vpcsv))
            with open(os.path.join(output_dir,vpcsv), 'r') as f:
                r = csv.reader(f, delimiter=',')
                for entry in r:
                    vp, prefix, ldef, tdef, ingr, count = entry
                    vpt = (vp, prefix)
                    count_by_ingr_by_tdef_by_ldef_by_vpt[vpt][ldef][tdef][ingr] = int(count)
        print("-I- Done loading mapped data.")

        # Calculate Popularity Ratios
        print("-I- Calculating popularity scores...")
        cumulative_ones_cnt, cumulative_cnt, ones_samples, n_samples = 0, 0, 0, 0
        with open(os.path.join(output_dir,'popularity_scores.csv'), 'w') as f:
            for vpt, count_by_ingr_by_tdef_by_ldef in count_by_ingr_by_tdef_by_ldef_by_vpt.items():
                print("\tScoring VPT {}...".format(vpt))
                for ldef in ['first-inside','first-outside']:
                    for tdef in ['asn','bgp','s24']:
                        max_ingr, max_count, total_count = '', 0, 0
                        for ingr, count in count_by_ingr_by_tdef_by_ldef[ldef][tdef].items():
                            if count > max_count:
                                max_ingr, max_count = ingr, count
                            total_count += count
                        size = prefix_class(*vpt)
                        pscore = max_count / total_count
                        f.write("{},{},{},{},{},{},{}\n".format(
                            vpt[0], vpt[1], ldef, tdef, max_count, total_count, pscore))
                        popularity_by_vpt_by_ldef_by_tdef[tdef][ldef][vpt] = pscore 
                        if pscore == 1.0: 
                            cumulative_ones_cnt += total_count
                            ones_samples += 1
                        cumulative_cnt, n_samples = cumulative_cnt + total_count, n_samples + 1
        print("Avg #-Measurements when pscore is perfect: {}".format( cumulative_ones_cnt / ones_samples ))
        print("Avg #-Measurements over all pscore calculations: {}".format( cumulative_cnt / total_count ))

    else: # popularity_scores.csv already exists
        
        print("-I- loading popularity_scores.csv ...")
        cumulative_ones_cnt, cumulative_cnt, ones_samples, n_samples = 0, 0, 0, 0
        with open(os.path.join(output_dir,'popularity_scores.csv'), 'r') as f:
            r = csv.reader(f, delimiter=',')
            for entry in r:
                vp, prefix, ldef, tdef, max_cnt, total_cnt, pscore = entry
                popularity_by_vpt_by_ldef_by_tdef[tdef][ldef][(vp,prefix)] = float(pscore)
                if float(pscore) == 1.0: 
                    cumulative_ones_cnt += int(total_cnt)
                    ones_samples += 1
                cumulative_cnt, n_samples = cumulative_cnt + int(total_cnt), n_samples + 1
        print("Avg #-Measurements when pscore is perfect: {}".format( cumulative_ones_cnt / ones_samples ))
        print("Avg #-Measurements over all pscore calculations: {}".format( cumulative_cnt / n_samples ))

    # Plot the 6 CDFs (first-inside, first-outside) * (asn, bgp, s24)
    # Each plot should have 4 lines: small, medium, large, all (destination prefix classifications)

    fig, axs = plt.subplots(2,3, figsize=(14,7), sharex='col', sharey='row')
    fig.suptitle('Popularity of Most Common Ingress')
    for i, ldef in enumerate(['first-inside','first-outside']):
        for j, tdef in enumerate(['asn','bgp','s24']):

            print("-I- making subplot {}*{}".format(ldef,tdef))
            none_cnt = 0
            one_cnt = 0
            total = 0.0

            # aggregate popularity by destination-prefix size
            print("\taggregating popularity scores...")
            aggregate_popularities = defaultdict(list)
            for vpt, popularity in popularity_by_vpt_by_ldef_by_tdef[tdef][ldef].items():
                size = prefix_class(*vpt)
                if size is not None:
                    total, one_cnt = total + 1, one_cnt + 1 if popularity == 1.0 else one_cnt
                    aggregate_popularities[size].append(popularity)
                    aggregate_popularities['all'].append(popularity)
                else:
                    none_cnt += 1
            n_dp = {size:len(data) for size,data in aggregate_popularities.items()}
            print("\t#-Datapoints for: small = {}, medium = {}, large = {}, all = {}".format(
                n_dp['small'], n_dp['medium'], n_dp['large'], n_dp['all']))
            print("\t%-Datapoints for which most popular ingress was used for 100% of measurements: {}".format(
                one_cnt / total))
            print("\t#-Destinations for which prefix-lookup failed: {}".format(none_cnt))

            print("\tbinning data and computing CDF...") 
            # bin the data using histogram
            counts_sm, bin_edges_sm = np.histogram(aggregate_popularities['small'], bins=1000, density=True)
            counts_med, bin_edges_med = np.histogram(aggregate_popularities['medium'], bins=1000, density=True)
            counts_lrg, bin_edges_lrg = np.histogram(aggregate_popularities['large'], bins=1000, density=True)
            counts_all, bin_edges_all = np.histogram(aggregate_popularities['all'], bins=1000, density=True)
            # compute CDFs
            cdf_sm = np.cumsum(counts_sm)
            cdf_med = np.cumsum(counts_med)
            cdf_lrg = np.cumsum(counts_lrg)
            cdf_all = np.cumsum(counts_all)
            # plot the CCDFs
            print("\tplotting...")
            axs[i,j].plot(bin_edges_sm[1:], 1-cdf_sm/cdf_sm[-1], label='small', color='r')
            axs[i,j].plot(bin_edges_all[1:], 1-cdf_all/cdf_all[-1], label='all', color='y')
            axs[i,j].plot(bin_edges_med[1:], 1-cdf_med/cdf_med[-1], label='medium', color='g')
            axs[i,j].plot(bin_edges_lrg[1:], 1-cdf_lrg/cdf_lrg[-1], label='large', color='b')
            axs[i,j].grid(True)
            axs[i,j].legend(fontsize=8)
            tdef = 'ASN' if j == 0 else 'BGP' if j == 1 else 'S24'
            if i == 0:
                axs[i,j].set_title(tdef, fontsize=9)

    for i, ax in enumerate(axs.flat):
        #row, col = i // 3, i % 3
        #xl = 'ASN' if col == 0 else 'BGP' if col == 1 else 'S24'
        yl = 'First Inside' if i // 3 == 0 else 'First Outside'
        #ax.set(xlabel=xl, ylabel=yl)
        ax.set(ylabel=yl)
        ax.set(xlim=(0,1), ylim=(0,1))
    # Hide x labels and tick labels for top plots and y ticks for right plots.
    for ax in axs.flat:
        ax.label_outer()
    fig.text(0.5, 0.02, 'Popularity Score', ha='center')
    fig.text(0.06, 0.5, '1 - CDF', va='center', rotation='vertical')
    plt.savefig('popularity_ccdfs.png')
    print("-I- All plots done.")
        

