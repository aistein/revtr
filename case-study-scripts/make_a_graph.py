#! /usr/local/bin/python3 -u

import os
import sys
import csv
import json
import pyasn
import networkx as nx
from collections import namedtuple, defaultdict
from matplotlib.colors import to_rgb, to_rgba
import matplotlib.pyplot as plt
import lookup_dnet as l

NodeInfo = namedtuple('NodeInfo', ['type','ip','asn','bgp','s24'])

try:
    measfile = sys.argv[1]
    target_prefix = sys.argv[2]
    ipasnfile = sys.argv[3]
except:
    exit('usage: ./make_a_graph.py <measfile>.json <target_prefix> <ipasnfile>.dat')

def isPrivateIpAddress(ip):
    octets = list(map(int,ip.split('.')))
    return any([octets[0] == 10,
            octets[0] == 172 and octets[1] >= 16 and octets[1] <= 31,
            octets[0] == 192 and octets[1] == 168])

G = nx.DiGraph()
ipasn = pyasn.pyasn(ipasnfile)
#sample_destinations = ['209.58.221.82','209.58.221.217','209.58.221.150']
#sample_destinations = ['58.230.169.82']
#destNode = NodeInfo('dest', '58.230.169.82', '9318', '58.224.0.0/13', '58.230.169.0/24')
sample_destinations = ['180.69.95.217']
destNode = NodeInfo('dest', '180.69.95.217', '9318', '180.69.0.0/16', '180.69.95.0/24')

with open(measfile, 'r') as f:
    measurements_to_target_prefix_by_vp = json.load(f) 

# make dummy source to orient all VPs and dummy sink to orient all destinations during plotting
dummySourceNodes = [NodeInfo('dummy','source'+str(i),'','','') for i in range(9)]
dummySinkNodes = [NodeInfo('dummy','sink'+str(i),'','','') for i in range(2)]
for dummy in dummySourceNodes:
    G.add_edge(dummy, dummySinkNodes[0], weight=0.1, isDummy=True)

num_good_measurements = 0
for vp, measurements_to_target_prefix in measurements_to_target_prefix_by_vp.items():
    vpNode = NodeInfo('vp',vp,'','','')
    for i, meas in enumerate(measurements_to_target_prefix):
        asn, bgp, s24, dest, dist, hops = (*meas[:5], meas[5:])
        if dest not in sample_destinations or int(dist) > 8:
            continue
        num_good_measurements += 1
        prev, curr, found = 0, 1, False
        lookup = l.dnets_of(hops[prev], ipasn)
        prevNode = NodeInfo('router',hops[prev], lookup.asn, lookup.bgp, lookup.s24)
        #G.add_edge(dummySourceNode, vpNode, weight=0.9)
        #G.add_edge(vpNode, dummySinkNode, weight=0.9) # to make the graph look prettier
        G.add_edge(vpNode, dummySourceNodes[i % len(dummySourceNodes)], weight=0.18, isDummy=True)
        #for dummy in dummySourceNodes:
        #    G.add_edge(vpNode, dummy, weight=0.6)
        G.add_edge(vpNode, prevNode, weight=1, isDummy=False)
        G.add_edge(prevNode, vpNode, weight=0.6, isDummy=True) # to make the graph look prettier
        #while curr < len(hops) and hops[prev] != dest:
        while curr < len(hops) and not found:
            if isPrivateIpAddress(hops[curr]):
                curr += 1
                continue
            lookup = l.dnets_of(hops[curr], ipasn)
            if hops[curr] == dest:
                currNode, found = destNode, True
            else:
                currNode = NodeInfo('router', hops[curr], lookup.asn, lookup.bgp, lookup.s24)
            G.add_edge(prevNode, currNode, isDummy=False)
            G.add_edge(currNode, prevNode, weight=0.7, isDummy=True) # to make the graph look prettier
            if 'weight' not in G.edges[prevNode, currNode]:
                G.edges[prevNode, currNode]['weight'] = 1
            G.edges[prevNode, currNode]['weight'] += 1
            #G.edges[prevNode, currNode]['weight'] += 0.2
            prev, curr, prevNode = prev+1, curr+1, currNode

# connect destination to dummySinkNode (TODO: get this out of the big loop)
G.add_edge(destNode, dummySinkNodes[1], weight=0.1*num_good_measurements, isDummy=True)

# dictionary by ASN for subgraph boxes
nodes_by_asn = defaultdict(list)
for node in G:
    nodes_by_asn[ node.asn ] = node
for asn, nodes in nodes_by_asn.items():
    H = G.subgraph(nodes)
    H.graph['name'] = asn
            
def isNextInside(G, node, networks):
    for neighbor in nx.neighbors(G, node):
        for network in networks:
            if network in [neighbor.asn, neighbor.bgp, neighbor.s24]:
                return True
    return False

def maxOutwardEdgeWeight(G, node):
    max_weight = float('-inf')
    for neighbor in nx.neighbors(G, node):
        max_weight = max( max_weight, G[node][neighbor]['weight'] )
    return max_weight

COLORS = {
    'transparent':(1,1,1,0),
    'green':to_rgba('green'),
    'red':to_rgba('red'),
    'yellow':to_rgba('yellow'),
    'orange':to_rgba('orange'),
    'cyan':to_rgba('cyan'),
    'blue':to_rgba('blue'),
    'black':to_rgba('black')
    }

# ordered edge-weights for plotting
#G.remove_edges_from(list(filter(lambda tup: G[tup[0]][tup[1]]['weight'] < 5, G.edges())))
#G.remove_nodes_from(list(filter(lambda u: nx.degree(G, u) < 5, G.nodes())))
weights = [G[u][v]['weight'] for u,v in G.edges()]
#edge_colors = ['white' if G[u][v]['weight'] == 0.9 else 'black' for u,v in G.edges()]
#edge_colors = ['blue' if (u.type == 'dummy' or v.type == 'dummy' or v.type == 'vp') else 'black' for u,v in G.edges()]
edge_colors = [COLORS['transparent'] if G[u][v]['isDummy'] else COLORS['black'] for u,v in G.edges()]

node_colors = []
labeldict = {}
dentries = [l.dnets_of(dest, ipasn) for dest in sample_destinations]
for node in G:
    labeldict[node] = node.ip
    if node.type == 'dummy':
        #node_colors.append('white')
        node_colors.append(COLORS['transparent'])
        labeldict[node] = ''
    elif node.type == 'vp':
        node_colors.append(COLORS['green'])
    elif node.type == 'dest':
        node_colors.append(COLORS['red'])
    elif node.type == 'router':
        print(node)
        if node.s24 in [dentry.s24 for dentry in dentries]:
            print("found s24!")
            node_colors.append(COLORS['yellow'])
        elif node.bgp in [dentry.bgp for dentry in dentries]:
            print("found bgp!")
            node_colors.append(COLORS['cyan'])
        elif node.asn == [dentry.asn for dentry in dentries]:
            print("found asn!")
            node_colors.append(COLORS['orange'])
        elif isNextInside(G, node, [dentry.asn for dentry in dentries]):
            print("next inside!")
            node_colors.append(COLORS['blue'])
        else: # this router isn't special :(
            node_colors.append(COLORS['blue'])
            labeldict[node] = ''
plt.figure(figsize=(25,20))
#nx.draw(G, node_color=node_colors, labels=labeldict, with_labels=True, width=weights, edge_color=edge_colors)
nx.draw_spring(G, node_color=node_colors, labels=labeldict, with_labels=True, width=weights, edge_color=edge_colors)
plt.savefig('./made_a_graph.png', dpi=250)
