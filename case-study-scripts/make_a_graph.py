#! /usr/local/bin/python3 -u

import os
import sys
import csv
import json
import pyasn
import networkx as nx
from collections import namedtuple, defaultdict
import matplotlib.pyplot as plt
import lookup_dnet as l

NodeInfo = namedtuple('NodeInfo', ['type','ip','asn','bgp','s24'])

try:
    measfile = sys.argv[1]
    target_prefix = sys.argv[2]
    ipasnfile = sys.argv[3]
except:
    exit('usage: ./make_a_graph.py <measfile>.json <target_prefix> <ipasnfile>.dat')

G = nx.DiGraph()
ipasn = pyasn.pyasn(ipasnfile)
#sample_destinations = ['209.58.221.82','209.58.221.217','209.58.221.150']
sample_destinations = ['58.230.169.82']

with open(measfile, 'r') as f:
    measurements_to_target_prefix_by_vp = json.load(f) 

# make dummy source to orient all VPs and dummy sink to orient all destinations during plotting
dummySourceNode = NodeInfo('dummy','source','','','')
dummySinkNode = NodeInfo('dummy','sink','','','')
destNode = NodeInfo('dest', '58.230.169.82', '9318', '58.224.0.0/13', '58.230.169.0/24')

for vp, measurements_to_target_prefix in measurements_to_target_prefix_by_vp.items():
    vpNode = NodeInfo('vp',vp,'','','')
    for meas in measurements_to_target_prefix:
        asn, bgp, s24, dest, dist, hops = (*meas[:5], meas[5:])
        if dest not in sample_destinations or int(dist) > 8:
            continue
        prev, curr = 0, 1
        lookup = l.dnets_of(hops[prev], ipasn)
        prevNode = NodeInfo('router',hops[prev], lookup.asn, lookup.bgp, lookup.s24)
        G.add_edge(dummySourceNode, vpNode, weight=0.9)
        G.add_edge(vpNode, prevNode, weight=1)
        while curr < len(hops) and hops[prev] != dest:
            lookup = l.dnets_of(hops[curr], ipasn)
            if hops[curr] == dest:
                currNode = destNode
            else:
                currNode = NodeInfo('router', hops[curr], lookup.asn, lookup.bgp, lookup.s24)
            G.add_edge(prevNode, currNode)
            if 'weight' not in G.edges[prevNode, currNode]:
                G.edges[prevNode, currNode]['weight'] = 1
            G.edges[prevNode, currNode]['weight'] += 1
            prev, curr, prevNode = prev+1, curr+1, currNode

# connect destination to dummySinkNode (TODO: get this out of the big loop)
G.add_edge(destNode, dummySinkNode, weight=1)

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

# ordered edge-weights for plotting
#G.remove_edges_from(list(filter(lambda tup: G[tup[0]][tup[1]]['weight'] < 5, G.edges())))
#G.remove_nodes_from(list(filter(lambda u: nx.degree(G, u) < 5, G.nodes())))
weights = [G[u][v]['weight'] for u,v in G.edges()]
edge_colors = ['white' if G[u][v]['weight'] == 0.9 else 'black' for u,v in G.edges()]

color_map = []
labeldict = {}
dentries = [l.dnets_of(dest, ipasn) for dest in sample_destinations]
for node in G:
    labeldict[node] = node.ip
    if node.type == 'dummy':
        color_map.append('white')
        labeldict[node] = ''
    elif node.type == 'vp':
        color_map.append('green')
    elif node.type == 'dest':
        color_map.append('red')
    elif node.type == 'router':
        print(node)
        if node.s24 in [dentry.s24 for dentry in dentries]:
            print("found s24!")
            color_map.append('yellow')
        elif node.bgp in [dentry.bgp for dentry in dentries]:
            print("found bgp!")
            color_map.append('cyan')
        elif node.asn == [dentry.asn for dentry in dentries]:
            print("found asn!")
            color_map.append('orange')
        elif isNextInside(G, node, [dentry.asn for dentry in dentries]):
            print("next inside!")
            color_map.append('blue')
        else: # this router isn't special :(
            color_map.append('blue')
            labeldict[node] = ''
plt.figure(figsize=(25,20))
#nx.draw(G, node_color=color_map, labels=labeldict, with_labels=True, width=weights, edge_color=edge_colors)
nx.draw_spring(G, node_color=color_map, labels=labeldict, with_labels=True, width=weights, edge_color=edge_colors)
plt.savefig('./made_a_graph.png')
