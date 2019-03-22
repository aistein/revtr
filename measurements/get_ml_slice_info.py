#! /usr/bin/python2

import xmlrpclib
import socket
import codecs
import random
import codecs
import sys
import os
from getpass import getpass
from collections import defaultdict

try:
    username = str(sys.argv[1])
    password = str(sys.argv[2])
    out_dir  = os.path.normpath(sys.argv[3])
except:
    exit("usage: get_ml_info.py <username> <password> <out_dir>")

pl_slice = "uw_geoloc4"
api_server = xmlrpclib.ServerProxy('https://www.planet-lab.org/PLCAPI/', allow_none=True)

# Create an empty dictionary (XML-RPC struct)
auth = {}

auth['AuthMethod'] = 'password'
auth['Username']   =  username
auth['AuthString'] =  password

print 'authenticated!  sending rpcs...'

# get all node ids for our slice
print "fetching node ids..."
site_filter = {'name' : pl_slice}
return_fields = ['node_ids']
res = api_server.GetSlices(auth, site_filter, return_fields)

print "done"
node_ids = res[0]['node_ids']

# get all booted nodes' hostnames and site ids
print "fetching hostnames and site ids..."
node_filter = {'node_id' : node_ids, 'boot_state' : 'boot'}
return_fields = ['hostname', 'site_id']
res = api_server.GetNodes(auth, node_filter, return_fields)

print "done"

# output mappings of booted hosts to site ids and all booted hosts
nodes_by_site = defaultdict(list)
fname = "%s_hostnames_site_ids.txt" % pl_slice
gname = "%s_all_hostnames.txt" % pl_slice
with open(os.path.join(out_dir, fname), "w+") as f,\
        open(os.path.join(out_dir, gname), "w+") as g:
    for record in res:
        hostname, site_id = record['hostname'], record['site_id']
        nodes_by_site[site_id].append(hostname)
        f.write("%s %s\n" % (hostname, site_id))
        g.write("%s\n" % hostname)

exit()
# choose a random host for each site
fname = "%s_candidate_hostnames.txt" % pl_slice
with open(os.path.join(out_dir, fname), "w+") as f:
    for site, nodes in nodes_by_site.items():
        candidate = random.choice(nodes)
        f.write("%s\n" % candidate)

# get site names
print "fetching site names..."
sites_by_node = {}
for r in res:
    site_filter = {'site_id' : r['site_id']}
    return_fields = ['name']
    site_res = api_server.GetSites(auth, site_filter, return_fields)
    sites_by_node[r['hostname']] = site_res[0]['name']

print "done"

fname = "%s_nodes_sitenames.txt" % pl_slice
with open(os.path.join(out_dir, fname), "w+") as f:

    utf8_writer = codecs.getwriter('utf8')
    f = utf8_writer(f)
    for hostname, site in sites_by_node.items():
        try:
            f.write("%s::%s\n" % (hostname, site) )
        except:
            f.write("%s::unknown\n" % hostname)
