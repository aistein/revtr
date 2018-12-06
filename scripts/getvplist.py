#!/usr/local/bin/python3

import re, pycurl, yaml
from io import BytesIO
from bs4 import BeautifulSoup

try:
    credfile = open("./credentials.yml", 'r')
    credentials = yaml.load(credfile)
    credfile.close()
except (FileNotFoundError,IndexError):
    raise ValueError('error: credentials.yml files either not found or incorrect.')

# userpwd to be used by cget function
userpwd = credentials['username'] + ":" + credentials['password']

url = "http://bgoodc.cs.columbia.edu/storage/measurements_2018/csv_echo_replies_20181201/"

# the python wget package has no options, so using curl instead
def cget(url):
    buffer = BytesIO()
    c = pycurl.Curl()
    c.setopt(c.URL, url)
    c.setopt(c.USERPWD, userpwd)
    c.setopt(c.WRITEDATA, buffer)
    c.perform()
    c.close()
    return buffer.getvalue().decode('iso-8859-1')

# get the list of vp-csv files
vplist = set()
vplisthtml = cget(url)
soup = BeautifulSoup(vplisthtml,'html.parser')
for hit in soup.find_all('a'):
    match = re.match(r'^.*csv', hit['href'])
    if match:
        vplist.add(match[0])

# for each CSV file in the list, process it!
print("\n".join(vplist))

