"""

Search and/or download data from Sentinel SciHub

Overview
========

This script handles the searching and downloading of data from the Sentinel SciHub. When searching the data, and .qry file is used to give the search parameters. The search results in an .xml file being download from the SciHub. If an output directory is given, the data found will be downloaded. Alternatively, the .xml file can be reviewed first. If no .qry file is given, the .xml file is assumed to exist, and the data contained in it will be downloaded instead of searching for new data.

Functions
=========

Main functions
--------------
  
  do_query:
    Performs the search query on SciHub, resulting in an .xm file with query results
  get_data:
    Downloads the data contained in .xml file to destination directory

Aux functions
-------------

  parse_query_file:
    Parses the .qry file and sets up query to SciHub

Contributors
============

Karsten Spaans, University of Leeds
-----------------------------------
October 2015: Original implementation

Usage
=====

S1_find_data.py -d </path/to/target/directory/> -q </path/to/query/file> -u <scihub username> -p <scihub password> -x </path/to/xmlfile>

    -d        Defines path to target directory for download. If omitted, data will
              not be downloaded
    -q        Path and name of file containing query parameters. If omitted, the file 
              given with the -x options must exists, and must be a valid SciHub xml file.
              This xml file will then be used to download
    -u        SciHub username
    -p        SciHub password
    -x        Path and name of SciHub query result .xml file
"""



import sys
import getopt
import os
import subprocess as subp
import sqlite3

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

import pdb

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
    if argv == None:
        argv = sys.argv
    queryfile = []
    xmlfile = []
    datadir = []

    try:
        try:
            opts, args = getopt.getopt(argv[1:], "hd:q:u:p:x:", ["help"])
        except getopt.error, msg:
            raise Usage(msg)
        for o, a in opts:
            if o == '-h' or o == '--help':
                print __doc__
                return 0
            elif o == '-d':
                datadir = a
            elif o == '-q':
                queryfile = a
            elif o == '-u':
                username = a
            elif o == '-p':
                password = a
            elif o == '-x':
                xmlfile = a
        
        if datadir:
            if not os.path.exists(datadir):
                raise Usage('Target data directory {0} does not exist.'.format(datadir))
            elif not os.path.isdir(datadir):
                raise Usage('Given target data directory {0} is not a directory.'.format(datadir))

        if queryfile:
            if not os.path.exists(queryfile):
                raise Usage('Given query file {0} does not exist.'.format(queryfile))     

        if not xmlfile:
            raise Usage('No xml query file location given, -x option is not optional!')
    
    except Usage, err:
        print >>sys.stderr, "\nWoops, something went wrong:"
        print >>sys.stderr, "  "+str(err.msg)
        print >>sys.stderr, "\nFor help, use -h or --help.\n"
        return 2
    
    if queryfile:
        query = parse_query_file(queryfile)
        rc = do_query(query,xmlfile,username,password)
        if rc > 0:
            print 'Something went wrong performing the query, exiting.'
            return 1
    dbfilename = '/nfs/a1/raw/sentinel/iceland/S1_iceland.sql'
    downloadlist = parse_xml(xmlfile)
    if datadir:
        get_data(datadir,downloadlist,username,password,dbfilename)


def get_data(ddir,dl,un,pw,dbfile):
    total = len(dl)
    dllist = []
    for i,e in enumerate(dl):
        filename = ddir+'/'+e['id']+'.zip'
        if os.path.exists(filename):
            print '{0} already exists.'.format(filename)
            continue
        conn = sqlite3.connect(dbfile)
        c = conn.cursor()
        query = 'SELECT * FROM files WHERE directory LIKE \"%{0}.SAFE\"'.format(e['id'])
        c.execute(query)
        res = c.fetchall()
        if res:
            print '{0}.SAFE is already in the database!'.format(e['id'])
            continue
        url = e['link'].strip('\"')
        dlcall = ['wget','--no-check-certificate','--user',un,'--password',pw,'-O',filename,url]
        print '\nDownloading {0} ({1} of {2})\n'.format(e['id'],i+1,total)
        rc = subp.call(dlcall)
        dllist.append(e['id'])
    return dllist



def parse_xml(xmlfile):
    tree = ET.ElementTree(file=xmlfile)
    root = tree.getroot()
    entrylist = []
    for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
        entrydict = {}
        for node in entry.iter():
            if ('link' in node.tag) and (not 'rel' in node.attrib):
                link = node.attrib['href'] 
            elif ('name' in node.attrib.keys()):
                if node.attrib['name'] == 'identifier':
                    ident = node.text
        entrydict['link'] = link
        entrydict['id'] = ident
        entrylist.append(entrydict)
    return entrylist

def parse_query_file(queryfile):
    querylist = ['(platformname:Sentinel-1)']
    with open(queryfile) as f:
        for l in f:
            ls = l.split(':')
            if ls[0][0] == '#':
                continue
            elif ls[0] == 'PRODUCT':
                querylist.append('(productType:\"{0}\")'.format(ls[1].strip()))
            elif ls[0] == 'DATERANGE':
                dr = ls[1].strip().split(' ')
                if len(dr) == 1:
                    date = dr[0]
                    querylist.append('(beginPosition:[{0}-{1}-{2}T00:00:00.000Z+TO+{0}-{1}-{2}T23:59:59.999Z])'.format(date[:4],date[4:6],date[6:8]))
                else:
                    date1,date2 = dr
                    querylist.append('(beginPosition:[{0}-{1}-{2}T00:00:00.000Z+TO+{3}-{4}-{5}T23:59:59.999Z])'.format(date1[:4],date1[4:6],date1[6:8],date2[:4],date2[4:6],date2[6:8]))
            elif ls[0] == 'POLYGON':
                poly = ls[1].strip().split(' ')
                querylist.append('(footprint:\"Intersects(POLYGON(({0}+{1},{2}+{1},{2}+{3},{0}+{3},{0}+{1})))\")'.format(poly[0],poly[1],poly[2],poly[3]))
    query = ''
    for q in querylist:
        query += q
        query += '+AND+'
    query = query[:-5] #Remove final +AND+
    return query

def do_query(query,outputfile,un,pw):
    url = 'https://scihub.esa.int/dhus/search?q={0}&rows=200000'.format(query)
    qcall = ['wget','--no-check-certificate','--user',un,'--password',pw,'-O',outputfile,url]
    returncode = subp.call(qcall)
    return returncode


if __name__ == "__main__":
    sys.exit(main())


