"""

Search local database for available data

Overview
========

Allows the user to search the database for available data. The initial search is done using parameters from a .qry file. The user is then presented with a map of all available data, and is asked to pick a track. Two files are created based on the users choice. One file is called burstid.list, and contains the burstids of all bursts in the search area. The second file is called dates.list, and contains the acquisition dates of available images. These files can be adjusted to fine-tune the processing, for example by removing bursts covering only water. 

Functions
=========

Main functions
--------------

  do_query:
    Reads parameters from .qry file and searches database, outputting burstids and dates 
    available

Aux functions
-------------

  plot_query:
    Plots the outlines of the available data, to allow user to make a choice

Contributors
============

Karsten Spaans. University of Leeds
-----------------------------------
October 2015: Original implementation

Usage
=====

S1_query_db.py -d </path/to/database/file> -q </path/to/query/file> -o </path/to/output/directory/>

    -d        Defines path and name of local database file
    -q        Defines path and name of .qry file containing query parameters
    -o        Defines path of output processing directory
"""


import sys
import getopt
import os
import shutil
import subprocess as subp
import h5py as h5
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
from multiprocessing import Process
from scipy.spatial import ConvexHull

import pdb

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
    if argv == None:
        argv = sys.argv

    dbfilename = []
    queryfilename = []
    outputdir = []

    try:
        try:
            opts, args = getopt.getopt(argv[1:], "hd:q:o:", ["help"])
        except getopt.error, msg:
            raise Usage(msg)
        for o, a in opts:
            if o == '-h' or o == '--help':
                print __doc__
                return 0
            elif o == '-d':
                dbfilename = a
            elif o == '-q':
                queryfilename = a
            elif o == '-o':
                outputdir = a
        
        if not dbfilename:
            raise Usage('No SQLite database file name give, -d option is not optional!')     
        if os.path.exists(dbfilename):
            conn = sqlite3.connect(dbfilename)
            c = conn.cursor()
        else:
            raise Usage('SQLite database {0} does not seem to exist?'.format(dbfilename))
        
        if not queryfilename:
            raise Usage('No query file given, -q option is not optional!')
        if not os.path.exists(queryfilename):
            raise Usage('Given query file {0} does not exist'.format(queryfilename))

    except Usage, err:
        print >>sys.stderr, "\nWoops, something went wrong:"
        print >>sys.stderr, "  "+str(err.msg)
        print >>sys.stderr, "\nFor help, use -h or --help.\n"
        return 2

    idlist, datelist = do_query(queryfilename,c)
    if outputdir:
        idoutputfilename = os.path.join(outputdir,'burstid.list')
        dateoutputfilename = os.path.join(outputdir,'date.list')
        with open(idoutputfilename,'w') as f:
            for i in idlist:
                f.write(i+'\n')
                
        with open(dateoutputfilename,'w') as f:
            for d in datelist:
                f.write(str(d)+'\n')
    else:
        print '\nSelected burst ids:'
        for i in idlist:
            print i
        print '\nSelected dates:'
        for d in datelist:
            print d

def do_query(queryfile, c):

    query = 'SELECT bursts.id, bursts.track, bursts.swath, bursts.orbit_direction, bursts.burstid, bursts.corner1_lat, bursts.corner1_lon, bursts.corner2_lat, bursts.corner2_lon, bursts.corner3_lat, bursts.corner3_lon, bursts.corner4_lat, bursts.corner4_lon '
    query += 'FROM bursts WHERE '

    datequery = ''
    with open(queryfile) as f:
        for l in f:
            ls = l.split(':')
            if ls[0][0] == '#':
                continue
            elif ls[0] == 'DATERANGE':
                dr = ls[1].strip().split(' ')
                if len(dr) == 1:
                    datequery += 'files.date = {0} AND '.format(dr[0])
                else:
                    datequery += 'files.date >= {0} AND files.date <= {1} AND '.format(dr[0], dr[1])
            elif ls[0] == 'POLYGON':
                poly = np.float32(ls[1].strip().split(' '))
                lat = sorted([poly[1],poly[3]])
                lon = sorted([poly[0],poly[2]])
                query += 'center_lon > {0} AND center_lon < {1} AND '.format(lon[0],lon[1])
                query += 'center_lat > {0} AND center_lat < {1} AND '.format(lat[0],lat[1])
                querybox = np.array( ( (lon[0], lat[0]) , (lon[1], lat[0]) ,
                                       (lon[1], lat[1]) , (lon[0], lat[1]) ,
                                       (lon[0], lat[0]) ) )
                
            elif ls[0] == 'TRACK':
                track = ls[1].strip().split(' ')
                query += 'files.track = {0} AND '.format(track)
            elif ls[0] == 'ORBITDIR':
                orbitdir = ls[1].strip().split(' ')[0].upper()
                query += 'files.orbit_direction = {0} AND '.format(orbitdir)
    
#    if len(datequery) > 0:
#        query += datequery
    
    if query[-4:] == 'AND ':
        query = query[:-5]+';'
        
    c.execute(query)
    result = c.fetchall()
    ids = []
    tracks = []
    swaths = []
    orbitdirs = []
    burstids = []
    corner_lats = []
    corner_lons = []
    
    for r in result:
        ids.append(r[0])
        tracks.append(r[1])
        swaths.append(r[2])
        orbitdirs.append(r[3])
        burstids.append(r[4])
        corner_lats.append((r[5],r[7],r[9],r[11]))
        corner_lons.append((r[6],r[8],r[10],r[12]))
    tracks_unique = set(tracks)

    trackthis = []
    
    burstid_dict = {}
    swath_dict = {}
    id_dict = {}
    corner_dict = {}
    for t, i, s, o, b, cla, clo in zip(tracks,ids,swaths,orbitdirs,burstids,corner_lats,corner_lons):
        if trackthis == t:
            idsthis.append(i)
            swathsthis.append(s)
            burstidsthis.append(s)
            cornersthis = np.concatenate((cornersthis,np.array((cla,clo)).T))
        else:
            if trackthis != []:
                id_dict[str(trackthis)] = idsthis
                swath_dict[str(trackthis)] = swathsthis
                burstid_dict[str(trackthis)] = burstidsthis
                corner_dict[str(trackthis)] = cornersthis
            trackthis = t
            orbitsdirthis = o
            idsthis = [i]
            swathsthis = [s]
            burstidsthis = [b]
            cornersthis = np.array((cla,clo)).T
    id_dict[str(trackthis)] = idsthis
    swath_dict[str(trackthis)] = swathsthis
    burstid_dict[str(trackthis)] = burstidsthis
    corner_dict[str(trackthis)] = cornersthis

    tracks = id_dict.keys()
    points = []
    ch = []
    no_date = []
    for t in tracks:
        points.append(corner_dict[t])
        ch.append(ConvexHull(corner_dict[t]))
        query = 'SELECT files.id '
        query += 'FROM files, files_bursts, bursts '
        query += 'WHERE bursts.id = files_bursts.burst_id AND '
        query += 'files.id = files_bursts.file_id AND '
        query += '(files.pol = \"VV\" OR files.pol = \"HH\") AND '
        query += datequery
        query += 'bursts.id = \"{0}\";'.format(id_dict[t][0])
        c.execute(query)
        res = c.fetchall()
        no_date = len(res)
    p = Process(target=plot_query, args=([querybox,points,ch,tracks,no_date]))
    p.start()

    print 'Available tracks:',
    for t in tracks:
        print t, 
        
        
    print ' '
    trackchoice = np.int32(input('\nPlease enter the track number of your choice: '))
    
    if not str(trackchoice) in tracks:
        print 'Track {0} is not a valid option. Please close figure window and try again'.format(trackchoice)
        p.join()
        return 1
    
    print 'Track {0} chosen, please close figure window to continue.'.format(trackchoice)
    p.join()

    query = 'SELECT files.id, files.date, files.directory, files.swath, files.pol, files_bursts.burst_no '
    query += 'FROM files, files_bursts, bursts '
    query += 'WHERE bursts.id = files_bursts.burst_id AND '
    query += 'files.id = files_bursts.file_id AND '
    

    id_choice = sorted(id_dict[str(trackchoice)])
    querythis = query+'(files.pol = \"VV\" OR files.pol = \"HH\") AND '
    querythis += 'bursts.id = "{0}";'.format(id_choice[0])
    c.execute(querythis)
    res = c.fetchall()
    hhcount = 0
    vvcount = 0
    for b in res:
         if b[4] == 'HH':
             hhcount+=1
         elif b[4] == 'VV':
             vvcount+=1
    if hhcount > vvcount:
        polchoice = 'HH'
        no_burst = hhcount
    else:
        polchoice = 'VV'
        no_burst = vvcount

    print '\n{0} images have polarisation HH, {1} images have polarisation VV, using polarisation {2}: '.format(hhcount,vvcount,polchoice)
    
    querythis = query+'files.pol = \"{0}\" AND '.format(polchoice)
    querythis += 'bursts.id = "{0}";'.format(id_choice[0])
    c.execute(querythis)
    res = c.fetchall()
    datelist = []
    for d in res:
        datelist.append(d[1])

    return id_choice,datelist

def plot_query(querybox, points, convhull,tracks, no_date):
    colours = ['r','b','g','y','m','c']
    coast = np.loadtxt('/nfs/a1/homes/eekhs/GMTplots/mapdata/is_coast.xy',delimiter=' ')
    plt.plot(coast[:,0],coast[:,1],'k')
    i = 0
    plt.plot(querybox[:,0],querybox[:,1],colours[i])
    for p, ch, t in zip(points, convhull, tracks):
        i+=1
        pt = p[ch.vertices]
        pt = np.concatenate((pt,pt[0,None]))
        plt.plot(pt[:,1], pt[:,0],colours[i])
        y,x = np.mean(pt,axis=0)
        plt.text(x,y,'{0},\n {1} images'.format(t,no_date),color=colours[i])
    plt.show()


if __name__ == "__main__":
    sys.exit(main())
