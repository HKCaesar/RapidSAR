"""

Extracts info from Sentinel images and inserts into SQLite database

Overview
========

Small script which extracts relevant information from Sentinal-1 .SAFE directories, and inserts it into the given SQLite database. The database consists of two tables (files and bursts) and a relation table. The files table contains all the measurement files and associated information like acquisition date, polarisation, swath, pass direction, etc. The bursts table contain information about each burst, mainly location information. The relation table provides information about which bursts are present in each file, and vice versa. Entries already in the database will be ignored.

Functions
=========

Main functions
--------------
  db_insert:
    Handles the insertion of all measurement files contained in the given
    .SAFE directory

Contributors
============

Karsten Spaans, University of Leeds
-----------------------------------
October 2015: Original implementation

Usage
=====

python S1_insert_db.py -d </path/to/.SAFE/directory/> -o </path/to/database/file>

    -d         Defines path to .SAFE directory containing measurement files to               be inserted
    -o         Defines location and name of SQLite database file to be used

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

    try:
        try:
            opts, args = getopt.getopt(argv[1:], "hd:o:", ["help"])
        except getopt.error, msg:
            raise Usage(msg)
        for o, a in opts:
            if o == '-h' or o == '--help':
                print __doc__
                return 0
            elif o == '-d':
                datadir = a
            elif o == '-o':
                dbfilename = a
        
        conn = sqlite3.connect(dbfilename)
        c = conn.cursor()

        if not os.path.exists(datadir):
            raise Usage('Output data directory {0} does not exist.'.format(datadir))
        elif not os.path.isdir(datadir):
            raise Usage('Given output data directory {0} is not a directory.'.format(datadir))

    except Usage, err:
        print >>sys.stderr, "\nWoops, something went wrong:"
        print >>sys.stderr, "  "+str(err.msg)
        print >>sys.stderr, "\nFor help, use -h or --help.\n"
        return 2

    dirlist = os.listdir(datadir)
    dirlist.sort()
    for d in dirlist:
        if os.path.isdir(os.path.join(datadir,d)) and d[-5:] == '.SAFE':
            c, conn = db_insert(os.path.join(datadir,d),c,conn)
    conn.close()

def db_insert(S1dir,c,conn):
    filelist = os.listdir(os.path.join(S1dir,'measurement'))
    orbitno = get_orbit(S1dir)
    for f in filelist:
        if f[-4:] != 'tiff':
            continue

        c.execute('SELECT * FROM files WHERE id=\"{0}\"'.format(f))
        res = c.fetchall()
        if res:
            print 'File {0} already in database, skipping...'.format(f)
            continue
 
        annotfilename = os.path.join(S1dir,'annotation',f.split('.')[0]+'.xml')
        root = ET.ElementTree(file=annotfilename)
        polid = root.find('adsHeader').find('polarisation').text
        swathid = root.find('adsHeader').find('swath').text
        orbitdir = root.find('generalAnnotation').find('productInformation').find('pass').text
        sensdate = root.find('adsHeader').find('startTime').text[:10].split('-')
        sensdate = sensdate[0]+sensdate[1]+sensdate[2]
        exe_str = 'INSERT INTO files '+\
                  '(id, directory, track, '+\
                  'orbit_direction, swath, pol, date) '+\
                  'VALUES (\"{m}\", \"{di}\", {tr}, \"{od}\", {sw}, \"{pol}\", {date})'.format(m=f,di=S1dir,tr=orbitno,od=orbitdir,sw=swathid[-1],pol=polid, date=sensdate)
             
        c.execute(exe_str)

        linesPerBurst = np.int(root.find('swathTiming').find('linesPerBurst').text)
        pixelsPerBurst = np.int(root.find('swathTiming').find('samplesPerBurst').text)
        burstlist = root.find('swathTiming').find('burstList')
        noBurst = np.int(burstlist.attrib['count'])
        geolocGrid = root.find('geolocationGrid')[0]
        first = {}
        last = {}

        # Get burst corner geolocation info
        for geoPoint in geolocGrid:
            if geoPoint.find('pixel').text == '0':
                first[geoPoint.find('line').text] = np.float32([geoPoint.find('latitude').text,geoPoint.find('longitude').text])
            elif geoPoint.find('pixel').text == str(pixelsPerBurst-1):
                last[geoPoint.find('line').text] = np.float32([geoPoint.find('latitude').text,geoPoint.find('longitude').text])

        for i, b in enumerate(burstlist):
            firstline = str(i*linesPerBurst)
            lastline = str((i+1)*linesPerBurst)
            aziAnxTime = np.float32(b.find('azimuthAnxTime').text)
            burstid = np.int32(np.round(aziAnxTime*10))
            # first and lastline sometimes shifts by 1 for some reason?
            try:
                firstthis = first[firstline]
            except:
                firstline = str(int(firstline)-1)
                try:
                    firstthis = first[firstline]
                except:
                    print 'First line not found in {0}'.format(annotfilename)
                    firstthis = []
            try:
                lastthis = last[lastline]
            except:
                lastline = str(int(lastline)-1)
                try:
                    lastthis = last[lastline]
                except:
                    print 'Last line not found in {0}'.format(annotfilename)
                    lastthis = []
            corners = np.zeros([4,2],dtype=np.float32)

            # Had missing info for 1 burst in a file, hence the check
            if len(firstthis) > 0 and len(lastthis) > 0:
                corners[0] = first[firstline]
                corners[1] = last[firstline]
                corners[3] = first[lastline]
                corners[2] = last[lastline]
                corners2 = corners[np.argsort(corners[:,1],axis=0),:]
                centercoord = (corners2[0,:]+corners2[3,:])/2

            # Check if burst is already in database, checking 10 seconds in
            # time forward and back for burstid, might be better to check 
            # less?
            c.execute('SELECT id, burstid FROM bursts WHERE track = {tn} AND swath = {sn} AND burstid > {bl} AND burstid < {bu}'.format(tn=orbitno,sn=swathid[-1],bl=burstid-10,bu=burstid+10))
            burst_res = c.fetchall()
            if burst_res: # Already in db
                burstdbid = burst_res[0][0] 
                burstid = burst_res[0][1]
            elif len(firstthis) > 0 and len(lastthis) > 0: # Missing geoloc check again. 
                burstdbid = 'T'+str(orbitno)+'-'+str(swathid)+'-'+str(burstid)
                exe_str = 'INSERT INTO bursts '+\
                          '(id, track, orbit_direction, swath, burstid, '+\
                          'center_lat, center_lon, corner1_lat, corner1_lon, '+\
                          'corner2_lat, corner2_lon, corner3_lat, '+\
                          'corner3_lon, corner4_lat, corner4_lon)'+\
                          'VALUES (\"{0}\", {1}, \"{2}\", '.format(burstdbid,
                                                                   orbitno,
                                                                   orbitdir)+\
                          '{0}, {1}, {2}, {3}, '.format(swathid[-1],
                                                        burstid,
                                                        centercoord[0],
                                                        centercoord[1])+\
                          '{0}, {1}, {2}, {3}, '.format(corners[0,0],
                                                        corners[0,1],
                                                        corners[1,0],
                                                        corners[1,1])+\
                          '{0}, {1}, {2}, {3})'.format(corners[2,0],
                                                       corners[2,1],
                                                       corners[3,0],
                                                       corners[3,1])
                c.execute(exe_str)
                
         
            exe_str = 'INSERT INTO files_bursts '+\
                      '(file_id, burst_id, burst_no) '+\
                      'VALUES (\"{fi}\", \"{bi}\", {i})'.format(fi=f,bi=burstdbid,i=i+1)
            
            c.execute(exe_str)
            conn.commit()
    return c, conn
                       
def get_orbit(datadir):
    manifestfile = os.path.join(datadir,'manifest.safe')
    tree = ET.ElementTree(file=manifestfile)
    root = tree.getroot()
    metadatasection = root.find('metadataSection')
    for el in metadatasection:
        if el.attrib['ID'] == 'measurementOrbitReference':
            for orbel in el[0][0][0].iter():
                if 'relativeOrbitNumber' in orbel.tag and orbel.attrib['type'] == 'stop':
                    orbnumber = orbel.text
                    return orbnumber
                          
                          




if __name__ == "__main__":
    sys.exit(main())
