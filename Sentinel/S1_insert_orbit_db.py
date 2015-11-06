"""

Insert Sentinel orbit files into database

Overview
========

Script that takes sentinel precise or restituted orbit files and inserts relevant information into a database. This information is then used during processing of Sentinel SLC data to correct the annotated orbits.

Functions
========

  db_insert:
    Inserts Sentinel EOF orbit files into the database

Contributors
============

Karsten Spaans, Universoty of Leeds
-----------------------------------
October 2015: Original implementation

Usage
=====

S1_insert_orbit_db.py -d </path/to/orbit/files/> -o </path/to/orbit/database/file>

    -d        Defines path to directory containing EOF orbit files
    -o        Defines path and name of SQLite database file to be used
"""

import sys
import re
import getopt
import os
import shutil
import subprocess as subp
import h5py as h5
import numpy as np
import sqlite3

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

    except Usage, err:
        print >>sys.stderr, "\nWoops, something went wrong:"
        print >>sys.stderr, "  "+str(err.msg)
        print >>sys.stderr, "\nFor help, use -h or --help.\n"
        return 2

    filelist = os.listdir(datadir)
    for f in filelist:
        if f[-4:] == '.EOF':
            db_insert(datadir,f,c,conn)
    conn.close()

def db_insert(orbitdir,orbitfile,c,conn):
    if orbitfile.split('_')[3] == 'POEORB':
        table = 'porbits'
    elif orbitfile.split('_')[3] == 'RESORB':
        table = 'rorbits'
    else:
        return;

    c.execute('SELECT * FROM {1} WHERE id=\"{0}\"'.format(orbitfile,table))
    res = c.fetchall()
    if res:
        print 'Orbit file {0} already in database, located in {1}.'.format(orbitfile,res[0][1])
        print 'Skipping...'
        return
    else:
        print orbitfile

    begin, end = re.split('[_.]',orbitfile)[-3:-1]
    begin = begin[1:]

    begin = '{0}-{1}-{2}:{3}:{4}'.format(begin[:4],begin[4:6],begin[6:11],begin[11:13],begin[13:])
    end = '{0}-{1}-{2}:{3}:{4}'.format(end[:4],end[4:6],end[6:11],end[11:13],end[13:])

    exe_str = 'INSERT INTO {0} '.format(table)+\
              '(id, directory, begintime, endtime) '+\
              'VALUES (\"{0}\", \"{1}\", \"{2}\", \"{3}\");'.format(orbitfile,orbitdir,begin,end)
    c.execute(exe_str)
    conn.commit()
    return


if __name__ == "__main__":
    sys.exit(main())

