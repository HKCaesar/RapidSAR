"""

Extracts bursts from Sentinel 1 SLC files and concatenate into user specified image

Overview
========

This script extracts bursts contained in burstid.list into a new SLC image, ready to be processed further. It tries all dates contained in date.list, and extracts the data if all bursts are available for that date. If all bursts are not available, the date is skipped. The script performs the mli-mosaicing and the slc-mosaicing as well, and outputs a bmp preview of the mli-mosaic. To adjust the image coverage, the burstid.list file can be adjusted. This script uses the Gamma software package.

Functions
=========

Main functions
--------------

  make_image:
    Main script handling the image creation
  par_s1_slc:
    Generates Gamma SLC parameter and image files from Sentinel SLC files
  copy_bursts:
    Copies chosen bursts from SLC files on a swath by swath basis
  slc_cat:
    Concatenates bursts from different files into 1 files, on a swath by swath basis
  mosaic_TOPS:
    Create SLC mosaic from swaths
  multi_TOPS:
    Create MLI mosaic from swaths
  apply_precise_orbit:
    Replace annotated orbits with precise or restituted orbit information

Aux functions
-------------

  make_SLC_tab:
    Create Gamma SLC_tab
  rename_SLC:
    Renames SLC, SLC_par and TOPS_par files
  remove_slc:
    Removes SLC, SLC_par and TOPS_par files
  parse_slc_tab:
    Extract relevant information from SLC_tab
  get_par_data:
    Extract line containing search string from slc_par file

Contributors
============

Karsten Spaans, University of Leeds
-----------------------------------
October 2015: Original Implementation

Usage
=====

S1_setup_image.py -d </path/to/database/file> -o </path/to/processing/directory>

    -d         Defines path and name of local database file
    -o         Defines path to output processing directory
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
    outputdir = []
    # BEWARE: Orbit database file hardcoded for now!!!!!!
    orbitdb = '/nfs/a1/raw/sentinel/iceland/S1_orbits.sql'

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
                dbfilename = a
            elif o == '-o':
                outputdir = a
        
        if not dbfilename:
            raise Usage('No SQLite database file name give, -d option is not optional!')     
        if os.path.exists(dbfilename):
            conn = sqlite3.connect(dbfilename)
            c = conn.cursor()
        else:
            raise Usage('SQLite database {0} does not seem to exist?'.format(dbfilename))

    except Usage, err:
        print >>sys.stderr, "\nWoops, something went wrong:"
        print >>sys.stderr, "  "+str(err.msg)
        print >>sys.stderr, "\nFor help, use -h or --help.\n"
        return 2

    idoutputfilename = os.path.join(outputdir,'burstid.list')
    dateoutputfilename = os.path.join(outputdir,'date.list')

    burstidlist = []
    with open(idoutputfilename,'r') as f:
        for l in f.read().strip().split('\n'):
            burstidlist.append(l)
            
    datelist = []
    with open(dateoutputfilename,'r') as f:
        for l in f.read().strip().split('\n'):
            datelist.append(l)

    for date in datelist:
        make_image(outputdir, burstidlist,date,c, orbitdb)
    


def make_image(destdir, burstidlist, date, c, orbitdb):
    slcdir = os.path.join(destdir,"SLC")
    datedir = os.path.join(slcdir,date)
    if not os.path.exists(slcdir):
        os.mkdir(slcdir)
    if not os.path.exists(datedir):
        os.mkdir(datedir)

    
    query = 'SELECT files.id, files.directory, files.swath, files_bursts.burst_no '
    query += 'FROM files, files_bursts, bursts '
    query += 'WHERE bursts.id = files_bursts.burst_id AND '
    query += 'files.id = files_bursts.file_id AND '
    query += '(files.pol = "HH" OR files.pol = "VV") AND '
    query += 'files.date = {0} AND '.format(date)

    filelist = []
    dirlist = []
    swathlist = []
    burstnolist = []
    
    for bi in burstidlist:
        querythis = query+'bursts.id = \"{0}\";'.format(bi)
        c.execute(querythis)
        res = c.fetchall()
        if res:
            filelist.append(res[0][0])
            dirlist.append(res[0][1])
            swathlist.append(res[0][2])
            burstnolist.append(res[0][3])
        else:
            os.rmdir(datedir)
            print 'Missing bursts from date {0}? Skipping...'.format(date)
            return

    for swath in range(1,4):
        if swath in swathlist:
            filesthis = [ f for f, s  in zip(filelist,swathlist) if s == swath ]
            for i, f in enumerate(sorted(set(filesthis))): # Sort important to ensure slices are processed in order of acquisition!
                burstnothis = np.array([ bn for bn, sl, fl  in zip(burstnolist,swathlist, filelist) if sl == swath and fl == f])
                dirthis = [ d for d, sl, fl  in zip(dirlist,swathlist, filelist) if sl == swath and fl == f ][0]
                tiffthis = os.path.join(dirthis,'measurement',f)
                timethis = tiffthis.split('t')[-2].split('-')[0]
                annotfile = '{0}xml'.format(f[:-4])
                annotthis = os.path.join(dirthis,'annotation',annotfile)
                calibfile = 'calibration-{0}'.format(annotfile)
                calibthis = os.path.join(dirthis,'annotation','calibration',calibfile)
                noisefile = 'noise-{0}'.format(annotfile)
                noisethis = os.path.join(dirthis,'annotation','calibration',noisefile)
                slcthis = os.path.join(slcdir,date,'{0}_{1}'.format(date,i))

                pol = par_s1_slc(tiffthis,annotthis,calibthis,noisethis,slcthis)
                tabname = os.path.join(destdir,'SLC{0}_tab'.format(i))
                filename = os.path.join(slcdir,date,'{0}_tmp'.format(date))
                make_SLC_tab(tabname,slcthis,[swath],pol)
                make_SLC_tab(os.path.join(destdir,'SLCtmp_tab'),filename,[swath],pol)
                copy_bursts(tabname,os.path.join(destdir,'SLCtmp_tab'),burstnothis.min(),burstnothis.max())
            if i == 0:
                tabname = os.path.join(destdir,'SLC_tab')
                filename = os.path.join(slcdir,date,'{0}'.format(date))
                make_SLC_tab(tabname,filename,[swath],pol)
                rename_slc(os.path.join(destdir,'SLC0_tab'),
                               os.path.join(destdir,'SLC_tab'))
            else:
                tabname = os.path.join(destdir,'SLCtmp_tab')
                filename = os.path.join(slcdir,date,'{0}_tmp'.format(date))
                make_SLC_tab(tabname,filename,[swath],pol)
                tabname = os.path.join(destdir,'SLC_tab')
                filename = os.path.join(slcdir,date,'{0}'.format(date))
                make_SLC_tab(tabname,filename,[swath],pol)
                for ix in range(i):
                    slc_cat(os.path.join(destdir,'SLC{0}_tab'.format(ix)),
                            os.path.join(destdir,'SLC{0}_tab'.format(ix+1)),
                            os.path.join(destdir,'SLC{0}_tab'.format('tmp')))
                    remove_slc(os.path.join(destdir,'SLC{0}_tab'.format(ix)))
                    remove_slc(os.path.join(destdir,'SLC{0}_tab'.format(ix+1)))
                    if ix < i-1:
                        rename_slc(os.path.join(destdir,'SLC{0}_tab'.format('tmp')),
                                   os.path.join(destdir,'SLC{0}_tab'.format(ix+1)))
                    else:
                        try:
                            rename_slc(os.path.join(destdir,'SLCtmp_tab'),
                                       os.path.join(destdir,'SLC_tab'))
                        except:
                            pdb.set_trace()
                    
                            
                
                
    make_SLC_tab(tabname,filename,sorted(set(swathlist)),pol)
    multi_TOPS(tabname,filename,5,1)
    mosaic_TOPS(tabname,filename,1,1)
    apply_precise_orbit(filename,orbitdb,date,timethis)

        

def apply_precise_orbit(filename,orbitdb,date,time):
    conn = sqlite3.connect(orbitdb)
    c = conn.cursor()
    datetime = '{0}-{1}-{2}T{3}:{4}:{5}'.format(date[:4],date[4:6],date[6:],time[:2],time[2:4],time[4:])

    exe_str = 'SELECT id, directory FROM porbits '+\
              'WHERE strftime(\'%s\',\"{0}\") BETWEEN strftime(\'%s\',begintime) AND strftime(\'%s\', endtime);'.format(datetime)

    c.execute(exe_str)
    res = c.fetchall()
    if res:
        orbitfile = os.path.join(res[0][1],res[0][0])
    else:
        exe_str = 'SELECT id, directory FROM rorbits '+\
              'WHERE strftime(\'%s\',\"{0}\") BETWEEN strftime(\'%s\',begintime) AND strftime(\'%s\', endtime);'.format(datetime)
        c.execute(exe_str)
        res = c.fetchall()
        if res:
            orbitfile = os.path.join(res[0][1],res[0][0])
        else:
            print 'No orbit file found for time {0}'.format(datetime)
            pdb.set_trace()
            return
    
    comm = 'S1_OPOD_vec {0}.mli.par {1}'.format(filename,orbitfile)
    os.system(comm)
    comm = 'S1_OPOD_vec {0}.slc.par {1}'.format(filename,orbitfile)
    os.system(comm)
 
def copy_bursts(SLCtab,SLCnewtab,minburst,maxburst):
    comm = 'SLC_copy_S1_TOPS {0} {1} 1 {2} 1 {3}'.format(SLCtab,SLCnewtab,minburst,maxburst)
    os.system(comm)
    rename_slc(SLCnewtab,SLCtab)

def mosaic_TOPS(tab,slcname,azml,rgml):
    comm = 'SLC_mosaic_S1_TOPS {0} {1}.slc {1}.slc.par {2} {3}'.format(tab,slcname,azml,rgml)
    os.system(comm)

def multi_TOPS(tab,mliname,azml,rgml):
    comm = 'multi_S1_TOPS {0} {1}.mli {1}.mli.par {2} {3}'.format(tab,mliname,azml,rgml)
    os.system(comm)
    mli_width = get_par_data(mliname+'.mli.par','range_samples')
    comm = 'raspwr {0}.mli {1}'.format(mliname,mli_width)
    os.system(comm)

def get_par_data(parfile,searchstring):
    with open(parfile) as f:
        for l in f:
            ll = l.strip().split(':')
            if len(ll) > 1:
                if searchstring in ll[0]:
                    return ll[1].strip()

def slc_cat(tab1,tab2,tab3):
    comm = 'SLC_cat_S1_TOPS {0} {1} {2}'.format(tab1,tab2,tab3)
    os.system(comm)

def make_SLC_tab(tabname,filename,swath,pol):
    with open(tabname,'w') as f:
        for s in swath:
            f.write('{0}.iw{1}.{2}.slc {0}.iw{1}.{2}.slc.par {0}.iw{1}.{2}.TOPS_par\n'.format(filename,s,pol))

def rename_slc(slcoldtab,slcnewtab):
    slc1, slc_par1, tops_par1 = parse_slc_tab(slcoldtab)
    slc2, slc_par2, tops_par2 = parse_slc_tab(slcnewtab)
    shutil.move(slc1,slc2)
    shutil.move(slc_par1,slc_par2)
    shutil.move(tops_par1,tops_par2)
    

def remove_slc(slctab):
    slc, slc_par, tops_par = parse_slc_tab(slctab)
    os.remove(slc)
    os.remove(slc_par)
    os.remove(tops_par)

def parse_slc_tab(slctab):
    with open(slctab) as f:
        slc, slc_par, tops_par = f.read().strip().split(' ')
    return slc, slc_par, tops_par

def par_s1_slc(geotiff,annotation,calibration,noise,slc):
    swath = geotiff[-65:-62]
    pol = geotiff[-57:-55]
    comm = 'par_S1_SLC {0} {1} {2} {3} {4}.{5}.{6}.slc.par {4}.{5}.{6}.slc {4}.{5}.{6}.TOPS_par'.format(geotiff,annotation,calibration,noise,slc,swath,pol)
    os.system(comm)
    return pol

if __name__ == "__main__":
    sys.exit(main())
