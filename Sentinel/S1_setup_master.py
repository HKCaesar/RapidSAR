"""

Performs the geocoding of the master

Overview
========

This script performs the geocoding of the master using an external DEM. Uses the Gamma software package

Functions
=========

Main functions
--------------

  calc_dem_lut:
    Calculates lookup table and DEM related products for terrain-corrected geocoding
  calc_terrain_norm:
    Calculate terrain-based sigma0 and gamma0 normalization area in slant-range geometry
  get_offset:
    Get offsets using cross-correlation and fit offset function
  calc_fine_dem_lut:
    Refine lookup table
  geocode:
    Geocode products

Contributors
============

Karsten Spaans, University of Leeds
-----------------------------------
October 2015: Original implementation

Usage
=====

S1_setup_master.py -d </path/to/processing/directory> -m <masterdate> -e </path/to/dem>

    -d      Defines path to processing directory
    -m      The masterdate chosen by the user, in the format <YYYYMMDD>
    -e      Path and filename of external DEM
"""
  


import sys
import getopt
import os
import shutil
import subprocess as subp
import h5py as h5
import numpy as np
from utils import grep

import pdb

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
    if argv == None:
        argv = sys.argv
    
    datadir = []
    masterdate = []
    demname = []
        
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "hd:m:e:", ["help"])
        except getopt.error, msg:
            raise Usage(msg)
        for o, a in opts:
            if o == '-h' or o == '--help':
                print __doc__
                return 0
            elif o == '-d':
                datadir = a
            elif o == '-m':
                masterdate = a
            elif o == '-e':
                demname = a
        
        if not datadir:
            raise Usage('No data directory given, -d option is not optional!')
        if not masterdate:
            raise Usage('No master date given, -m option is not optional!')
        if not demname:
            raise Usage('No dem file given, -e option is not optional!')

        if not os.path.exists(datadir):
            raise Usage('Data directory {0} does not seem to exist?'.format(datadir))
        if not os.path.exists(os.path.join(datadir,'SLC',masterdate)):
            raise Usage('Masterdate directory {0} does not seem to exist?'.format(os.path.join(datadir,'SLC',masterdate)))
        if not os.path.exists(demname) or not os.path.exists(demname+'.par'):
            raise Usage('Dem files {0} and/or {1} do not seem to exist'.format(demname, demname+'.par'))

    except Usage, err:
        print >>sys.stderr, "\nWoops, something went wrong:"
        print >>sys.stderr, "  "+str(err.msg)
        print >>sys.stderr, "\nFor help, use -h or --help.\n"
        return 2

    slavelist = [d for d in os.listdir(os.path.join(datadir,'SLC')) if d[0] == '2' and d != masterdate]
    with open(os.path.join(datadir,'slave.list'),'w') as f:
        for l in slavelist:
            f.write('{0}\n'.format(l))
    
    calc_dem_lut(datadir,masterdate,demname)
    calc_terrain_norm(datadir,masterdate)
    get_offset(datadir,masterdate)
    dem_width = calc_fine_dem_lut(datadir,masterdate)
    geocode(datadir,masterdate,dem_width)

def geocode(datadir,masterdate,dem_width):
    mli = os.path.join(datadir,'SLC',masterdate,masterdate+'.mli')
    geodir=os.path.join(datadir,'Geo')
    res = grep('range_samples','{mli}.par'.format(mli=mli))
    width = np.int32(res.split(':')[1].strip())
    res = grep('nlines','{gd}/{md}.dem.par'.format(gd=geodir,md=masterdate))
    dem_length = np.int32(res.split(':')[1].strip())
    res = grep('azimuth_lines','{mli}.par'.format(mli=mli))
    length = np.int32(res.split(':')[1].strip())
    
    exe_str = 'geocode_back {mli} {w} {gd}/{md}.lut_fine '.format(mli=mli,
                                                                   w=width,
                                                                   gd=geodir,
                                                                   md=masterdate)
    exe_str += '{gd}/DEM.{md}.mli {dw} {dl} 2 0'.format(gd=geodir,
                                                        md=masterdate,
                                                        dw=dem_width,
                                                        dl=dem_length)
    os.system(exe_str)
    
    exe_str = 'geocode {gd}/{md}.lut_fine {gd}/{md}.dem '.format(gd=geodir,
                                                                 md=masterdate)
    exe_str += '{wd} {gd}/{md}.hgt {w} {l} 2 0'.format(wd=dem_width,
                                                       gd=geodir,
                                                       md=masterdate,
                                                       w=width,
                                                       l=length)
    os.system(exe_str)
    
    exe_str = 'rashgt {gd}/{md}.hgt {mli} {w} - - - - - 500'.format(gd=geodir,
                                                                    md=masterdate,
                                                                    mli=mli,
                                                                    w=width)
    os.system(exe_str)
    
    exe_str = 'rashgt {gd}/{md}.dem {gd}/DEM.{md}.mli {wd} - - - - - 500'.format(gd=geodir,
                                                                                  md=masterdate,
                                                                                  wd=dem_width)
    os.system(exe_str)

def calc_fine_dem_lut(datadir,masterdate):
    mli = os.path.join(datadir,'SLC',masterdate,masterdate+'.mli')
    geodir=os.path.join(datadir,'Geo')
    res = grep('width','{gd}/{md}.dem.par'.format(gd=geodir,md=masterdate))
    dem_width = np.int32(res.split(':')[1].strip())
    
    exe_str = 'gc_map_fine {gd}/{md}.lut {dw} '.format(gd=geodir,
                                                       md=masterdate,
                                                       dw=dem_width)
    exe_str += '{gd}/{md}.diff.par {gd}/{md}.lut_fine 1'.format(gd=geodir,
                                                                md=masterdate)

    os.system(exe_str)
    return dem_width        
    
    

def get_offset(datadir,masterdate):
    mli = os.path.join(datadir,'SLC',masterdate,masterdate+'.mli')
    geodir=os.path.join(datadir,'Geo')
    
    exe_str = 'offset_pwrm {gd}/pix_sigma0 {mli} '.format(gd=geodir,
                                                          mli=mli)
    exe_str += '{gd}/{md}.diff.par {gd}/{md}.offs '.format(gd=geodir,
                                                           md=masterdate)
    exe_str += '{gd}/{md}.snr 512 512 - 2 128 128 7.0'.format(gd=geodir,
                                                              md=masterdate)

    os.system(exe_str)
    
    exe_str = 'offset_fitm {gd}/{md}.offs {gd}/{md}.snr '.format(gd=geodir,
                                                                 md=masterdate)
    exe_str += '{gd}/{md}.diff.par - - 7.0 1'.format(gd=geodir,
                                                     md=masterdate)
    os.system(exe_str)

def calc_dem_lut(datadir,masterdate,demfile):
    mlipar = os.path.join(datadir,'SLC',masterdate,masterdate+'.mli.par')
    geodir=os.path.join(datadir,'Geo')
    if not os.path.exists(geodir):
        os.mkdir(geodir)
                          
    exe_str = 'gc_map {mli} - {dem}.par {dem} '.format(mli=mlipar,
                                                       dem=demfile)
    exe_str += '{gd}/{md}.dem.par {gd}/{md}.dem '.format(gd=geodir,
                                                         md=masterdate)
    exe_str += '{gd}/{md}.lut 4 5 {gd}/{md}.sim_sar '.format(gd=geodir,
                                                             md=masterdate)
    exe_str += '{gd}/u {gd}/v {gd}/inc {gd}/psi {gd}/pix {gd}/ls_map - 2'.format(gd=geodir)

    os.system(exe_str)

def calc_terrain_norm(datadir,masterdate):
    mlipar = os.path.join(datadir,'SLC',masterdate,masterdate+'.mli.par')
    geodir=os.path.join(datadir,'Geo')
    
    exe_str = 'pixel_area {mli} {gd}/{md}.dem.par '.format(mli=mlipar,
                                                           gd=geodir,
                                                           md=masterdate)
    exe_str += '{gd}/{md}.dem {gd}/{md}.lut {gd}/ls_map '.format(gd=geodir,
                                                                md=masterdate)
    exe_str += '{gd}/inc {gd}/pix_sigma0 {gd}/pix_gamma0'.format(gd=geodir)
 
    os.system(exe_str)

    exe_str = 'create_diff_par {mli} - {gd}/{md}.diff.par 1 0'.format(mli=mlipar,
                                                                      gd=geodir,
                                                                      md=masterdate)
    
    os.system(exe_str)
    
                        



if __name__ == "__main__":
    sys.exit(main())
