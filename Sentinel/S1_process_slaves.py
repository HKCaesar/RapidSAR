"""

Coregister and form interferograms of all slaves with respect to the chosen master

Overview
========

This program cycles through all slave images in turn, coregisters them using cross correlation and spectral diversity, and forms the interferograms. The slave dates are determined either based on a list of dates specified by the user, or if omitted, by all dates present in the processing directory besides the chosen master. 

Functions
=========

Main functions
--------------


Aux functions
-------------

  get_swath_pol:
    Retrieves the swath numbers and polarisation of data in the given list

"""



import sys
import getopt
import os
import shutil
import subprocess as subp
import h5py as h5
import numpy as np
import datetime as dt
from RIMoDe.utils import grep
from RIMoDe.Sentinel.S1_setup_image import make_SLC_tab

import pdb

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg
class cd:
    """Context manager for changing the current working directory"""
    def __init__(self, newPath):
        self.newPath = os.path.expanduser(newPath)

    def __enter__(self):
        self.savedPath = os.getcwd()
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)




def main(argv=None):
    if argv == None:
        argv = sys.argv
    
    datadir = []
    slavelistname = []
        
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "hd:s:", ["help"])
        except getopt.error, msg:
            raise Usage(msg)
        for o, a in opts:
            if o == '-h' or o == '--help':
                print __doc__
                return 0
            elif o == '-d':
                datadir = a
            elif o == '-s':
                slavelistname = a
        
        if not datadir:
            raise Usage('No data directory given, -d option is not optional!')
        if not os.path.exists(datadir):
            raise Usage('Data directory {0} does not seem to exist?'.format(datadir))
        if not os.path.exists(os.path.join(datadir,'Geo')):
            raise Usage('Did not find results from master setup in expected location {0}'.format(os.path.join(datadir,'Geo')))
        
        if slavelistname:
           if not os.path.exists(slavelistname): 
               raise Usage('Could not find given file containing slave images {0}'.format(slavelistname))

    except Usage, err:
        print >>sys.stderr, "\nWoops, something went wrong:"
        print >>sys.stderr, "  "+str(err.msg)
        print >>sys.stderr, "\nFor help, use -h or --help.\n"
        return 2

    for f in os.listdir(os.path.join(datadir,'Geo')):
        if f[-4:] == '.dem':
            masterdate = np.int32(f[:-4])

    slavelist = []
    if slavelistname:
        with open(slavelistname) as f:
            for l in f:
                if l.strip() != str(masterdate) and len(l) > 0:
                    slavelist.append(dt.datetime(int(l[:4]),int(l[4:6]),int(l[6:])))
    else:
        for l in os.listdir(os.path.join(datadir,'SLC')):
            if l != str(masterdate) and l[0] == '2':
                slavelist.append(dt.datetime(int(l[:4]),int(l[4:6]),int(l[6:])))
    
    masterdate = dt.datetime(int(str(masterdate)[:4]),int(str(masterdate)[4:6]),int(str(masterdate)[6:]))

    tempbaseline = [abs(masterdate-sd) for sd in slavelist] 
    sortix = np.argsort(tempbaseline)
    swathlist, pol = get_swath_pol(datadir,masterdate.strftime('%Y%m%d'))
    res = grep('range_samples',os.path.join(datadir,'SLC',masterdate.strftime('%Y%m%d'),'{md}.mli.par'.format(md=masterdate.strftime('%Y%m%d'))))
    mliwidth = np.int32(res.split(':')[1].strip())

    for i in sortix:
        process_slave(datadir,masterdate.strftime('%Y%m%d'),slavelist[i].strftime('%Y%m%d'),tempbaseline[i],swathlist,pol,mliwidth)


def process_slave(datadir,masterdate,slavedate,masterbaseline,swathlist,pol,mliwidth):
    derive_lut(datadir,masterdate,slavedate,swathlist,pol)
    calc_offset(datadir,masterdate,slavedate)
    if masterbaseline <= dt.timedelta(days=60):
        #No auxiliary image used if tempbaseline is less than 60 days
        coreg_overlap(datadir,masterdate,slavedate,[],1)
        coreg_overlap(datadir,masterdate,slavedate,[],2)
    else:
        auxtab = get_auxtab(datadir,slavedate,masterdate,swathlist,pol)
        coreg_overlap(datadir,masterdate,slavedate,auxtab,1)
        coreg_overlap(datadir,masterdate,slavedate,auxtab,2)
    make_ifg(datadir,masterdate,slavedate,mliwidth)

def get_slave_list(datadir,masterdate):
    slavelist = []
    for l in os.listdir(os.path.join(datadir,'RSLC')):
            if l != str(masterdate) and l[0] == '2' and len(l) == 8:
                slavelist.append(dt.datetime(int(l[:4]),int(l[4:6]),int(l[6:])))
    return slavelist

def get_auxtab(datadir,slavedate,masterdate,swathlist,pol):
    procslavelist = []
    slavedate_dt = dt.datetime(int(slavedate[:4]),int(slavedate[4:6]),int(slavedate[6:]))
    masterdate_dt = dt.datetime(int(masterdate[:4]),int(masterdate[4:6]),int(masterdate[6:]))
    for l in os.listdir(os.path.join(datadir,'RSLC')):
        if len(l) == 8 and l != slavedate and l[0] == '2':
            procslavelist.append(dt.datetime(int(l[:4]),int(l[4:6]),int(l[6:])))
    
    procbaseline = [abs(slavedate_dt-sd) for sd in procslavelist]
    if min(procbaseline) < abs(slavedate_dt-masterdate_dt):
        auxdate = procslavelist[np.argsort(procbaseline)[0]]
        auxtab = os.path.join(datadir,'RSLC3_tab')
        make_SLC_tab(os.path.join(datadir,'RSLC3_tab'),
                     os.path.join(datadir,'RSLC',
                                  auxdate.strftime('%Y%m%d'),
                                  auxdate.strftime('%Y%m%d')),
                     swathlist,pol)
    else:
        auxtab = []
    return auxtab

def make_ifg(datadir,masterdate,slavedate,mliwidth):
    slcdir = os.path.join(datadir,'SLC')
    rslcdir = os.path.join(datadir,'RSLC')
    geodir = os.path.join(datadir,'Geo')
    ifgdir = os.path.join(datadir,'IFG')
    if not os.path.exists(ifgdir):
        os.mkdir(ifgdir)
    exe_str = 'phase_sim_orb {sd}/{md}/{md}.slc.par {sd}/{sld}/{sld}.slc.par '.format(sd=slcdir,
                                                                                      md=masterdate,
                                                                                      sld=slavedate)
    exe_str += '{rd}/{md}_{sld}.off {gd}/{md}.hgt {ifd}/{md}_{sld}.sim_unw '.format(rd=rslcdir,
                                                                                   md=masterdate,
                                                                                   sld=slavedate,
                                                                                   gd=geodir,
                                                                                   ifd=ifgdir)
    exe_str += '{sd}/{md}/{md}.slc.par - - 1 1'.format(sd=slcdir,
                                                       md=masterdate)
    os.system(exe_str)
    
    exe_str = 'SLC_diff_intf {sd}/{md}/{md}.slc {rd}/{sld}/{sld}.rslc '.format(sd=slcdir,
                                                                               md=masterdate,
                                                                               rd=rslcdir,
                                                                               sld=slavedate)
    exe_str += '{sd}/{md}/{md}.slc.par {rd}/{sld}/{sld}.rslc.par '.format(sd=slcdir,
                                                                          md=masterdate,
                                                                          rd=rslcdir,
                                                                          sld=slavedate)
    exe_str += '{rd}/{md}_{sld}.off.cor2 {ifd}/{md}_{sld}.sim_unw '.format(rd=rslcdir,
                                                                          md=masterdate,
                                                                          sld=slavedate,
                                                                          ifd=ifgdir)
    exe_str += '{ifd}/{md}_{sld}.diff 5 1 0 0 0.2 1 1'.format(ifd=ifgdir,
                                                              md=masterdate,
                                                              sld=slavedate)

    os.system(exe_str)
    exe_str = 'rasmph_pwr {ifd}/{md}_{sld}.diff {sd}/{md}/{md}.mli {mw}'.format(ifd=ifgdir,
                                                                                md=masterdate,
                                                                                sld=slavedate,
                                                                                sd=slcdir,
                                                                                mw=mliwidth)
    os.system(exe_str)
                                                                          

def coreg_overlap(datadir,masterdate,slavedate,auxtab,specdivno):
    slcdir = os.path.join(datadir,'SLC')
    rslcdir = os.path.join(datadir,'RSLC')
    if specdivno == 1:
        cor=''
    else:
        cor = '.cor{0}'.format(specdivno-1)
    exe_str = 'S1_coreg_overlap {dd}/SLC1_tab {dd}/RSLC2_tab {md}_{sld} '.format(dd=datadir,
                                                                            md=masterdate,
                                                                       sld=slavedate)
    exe_str += '{rd}/{md}_{sld}.off{c} {rd}/{md}_{sld}.off.cor{sn} '.format(rd=rslcdir,
                                                                            md=masterdate,
                                                                            sld=slavedate,
                                                                            c=cor,
                                                                            sn=specdivno)
    exe_str += '0.8 0.01 0.8 1'
    if auxtab:
        exe_str += ' '+auxtab
    with cd(datadir):
        os.system(exe_str)
    
    SLC_interp(datadir,masterdate,slavedate,'{rd}/{md}_{sld}.off.cor{sn}'.format(rd=rslcdir,
                                                                                 md=masterdate,
                                                                                 sld=slavedate,
                                                                                 sn=specdivno))
            

def calc_offset(datadir,masterdate,slavedate):
    slcdir = os.path.join(datadir,'SLC')
    rslcdir = os.path.join(datadir,'RSLC')    

    exe_str = 'create_offset {sd}/{md}/{md}.slc.par '.format(sd=slcdir,
                                                             md=masterdate)
    exe_str += '{sd}/{sld}/{sld}.slc.par {rd}/{md}_{sld}.off 1 5 1 0'.format(sd=slcdir,
                                                                             sld=slavedate,
                                                                             rd=rslcdir,
                                                                             md=masterdate)
    os.system(exe_str)

    exe_str = 'offset_pwr {sd}/{md}/{md}.slc {rd}/{sld}/{sld}.rslc '.format(sd=slcdir,
                                                                            md=masterdate,
                                                                            rd=rslcdir,
                                                                            sld=slavedate)
    exe_str += '{sd}/{md}/{md}.slc.par {rd}/{sld}/{sld}.rslc.par '.format(sd=slcdir,
                                                                          md=masterdate,
                                                                          rd=rslcdir,
                                                                          sld=slavedate)
    exe_str += '{rd}/{md}_{sld}.off {rd}/{md}_{sld}.offs '.format(rd=rslcdir,
                                                                  md=masterdate,
                                                                  sld=slavedate)
    exe_str += '{rd}/{md}_{sld}.snr 256 64 - 1 64 64 7.0 4 0 0'.format(rd=rslcdir,
                                                                       md=masterdate,
                                                                       sld=slavedate)

    os.system(exe_str)

    exe_str = 'offset_fit {rd}/{md}_{sld}.offs {rd}/{md}_{sld}.snr '.format(rd=rslcdir,
                                                                            md=masterdate,
                                                                            sld=slavedate)
    exe_str += '{rd}/{md}_{sld}.off - - 10.0 1 0'.format(rd=rslcdir,
                                                         md=masterdate,
                                                         sld=slavedate)
    os.system(exe_str)
    SLC_interp(datadir,masterdate,slavedate,'{rd}/{md}_{sld}.off'.format(rd=rslcdir,
                                                                         md=masterdate,
                                                                         sld=slavedate))
                                                                
            
def get_swath_pol(datadir,masterdate):
    filelist = os.listdir(os.path.join(datadir,'SLC',masterdate))
    swathlist = [int(l.split('.')[1][-1]) for l in filelist if len(l.split('.')) == 4]
    pol = [l.split('.')[2] for l in filelist if len(l.split('.')) == 4][0]
    return set(swathlist), pol


def derive_lut(datadir,masterdate,slavedate,swathlist,pol):
    slcdir = os.path.join(datadir,'SLC')
    geodir = os.path.join(datadir,'Geo')
    rslcdir = os.path.join(datadir,'RSLC')
    if not os.path.exists(rslcdir):
        os.mkdir(rslcdir)
    if not os.path.exists(os.path.join(rslcdir,slavedate)):
        os.mkdir(os.path.join(rslcdir,slavedate))
    exe_str = 'rdc_trans {sd}/{md}/{md}.mli.par {gd}/{md}.hgt '.format(sd=slcdir,
                                                                       md=masterdate,
                                                                       gd=geodir)
    exe_str += '{sd}/{sld}/{sld}.mli.par {rd}/{sld}.mli.lut'.format(sd=slcdir,
                                                                    sld=slavedate,
                                                                    rd=rslcdir)
    os.system(exe_str)
    make_SLC_tab(os.path.join(datadir,'SLC1_tab'),
                 os.path.join(slcdir,masterdate,masterdate),
                 swathlist,pol)
    make_SLC_tab(os.path.join(datadir,'SLC2_tab'),
                 os.path.join(slcdir,slavedate,slavedate),
                 swathlist,pol)
    make_SLC_tab(os.path.join(datadir,'RSLC2_tab'),
                 os.path.join(rslcdir,slavedate,slavedate),
                 swathlist,pol)
    SLC_interp(datadir,masterdate,slavedate,'-')

def SLC_interp(datadir,masterdate,slavedate,offfile):
    slcdir = os.path.join(datadir,'SLC')
    rslcdir = os.path.join(datadir,'RSLC')
    
    exe_str = 'SLC_interp_lt_S1_TOPS {dd}/SLC2_tab {sd}/{sld}/{sld}.slc.par '.format(dd=datadir,
                                                                                     sd=slcdir,
                                                                                     sld=slavedate)
    exe_str += '{dd}/SLC1_tab {sd}/{md}/{md}.slc.par {rd}/{sld}.mli.lut '.format(dd=datadir,
                                                                                 sd=slcdir,
                                                                                 md=masterdate,
                                                                            rd=rslcdir,
                                                                            sld=slavedate)
    exe_str += '{sd}/{md}/{md}.mli.par {sd}/{sld}/{sld}.mli.par '.format(sd=slcdir,
                                                                         md=masterdate,
                                                                         sld=slavedate)
    exe_str += '{off} {dd}/RSLC2_tab {rd}/{sld}/{sld}.rslc '.format(dd=datadir,
                                                                    off=offfile,
                                                                    rd=rslcdir,
                                                                    sld=slavedate)
    exe_str += '{rd}/{sld}/{sld}.rslc.par'.format(rd=rslcdir,
                                                  sld=slavedate)
    os.system(exe_str)
    
                                                                   

    


if __name__ == "__main__":
    sys.exit(main())
