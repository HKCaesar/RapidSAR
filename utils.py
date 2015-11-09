import numpy as np
import subprocess
import time
import struct as st

import pdb

VERSION = 'v0.1'
AUTHOR = 'Karsten Spaans'
DATE = 'February 2014'

def get_parm_dict(parmgroup):
    parm_dict = {}
    for k in parmgroup:
        parm_dict[k] = parmgroup[k][0][0]
    return parm_dict

def print_parms(d):
    print 'The following parameters will be used:\n'    
    print '\nParameter:               Value:'
    print '-------------------------------'
    for k in sorted(d):
        print k+(25-len(k))*' '+str(d[k])
    print ''

def write_parms(parm_dict,parm_grp):
    for k in parm_dict:
        try:
            dt = parm_dict[k].dtype
        except:
            dt = np.int32
        try:
            a = parm_grp.require_dataset(k,(1,1),dtype=dt) 
        except:
            pdb.set_trace()
        a[()] = parm_dict[k]

def print_start_message():
    print '\n###################################'
    print '##          VolcMon {version}'.format(version=VERSION)+(13-len(str(VERSION)))*' '+'##'
    print '##         {author}'.format(author=AUTHOR)+(22-len(str(AUTHOR)))*' '+'##'
    print '##         {date}'.format(date=DATE)+(22-len(str(DATE)))*' '+'##'
    print '###################################\n'

def grep(arg,file):
    res = subprocess.check_output(['grep',arg,file])
    return res

def count_lines(file):
    res = subprocess.check_output(['wc','-l',file]).split()[0]
    return res

def time_it(t1):
    elapsed = time.time()-t1
    if elapsed > 3600:
        elapsed /= 3600
        s = '{0:.2f} hours...'.format(elapsed)
    elif elapsed > 120:
        elapsed /= 60
        s = '{0:.2f} minutes...'.format(elapsed)
    else:
        s = '{0:.2f} seconds...'.format(elapsed)
    return s

def multilook(im,fa,fr):
    nr = np.floor(len(im[0,:])/float(fr))*fr
    na = np.floor(len(im[:,0])/float(fa))*fa
    im = im[:na,:nr]
    im[np.where(np.isnan(im))] = 0
    aa = np.zeros((na/fa,nr))
    for k in range(fa) :
        aa = aa+im[k::fa,:]
    imout=np.zeros((na/fa,nr/fr))
    for k in range(fr) :
        imout = imout+aa[:,k::fr]
    return imout/fa/fr

def isnumber(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def read_complex(l,length,width,fmt='f') :
    if l[-1] == '\n':
        l = l[:-1]
    with open(l,'r') as ifgfile:
        print 'Reading file '+l+'...'
        d = ifgfile.read()
        if len(d) != st.calcsize(str(length*width*2)+fmt):
            raise IOError('Size of data in '+l[:-1]+\
                          ' does not seem to match given size '\
                          +str(length)+'x'+str(width)+'...')
        if fmt == 'f':
            res = np.fromstring(d,dtype=np.complex64)
        elif fmt == 'h':
            res = np.fromstring(d,dtype=np.int16)
            res = np.float32(res)
            res.dtype = np.complex64
        else:
            'Warning, format {0} unknown...'.format(fmt)
            return 1
        #res = np.angle(res)
        res = np.reshape(res,(length,width))
        return res                

def read_real(l,length,width,fmt='f') :
    if l[-1] == '\n':
        l = l[:-1]
    with open(l,'r') as slcfile:
        print 'Reading file '+l+'...'
        d = slcfile.read()
        if len(d) != st.calcsize(str(length*width)+fmt):
            raise IOError('Size of data in '+l[:-1]+\
                          ' does not seem to match given size '\
                          +str(length)+'x'+str(width)+'...')
        res = np.fromstring(d,dtype=np.float32)
        #res = np.abs(res)
        res = np.reshape(res,(length,width))
        return res

def ll2xy(lon,lat,lon_orig,lat_orig):
    """
    Converts latitude and longitude to local xy coordinates

    Converted from Matlab script llh2local by Peter Cervelli
    """
    a = 6378137.0
    e = 0.08209443794970

    x = np.zeros_like(lon)
    y = np.zeros_like(lat)

    lon = deg2rad(lon)
    lat = deg2rad(lat)
    lon_orig = deg2rad(lon_orig)
    lat_orig = deg2rad(lat_orig)

    nonzeroix = lat != 0
    dlambda = lon[nonzeroix]-lon_orig

    M = a*((1-e**2/4-3*e**4/64-5*e**6/256)*lat[nonzeroix] -
           (3*e**2/8+3*e**4/32+45*e**6/1024)*np.sin(2*lat[nonzeroix]) + 
           (15*e**4/256 +45*e**6/1024)*np.sin(4*lat[nonzeroix]) - 
           (35*e**6/3072)*np.sin(6*lat[nonzeroix]))

    M0 = a*((1-e**2/4-3*e**4/64-5*e**6/256)*lat_orig -
           (3*e**2/8+3*e**4/32+45*e**6/1024)*np.sin(2*lat_orig) + 
           (15*e**4/256 +45*e**6/1024)*np.sin(4*lat_orig) - 
           (35*e**6/3072)*np.sin(6*lat_orig));
   
    N = a/np.sqrt(1-e**2*np.sin(lat[nonzeroix])**2)
    E = dlambda*np.sin(lat[nonzeroix])

    x[nonzeroix] = N/np.tan(lat[nonzeroix])*np.sin(E)
    y[nonzeroix] = M-M0+N/np.tan(lat[nonzeroix])*(1-np.cos(E))

    x[~nonzeroix] = a*dlambda[~nonzeroix]
    y[~nonzeroix] = -M0

    return x, y

def deg2rad(a):
    return a*np.pi/180
