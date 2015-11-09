import sys
import getopt
import os
import shutil
import subprocess as subp
from S1_insert_db import db_insert

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

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
    queryfile = []
    xmlfile = []
    datadir = []

    try:
        try:
            opts, args = getopt.getopt(argv[1:], "hd:i:", ["help"])
        except getopt.error, msg:
            raise Usage(msg)
        for o, a in opts:
            if o == '-h' or o == '--help':
                print __doc__
                return 0
            elif o == '-d':
                datadir = a
            elif o == '-i':
                hopperdir = a

        if not os.path.exists(hopperdir):
            raise Usage('Hopper directory {0} does not exist.'.format(datadir))
        elif not os.path.isdir(hopperdir):
            raise Usage('Given hopper directory {0} is not a directory.'.format(datadir))

        if not os.path.exists(datadir):
            raise Usage('Output data directory {0} does not exist.'.format(datadir))
        elif not os.path.isdir(datadir):
            raise Usage('Given output data directory {0} is not a directory.'.format(datadir))

    
    except Usage, err:
        print >>sys.stderr, "\nWoops, something went wrong:"
        print >>sys.stderr, "  "+str(err.msg)
        print >>sys.stderr, "\nFor help, use -h or --help.\n"
        return 2
        
    failed_list = unzip_files(hopperdir)
    with open(os.path.join(hopperdir,'failed.list'),'a') as f:
        for l in failed_list:
            f.write('{0}\n'.format(l))
    dbfilename = '/nfs/a1/raw/sentinel/iceland/S1_iceland.sql'
    conn = sqlite3.connect(dbfilename)
    c = conn.cursor()
    distribute_data(datadir,hopperdir,c,conn)
    conn.close()

def distribute_data(datadir,hopperdir,c,conn):
    datalist = os.listdir(hopperdir)
    for d in datalist:
        if os.path.isdir(os.path.join(hopperdir,d)) and d[-5:] == '.SAFE':
            orbnumber = get_orbit(os.path.join(hopperdir,d))
            orbdir = os.path.join(datadir,'T'+orbnumber)
            if not os.path.exists(orbdir):
                os.mkdir(orbdir)
            if not os.path.exists(os.path.join(orbdir,d)):
                shutil.move(os.path.join(hopperdir,d),orbdir)
            else:
                print 'WARNING: Data directory {0} already in destination directory {1}'.format(d,orbdir)
            db_insert(os.path.join(orbdir,d),c,conn)
            

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

def unzip_files(datadir):
    ziplist = os.listdir(datadir)
    faillist = []
    for f in ziplist:
        if f[-4:] == '.zip':
            unzipcall = ['jar','xvf',f]
            with cd(datadir):
                rc = subp.call(unzipcall)
            if rc == 0:
                os.remove(os.path.join(datadir,f))
            else:
                print 'WARNING: Could not unpack {0}, adding to failed list'.format(f)
                if os.path.exists(os.path.join(datadir,f[:-4]+'.SAFE')):
                     shutil.rmtree(os.path.join(datadir,f[:-4]+'.SAFE'))
                                  
                faillist.append(f)
    return faillist




if __name__ == "__main__":
    sys.exit(main())
