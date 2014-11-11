#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# how to use the script: 
#   > python script.py -h
#
# C.Schlundt: September, 2014
#
# -------------------------------------------------------------------

import numpy as np
import os, sys, getopt
import datetime, re
import argparse
import subs_avhrrgac as mysub

# -------------------------------------------------------------------

parser = argparse.ArgumentParser(description='''%s
searches for the L1b file, which was passed to pyGAC but not 
processed by pyGAC without emmitting a Traceback notification.
Input files are *L1b_pass.log and *L1c_proc.log files, which are
compared to figure out which L1b orbit failed.''' 
% os.path.basename(__file__))

parser.add_argument('-i', '--ipass', 
        help='log file containing the L1b files passed to pyGAC', 
        required=True)
parser.add_argument('-o', '--oproc', 
        help='log file containing the L1c files produced by pyGAC', 
        required=True)
parser.add_argument('-v', '--verbose', 
        help='increase output verbosity', action="store_true")

args = parser.parse_args()

if args.verbose == True:
    print ("\n *** Parameter passed" )
    print (" ---------------------- ")
    print ("   - ipass   : %s" % args.ipass)
    print ("   - oproc   : %s" % args.oproc)
    print ("   - Verbose : %s" % args.verbose)
  
# -------------------------------------------------------------------
# read both log files

obj = open(args.ipass, mode="r")
inp = obj.readlines()
obj.close()

obj = open(args.oproc, mode="r")
out = obj.readlines()
obj.close()
    
# -------------------------------------------------------------------
# create avhrr and sunsatangles list based on proc.log

alst = []
slst = []

for lin in out:
    line = lin.strip('\n')
    sepa = line.split()
    nele = len(sepa)
    last = sepa[nele-1]
    if '_avhrr_' in last: 
        alst.append(last)
    if '_sunsatangles_' in last:
        slst.append(last)
    
if len(inp) != len(alst) and len(inp) != len(slst):
    print ("   * %d L1b NE %d L1c_avhrr (%d L1c_sunsatangles) !" 
    % (len(inp), len(alst), len(slst) ))
else:
    exit(0)

# -------------------------------------------------------------------
# loop over L1b input files in order to search for missing L1c orbit

for idx, lin in enumerate(inp):
    line = lin.strip('\n')
    sepa = line.split()
    nele = len(sepa)
    
    last = sepa[nele-1]
    # n09_1985/NSS.GHRR.NF.D85065.S0010.E0159.B0117980.GC
    lstr = last.split('/')
    fstr = lstr[1].split('.')
    ifil = lstr[1]
    year = lstr[0][4:8] 
    fdoy = fstr[3][3:6]
    
    plat = mysub.full_sat_name(fstr[2])[1]
    
    dt = datetime.datetime(int(year),1,1)
    dtdelta = datetime.timedelta(days=int(fdoy)-1)
    date = (dt+dtdelta).strftime("%Y%m%d")
    
    #stim = fstr[4].replace('S', 'T')
    #etim = fstr[5].replace('E', 'T')
    # zeiten sind gerundet!
    stim = fstr[4][0:3].replace('S', 'T')
    etim = fstr[5][0:3].replace('E', 'T')

    print ("   + %3d - Working on: %s (%s, %s) == %s: %s to %s on %s" 
    %(idx, ifil, year, fdoy, date, stim, etim, plat))
    
    # ECC_GAC_avhrr_noaa09_99999_19850306T0010284Z_19850306T0159469Z.h5
    #pattern = 'ECC_GAC_avhrr_{0}_\d{{5}}_{1}{2}\d{{3}}Z_\d{{8}}{3}\d{{3}}Z.h5'.format(plat, date, stim, etim)
    pattern = 'ECC_GAC_avhrr_{0}_\d{{5}}_{1}{2}\d{{5}}Z_\d{{8}}{3}\d{{5}}Z.h5'.format(plat, date, stim, etim)
  
    for fil in alst: 
        mflag = False
        match = re.search(pattern=pattern, string=fil) 
        
        if match:
            mflag = True
            mfile = match.group()
            alst.remove(mfile)
            break

        # If-statement after search() tests if it succeeded
        if mflag == True:
            print ("   + FOUND: %s") % (mfile) 
        else: 
            print ("   + DID NOT FIND! ------------------------------------") 
            #exit(1) 

print (" \n!!! remaining files (not found) : %s" % alst)
# -------------------------------------------------------------------
print ( "\n *** %s finished for \n     %s and \n     %s\n" 
  % (sys.argv[0], args.ipass, args.oproc) )
# -------------------------------------------------------------------
