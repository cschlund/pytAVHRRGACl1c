#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 

import os, sys
import calendar
import time, datetime

basedir   = os.getcwd()
proname   = 'GAC_overlap'
pytname   = os.path.join(basedir, proname + '.py')
cfgfile   = os.path.join(basedir, 'config_'+ proname + '.file')
timestamp = int(time.time())

# read cfgfile: get configuration for job
f = open(cfgfile, mode="r+")
for lines in f:
  line = lines.rstrip('\n')
  if "ipath=" in line:
    ipath = line.split("=")[1]
  if "dbfile=" in line:
    dbfile = line.split("=")[1]
f.close()

# cmd file
cmdfile = os.path.join(basedir, proname+'.cmd')
sqlcomp = os.path.join(ipath, dbfile)

# create err and out logfilenames
base_filename = proname + '_' + str(timestamp)
errfile = os.path.join(basedir, "log", base_filename + "_log.err")
outfile = os.path.join(basedir, "log", base_filename + "_log.out")
errfil2 = os.path.join(basedir, "log", base_filename + "_log2.err")
outfil2 = os.path.join(basedir, "log", base_filename + "_log2.out")

# write cmd file
f = open(cmdfile, mode="w")

line = '''#!/bin/ksh
#PBS -N ''' + proname + '''
#PBS -q ns
#PBS -S /usr/bin/ksh
#PBS -m e
#PBS -M dec4@ecmwf.int
#PBS -p 70
#PBS -l EC_threads_per_task=1
#PBS -l EC_memory_per_task=3000mb
#PBS -l EC_ecfs=0
#PBS -l EC_mars=0
#PBS -o ''' + outfile + ''' 
#PBS -e ''' + errfile + '''

set -x

cd ''' + basedir + '''
mkdir -p ''' + basedir + '''/log

# --- options for python script
PROG=''' + pytname + '''
SLQT="-g '''+sqlcomp+''' "
python ${PROG} ${SQLT} > ''' + outfil2 + ''' 2> ''' + errfil2 + '''

status=${?}
if [ $status -ne 0 ]; then
  echo " --- FAILED"
  return 1
fi

# --- end of ''' + cmdfile + ''' ---
'''

f.write(line)
f.close()

print (" *** %s finished for %s " % (sys.argv[0], cmdfile))
