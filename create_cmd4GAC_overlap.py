#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 

import os, sys
import calendar
import time, datetime
from subs_avhrrgac import get_satellite_list

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
satlist = get_satellite_list()

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

'''
f.write(line)

for sat in satlist:
    line = '''python '''+pytname+''' -g '''+sqlcomp+''' -s '''+sat+''' >> '''+outfil2+''' 2>> '''+errfil2
    f.write(line+'\n')

    line = '''status=${?}'''
    f.write(line+'\n')
    line = '''if [ $status -ne 0 ]; then'''
    f.write(line+'\n')
    line = '''   echo " --- FAILED for '''+sat+'''"'''
    f.write(line+'\n')
    line = '''   return 1'''
    f.write(line+'\n')
    line = '''fi'''
    f.write(line+'\n\n')

f.close()

print (" *** %s finished for %s " % (sys.argv[0], cmdfile))
