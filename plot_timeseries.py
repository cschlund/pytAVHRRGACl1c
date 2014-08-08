#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# how to use the script: 
#   > python script.py -h
#
# C.Schlundt: August, 2014
#
# -------------------------------------------------------------------

import scipy as sp
import numpy as np
import matplotlib.pyplot as plt
import datetime
import os
import sys, getopt
import argparse
import subs_avhrrgac as mysub


parser = argparse.ArgumentParser(description='''%s 
displays global means of 1 channel available for all satellites 
processed with pyGAC tool.''' % os.path.basename(__file__))

parser.add_argument('-c', '--channel', help='Channel abbreviation, i.e. ch1/ch2/ch3a/ch3b/ch4/ch5', required=True)
parser.add_argument('-t', '--time', help='Time abbreviation, e.g. day/night/twilight', required=True)
parser.add_argument('-i', '--inpdir', help='Path, e.g. /path/to/input.txt', required=True)
parser.add_argument('-o', '--outdir', help='Path, e.g. /path/to/output.png', required=True)
parser.add_argument('-z', '--zoom', help='Plot 4 pannels', action="store_true")
parser.add_argument('-s', '--sdate', help='Start Date String, e.g. 20090101')
parser.add_argument('-e', '--edate', help='End Date String, e.g. 20121231')
parser.add_argument('-v', '--verbose', help='increase output verbosity', action="store_true")

args = parser.parse_args()

# -------------------------------------------------------------------

if args.verbose == True:
  print ("\n *** Parameter passed" )
  print (" ---------------------- ")
  print ("   - Channel    : %s" % args.channel)
  print ("   - Time       : %s" % args.time)
  print ("   - Input Path : %s" % args.inpdir)
  print ("   - Output Path: %s" % args.outdir)
  print ("   - Zoom       : %s" % args.zoom)
  print ("   - Start Date : %s" % args.sdate)
  print ("   - End Date   : %s" % args.edate)
  print ("   - Verbose    : %s" % args.verbose)

  
# -------------------------------------------------------------------
if not os.path.exists(args.outdir):
  os.makedirs(args.outdir)
  
# -------------------------------------------------------------------
if args.zoom == True:
  filename = 'Plot_TimeSeries_'+'pyGAC_'+args.channel+'_'+args.time+'_zoom.png' #eps,pdf
else:
  filename = 'Plot_TimeSeries_'+'pyGAC_'+args.channel+'_'+args.time+'.png' #eps,pdf
  
ptitle   = 'AVHRRGAC time series (pyGAC): '
fig = plt.figure()

if args.zoom == True:
  ax_val = fig.add_subplot(411)
  ax_zoo = fig.add_subplot(412)
  ax_std = fig.add_subplot(413)
  ax_rec = fig.add_subplot(414)
else:
  ax_val = fig.add_subplot(311)
  ax_std = fig.add_subplot(312)
  ax_rec = fig.add_subplot(313)

# -------------------------------------------------------------------
if args.sdate == True and args.edate == True:
  sd = datetime.datetime.strptime(args.sdate, '%Y%m%d').date()
  ed = datetime.datetime.strptime(args.edate, '%Y%m%d').date()
  ax_val.set_xlim([sd,ed])
  ax_std.set_xlim([sd,ed])
  ax_rec.set_xlim([sd,ed])
  
  if args.zoom == True:
    ax_zoo.set_xlim([sd,ed])

# -------------------------------------------------------------------
cnt = -1
colorlst = ['Red','DodgerBlue','DarkOrange','Lime',
            'Navy','Magenta','DarkGreen','Turquoise',
            'DarkMagenta','Sienna','Gold','Olive','MediumSlateBlue',
            'DimGray']

# -------------------------------------------------------------------
pattern  = "Global_statistics_AVHRRGACl1c_*.txt"
fil_list = mysub.find(pattern, args.inpdir)
fil_list.sort()

allave = []
allstd = []
  
for fil in fil_list:
  str_lst = mysub.split_filename(fil)
  
  for s in str_lst:
    if 'noaa' in s or 'metop' in s:
      satname = s
  
  cnt += 1
  flag = True
    
  obj = open(fil, mode="r")
  lines = obj.readlines()
  obj.close()

  # Global statistics for AVHRR GAC on NOAA-15
  # channel | date | time | mean | stdv | nobs
  lstar = []
  lsdat = []
  lstim = []
  lsave = []
  lsstd = []
  lsrec = []
  
  for line in lines:
    
    if '#' in line:
      continue
    if '-9999.0000' in line:
      continue
    
    string = line.split( )
    
    if string[0] == args.channel:
      if string[2] == args.time:
	lstar.append(string[0])
	date = datetime.datetime.strptime(string[1], '%Y%m%d').date()
	lsdat.append(date)
	lstim.append(string[2])
	lsave.append(float(string[3]))
	lsstd.append(float(string[4]))
	lsrec.append(float(string[5]))


  if len(lstar) == 0:
    continue
  
  # for zoom range calculation
  allave.append(np.mean(lsave))
  allstd.append(np.mean(lsstd))
  
  satlabel = mysub.full_sat_name(satname)[0]
  
  if flag == True:
    ax_val.plot(lsdat, lsave, 'o',  color=colorlst[cnt])
    ax_val.plot(lsdat, lsave, label=satlabel, color=colorlst[cnt], linewidth=2)
    ax_std.plot(lsdat, lsstd, 'o', color=colorlst[cnt])
    ax_std.plot(lsdat, lsstd, label=satlabel, color=colorlst[cnt], linewidth=2)
    ax_rec.plot(lsdat, lsrec, 'o', color=colorlst[cnt])
    ax_rec.plot(lsdat, lsrec, label=satlabel, color=colorlst[cnt], linewidth=2)
    
    if args.zoom == True:
      ax_zoo.plot(lsdat, lsave, 'o',  color=colorlst[cnt])
      ax_zoo.plot(lsdat, lsave, label=satlabel, color=colorlst[cnt], linewidth=2)
      
    flag = False
    
  elif flag == False:
    ax_val.plot(lsdat, lsave, 'o', color=colorlst[cnt])
    ax_val.plot(lsdat, lsave, color=colorlst[cnt], linewidth=2)
    ax_std.plot(lsdat, lsstd, 'o', color=colorlst[cnt])
    ax_std.plot(lsdat, lsstd, color=colorlst[cnt], linewidth=2)
    ax_rec.plot(lsdat, lsrec, 'o', color=colorlst[cnt])
    ax_rec.plot(lsdat, lsrec, color=colorlst[cnt], linewidth=2)
    
    if args.zoom == True:
      ax_zoo.plot(lsdat, lsave, 'o', color=colorlst[cnt])
      ax_zoo.plot(lsdat, lsave, color=colorlst[cnt], linewidth=2)

# -------------------------------------------------------------------
targetname = mysub.full_target_name(args.channel)

ax_val.set_title(ptitle+targetname+' ('+args.time+')\n')
ax_val.set_ylabel('Global Mean\n')
ax_std.set_ylabel('Standard Deviation\n')
ax_rec.set_xlabel('Time\n')
ax_rec.set_ylabel('# of Observations\n')
ax_val.grid()
ax_std.grid()
ax_rec.grid()

if args.zoom == True:
  gmean = np.mean(allave)
  gstdd = np.std(allave)
  smean = np.mean(allstd)
  sstdd = np.std(allstd)

  ax_zoo.set_ylabel('Zoom Global Mean\n')
  ax_zoo.grid()
  
  if args.channel == 'ch1':
    ax_zoo.set_ylim([gmean-gstdd,gmean+gstdd])
    #ax_std.set_ylim([smean-sstdd,smean+sstdd])
  if args.channel == 'ch2':
    ax_zoo.set_ylim([gmean-gstdd,gmean+gstdd])
    #ax_std.set_ylim([smean-sstdd,smean+sstdd])
  if args.channel == 'ch3a':
    ax_zoo.set_ylim([gmean-gstdd,gmean+gstdd])
    #ax_std.set_ylim([smean-sstdd,smean+sstdd])
  if args.channel == 'ch3b':
    ax_zoo.set_ylim([gmean-gstdd,gmean+gstdd])
    #ax_std.set_ylim([smean-sstdd,smean+sstdd])
  if args.channel == 'ch4':
    ax_zoo.set_ylim([gmean-gstdd,gmean+gstdd])
    #ax_std.set_ylim([smean-sstdd,smean+sstdd])
  if args.channel == 'ch5':
    ax_zoo.set_ylim([gmean-gstdd,gmean+gstdd])
    #ax_std.set_ylim([smean-sstdd,smean+sstdd])

  
if len(fil_list) > 5:
  leg = ax_val.legend(bbox_to_anchor=(1.125, 1.05), fontsize=11)
else:
  plt.tight_layout()
  leg = ax_val.legend(loc='upper center', fancybox=True)
leg.get_frame().set_alpha(0.5)

plt.savefig(args.outdir+filename)
#plt.show()
plt.close()
# -------------------------------------------------------------------

