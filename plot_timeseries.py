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

parser.add_argument('-i', '--inpdir', help='Path, e.g. /path/to/input.txt', required=True)
parser.add_argument('-o', '--outdir', help='Path, e.g. /path/to/output.png', required=True)
parser.add_argument('-c', '--channel', help='Channel abbreviation, i.e. ch1/ch2/ch3a/ch3b/ch4/ch5')
parser.add_argument('-t', '--time', help='Time abbreviation, e.g. day/night/twilight')
parser.add_argument('-z', '--zoom', help='y-lim: min/max', action="store_true")
parser.add_argument('-s', '--sdate', help='Start Date String, e.g. 20090101')
parser.add_argument('-e', '--edate', help='End Date String, e.g. 20121231')
parser.add_argument('-v', '--verbose', help='increase output verbosity', action="store_true")

args = parser.parse_args()

# -------------------------------------------------------------------

if args.verbose == True:
  print ("\n *** Parameter passed" )
  print (" ---------------------- ")
  print ("   - Input Path : %s" % args.inpdir)
  print ("   - Output Path: %s" % args.outdir)
  print ("   - Channel    : %s" % args.channel)
  print ("   - Time       : %s" % args.time)
  print ("   - Zoom       : %s" % args.zoom)
  print ("   - Start Date : %s" % args.sdate)
  print ("   - End Date   : %s" % args.edate)
  print ("   - Verbose    : %s" % args.verbose)

  
# -------------------------------------------------------------------
if not os.path.exists(args.outdir):
  os.makedirs(args.outdir)

# -------------------------------------------------------------------
colorlst = ['Red','DodgerBlue','DarkOrange','Lime',
            'Navy','Magenta','DarkGreen','Turquoise',
            'DarkMagenta','Sienna','Gold','Olive','MediumSlateBlue',
            'DimGray']

# -------------------------------------------------------------------
if args.channel == None or args.time == None:
  cha_list  = ['ch1', 'ch2', 'ch3b', 'ch4', 'ch5', 'ch3a']
  sel_list  = ['day', 'night', 'twilight']
else:
  cha_list = [args.channel]
  sel_list = [args.time]
  
# -------------------------------------------------------------------
if args.sdate != None and args.edate != None:
  sd = datetime.datetime.strptime(args.sdate, '%Y%m%d').date()
  ed = datetime.datetime.strptime(args.edate, '%Y%m%d').date()
  ax_val.set_xlim([sd,ed])
  ax_std.set_xlim([sd,ed])
  ax_rec.set_xlim([sd,ed])

# -------------------------------------------------------------------
pattern  = "Global_statistics_AVHRRGACl1c_*.txt"
fil_list = mysub.find(pattern, args.inpdir)
fil_list.sort()

# -------------------------------------------------------------------
for channel in cha_list:
  for time in sel_list:

    if args.zoom == True:
      filename = 'Plot_TimeSeries_'+'pyGAC_'+channel+'_'+time+'_zoom.png'
    else:
      filename = 'Plot_TimeSeries_'+'pyGAC_'+channel+'_'+time+'.png'
    
    ptitle   = 'AVHRRGAC time series (pyGAC): '
    outfile  = args.outdir+filename
    fig      = plt.figure()

    ax_val = fig.add_subplot(311)
    ax_std = fig.add_subplot(312)
    ax_rec = fig.add_subplot(313)
	
    allave = []
    maxave = []
    allstd = []
    maxstd = []
    maxrec = []
    
    cnt = -1
      
    for fil in fil_list:
      str_lst = mysub.split_filename(fil)
      
      for s in str_lst:
	if 'noaa' in s or 'metop' in s:
	  satname = s
      
      cnt += 1
      flag = True
      
      (lstar,lsdat,lstim,
       lsave,lsstd,lsrec) = mysub.read_globstafile(fil,channel,time)

      if len(lstar) == 0:
	print ("   *** No data for %s (%s) on %s " %(channel, time, satname))
	continue
    
      # for zoom range calculation
      allave.append(np.mean(lsave))
      maxave.append(np.max(lsave))
      allstd.append(np.mean(lsstd))
      maxstd.append(np.max(lsstd))
      maxrec.append(np.max(lsrec))
      
      satlabel = mysub.full_sat_name(satname)[0]
      
      if flag == True:
	ax_val.plot(lsdat, lsave, 'o',  color=colorlst[cnt])
	ax_val.plot(lsdat, lsave, label=satlabel, color=colorlst[cnt], linewidth=2)
	ax_std.plot(lsdat, lsstd, 'o', color=colorlst[cnt])
	ax_std.plot(lsdat, lsstd, label=satlabel, color=colorlst[cnt], linewidth=2)
	ax_rec.plot(lsdat, lsrec, 'o', color=colorlst[cnt])
	ax_rec.plot(lsdat, lsrec, label=satlabel, color=colorlst[cnt], linewidth=2)
	flag = False
	
      elif flag == False:
	ax_val.plot(lsdat, lsave, 'o', color=colorlst[cnt])
	ax_val.plot(lsdat, lsave, color=colorlst[cnt], linewidth=2)
	ax_std.plot(lsdat, lsstd, 'o', color=colorlst[cnt])
	ax_std.plot(lsdat, lsstd, color=colorlst[cnt], linewidth=2)
	ax_rec.plot(lsdat, lsrec, 'o', color=colorlst[cnt])
	ax_rec.plot(lsdat, lsrec, color=colorlst[cnt], linewidth=2)


    bname = mysub.full_cha_name(channel)

    # plot title
    ax_val.set_title(ptitle+bname+' ('+time+')\n')
    
    # global mean
    ax_val.set_ylabel('Global Mean\n')
    if args.zoom == False:
      ax_val.set_ylim(0, 1.1*np.ma.max(maxave))
    
    # standard deviation
    ax_std.set_ylabel('Standard Deviation\n')
    if args.zoom == False:
      ax_std.set_ylim(0, 1.1*np.ma.max(maxstd))
    
    # number of observations
    ax_rec.set_xlabel('Time\n')
    ax_rec.set_ylabel('# of Observations\n')
    ax_rec.set_ylim(0, 1.1*np.ma.max(maxrec))
    
    # make grid
    ax_val.grid()
    ax_std.grid()
    ax_rec.grid()

      
    if len(fil_list) > 5:
      leg = ax_val.legend(bbox_to_anchor=(1.125, 1.05), fontsize=11)
    else:
      plt.tight_layout()
      leg = ax_val.legend(loc='best', fancybox=True)

    leg.get_frame().set_alpha(0.5)
    plt.savefig(outfile)
    #plt.show()
    plt.close()
    
    print ("   *** %s done!" % outfile)
    
    if channel is 'ch1' or channel is 'ch2' or channel is 'ch3a':
      break

# -------------------------------------------------------------------
print ( "\n *** %s finished for %s \n" % (sys.argv[0], args.inpdir) )
# -------------------------------------------------------------------
