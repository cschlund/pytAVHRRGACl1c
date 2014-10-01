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
import matplotlib
from scipy import stats

min_nobs_day_night = 2.5e7
min_nobs_twilight  = 0.6e7

parser = argparse.ArgumentParser(description='''%s 
displays daily global means of 1 satellite, 1 channel and
1 time selection including a linear regressioin.''' % os.path.basename(__file__))


parser.add_argument('-i', '--inpfil', help='Path, e.g. /path/to/Global_stat.txt', required=True)
parser.add_argument('-o', '--outdir', help='Path, e.g. /path/to/output.png', required=True)
parser.add_argument('-c', '--channel', help='Channel abbreviation, i.e. ch1/ch2/ch3a/ch3b/ch4/ch5')
parser.add_argument('-t', '--time', help='Time abbreviation, e.g. day/night/twilight')
parser.add_argument('-s', '--sdate', help='Start Date String, e.g. 20090101')
parser.add_argument('-e', '--edate', help='End Date String, e.g. 20121231')
parser.add_argument('-v', '--verbose', help='increase output verbosity', action="store_true")
parser.add_argument('-n', '--nofilter', help='''do not set minimum number of observations, 
i.e. take all days into account''')

args = parser.parse_args()

# -------------------------------------------------------------------
if args.verbose == True:
  print ("\n *** Parameter passed" )
  print ("   - Input File : %s" % args.inpfil)
  print ("   - Output Path: %s" % args.outdir)
  print ("   - Channel    : %s" % args.channel)
  print ("   - Time       : %s" % args.time)
  print ("   - Start Date : %s" % args.sdate)
  print ("   - End Date   : %s" % args.edate)
  print ("   - Verbose    : %s" % args.verbose)
  print ("   - Nofilter   : %s\n" % args.nofilter)

# -------------------------------------------------------------------
if not os.path.exists(args.outdir):
  os.makedirs(args.outdir)
  
# -------------------------------------------------------------------
col_lst = ['Red','DodgerBlue','DarkOrange','Lime',
           'Navy','Magenta','DarkGreen','Turquoise',
           'DarkMagenta','Sienna','Gold','Olive',
           'MediumSlateBlue','DimGray']
           
# -------------------------------------------------------------------
if args.channel == None or args.time == None:
  cha_list  = ['ch1', 'ch2', 'ch3b', 'ch4', 'ch5', 'ch3a']
  sel_list  = ['day', 'night', 'twilight']
else:
  cha_list = [args.channel]
  sel_list = [args.time]

## -------------------------------------------------------------------
#if args.sdate != None and args.edate != None:
  #sd = datetime.datetime.strptime(args.sdate, '%Y%m%d').date()
  #ed = datetime.datetime.strptime(args.edate, '%Y%m%d').date()
  
  #ax_val.set_xlim([sd,ed])
  #ax_std.set_xlim([sd,ed])
  #ax_rec.set_xlim([sd,ed])
  
# -------------------------------------------------------------------
str_lst = mysub.split_filename(args.inpfil)

for s in str_lst:
  if 'noaa' in s or 'metop' in s:
    satname = s
    
# -------------------------------------------------------------------
for channel in cha_list:
  for time in sel_list:
    
    if time is 'twilight':
      min_nobs = min_nobs_twilight
    else:
      min_nobs = min_nobs_day_night

    # read file
    (lstar,lsdat,lstim,
    lsave,lsstd,lsrec) = mysub.read_globstafile(args.inpfil,channel,time)

    if len(lstar) == 0:
      print ("   *** No data for %s (%s) on %s " %(channel, time, satname))
      break

    platf = mysub.full_sat_name(satname)[0]
    bname = mysub.full_cha_name(channel)

    basename = satname+'_'+channel+'_'+time
    filename = 'Plot_TimeSeries_LiFIT_'+basename+'.png' #eps,pdf
    
    if args.sdate != None and args.edate != None:
      datestr  = '_' + args.sdate + '_' + args.edate
      basename = satname+'_'+channel+'_'+time+datestr
      filename = 'Plot_TimeSeries_LiFIT_'+basename+'.png' #eps,pdf
    else:
      basename = satname+'_'+channel+'_'+time
      filename = 'Plot_TimeSeries_LiFIT_'+basename+'.png' #eps,pdf
      
    ptitle   = 'AVHRRGAC '+bname+' ('+time+') on '+platf+'\n'
    outfile  = os.path.join(args.outdir,filename)

    fig = plt.figure()
    ax_val = fig.add_subplot(311)
    ax_std = fig.add_subplot(312)
    ax_rec = fig.add_subplot(313)
    
    if args.sdate != None and args.edate != None:
      sd = datetime.datetime.strptime(args.sdate, '%Y%m%d').date()
      ed = datetime.datetime.strptime(args.edate, '%Y%m%d').date()
      
      ax_val.set_xlim([sd,ed])
      ax_std.set_xlim([sd,ed])
      ax_rec.set_xlim([sd,ed])
  
    # list to array
    rec = np.asarray(lsrec)
    ave = np.asarray(lsave)
    std = np.asarray(lsstd)
    dat = np.asarray(lsdat)

    # get start and end date
    if args.sdate == None and args.edate == None:
      sd = datetime.datetime.strptime(str(dat[0]), '%Y-%m-%d').date()
      ed = datetime.datetime.strptime(str(dat[len(dat)-1]), '%Y-%m-%d').date()
      ax_val.set_xlim([sd,ed])
      ax_std.set_xlim([sd,ed])
      ax_rec.set_xlim([sd,ed])


    if args.nofilter == None:
      # mask too low observations = filter good statistics
      msk = np.ma.less(rec, min_nobs)
      mave = np.ma.masked_where(msk, ave)
      mstd = np.ma.masked_where(msk, std)
    else:
      mave = ave
      mstd = std

    # linear regression:

    # convert date list to a set of numbers counting the number of days
    # having passed from the first day of the file
    x = [(e - min(lsdat)).days for e in lsdat]

    #slope : float
	#slope of the regression line
    #intercept : float
	#intercept of the regression line
    #r-value : float
	#correlation coefficient
    #p-value : float
	#two-sided p-value for a hypothesis test whose null hypothesis is that the slope is zero.
    #stderr : float
	#Standard error of the estimate

    (slope, intercept, r_value, 
    p_value, std_err) = stats.linregress(x,mave)
    
    (slope2, intercept2, r_value2, 
    p_value2, std_err2) = stats.linregress(x,mstd)
    
    # either
    projection = [slope * (e - min(lsdat)).days + intercept for e in lsdat]
    projection2 = [slope2 * (e - min(lsdat)).days + intercept2 for e in lsdat]
    # or
    yp = np.polyval([slope,intercept],x)
    yp2 = np.polyval([slope2,intercept2],x)

    if args.verbose == False:
      print ("\n *** Linear regression: global mean" )
      print ("   - slope     : %s" % slope)
      print ("   - intercept : %s" % intercept)
      print ("   - r-value   : %s" % r_value)
      print ("   - p-value   : %s" % p_value)
      print ("   - stderr    : %s" % std_err)
      print ("\n *** Linear regression: global standard deviation" )
      print ("   - slope     : %s" % slope2)
      print ("   - intercept : %s" % intercept2)
      print ("   - r-value   : %s" % r_value2)
      print ("   - p-value   : %s" % p_value2)
      print ("   - stderr    : %s" % std_err2)

    # plot global mean and stdv
    # filtered data
    ax_val.plot(dat, mave, 'o',  color=col_lst[8])
    ax_val.plot(dat, mave, label='Data', color=col_lst[8], linewidth=2)
    ax_val.plot(dat,yp, '--', color=col_lst[2], 
    label="Linear fit: y = %.5f * x + %.5f" %(slope, intercept), lw=2.0)
    #ax_val.plot(dat, projection, 'g--', lw=2.0)
    ax_std.plot(dat, mstd, 'o', color=col_lst[1])
    ax_std.plot(dat, mstd, label='Data', color=col_lst[1], linewidth=2)
    ax_std.plot(dat,yp2, '--', color=col_lst[2], 
    label="Linear fit: y = %.5f * x + %.5f" %(slope2, intercept2), lw=2.0)

    # records not filtered (show all data)
    ax_rec.plot(dat, rec, 'o', color=col_lst[6])
    ax_rec.plot(dat, rec, color=col_lst[6], linewidth=2)

    if args.nofilter == None:
      # plot min_nobs on top of them:
      ax_rec.plot(dat, np.ma.ones(len(dat))*min_nobs, 'r--',
      label="Min.Nobs = %.1e" % (min_nobs), lw=2.0)

    # plot title
    ax_val.set_title(ptitle)

    # global mean
    ax_val.set_ylabel('Global Mean\n')
    #ax_val.set_ylim(0, 1.2*np.ma.max(mave))
    leg = ax_val.legend(loc='best', fancybox=True)
    leg.get_frame().set_alpha(0.5)

    # standard deviation
    ax_std.set_ylabel('Standard Deviation\n')
    #ax_std.set_ylim(0, 1.2*np.ma.max(mstd))
    leg = ax_std.legend(loc='best', fancybox=True)
    leg.get_frame().set_alpha(0.5)

    # number of observations
    ax_rec.set_xlabel('Time\n')
    ax_rec.set_ylabel('# of Observations\n')
    ax_rec.set_ylim(0, 1.2*np.ma.max(rec))
    if args.nofilter == None:
      leg = ax_rec.legend(loc='lower left', fancybox=True)
      leg.get_frame().set_alpha(0.5)

    # plot grid
    ax_val.grid()
    ax_std.grid()
    ax_rec.grid()

    # finish plot
    plt.tight_layout()
    plt.savefig(outfile)
    #plt.show()
    plt.close()
    
    print ("   *** %s done!" % outfile)
    
    if channel is 'ch1' or channel is 'ch2' or channel is 'ch3a':
      break

# -------------------------------------------------------------------
print ( "\n *** %s finished for %s \n" % (sys.argv[0], args.inpfil) )
# -------------------------------------------------------------------