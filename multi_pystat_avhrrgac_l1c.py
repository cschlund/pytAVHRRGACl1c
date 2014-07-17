#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# how to use the script: 
#   > python script.py -h
#
# C.Schlundt: July, 2014
#
# -------------------------------------------------------------------

import numpy as np
import h5py
import os, sys, getopt
import argparse
import subs_avhrrgac as mysub
import read_avhrrgac_h5 as rh5
from multiprocessing import Pool

# -------------------------------------------------------------------

def readfiles(tup):
  
  idx, fil = tup #tuple
  
  global zone_size
  global nzones
  
  # initialize global mean, stdv, nobs parameters
  # saving output for each orbit
  gmean = dict()
  gstdv = dict()
  gnobs = dict()
  zmean = dict()
  zstdv = dict()
  znobs = dict()

  for cha in cha_list:
    gmean[cha] = dict()
    gstdv[cha] = dict()
    gnobs[cha] = dict()
    zmean[cha] = dict()
    zstdv[cha] = dict()
    znobs[cha] = dict()
    
    for sel in sel_list:
      gmean[cha][sel] = 0.
      gstdv[cha][sel] = 0.
      gnobs[cha][sel] = 0.
      zmean[cha][sel] = np.ma.zeros(nzones)
      zstdv[cha][sel] = np.ma.zeros(nzones)
      znobs[cha][sel] = np.ma.zeros(nzones)
      
      if cha is 'ch1' or cha is 'ch2' or cha is 'ch3a':
	break
	
      
  str_list = mysub.split_filename(fil)
  for item in str_list:
    if 'T' in item or 'Z' in item:
      dat_str = item
      break

  #search for corresponding sunsatangles file
  dirf = os.path.dirname(fil)
  basf = os.path.basename(fil)
  patt = '*sunsatangles*'+args.satellite+'*'+dat_str+'*'
  afil = mysub.find(patt, dirf)[0]
  
  if args.verbose == True:
    print ("   * %s = %s/%s" 
    % (idx, os.path.basename(fil),os.path.basename(afil)))
  
  # open H5 files
  f = h5py.File(fil, "r+")
  a = h5py.File(afil, "r+")

  #if ver == True:
  #rh5.show_properties(f)
  #rh5.show_properties(a)

  #------------------------------------------------------------------
  # cha_list  = ['ch1', 'ch2', 'ch3b', 'ch4', 'ch5', 'ch3a']
  for channel in cha_list:
  #------------------------------------------------------------------
  
    #----------------------------------------------------------------
    # sel_list  = ['day', 'night', 'twilight']
    for select in sel_list:
    #----------------------------------------------------------------

      try:
	check = global_mean[channel][select]
	
	if args.verbose == True:
	  print ("   * %s = %s (%s)" % (idx, mysub.full_target_name(channel), select))

	(lat, lon, tar) = rh5.read_avhrrgac(f, a, select, channel, False)
	#(lat, lon, tar) = rh5.read_avhrrgac(f, a, select, channel, args.verbose)
	
	# check is channel is filled with measurements
	if np.ma.count(tar) == 0:
	  break
	
	# global statistics
	gn = tar.count()
	gm = tar.mean(dtype=np.float64)
	gs = tar.std(dtype=np.float64)
	
	#zonal statistics
	(zm, zs, zn) = mysub.cal_zonal_means(lat, tar, zone_size)
      
	gmean[channel][select] = gm
	gstdv[channel][select] = gs
	gnobs[channel][select] = gn
	
	zmean[channel][select] = zm
	zstdv[channel][select] = zs
	znobs[channel][select] = zn
	
	# clear variables
	del(gm, gs, gn, zm, zs, zn)
	  
      except KeyError:
	break
	
      #--------------------------------------------------------------
      # select loop
      #break
      #--------------------------------------------------------------
      
    #----------------------------------------------------------------
    # channel loop
    #break
    #----------------------------------------------------------------
  
  # close H5 files
  a.close()
  f.close()
  
  # return pro orbit=file
  return (idx, gmean, gstdv, gnobs, zmean, zstdv, znobs)


# -------------------------------------------------------------------

if __name__ == '__main__':

  parser = argparse.ArgumentParser(description='''%s
  calculates statistics (daily zonal and global means) of 
  AVHRR GAC Level 1c data processed in the framework of cloud_cci (gyGAC).
  For the VIS channels, statistics is based on daytime observations only,
  i.e. SZA less than 80. For the IR channels day/twilight/night 
  observations are considered. Additionally, 2 different PNG files
  are created based on the statistics. Orbits are processed in 
  parallel mode.''' % os.path.basename(__file__))

  # -------------------------------------------------------------------

  parser.add_argument('-d', '--date', help='Date String, e.g. 20090126', required=True)
  parser.add_argument('-s', '--satellite', help='Satellite, e.g. metop02', required=True)
  parser.add_argument('-p', '--path', help='Path, e.g. /path/to/files', required=True)
  parser.add_argument('-b', '--binsize', help='Define binsize for latitudinal belts', default=5)
  parser.add_argument('-v', '--verbose', help='increase output verbosity', action="store_true")
  parser.add_argument('-g', '--gfile', help='''/path/to/Global_statistics_avhrrgac_satellite.txt, 
  which collects global means/stdvs for each satellite, # channel | date | time | mean | stdv | nobs''')

  args = parser.parse_args()

  # -------------------------------------------------------------------

  if args.verbose == True:
    print ("\n *** Parameter passed" )
    print (" ---------------------- ")
    print ("   - Date       : %s" % args.date)
    print ("   - Satellite  : %s" % args.satellite)
    print ("   - Path       : %s" % args.path)
    print ("   - Binsize    : %s" % args.binsize)
    print ("   - Verbose    : %s" % args.verbose)
    print ("   - File4Global: %s" % args.gfile)

  # -------------------------------------------------------------------
  outdir     = './PYSTA/'
  basestr    = args.satellite+'_'+args.date
  ofilebase  = 'GlobalZonalMeans_avhrrGAC_'+basestr
  oplotbase  = 'Plot_'+ofilebase
  oplotbas2  = 'Plot2_'+ofilebase
  fill_value = -9999.
  pattern    = 'ECC_GAC_avhrr*'+args.satellite+'*'+args.date+'T*'
  fil_list   = mysub.find(pattern, args.path)
  nfiles     = len(fil_list)

  if not os.path.exists(outdir):
    os.makedirs(outdir)

  if args.gfile != None:
    #full_gfpath = os.path.join(outdir, args.gfile)
    check_gfile = os.path.exists(args.gfile)

    if check_gfile is False:
      f = open(args.gfile, mode="w")
      hlin1 = '# Global statistics for AVHRR GAC on '+mysub.full_sat_name(args.satellite)[0]+'\n'
      hlin2 = '# channel | date | time | mean | stdv | nobs \n'
      f.write(hlin1)
      f.write(hlin2)
      f.close()
    
  # -------------------------------------------------------------------

  # lists for generating total arrays
  cha_list  = ['ch1', 'ch2', 'ch3b', 'ch4', 'ch5', 'ch3a']
  sel_list  = ['day', 'night', 'twilight']

  # -------------------------------------------------------------------

  # define latitudinal zone size :
  global zone_size
  global nzones
  
  zone_size = float(args.binsize)
  zone_rad = zone_size/2.0

  # determine zone centers:
  zone_centers = np.arange(-90 + zone_rad, 90 + zone_rad, zone_size)
  nzones = len(zone_centers)

  # -------------------------------------------------------------------

  # initialize global mean, stdv, nobs parameters
  # saving output for each orbit
  global_mean = dict()
  global_stdv = dict()
  global_nobs = dict()
  zonal_mean = dict()
  zonal_stdv = dict()
  zonal_nobs = dict()
  # saving mean values based on all orbits/day
  all_global_mean = dict()
  all_global_stdv = dict()
  all_global_nobs = dict()
  all_zonal_mean = dict()
  all_zonal_stdv = dict()
  all_zonal_nobs = dict()

  for cha in cha_list:
    global_mean[cha] = dict()
    global_stdv[cha] = dict()
    global_nobs[cha] = dict()
    zonal_mean[cha] = dict()
    zonal_stdv[cha] = dict()
    zonal_nobs[cha] = dict()
    
    all_global_mean[cha] = dict()
    all_global_stdv[cha] = dict()
    all_global_nobs[cha] = dict()
    all_zonal_mean[cha] = dict()
    all_zonal_stdv[cha] = dict()
    all_zonal_nobs[cha] = dict()
    
    for sel in sel_list:
      global_mean[cha][sel] = np.ma.zeros(nfiles)
      global_stdv[cha][sel] = np.ma.zeros(nfiles)
      global_nobs[cha][sel] = np.ma.zeros(nfiles)
      zonal_mean[cha][sel] = np.ma.zeros((nfiles,nzones))
      zonal_stdv[cha][sel] = np.ma.zeros((nfiles,nzones))
      zonal_nobs[cha][sel] = np.ma.zeros((nfiles,nzones))
      
      all_global_mean[cha][sel] = 0.
      all_global_stdv[cha][sel] = 0.
      all_global_nobs[cha][sel] = 0.
      all_zonal_mean[cha][sel] = np.ma.zeros(nzones)
      all_zonal_stdv[cha][sel] = np.ma.zeros(nzones)
      all_zonal_nobs[cha][sel] = np.ma.zeros(nzones)
      
      if cha is 'ch1' or cha is 'ch2' or cha is 'ch3a':
	break

  # -------------------------------------------------------------------

  # creating jobs as tuple
  #jobs = [(pos, fil) for pos, fil in enumerate(fil_list)]
  arglist = []
  for pos, fil in enumerate(fil_list):
    arglist.append((pos,fil))
  
  pool = Pool(processes=nfiles)
  results = pool.map(func=readfiles, iterable=arglist)
  
  # idx, gmean, gstdv, gnobs, zmean, zstdv, znobs
  outputs = [(res[0], res[1], res[2], 
	      res[3], res[4], res[5], res[6]) for res in results]
  
  for out in outputs:
    #print ("   + IDX  :%s " % out[0])
    #print ("   + gmean:%s " % out[1])
    #print ("   + gstdv:%s " % out[2])
    #print ("   + gnobs:%s " % out[3])
    #print ("   + zmean:%s " % out[4])
    #print ("   + zstdv:%s " % out[5])
    #print ("   + znobs:%s " % out[6])
    for cha in cha_list:
      for sel in sel_list:
	try:
	  check = global_mean[cha][sel]
	  global_mean[cha][sel][out[0]] = out[1][cha][sel]
	  global_stdv[cha][sel][out[0]] = out[2][cha][sel]
	  global_nobs[cha][sel][out[0]] = out[3][cha][sel]

	  zonal_mean[cha][sel][out[0],:] = out[4][cha][sel]
	  zonal_stdv[cha][sel][out[0],:] = out[5][cha][sel]
	  zonal_nobs[cha][sel][out[0],:] = out[6][cha][sel]
	  
	except KeyError:
	  break
  
  #print "   * Global arrays:"
  #for chakey in global_mean:
    #for selkey,selval in global_mean[chakey].items():
      #print chakey , "=>", selkey, " : ", selval
  #quit()
  
  # -------------------------------------------------------------------

  # create lists of mean, stdv, nobs for globa/zonal
  global_list = [global_mean, global_stdv, global_nobs]
  zonal_list  = [zonal_mean, zonal_stdv, zonal_nobs]
  
  all_global_list = [all_global_mean, all_global_stdv, all_global_nobs]
  all_zonal_list  = [all_zonal_mean, all_zonal_stdv, all_zonal_nobs]

  # -------------------------------------------------------------------

  # Global means/stdv/nobs
  for position, item in enumerate(global_list):
    for chakey in item:
      for selkey,selval in item[chakey].items():
	# mask zeros
	mask = np.ma.equal(item[chakey][selkey], 0.)
	data = np.ma.masked_where(mask, item[chakey][selkey])
	#print chakey , "=>", selkey, " : ", selval, " ; ", data
	    
	if position is 2:
	  ave = np.sum(data)
	else:
	  ave = data.mean()
	
	try:
	  all_item = all_global_list[position]
	  check = all_item[chakey][selkey]
	  all_item[chakey][selkey] = ave
	  del ave
	  
	except KeyError:
	  break

  #print "   * All Global arrays:"
  #for position, elem in enumerate(all_global_list):
    #for chakey in elem:
      #for selkey,selval in elem[chakey].items():
	#print chakey , "=>", selkey, " : ", selval
  #quit()
  # -------------------------------------------------------------------

  # Zonal means/stdv/nobs
  for position, item in enumerate(zonal_list):
    for chakey in item:
      for selkey,selval in item[chakey].items():
	# mask zeros
	mask = np.ma.equal(item[chakey][selkey], 0.)
	data = np.ma.masked_where(mask, item[chakey][selkey])
	#print chakey , "=>", selkey, " : ", selval, " ; ", data
	
	if position is 2:
	  ave = np.sum(data, axis=0)
	else:
	  ave = data.mean(axis=0)
	  
	try:
	  all_item = all_zonal_list[position]
	  check = all_item[chakey][selkey]
	  all_item[chakey][selkey][:] = ave
	  del ave
	  
	except KeyError:
	  break

  #print "   * All Zonal arrays:"
  #for position, elem in enumerate(all_zonal_list):
    #for chakey in elem:
      #for selkey,selval in elem[chakey].items():
	#print chakey , "=>", selkey, " : ", selval

  # -------------------------------------------------------------------

  # plot output: histogram
  if args.verbose == True:
    print ("\n   *** Plot histogram for all channels !")

  for chakey in cha_list:
    for selkey in sel_list:
      try:
	check = all_zonal_mean[chakey][selkey]
	if np.ma.count(check) == 0:
	  continue
	print ("      + %s (%s) "  % (mysub.full_target_name(chakey), selkey) )
	mysub.plt_zonal_means(
	  all_zonal_list[0][chakey][selkey], 
	  all_zonal_list[2][chakey][selkey], 
	  all_global_list[0][chakey][selkey], 
	  zone_size, 
	  outdir+oplotbase+'_'+chakey+'_'+selkey+'.png',
	  args.date+' ('+selkey+')',
	  mysub.full_target_name(chakey),
	  mysub.full_sat_name(args.satellite)[0])
      except KeyError:
	break

  # -------------------------------------------------------------------

  # plot2 output: lat. plot
  if args.verbose == True:
    print ("\n   *** Plot2 zonal means for all channels !")
    
  for chakey in cha_list:
    for selkey in sel_list:
      try:
	check = all_zonal_mean[chakey][selkey]
	if np.ma.count(check) == 0:
	  continue
	print ("      + %s (%s) "  % (mysub.full_target_name(chakey), selkey) )
	mysub.plt_zonal_mean_stdv(
	  all_zonal_list[0][chakey][selkey], 
	  all_zonal_list[1][chakey][selkey],
	  all_zonal_list[2][chakey][selkey], 
	  zone_centers, zone_size,
	  outdir+oplotbas2+'_'+chakey+'_'+selkey+'.png',
	  args.date+' ('+selkey+')',
	  mysub.full_target_name(chakey),
	  mysub.full_sat_name(args.satellite)[0])
      except KeyError:
	break

  # -------------------------------------------------------------------
  # save output

  if args.verbose == True:
    print ("\n   *** Write global/zonal output files for all channels !")


  # --- Write global statistics ---
  if args.gfile != None:
    f = open(args.gfile, mode="a")

  for chakey in cha_list:
    for selkey in sel_list:
      try:
	check = all_zonal_mean[chakey][selkey]
	if np.ma.count(check) == 0:
	  continue
	print ("      + %s (%s) "  % (mysub.full_target_name(chakey), selkey) )
	
	zm = all_zonal_list[0][chakey][selkey]
	zn = all_zonal_list[2][chakey][selkey]
	gn = all_global_list[2][chakey][selkey]
	gmean_check = np.ma.dot(zm, zn)/gn
	
	print ("        - Global mean based on zonal means: %f = %f (global)" 
	% (gmean_check, all_global_list[0][chakey][selkey]) )
	print ("        - Global nobs based on zonal nobs: %d = %d (global)" 
	% (np.sum(zn), gn))
	
	if np.sum(zn) != gn:
	  print (" --- FAILED: Global nobs based on zonal nobs: %d = %d (global)" 
	  % (np.sum(zn), gn))

	# write2file
	mysub.write_zonal_means(
	  outdir+ofilebase+'_'+chakey+'_'+selkey+'.sta', 
	  zone_centers, fill_value,
	  mysub.full_sat_name(args.satellite)[0], 
	  args.date+' ('+selkey+')', 
	  mysub.full_target_name(chakey), 
	  all_zonal_list[0][chakey][selkey], 
	  all_zonal_list[1][chakey][selkey], 
	  all_zonal_list[2][chakey][selkey],
	  all_global_list[0][chakey][selkey],
	  all_global_list[1][chakey][selkey],
	  all_global_list[2][chakey][selkey] )
	  
	# write2gfile
	if args.gfile != None:
	  # channel | date | time | mean | stdv | nobs
	  gformat = "%4s %10s %10s %12.6f %12.6f %12d\n"
	  
	  glm = np.ma.filled(all_global_list[0][chakey][selkey], fill_value)
	  gls = np.ma.filled(all_global_list[1][chakey][selkey], fill_value)
	  gln = np.ma.filled(all_global_list[2][chakey][selkey], fill_value)

	  if glm != fill_value and gls != fill_value:
	    line = gformat % (chakey, args.date, selkey, glm, gls, gln)
	    f.write(line)
      except KeyError:
	break

  # --- Write global statistics ---
  if args.gfile != None:
    f.close()


  # -------------------------------------------------------------------
  print ( "\n *** %s finished for %s and %s\n" 
    % (sys.argv[0], args.satellite, args.date) )
  # -------------------------------------------------------------------
