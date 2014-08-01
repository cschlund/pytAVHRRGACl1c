
#
# reading avhrrgac level 1c H5 files
# C. Schlundt, July 2014
#
# -------------------------------------------------------------------

import h5py
import os, sys
import numpy as np
import numpy.ma as ma
import subs_avhrrgac as mysub

#----------------------------------------------------------------------------
def show_properties(fil):
  
  print ("   + Available KEYS: ")

  groups = fil.keys()
  
  for item in groups:

    print ("   - %s" % item)
    g = fil['/'+item+'/']
    
    for key in g:
      print ("     * KEY=%s ---> %s" % (key, g[key]) )
      
      if key == 'data':
	print ("       + shape=%s, size=%s, dype=%s" % (g[key].shape, g[key].size, g[key].dtype) )
	
      if key == 'what' or key == 'how':
	for att in g[key].attrs.keys():
	  print ("       + ATT=%s ---> %s" % (att, g[key].attrs[att]) )

    for att in g.attrs.keys():
      print ("     * ATT=%s ---> %s" % (att, g.attrs[att]) )

    print
  
  print (" --- FILE: %s" % fil )
  
  
#----------------------------------------------------------------------------
def read_var(fil, varstr, ver):
  
  flg = False
  
  while flg == False:
    try:
      for key in fil.keys():
	g = fil['/'+key+'/']
	
	if key == varstr:
	  
	  flg = True 
	  add = fil[g.name+'/what']
	  #how = fil[g.name+'/how'] #not in sunsatangles h5
	  var = fil[g.name+'/data'].value

	  gain = add.attrs["gain"]
	  offs = add.attrs["offset"]
	  noda = add.attrs["nodata"]
	  miss = add.attrs["missingdata"]
	  name = add.attrs["dataset_name"]

	  mask_var = ma.masked_values(var, miss)
	  var = ( mask_var * gain ) +  offs
	    
	  #print (" + %s: %s" % (key, g.name) )
	  #print ("   - shape=%s, size=%s, dype=%s" % (dat.shape, dat.size, dat.dtype) )
	  #for att in g.attrs.keys():
	    #print ("   - gattr: %s -> %s" % (att, g.attrs[att]) )
	  #for att in add.attrs.keys():
	    #print ("   - aattr: %s -> %s" % (att, add.attrs[att]) )
	  #for att in how.attrs.keys():
	    #print ("   - hattr: %s -> %s" % (att, how.attrs[att]) )
	  break
	
	
	for key2 in g.keys():
	  
	  if key2 == varstr:
	    flg = True

	    add = fil[g[key2].name+'/what']
	    var = fil[g[key2].name+'/data'].value

	    gain = add.attrs["gain"]
	    offs = add.attrs["offset"]
	    noda = add.attrs["nodata"]
	    miss = add.attrs["missingdata"]
	    name = add.attrs["dataset_name"]
	    
	    mask_var = ma.masked_values(var, miss)
	    var = ( mask_var * gain ) +  offs
	    #var = ( var * gain ) +  offs
	    
	    #print (" + %s -> %s: %s" % (key, key2, g[key2].name) )
	    #print ("   - shape=%s, size=%s, dype=%s" % (dat.shape, dat.size, dat.dtype) )
	    #for att in g.attrs.keys():
	      #print ("   - gattr: %s -> %s" % (att, g.attrs[att]) )
	    #for att in add.attrs.keys():
	      #print ("   - aattr: %s -> %s" % (att, add.attrs[att]) )

	    
	    break

	    
    finally:
      if flg == False:
	print (" *** Cannot find %s variable in file ***" % varstr)
      else:
	if ver == True:
	  print ("   + Variable: %s, shape: %s, size: %s, type: %s , min: %s, max: %s" 
	  % (name, var.shape, var.size, var.dtype, var.min(), var.max() ) )
	return (var, name)


#----------------------------------------------------------------------------
def read_avhrrgac(f, a, tim, cha, ver):
  
  try:
    #if ver == True:
      #show_properties(f)
      #show_properties(a)

    sza, szanam = read_var(a, 'image1', ver)
    lat, latnam = read_var(f, 'lat', ver)
    lon, lonnam = read_var(f, 'lon', ver)

    
    if cha == 'ch1':
      tardat, tarname = read_var(f, 'image1', ver)
      tar = tardat / 100.
    if cha == 'ch2':
      tardat, tarname = read_var(f, 'image2', ver)
      tar = tardat / 100.
    if cha == 'ch3b':
      tar, tarname = read_var(f, 'image3', ver)
    if cha == 'ch4':
      tar, tarname = read_var(f, 'image4', ver)
    if cha == 'ch5':
      tar, tarname = read_var(f, 'image5', ver)
    if cha == 'ch3a':
      tardat, tarname = read_var(f, 'image6', ver)
      tar = tardat / 100.
      
    
    if tim == 'day':
      #consider only daytime, i.e. sza < 80
      lon = ma.masked_where(sza >= 80., lon)
      lat = ma.masked_where(sza >= 80., lat)
      tar = ma.masked_where(sza >= 80., tar)
    
    if tim == 'twilight':
      #consider only twilight, i.e. 80 <= sza < 90
      # mask everything outside the current sza range
      omask = ma.mask_or(sza <= 80, sza > 90)
      lon = ma.masked_where(omask, lon)
      lat = ma.masked_where(omask, lat)
      tar = ma.masked_where(omask, tar)
    
    if tim == 'night':
      #consider only night, i.e. sza >= 90
      lon = ma.masked_where(sza < 90., lon)
      lat = ma.masked_where(sza < 90., lat)
      tar = ma.masked_where(sza < 90., tar)


    parlst  = [latnam, lonnam, tarname]
    datlst  = [lat, lon, tar]

    if ver == True:
      cnt = 0
      print ("   + Masked Arrays: %s" % tim)
      for item2 in datlst:
	print ("   + Variable: %s, shape: %s, size: %s, type: %s , min: %s, max: %s" 
	% (parlst[cnt], item2.shape, item2.size, item2.dtype, item2.min(), item2.max() ))
	cnt += 1

    
    return (lat, lon, tar)
    
  except IndexError as ie:
    print "\n --- FAILED (IndexError): ", ie
    sys.exit(0)
    
  except ValueError as ve:
    print "\n --- FAILED (ValueError): ", ve
    sys.exit(0)
    
  except RuntimeError as re:
    print "\n --- FAILED (RuntimeError): ", re
    sys.exit(0)
    
  except Exception as e:
    print "\n --- FAILED (Exception): ", e
    sys.exit(0)
    