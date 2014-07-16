#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# how to use the script: 
#   > python script.py -h
#
# C.Schlundt: July, 2014
# S.Finkensieper: July, 2014: INIT plot
#
# -------------------------------------------------------------------

from mpl_toolkits.basemap import Basemap, cm
import numpy as np
import matplotlib.pyplot as plt
import regionslist as rl
import h5py
#from netCDF4 import Dataset as NetCDFFile
import os, sys, getopt
import argparse
import subs_avhrrgac as mysub
import read_avhrrgac_h5 as rh5


avail = sorted(rl.REGIONS.keys())
defin = ', '.join(map(str, avail))

parser = argparse.ArgumentParser(description='''This script 
displays one AVHRR GAC Level 1c orbit processed in the framework 
of cloud_cci (gyGAC).''')

parser.add_argument('-c', '--channel', help='Available channels are: ch1/ch2/ch3b/ch4/ch5/ch3a', required=True)
parser.add_argument('-f', '--filename', help='/path/to/ECC_GAC_file.h5', required=True)
parser.add_argument('-r', '--region', help=defin, default='glo')
parser.add_argument('-t', '--time', help='day (sza < 80) / night (sza >= 90) / twilight / all (default)', default='all')
parser.add_argument('-v', '--verbose', help='increase output verbosity', action="store_true")

args = parser.parse_args()

if args.verbose == True:
  print ("\n *** Parameter passed" )
  print (" ---------------------- ")
  print ("   - Channel  : %s" % mysub.full_target_name(args.channel))
  print ("   - Filename : %s" % args.filename)
  print ("   - Region   : %s" % args.region)
  print ("   - Time     : %s\n" % args.time)


basfil = os.path.basename(args.filename)
bastxt = os.path.splitext(basfil)[0]
outdir = './OUT/'
outfil = bastxt+'_'+args.channel+'_'+args.region+'_'+args.time+'.png'
outtit = "AVHRR GAC L1c - "+rl.REGIONS[args.region]["nam"]+" ("+args.time+")\n\n"

#print rl.REGIONS[args.region]["nam"]
#print rl.REGIONS[args.region]["geo"]
#for key,val in rl.REGIONS[args.region].items():
  #print key, "=>", val

# -------------------------------------------------------------------
# SPLIT infile in order to find the corresp. sunsatangles file
istrlst = mysub.split_filename(args.filename)
datelst = []

for item in istrlst:
  if 'met' in item or 'noa' in item:
    platform = item
    satlabel = mysub.full_sat_name(item)
  if 'T' in item or 'Z' in item:
    datelst.append(item)

# -------------------------------------------------------------------
# READ H5 input

#search for corresponding sunsatangles file
dirf = os.path.dirname(args.filename)
basf = os.path.basename(args.filename)
patt = '*sunsatangles*'+platform+'*'+datelst[0]+'*'
afil = mysub.find(patt, dirf)[0]

# open H5 files
f = h5py.File(args.filename, "r+")
a = h5py.File(afil, "r+")

#if ver == True:
#rh5.show_properties(f)
#rh5.show_properties(a)
  
# get data
(lat, lon, tar) = rh5.read_avhrrgac(f, a, args.time,
		  args.channel, args.verbose)

# close H5 files
a.close()
f.close()

# -------------------------------------------------------------------
# INIT plot

# initialize figure
fig = plt.figure()
ax  = fig.add_subplot(111)

# create basemap
m = Basemap(**rl.REGIONS[args.region]["geo"])
m.drawcoastlines(linewidth=0.25)
m.drawcountries(linewidth=0.25)
#m.fillcontinents(color='grey',lake_color='lightblue')
#m.drawmapboundary(fill_color='lightblue')

#m.drawlsmask(land_color='grey', ocean_color='lightblue', lakes=True)
lons = np.arange(*rl.REGIONS[args.region]["mer"])
lats = np.arange(*rl.REGIONS[args.region]["par"])

# draw parallels and meridians.
m.drawparallels(lats, labels=[True, False, False, False])
m.drawmeridians(lons, labels=[False, False, True, True])

# Split dataset west-east at the prime meridian in order to avoid misplaced
# polygons produced by pcolor when lon crosses the dateline (i.e. jumps from
# 180 to -180 or vice versa). Use 5 degrees of overlap to avoid polygon gaps
# at lon=0.
wmask = lon > 5     # cut all values where lon > 5
emask = lon < -5    # cut all values where lon < -5

for mask in (wmask, emask):
    # mask lat, lon & data arrays:
    mlon = np.ma.masked_where(mask, lon)
    mlat = np.ma.masked_where(mask, lat)
    mtar = np.ma.masked_where(mask, tar)

    # find x,y values of map projection grid:
    x, y = m(mlon, mlat)

    # Plot data. Note the vmin and vmax arguments. They have to be identical
    # for both the east- and west-plot in order to assure an identical colorbar
    # scaling. Here, vmin & vmax are set to the global minimum and maximum of
    # the data, respectively.
    #pcolor = m.pcolor(x, y, mtar, cmap='jet', vmin=0.0, vmax=1.0)
    pcolor = m.pcolor(x, y, mtar, cmap='jet', vmin=np.min(tar), vmax=np.max(tar))

# add colorbar with units:
cbar = m.colorbar(pcolor)
cbar.set_label(mysub.full_target_name(args.channel))

# add title:
ax.set_title(outtit)

# save to file:
fig.savefig(outdir + outfil, bbox_inches='tight')
plt.close()

# -------------------------------------------------------------------
print ( "\n *** %s finished for \n     %s\n" % 
((sys.argv[0]), outdir+outfil) )
# -------------------------------------------------------------------
