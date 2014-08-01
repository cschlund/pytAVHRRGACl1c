
#
# subroutines for plotting AVHRR GAC L1c data
# C. Schlundt, May 2014
# July 2014: cal_zonal_means, plt_zonal_means
# -------------------------------------------------------------------

import os, sys, fnmatch
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import host_subplot
import mpl_toolkits.axisartist as AA
from matplotlib import gridspec


def split_filename(fil):
  dirname  = os.path.dirname(fil)  
  basename = os.path.basename(fil)
  basefile = os.path.splitext(basename)
  return basefile[0].split('_')
  
  

def find(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result
    
    

def full_sat_name(sat):
  if sat == 'm02' or sat == 'metop02':
    name = "MetOp-2"
    abbr = "metop02"
  if sat == 'n07' or sat == 'noaa07':
    name = "NOAA-7"
    abbr = "noaa7"
  if sat == 'n09' or sat == 'noaa09':
    name = "NOAA-9"
    abbr = "noaa9"
  if sat == 'n10' or sat == 'noaa10':
    name = "NOAA-10"
    abbr = "noaa10"
  if sat == 'n11' or sat == 'noaa11':
    name = "NOAA-11"
    abbr = "noaa11"
  if sat == 'n12' or sat == 'noaa12':
    name = "NOAA-12"
    abbr = "noaa12"
  if sat == 'n14' or sat == 'noaa14':
    name = "NOAA-14"
    abbr = "noaa14"
  if sat == 'n15' or sat == 'noaa15':
    name = "NOAA-15"
    abbr = "noaa15"
  if sat == 'n16' or sat == 'noaa16':
    name = "NOAA-16"
    abbr = "noaa16"
  if sat == 'n17' or sat == 'noaa17':
    name = "NOAA-17"
    abbr = "noaa17"
  if sat == 'n18' or sat == 'noaa18':
    name = "NOAA-18"
    abbr = "noaa18"
  if sat == 'n19' or sat == 'noaa19':
    name = "NOAA-19"
    abbr = "noaa19"
  return(name, abbr)
  
# -------------------------------------------------------------------

def full_target_name(target):
  if target == 'rf1' or target == 'ch1':
    name = "Channel 1 reflectance"
  if target == 'rf2' or target == 'ch2':
    name = "Channel 2 reflectance"
  if target == 'rf3' or target == 'ch3a':
    name = "Channel 3a reflectance"
  if target == 'bt3' or target == 'ch3b':
    name = "Channel 3b brightness temperature [K]"
  if target == 'bt4' or target == 'ch4':
    name = "Channel 4 brightness temperature [K]"
  if target == 'bt5' or target == 'ch5':
    name = "Channel 5 brightness temperature [K]"
  return(name)

# -------------------------------------------------------------------

# STA/m02/Zonal_20070115_20121215_avhrr_m02_L1c_esa_cci_c_20121215_rf3.sta
def split_sta_filename(ifile):
  
  if "/" in ifile:
    list_of_strings = ifile.split('/')
    filename = list_of_strings[len(list_of_strings)-1]
    
    if filename.endswith('.sta'):
      print ' *', filename
    else:
      print " ! Wrong input file: <*.sta>\n"
      return None

  else:
    filename = ifile
    
    if filename.endswith('.sta'):
      print ' *', filename
    else:
      print " ! Wrong input file: <*.sta>\n"
      return None

  #['Zonal', '20090115', '20091215', 'avhrr', 'metop02', 'L1c', 'cmsaf', '20090115', 'rf1.sta']
  #['Zonal', '20070115', '20121215', 'avhrr', 'm02', 'L1c', 'esa', 'cci', 'c', '20080115', 'rf1.sta']

  lastele = filename.split('_')[len(filename.split('_'))-1]
  datestr = filename.split('_')[len(filename.split('_'))-2]
  targstr = lastele.split('.')[0]
  satestr = filename.split('_')[4]
  noaastr = full_sat_name(satestr)[1]
  
  if 'cmsaf' in filename:
    print ' * CMSAF:', targstr, datestr, noaastr
    return('cmsaf_pps', targstr, datestr, noaastr, satestr)
  else:
    print ' * ESA Cloud_cci:', targstr, datestr, noaastr
    return('cloud_cci', targstr, datestr, noaastr, satestr)
  

# -------------------------------------------------------------------
# calculate zonal means of input (i.e. orbit)
# S. Finkensieper, July 2014
def cal_zonal_means(lat, tar, zone_size):
  
  # define latitudinal zone size :
  #zone_size = 5
  zone_rad = zone_size/2.0

  # determine zone centers:
  zone_centers = np.arange(-90 + zone_rad, 90 + zone_rad, zone_size)
  #print np.sort(lat.compressed())
  nzones = len(zone_centers)

  # initialize array holding zonal means and the number of observations:
  zonal_means = np.ma.zeros(nzones)
  zonal_stdev = np.ma.zeros(nzones)
  nobs = np.ma.zeros(nzones)

  # 20140711 ----
  # calculate zonal means:
  for zone_center, izone in zip(zone_centers, range(nzones)):
    
      # mask everything outside the current zone:
      if izone == 0:
	  # include left boundary for the first interval
	  zonal_mask = np.ma.mask_or(lat < (zone_center - zone_rad),
				    lat > (zone_center + zone_rad))
      else:
	  # exclude left boundary for all remaining intervals:
	  zonal_mask = np.ma.mask_or(lat <= (zone_center - zone_rad),
				    lat > (zone_center + zone_rad))

      # cut data:
      zonal_data = np.ma.masked_where(zonal_mask, tar)

      # calculate mean:
      zonal_mean = zonal_data.mean(dtype=np.float64)
      
      # calculate standard deviation
      zonal_stdv = zonal_data.std(dtype=np.float64)

      # get number of observations used to calculate this mean:
      n = np.ma.count(zonal_data)

      #print izone, zonal_mean, n
	
      # save results:
      zonal_means[izone] = zonal_mean
      zonal_stdev[izone] = zonal_stdv
      nobs[izone] = n

  # double check number of observations:
  #print "   * Number of observations in all zones together: {0}".format(sum(nobs))
  #print "   * Total number of datapoints: {0}".format(np.ma.count(tar))
  #print tar.mean(dtype=np.float64), zonal_means.mean(dtype=np.float64)
  
  if nobs.sum() != np.ma.count(tar):
    print ( "\n --- FAILED: Something went wrong in def cal_zonal_means(lat, tar, zone_size): %s != %s \n" 
    % (int(nobs.sum()), np.ma.count(tar)) )
    sys.exit(0)

  return (zonal_means, zonal_stdev, nobs)
  
  
# -------------------------------------------------------------------
# plot global, zonal means
# S. Finkensieper, July 2014
def plt_zonal_means(zonal_mean, zonal_nobs, global_mean, 
  zone_size, ofil_name, date_str, chan_str, plat_str):
  
  glo_mask = np.ma.equal(global_mean, 0.)
  zon_mask = np.ma.equal(zonal_mean, 0.)
  
  glm = np.ma.masked_where(glo_mask, global_mean)
  
  zonal_means = np.ma.masked_where(zon_mask, zonal_mean)
  nobs = np.ma.masked_where(zon_mask, zonal_nobs)

  if np.ma.count(zonal_means) == 0:
    #print "        - No measurements! "
    return 
  
  # define latitudinal zone size :
  zone_rad = zone_size/2.0

  # determine zone centers:
  zone_centers = np.arange(-90 + zone_rad, 90 + zone_rad, zone_size)
  nzones = len(zone_centers)

  # create one host axes object and one child axes object which share their
  # x-axis, but have individual y-axes:
  ax_means = host_subplot(111, axes_class=AA.Axes)
  ax_nobs = ax_means.twinx()

  # plot zonal mean into the host axes:
  width = zone_rad*0.5
  ax_means.bar(zone_centers, zonal_means, 
	      label='Zonal Mean using lat. zone size of {0} degrees'.format(zone_size),
	      width=width, color='DarkOrange')

  # plot number of observations into the child axes:
  ax_nobs.bar(zone_centers + width, nobs, 
	      label='# of Observations (total: '+format(int(nobs.sum()))+')',
	      width=width, color='g')

  # plot global mean on top of them:
  ax_means.plot(zone_centers, np.ma.ones(nzones)*glm, 'b--', lw=2.0,
		label='Global Mean: '+format('%.4f' % glm))

  # set axes labels:
  ax_means.set_ylabel('Zonal Mean of '+chan_str)
  ax_nobs.set_ylabel('# of Observations of AVHRR GAC / '+plat_str+' for '+date_str)
  ax_means.set_xlabel('Latitudinal Zone Center [degrees]')

  # set axes range:
  ax_means.set_ylim(0, 1.2*np.ma.max(zonal_means))
  ax_nobs.set_ylim(0, 1.2*np.ma.max(nobs))

  # add title & legend:
  #plt.title('Zonal means using a latitudinal zone size of {0} degrees'
	    #.format(zone_size))
  plt.legend(loc='upper center')

  # ensure 'tight layout' (prevents the axes labels from being placed outside
  # the figure):
  plt.tight_layout()

  # save figure to file:
  #plt.savefig('zonal_means.png', bbox_inches='tight')
  with np.errstate(all='ignore'):
    plt.savefig(ofil_name)
    plt.close()
  #return
  

# -------------------------------------------------------------------
# meine plotting routine
def plt_zonal_mean_stdv(zonal_mean, zonal_stdv, zonal_nobs, 
  zone_centers, zone_size, ofil_name, date_str, chan_str, plat_str):

  zon_mask = np.ma.equal(zonal_mean, 0.)
  
  avearr = np.ma.masked_where(zon_mask, zonal_mean)
  devarr = np.ma.masked_where(zon_mask, zonal_stdv)
  cntarr = np.ma.masked_where(zon_mask, zonal_nobs)
  belarr = np.ma.masked_where(zon_mask, zone_centers)
  
  xlabel = 'Latitude using zone size of {0} degrees'.format(zone_size)
  mean_label = 'Zonal Mean'
  stdv_label = 'Zonal Standard Deviation'
  
  if np.ma.count(avearr) == 0:
    #print "        - No measurements! "
    return 
  
  fig = plt.figure()
  gs = gridspec.GridSpec(2, 1, height_ratios=[3,1])
  ax_val = fig.add_subplot(gs[0])
  ax_rec = fig.add_subplot(gs[1])

  y1 = avearr + devarr
  y2 = avearr - devarr
  
  allcnt = int(zonal_nobs.sum())
  maxcnt = int(zonal_nobs.max())

  # plot zonal mean & stdv
  ax_val.plot(belarr, avearr, 'o', color='red')
  ax_val.plot(belarr, avearr, color='red', linewidth=2)
  ax_val.fill_between(belarr, y1, y2, facecolor='SkyBlue', alpha=0.5)
  ax_val.set_title('AVHRR GAC / '+plat_str+' for '+date_str)
  ax_val.set_ylabel('Zonal Mean of '+chan_str)
  ax_val.grid()

  # plot number of observations / lat. zone
  ax_rec.plot(belarr, cntarr, 'o', color='black')
  ax_rec.plot(belarr, cntarr, color='black', linewidth=2, label=format(allcnt)+' records')
  ax_rec.set_ylabel('# of Observations')
  ax_rec.set_xlabel(xlabel)
  ax_rec.grid()

  # set axes range:
  ax_rec.set_ylim(0, 1.1*maxcnt)

  # plot legend for mean & stdv
  m = plt.Line2D((0,1), (1,1), c='red', lw=2)
  s = plt.Rectangle((0,0), 1, 1, fc='SkyBlue')
  leg = ax_val.legend([m, s], [mean_label, stdv_label], loc='best', fancybox=True)
  leg.get_frame().set_alpha(0.5)
  
  # plot legend for observations
  leg2 = ax_rec.legend(loc='upper center', fancybox=True)
  leg.get_frame().set_alpha(0.5)
  
  # ensure 'tight layout' (prevents the axes labels from being placed outside
  # the figure):
  plt.tight_layout()
  
  plt.savefig(ofil_name)
  plt.close()

  return
  

# -------------------------------------------------------------------
# write zonal/global output
def write_zonal_means(ofil, zones, fill_value, pstr, dstr, cstr, 
  zonal_mean, zonal_stdv, zonal_nobs, 
  global_mean, global_stdv, global_nobs):
  
  glo_mask = np.ma.equal(global_mean, 0.)
  zon_mask = np.ma.equal(zonal_mean, 0.)
  
  glm = np.ma.masked_where(glo_mask, global_mean)
  gls = np.ma.masked_where(glo_mask, global_stdv)
  gln = np.ma.masked_where(glo_mask, global_nobs)
  
  glm = np.ma.filled(glm, fill_value)
  gls = np.ma.filled(gls, fill_value)
  gln = np.ma.filled(gln, fill_value)
  
  mean = np.ma.masked_where(zon_mask, zonal_mean)
  stdv = np.ma.masked_where(zon_mask, zonal_stdv)
  nobs = np.ma.masked_where(zon_mask, zonal_nobs)
  
  mean = np.ma.filled(mean, fill_value)
  stdv = np.ma.filled(stdv, fill_value)
  nobs = np.ma.filled(nobs, fill_value)
  
  if glm == fill_value:
    return
  
  obj = open(ofil, mode="w")
  
  hlin1 = '# Global statistics of '+cstr+' for '+dstr+' on '+pstr+'\n'
  hlin2 = '# mean | stdv | nobs \n'
  
  obj.write(hlin1)
  obj.write(hlin2)
  
  line = "%f %f %d\n" % (glm, gls, gln)
  obj.write(line)
  
  hlin3 = '# Zonal statistics of '+cstr+' for '+dstr+' on '+pstr+'\n'
  hlin4 = '# mean | stdv | lat.center | nobs \n'
  
  obj.write(hlin3)
  obj.write(hlin4)
  
  nitems = len(mean)
  
  for ix in range(len(mean)):
    line = "%f %f %f %d\n" % (mean[ix], stdv[ix], zones[ix], nobs[ix])
    obj.write(line)
    
  obj.close()
