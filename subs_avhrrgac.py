#
# subroutines for plotting AVHRR GAC L1c data
# C. Schlundt, May 2014
# July 2014: cal_zonal_means, plt_zonal_means
# -------------------------------------------------------------------

import os, sys, fnmatch, datetime
import string
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import host_subplot
import mpl_toolkits.axisartist as AA
from matplotlib import gridspec
  
# -------------------------------------------------------------------
def split_filename(fil):
    dirname  = os.path.dirname(fil)  
    basename = os.path.basename(fil)
    basefile = os.path.splitext(basename)
    return basefile[0].split('_')
  
# -------------------------------------------------------------------
def find(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result
    
# -------------------------------------------------------------------
def check_if_table_exists(cursor, tablename):
    check = "select count(*) from sqlite_master "\
            "where name = \'{0}\' ".format(tablename)
    cursor.execute(check)
    res = cursor.fetchone()
    return res['count(*)']

# -------------------------------------------------------------------
def dict_factory(cursor, row):
    """
    Organize database queries by column names.
    """
    d = dict()
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d
 
# -------------------------------------------------------------------
def create_statistics_table(db):
    act = "CREATE TABLE IF NOT EXISTS statistics ( "\
          "satelliteID INTEGER, date DATE, "\
          "channelID INTEGER, selectID INTEGER, "\
          "FOREIGN KEY (satelliteID) REFERENCES satellites (id), "\
          "FOREIGN KEY (channelID) REFERENCES channels (id), "\
          "FOREIGN KEY (selectID) REFERENCES selects (id), "\
          "PRIMARY KEY (satelliteID, date, channelID, selectID) )"
    db.execute(act)

# -------------------------------------------------------------------
def alter_statistics_table(db, belts):
    glob_list = ('GlobalMean', 'GlobalStdv', 'GlobalNobs')
    zona_list = ('ZonalMean', 'ZonalStdv', 'ZonalNobs')
    type_list = ('FLOAT', 'FLOAT', 'INTEGER')

    for glo, typ in zip(glob_list, type_list):
        act = "ALTER TABLE statistics ADD COLUMN "\
              "{0} {1}".format(glo, typ)
        db.execute(act)

    for zon, typ in zip(zona_list, type_list):
        for idx,lat in enumerate(belts):
            bel = zon+str(idx)
            act = "ALTER TABLE statistics ADD COLUMN "\
                  "{0} {1}".format(bel, typ)
            db.execute(act)

# -------------------------------------------------------------------
def create_id_name_table(db, table, lst):
    if table is 'latitudes':
        act = "CREATE TABLE {0} "\
              "(id INTEGER PRIMARY KEY, belt FLOAT)".format(table)
        db.execute(act)
        for position, item in enumerate (lst):
          act = "INSERT OR ABORT INTO {0} "\
                "VALUES ({1}, {2})".format(table, position, item)
          db.execute(act)
    else:
        act = "CREATE TABLE {0} "\
              "(id INTEGER PRIMARY KEY, name TEXT)".format(table)
        db.execute(act)
        for position, item in enumerate (lst):
          act = "INSERT OR ABORT INTO {0} "\
                "VALUES ({1}, \'{2}\')".format(table, position, item)
          db.execute(act)

# -------------------------------------------------------------------
def get_satellite_list():
    satellites = ['NOAA7', 'NOAA9', 'NOAA11', 'NOAA12', 
                  'NOAA14', 'NOAA15', 'NOAA16', 'NOAA17', 
                  'NOAA18', 'NOAA19', 'METOPA', 'METOPB']
    return satellites

# -------------------------------------------------------------------
def get_channel_list():
    channels  = ['ch1', 'ch2', 'ch3b', 'ch4', 'ch5', 'ch3a']
    return channels

# -------------------------------------------------------------------
def get_select_list():
    selects  = ['day', 'night', 'twilight']
    return selects

# -------------------------------------------------------------------
def get_color_list():
    colorlst = ['Red','DodgerBlue','DarkOrange','Lime',
		'Navy','Magenta','DarkGreen','Turquoise',
		'DarkMagenta','Sienna','Gold','Olive',
		'MediumSlateBlue','DimGray']
            
# -------------------------------------------------------------------
def full_sat_name(sat):
    if sat == 'm01' or sat == 'metop01' or sat == 'M1' or sat == 'METOPB':
        name = "MetOp-1"	# plotting name
        abbr = "metop01"	# pygac output name
        lite = "METOPB"	# AVHRR GAC sqlite3 db name
      
    elif sat == 'm02' or sat == 'metop02' or sat == 'M2' or sat == 'METOPA':
        name = "MetOp-2"
        abbr = "metop02"
        lite = "METOPA"
      
    elif sat == 'n07' or sat == 'noaa07' or sat == 'NC' or sat == 'NOAA7':
        name = "NOAA-7"
        abbr = "noaa07"
        lite = "NOAA7"
      
    elif sat == 'n09' or sat == 'noaa09' or sat == 'NF' or sat == 'NOAA9':
        name = "NOAA-9"
        abbr = "noaa09"
        lite = "NOAA9"
      
    elif sat == 'n10' or sat == 'noaa10' or sat == 'NG' or sat == 'NOAA10':
        name = "NOAA-10"
        abbr = "noaa10"
        lite = "NOAA10"
      
    elif sat == 'n11' or sat == 'noaa11' or sat == 'NH' or sat == 'NOAA11':
        name = "NOAA-11"
        abbr = "noaa11"
        lite = "NOAA11"
      
    elif sat == 'n12' or sat == 'noaa12' or sat == 'ND' or sat == 'NOAA12':
        name = "NOAA-12"
        abbr = "noaa12"
        lite = "NOAA12"
      
    elif sat == 'n14' or sat == 'noaa14' or sat == 'NJ' or sat == 'NOAA14':
        name = "NOAA-14"
        abbr = "noaa14"
        lite = "NOAA14"
      
    elif sat == 'n15' or sat == 'noaa15' or sat == 'NK' or sat == 'NOAA15':
        name = "NOAA-15"
        abbr = "noaa15"
        lite = "NOAA15"
      
    elif sat == 'n16' or sat == 'noaa16' or sat == 'NL' or sat == 'NOAA16':
        name = "NOAA-16"
        abbr = "noaa16"
        lite = "NOAA16"
      
    elif sat == 'n17' or sat == 'noaa17' or sat == 'NM' or sat == 'NOAA17':
        name = "NOAA-17"
        abbr = "noaa17"
        lite = "NOAA17"
      
    elif sat == 'n18' or sat == 'noaa18' or sat == 'NN' or sat == 'NOAA18':
        name = "NOAA-18"
        abbr = "noaa18"
        lite = "NOAA18"
      
    elif sat == 'n19' or sat == 'noaa19' or sat == 'NP' or sat == 'NOAA19':
        name = "NOAA-19"
        abbr = "noaa19"
        lite = "NOAA19"
      
    else:
        print "\n * The satellite name you've chosen is not "\
        "available in the current list!\n"
        exit(0)
      
    return(name, abbr, lite)
  
# -------------------------------------------------------------------
def full_cha_name(target):
    if target == 'rf1' or target == 'ch1':
        name = "Channel 1 reflectance"
    elif target == 'rf2' or target == 'ch2':
        name = "Channel 2 reflectance"
    elif target == 'rf3' or target == 'ch3a':
        name = "Channel 3a reflectance"
    elif target == 'bt3' or target == 'ch3b':
        name = "Channel 3b brightness temperature [K]"
    elif target == 'bt4' or target == 'ch4':
        name = "Channel 4 brightness temperature [K]"
    elif target == 'bt5' or target == 'ch5':
        name = "Channel 5 brightness temperature [K]"
    else:
        print "\n * Wrong target name! see help message !\n"
        exit(0)
    return(name)

# -------------------------------------------------------------------
def datestring(dstr):
    if '-' in dstr:
        correct_date_string = string.replace(dstr,'-','')
    elif '_' in dstr:
        correct_date_string = string.replace(dstr,'_','')
    elif '/' in dstr:
        correct_date_string = string.replace(dstr,'/','')
    else:
        correct_date_string = dstr
    return correct_date_string
  
# -------------------------------------------------------------------
def satstring(sstr):
    return full_sat_name(sstr)[1]
  
# -------------------------------------------------------------------
# calculate zonal means of input (i.e. orbit)
# S. Finkensieper, July 2014
def cal_zonal_means(lat, tar, zone_size):
  
    # define latitudinal zone size :
    zone_rad = zone_size/2.0

    # determine zone centers:
    zone_centers = np.arange(-90 + zone_rad, 90 + zone_rad, zone_size)
    nzones = len(zone_centers)

    # initialize array holding zonal means and the number of observations:
    zonal_means = np.ma.zeros(nzones)
    zonal_stdev = np.ma.zeros(nzones)
    nobs = np.ma.zeros(nzones)

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

        # save results:
        zonal_means[izone] = zonal_mean
        zonal_stdev[izone] = zonal_stdv
        nobs[izone] = n

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
        zone_centers, zone_size, ofil_name, 
        date_str, chan_str, plat_str):

    zon_mask = np.ma.equal(zonal_mean, 0.)
    
    avearr = np.ma.masked_where(zon_mask, zonal_mean)
    devarr = np.ma.masked_where(zon_mask, zonal_stdv)
    cntarr = np.ma.masked_where(zon_mask, zonal_nobs)
    belarr = np.ma.masked_where(zon_mask, zone_centers)
    
    xlabel = 'Latitude using zone size of {0} degrees'.format(zone_size)
    mean_label = 'Zonal Mean'
    stdv_label = 'Zonal Standard Deviation'
    
    if np.ma.count(avearr) == 0:
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
    ax_rec.plot(belarr, cntarr, color='black', linewidth=2, 
            label=format(allcnt)+' records')
    ax_rec.set_ylabel('# of Observations')
    ax_rec.set_xlabel(xlabel)
    ax_rec.grid()

    # set axes range:
    ax_rec.set_ylim(0, 1.1*maxcnt)

    # plot legend for mean & stdv
    m = plt.Line2D((0,1), (1,1), c='red', lw=2)
    s = plt.Rectangle((0,0), 1, 1, fc='SkyBlue')
    leg = ax_val.legend([m, s], [mean_label, stdv_label], 
            loc='best', fancybox=True)
    leg.get_frame().set_alpha(0.5)
    
    # plot legend for observations
    leg2 = ax_rec.legend(loc='upper center', fancybox=True)
    leg.get_frame().set_alpha(0.5)
    
    # ensure 'tight layout' (prevents the axes labels from being 
    # placed outside the figure):
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

# -------------------------------------------------------------------
# read Global_statistics_AVHRRGACl1c_*.txt
# Global statistics for AVHRR GAC on NOAA-15
# channel | date | time | mean | stdv | nobs
def read_globstafile(fil,cha,sel):

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
    
    for ll in lines: 
        line = ll.strip('\n')
      
        if '#' in line:
            continue
        if '-9999.0000' in line:
            continue
        
        string = line.split( )
        
        if string[0] == cha: 
            if string[2] == sel: 
                lstar.append(string[0])
                date = datetime.datetime.strptime(string[1], '%Y%m%d').date()
                lsdat.append(date)
                lstim.append(string[2])
                lsave.append(float(string[3]))
                lsstd.append(float(string[4]))
                lsrec.append(int(string[5]))
      
    return (lstar,lsdat,lstim,lsave,lsstd,lsrec)

  
# -------------------------------------------------------------------
def set_fillvalue(fill_value, zonal_mean, zonal_stdv, zonal_nobs, 
        global_mean, global_stdv, global_nobs):
  
    # -- set bad values to fill_value
    if global_nobs == 0: 
        glm = fill_value
        gls = fill_value
        gln = fill_value
    else:
        glm = global_mean
        gls = global_stdv
        gln = global_nobs

    # -- set bad values to fill_value
    mean = np.ma.filled(zonal_mean, fill_value)
    stdv = np.ma.filled(zonal_stdv, fill_value)
    nobs = np.ma.filled(zonal_nobs, fill_value)
    
    return(np.asscalar(glm), np.asscalar(gls), 
           np.asscalar(gln.astype(int)), 
           mean, stdv, nobs.astype(int))

# -------------------------------------------------------------------

