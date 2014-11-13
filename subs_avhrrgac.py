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
    """
    run_pystat_add2sqlite.py: check if table already exists.
    """

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
 
# --------------------------------------------------------------------
def calc_midnight(stime, etime): 
    """
    Check for midnight orbit & check if day has changed
    """

    if stime.day < etime.day: 
        
        # calculate how much time has passed 
        # between start time and midnight 

        midnight = datetime.datetime.strptime( 
                    str(etime.day*1000000), '%d%H%M%S')
        midnight = midnight + datetime.timedelta(microseconds=0)
        midnight_diff = midnight-stime
        
        # calculate the orbit line under the 
        # assumption of 2 scanlines/second

        midnight_diff_msec  = midnight_diff.seconds+\
                              midnight_diff.microseconds/1000000
        midnight_orbit_calc = midnight_diff_msec*2

    else:
        
        # set midnight variable to -1 if the day hasn't changed 
        midnight_orbit_calc = -1

    return midnight_orbit_calc

# --------------------------------------------------------------------
def calc_overlap(stime, etime):
    """
    GAC_overlap.py: calculation of overlapping scanlines
    """

    # time difference between 
    # start of next orbit and end of current orbit
    # assumption: 2 scanlines per second

    timediff      = etime - stime
    timediff_msec = timediff.days*24*60*60+timediff.seconds\
                    +timediff.microseconds/1000000
    overlap_rows  = timediff_msec*2

    return overlap_rows

# --------------------------------------------------------------------
def get_new_cols(): 
    """
    These columns will be added to already existing database
    providing information about overlapping scanlines and
    midnight orbit scanline
    [ GAC_overlap.py ]
    """

    new_cols = ["start_scanline_begcut", "end_scanline_begcut", 
                "start_scanline_endcut", "end_scanline_endcut",
                "midnight_orbit_scanline"]
    return new_cols

# --------------------------------------------------------------------
def update_db_without_midnight(vals, db):
    """
    GAC_overlap.py: update database
                    vals = [string, start_line, string, end_line,
                            stime, etime, satellite]
    """

    act = "UPDATE orbits SET " \
          "{0} = {1}, {2} = {3} WHERE blacklist=0 AND " \
          "start_time_l1c=\'{4}\' AND " \
          "end_time_l1c=\'{5}\' AND " \
          "sat=\'{6}\'".format(
                  vals[0], vals[1], vals[2], vals[3], vals[4],
                  vals[5], vals[6])

    #print ("    - without_midnight: %s" % act)
    db.execute(act)

# --------------------------------------------------------------------
def update_db_with_midnight(vals, db):
    """
    GAC_overlap.py: update database
                    vals = [string, start_line, string, end_line,
                            string, midnight_calc,
                            stime, etime, satellite]
    """

    act = "UPDATE orbits SET " \
          "{0} = {1}, {2} = {3}, {4} = {5} "\
          "WHERE blacklist=0 AND " \
          "start_time_l1c=\'{6}\' AND " \
          "end_time_l1c=\'{7}\' AND " \
          "sat=\'{8}\'".format(
                  vals[0], vals[1], vals[2], vals[3], vals[4],
                  vals[5], vals[6], vals[7], vals[8])

    # print ("    - with_midnight: %s" % act)
    db.execute(act)

# --------------------------------------------------------------------
def get_record_lists(satellite, db): 
    """
    GAC_overlap.py: get start, end and along track information
    from a sqlite3 database: table orbits
    """

    start_dates=[] 
    end_dates=[] 
    data_along=[] 

    get_data = "SELECT start_time_l1c, end_time_l1c, "\
               "along_scanline FROM orbits WHERE "\
               "sat=\'{satellite}\' ORDER BY "\
               "start_time_l1c".format(satellite=satellite)

    results = db.execute(get_data)

    for result in results: 
        if result['start_time_l1c'] != None: 
            start_dates.append(result['start_time_l1c']) 
            end_dates.append(result['end_time_l1c']) 
            data_along.append(result['along_scanline'])

    return(start_dates, end_dates, data_along)

# -------------------------------------------------------------------
def create_statistics_table(db):
    """
    run_pystat_add2sqlite.py: create statistics table.
    """

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
    """
    run_pystat_add2sqlite.py: add statistics to existing table
    """

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
    """
    run_pystat_add2sqlite.py: create new table.
    """

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
    """
    SATELLITE list: sqlite3 nomenclature
    """

    satellites = ['NOAA7', 'NOAA9', 'NOAA11', 'NOAA12', 
                  'NOAA14', 'NOAA15', 'NOAA16', 'NOAA17', 
                  'NOAA18', 'NOAA19', 'METOPA', 'METOPB']
    return satellites

# -------------------------------------------------------------------
def get_channel_list():
    """
    List of AVHRR GAC channels.
    """

    channels  = ['ch1', 'ch2', 'ch3b', 'ch4', 'ch5', 'ch3a']
    return channels

# -------------------------------------------------------------------
def get_select_list():
    """
    List of selected times in pystat calculation and 
    later for the routines which are plotting statistics.
    """

    selects  = ['day', 'night', 'twilight']
    return selects

# -------------------------------------------------------------------
def get_color_list():
    """
    List of colors used in the plotting routines.
    """

    colorlst = ['Red','DodgerBlue','DarkOrange','Lime',
		        'Navy','Magenta','DarkGreen','Turquoise',
		        'DarkMagenta','Sienna','Gold','Olive',
		        'MediumSlateBlue','DimGray']
            
# -------------------------------------------------------------------
def full_sat_name(sat):
    """
    List of satellite names occurring in L1b and L1c filenames,
    as well as in sqlite3 databases.
            name = for plotting routines (name on the PLOT)
            abbr = pygac nomenclature
            lite = sqlite3 nomenclature
    """

    m1_list = ["m01", "metopb", "metop01", "M1", "METOPB"]
    m2_list = ["m02", "metopa", "metop02", "M1", "METOPA"]
    nc_list = ["n07", "noaa7",  "noaa07",  "NC", "NOAA7"]
    nf_list = ["n09", "noaa9",  "noaa09",  "NF", "NOAA9"]
    ng_list = ["n10",           "noaa10",  "NG", "NOAA10"]
    nh_list = ["n11",           "noaa11",  "NH", "NOAA11"]
    nd_list = ["n12",           "noaa12",  "ND", "NOAA12"]
    nj_list = ["n14",           "noaa14",  "NJ", "NOAA14"]
    nk_list = ["n15",           "noaa15",  "NK", "NOAA15"]
    nl_list = ["n16",           "noaa16",  "NL", "NOAA16"]
    nm_list = ["n17",           "noaa17",  "NM", "NOAA17"]
    nn_list = ["n18",           "noaa18",  "NN", "NOAA18"]
    np_list = ["n19",           "noaa19",  "NP", "NOAA19"]

    if sat in m1_list:
        name = "MetOp-B"
        abbr = "metopb"	
        lite = "METOPB"	
      
    elif sat in m2_list:
        name = "MetOp-A"
        abbr = "metopa"
        lite = "METOPA"
      
    elif sat in nc_list:
        name = "NOAA-7"
        abbr = "noaa7"
        lite = "NOAA7"
      
    elif sat in nf_list:
        name = "NOAA-9"
        abbr = "noaa9"
        lite = "NOAA9"
      
    elif sat in ng_list:
        name = "NOAA-10"
        abbr = "noaa10"
        lite = "NOAA10"
      
    elif sat in nh_list:
        name = "NOAA-11"
        abbr = "noaa11"
        lite = "NOAA11"
      
    elif sat in nd_list:
        name = "NOAA-12"
        abbr = "noaa12"
        lite = "NOAA12"
      
    elif sat in nj_list:
        name = "NOAA-14"
        abbr = "noaa14"
        lite = "NOAA14"
      
    elif sat in nk_list:
        name = "NOAA-15"
        abbr = "noaa15"
        lite = "NOAA15"
      
    elif sat in nl_list:
        name = "NOAA-16"
        abbr = "noaa16"
        lite = "NOAA16"
      
    elif sat in nm_list:
        name = "NOAA-17"
        abbr = "noaa17"
        lite = "NOAA17"
      
    elif sat in nn_list:
        name = "NOAA-18"
        abbr = "noaa18"
        lite = "NOAA18"
      
    elif sat in np_list:
        name = "NOAA-19"
        abbr = "noaa19"
        lite = "NOAA19"
      
    else:
        message = "\n * The satellite name you've chosen is not "\
        "available in the current list!\n"
        sys.exit(message)
      
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

