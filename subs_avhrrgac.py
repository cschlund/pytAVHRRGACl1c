#
# subroutines for pystat, sqlite updating/writing/creating
#

import os
import sys
import fnmatch
import datetime
import string
import time
import numpy as np
import logging
import calendar
from dateutil.rrule import rrule, MONTHLY, DAILY
from math import floor
from datetime import timedelta

logger = logging.getLogger('root')


class ColumnError(Exception):
    pass


class NegativeScanline(Exception):
    pass


def str2upper(string_object):
    return string_object.upper()


def datestring(dstr):
    """
    Convert date string containing '-' or '_' or '/'
    into date string without any character.
    """
    if '-' in dstr:
        return string.replace(dstr, '-', '')
    elif '_' in dstr:
        return string.replace(dstr, '_', '')
    elif '/' in dstr:
        return string.replace(dstr, '/', '')
    else:
        return dstr


def date2str(dateobject):
    """
    Create a date string from a given datetime.date object.
    """
    return dateobject.strftime("%Y%m%d")


def str2date(datestr):
    """
    Create a datetime.date object from a given datestring.
    """
    return datetime.datetime.strptime(datestr, '%Y%m%d').date()


def get_satellite_list():
    """
    SATELLITE list: sqlite3 nomenclature
    """
    return ['TIROSN', 'NOAA6',  'NOAA7',  'NOAA8',  
            'NOAA9',  'NOAA10', 'NOAA11', 'NOAA12', 'NOAA14', 
            'NOAA15', 'NOAA16', 'NOAA17', 'NOAA18', 
            'NOAA19', 'METOPA', 'METOPB']


def get_cci_satellite_list():
    """
    SATELLITE list: sqlite3 nomenclature
    """
    return ['ERS-2', 'ENVISAT', 'TERRA', 'AQUA',
            'NOAA7',  'NOAA9',  'NOAA11', 'NOAA12', 'NOAA14', 
            'NOAA15', 'NOAA16', 'NOAA17', 'NOAA18', 
            'NOAA19', 'METOPA']


def get_constant_ects(satellite):
    """
    Stable equator crossing times for TERRA, AQUA, ENVISAT, ERS-2
    return tuple (ect_hour, ect_minute)
    """
    if satellite == "ENVISAT":
        return (10, 00)
    elif satellite == "TERRA":
        return (10, 30)
    elif satellite == "ERS-2":
        return (10, 30)
    elif satellite == "AQUA":
        return (13, 30)
    else:
        logger.info("{0} not defined in \'get_constant_ects\'".format(satellite))
        return (0, 0)


def get_channel_list():
    """
    List of AVHRR GAC channels.
    """
    return ['ch1', 'ch2', 'ch3b', 'ch4', 'ch5', 'ch3a']


def get_select_list():
    """
    List of selected times in pystat calculation and
    later for the routines which are plotting statistics.
    """
    return ['day', 'night', 'twilight']


def get_pystat_select_list():
    """
    List of selected times in pystat calculation and
    later for the routines which are plotting statistics.
    """
    return ['day_90sza', 'day', 'night', 'twilight']


def get_color_list():
    """
    List of colors used in the plotting routines.
    """
    return ['Magenta', 'DodgerBlue', 'DarkOrange', 'Lime',
            'Sienna', 'Red', 'DarkGreen', 'Turquoise',
            'DarkMagenta', 'Navy', 'Gold', 'Olive',
            'MediumSlateBlue', 'DimGray']


def plot_satstring(sstr):
    return full_sat_name(sstr)[0]


def pygac_satstring(sstr):
    return full_sat_name(sstr)[1]


def lite_satstring(sstr):
    return full_sat_name(sstr)[2]


def color_satstring(sstr):
    return full_sat_name(sstr)[3]


def color_satstring_cci(sstr):
    return full_sat_name(sstr)[4]


def full_sat_name(sat):
    """
    List of satellite names occurring in L1b and L1c filenames,
    as well as in sqlite3 databases.
    0. = for plotting routines (name on the PLOT)
    1. = pygac nomenclature
    2. = sqlite3 nomenclature
    3. = color for plotting
    4. = color for Cloud_cci datasets: 
         AVHRR-AM/PM, MODIS-Terra/Aqua, ATSR-2+AATSR, AATSR+MERIS
    """
    tn_list = ["n05", "noaa5", "noaa05", "TN", "NOAA5", "TIROSN"]
    na_list = ["n06", "noaa6", "noaa06", "NA", "NOAA6"]
    nc_list = ["n07", "noaa7", "noaa07", "NC", "NOAA7"]
    ne_list = ["n08", "noaa8", "noaa08", "NE", "NOAA8"]
    nf_list = ["n09", "noaa9", "noaa09", "NF", "NOAA9"]
    ng_list = ["n10", "noaa10", "NG", "NOAA10"]
    nh_list = ["n11", "noaa11", "NH", "NOAA11"]
    nd_list = ["n12", "noaa12", "ND", "NOAA12"]
    nj_list = ["n14", "noaa14", "NJ", "NOAA14"]
    nk_list = ["n15", "noaa15", "NK", "NOAA15"]
    nl_list = ["n16", "noaa16", "NL", "NOAA16"]
    nm_list = ["n17", "noaa17", "NM", "NOAA17"]
    nn_list = ["n18", "noaa18", "NN", "NOAA18"]
    np_list = ["n19", "noaa19", "NP", "NOAA19"]
    m1_list = ["m01", "metopb", "metop01", "M1", "METOPB"]
    m2_list = ["m02", "metopa", "metop02", "M2", "METOPA"]
    # for cci_sensors
    mod_list = ["mod", "MOD", "MODIS", "Terra", "TERRA"]
    myd_list = ["myd", "MYD", "MODIS", "Aqua", "AQUA"]
    env_list = ["env", "Envisat", "ENVISAT"]
    ers_list = ["ers", "ERS-2", "ers-2"]

    if sat in tn_list:
        return "NOAA-5", "noaa5", "TIROSN", 'Black', 'Navy'
    elif sat in na_list:
        return "NOAA-6", "noaa6", "NOAA6", 'DarkSlateGray', 'DodgerBlue'
    elif sat in nc_list:
        return "NOAA-7", "noaa7", "NOAA7", 'DarkOrchid', 'Navy'
    elif sat in ne_list:
        return "NOAA-8", "noaa8", "NOAA8", 'DeepPink', 'DodgerBlue'
    elif sat in nf_list:
        return "NOAA-9", "noaa9", "NOAA9", 'DodgerBlue', 'Navy'
    elif sat in ng_list:
        return "NOAA-10", "noaa10", "NOAA10", 'Blue', 'DodgerBlue'
    elif sat in nh_list:
        return "NOAA-11", "noaa11", "NOAA11", 'DarkOrange', 'Navy'
    elif sat in nd_list:
        return "NOAA-12", "noaa12", "NOAA12", 'LimeGreen', 'DodgerBlue'
    elif sat in nj_list:
        return "NOAA-14", "noaa14", "NOAA14", 'Sienna', 'Navy'
    elif sat in nk_list:
        return "NOAA-15", "noaa15", "NOAA15", 'Red', 'DodgerBlue'
    elif sat in nl_list:
        return "NOAA-16", "noaa16", "NOAA16", 'DarkGreen', 'Navy'
    elif sat in nm_list:
        return "NOAA-17", "noaa17", "NOAA17", 'DarkCyan', 'DodgerBlue'
    elif sat in nn_list:
        return "NOAA-18", "noaa18", "NOAA18", 'DarkMagenta', 'Navy'
    elif sat in np_list:
        return "NOAA-19", "noaa19", "NOAA19", 'Navy', 'Navy'
    elif sat in m1_list:
        return "MetOp-B", "metopb", "METOPB", 'Olive', 'DodgerBlue'
    elif sat in m2_list:
        return "MetOp-A", "metopa", "METOPA", 'Coral', 'DodgerBlue'

    ## for cci_sensors
    # return platform | sensor | sensor/platform | color
    elif sat in mod_list:
        return "TERRA", "MODIS", "MODIS/Terra", 'Firebrick', 'DarkGreen'
    elif sat in myd_list:
        return "AQUA", "MODIS", "MODIS/Aqua", 'goldenrod', 'LimeGreen'
    elif sat in env_list:
        return "ENVISAT", "AATSR", "AATSR/Envisat", 'SkyBlue', 'DarkOrange'
    elif sat in ers_list:
        return "ERS-2", "ATSR", "ATSR/ERS-2", "Violet", 'DarkOrange'

    else:
        message = "\n * The satellite name you've chosen is not " \
                  "available in the current list!\n"
        sys.exit(message)


def full_cha_name(target):
    """
    Decoding of channel abbreviations.
    """
    if target == 'rf1' or target == 'ch1':
        return "Ch1 reflectance"
    elif target == 'rf2' or target == 'ch2':
        return "Ch2 reflectance"
    elif target == 'rf3' or target == 'ch3a':
        return "Ch3a reflectance"
    elif target == 'bt3' or target == 'ch3b':
        return "Ch3b brightness temperature"
    elif target == 'bt4' or target == 'ch4':
        return "Ch4 brightness temperature"
    elif target == 'bt5' or target == 'ch5':
        return "Ch5 brightness temperature"
    else:
        message = "\n * Wrong target name! see help message !\n"
        sys.exit(message)


def pre_blacklist_reasons():
    """
    These blacklist reasons have been found and implemented so far,
    before the AVHRR GAC L1c processing based on L1B filenames.
    Additionally blacklisted are L1B files, which are redudant, 
    i.e. where sql column "redundant=1".
    """
    return {'pre1':'old', 
            'pre2':'too_small',
            'pre3':'too_long',
            'pre4':'ground_station_duplicate',
            'pre5':'redundant'}


def proc_blacklist_reasons():
    """
    These blacklist reasons have been found and implemented so far,
    during the AVHRR GAC L1c processing based on start and end l1c
    timestamps.
    """
    return {'proc1':'orbit_length_too_long', 
            'proc2':'negative_orbit_length'}


def post_blacklist_reasons():
    """
    These blacklist reasons have been found and implemented so far,
    after the AVHRR GAC L1c processing based on logfile and pystat
    analyses.
    """
    return {'post1':'wrong_l1c_timestamp', 
            'post2':'no_valid_l1c_data', 
            'post3':'bad_l1c_quality', 
            'post4':'along_track_too_long',
            'post5':'pygac_indexerror',
            'post6':'ch3a_zero_reflectance',
            'post7':'temporary_scan_motor_issue'}


def split_filename(fil):
    # dirname  = os.path.dirname(fil)
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


def check_if_table_exists(cursor, tablename):
    """
    run_pystat_add2sqlite.py: check if table already exists.
    """
    check = "select count(*) from sqlite_master " \
            "where name = \'{0}\' ".format(tablename)
    cursor.execute(check)
    res = cursor.fetchone()
    return res['count(*)']


def dict_factory(cursor, row):
    """
    Organize database queries by column names.
    """
    d = dict()
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def get_datagaps_records(satellite, db):
    """
    Read SQL from AVHRR GAC L1c processing and 
    return missing scanlines information.
    """
    gaps = list()
    dates = list()
    counts = list()
    endline = list()
    alongtrack = list()

    cmd = "SELECT start_time_l1c, missing_scanlines, " \
          "along_track, end_scanline_endcut, " \
          "number_of_missing_scanlines " \
          "FROM vw_std WHERE blacklist=0 AND " \
          "start_time_l1c is not null AND " \
          "number_of_missing_scanlines is not null AND " \
          "number_of_missing_scanlines < 15000 AND " \
          "satellite_name=\'{satellite}\' ORDER BY " \
          "start_time_l1c".format(satellite=satellite)

    rec = db.execute(cmd)

    for r in rec:
        if r['start_time_l1c'] is not None:
            gaps.append(r['missing_scanlines'])
            dates.append(r['start_time_l1c'])
            counts.append(r['number_of_missing_scanlines'])
            endline.append(r['end_scanline_endcut'])
            alongtrack.append(r['along_track'])

    return gaps, dates, counts, endline, alongtrack


def get_cci_sensors_dict():
    """
    Cloud_cci dictionary containing start and end dates of 
    archive (MODIS, AATSR, ATSR) and primes for AVHRR.
    :rtype: dictionary
    """
    cci_dict = dict()
    cci_list = get_cci_satellite_list()

    # initialize dictionary
    for sat in cci_list:
        cci_dict[sat] = dict()
        for dt in ("start_date", "end_date"):
            cci_dict[sat][dt] = 0

    # --------------------------------------------------------------------
    cci_dict["NOAA7"]["start_date"]  = datetime.datetime(1982, 1, 1)
    cci_dict["NOAA9"]["start_date"]  = datetime.datetime(1985, 2, 25)
    cci_dict["NOAA11"]["start_date"] = datetime.datetime(1988, 11, 1)
    cci_dict["NOAA12"]["start_date"] = datetime.datetime(1991, 9, 17)
    cci_dict["NOAA14"]["start_date"] = datetime.datetime(1995, 1, 20)
    cci_dict["NOAA15"]["start_date"] = datetime.datetime(1998, 12, 15)
    cci_dict["NOAA16"]["start_date"] = datetime.datetime(2001, 4,  1) #(2001, 3, 20)
    cci_dict["NOAA17"]["start_date"] = datetime.datetime(2002, 11, 1) #(2002, 10, 15)
    cci_dict["NOAA18"]["start_date"] = datetime.datetime(2005, 9,  1) #(2005, 8, 30)
    cci_dict["NOAA19"]["start_date"] = datetime.datetime(2009, 6,  1)
    cci_dict["METOPA"]["start_date"] = datetime.datetime(2007, 7,  1) #(2007, 5, 21)
    #cci_dict["METOPB"]["start_date"] = datetime.datetime(2020, 1,  1) #datetime.date(2013, 5,  1) #(2013, 4, 24)
    # --------------------------------------------------------------------
    cci_dict["NOAA7"]["end_date"]  = datetime.datetime(1985, 2, 1)
    cci_dict["NOAA9"]["end_date"]  = datetime.datetime(1988, 10, 31)
    cci_dict["NOAA11"]["end_date"] = datetime.datetime(1994, 10, 16)
    cci_dict["NOAA12"]["end_date"] = cci_dict["NOAA15"]["start_date"]
    cci_dict["NOAA14"]["end_date"] = cci_dict["NOAA16"]["start_date"]
    cci_dict["NOAA15"]["end_date"] = cci_dict["NOAA17"]["start_date"]
    cci_dict["NOAA16"]["end_date"] = cci_dict["NOAA18"]["start_date"]
    cci_dict["NOAA17"]["end_date"] = cci_dict["METOPA"]["start_date"]
    cci_dict["NOAA18"]["end_date"] = cci_dict["NOAA19"]["start_date"] - timedelta(days=1)
    cci_dict["NOAA19"]["end_date"] = datetime.datetime(2014, 12, 31)
    cci_dict["METOPA"]["end_date"] = datetime.datetime(2014, 12, 31) # cci_dict["METOPB"]["start_date"]
    #cci_dict["METOPB"]["end_date"] = datetime.datetime(2020, 1,  2) #datetime.date(2014, 12, 31)
    # --------------------------------------------------------------------
    cci_dict["TERRA"]["start_date"] = datetime.datetime(2000, 2, 24)
    cci_dict["TERRA"]["end_date"] = datetime.datetime(2014, 12, 31)
    cci_dict["AQUA"]["start_date"] = datetime.datetime(2002, 7, 4)
    cci_dict["AQUA"]["end_date"] = datetime.datetime(2014, 12, 31)
    # --------------------------------------------------------------------
    cci_dict["ERS-2"]["start_date"] = datetime.datetime(1995, 8, 1)
    cci_dict["ERS-2"]["end_date"] = datetime.datetime(2002, 12, 31)
    cci_dict["ENVISAT"]["start_date"] = datetime.datetime(2002, 7, 23)
    cci_dict["ENVISAT"]["end_date"] = datetime.datetime(2012, 4, 8)
    # --------------------------------------------------------------------

    return cci_dict


def get_ect_records(satellite, db, primes=None):
    """
    plot_avhrr_ect_ltan.py:
    get equator crossing time for given satellite
    from a sqlite3 database: table orbits
    and return ect list and date list
    """
    ect_list = []
    date_list = []

    if satellite.startswith("NOAA") or satellite.startswith("METOP") or \
            satellite.startswith("TIROS"): 
        # avhrr 
        get_data = "SELECT start_time_l1c, equator_crossing_time " \
                   "FROM vw_std WHERE blacklist=0 AND " \
                   "equator_crossing_time is not null AND " \
                   "start_time_l1c is not null AND " \
                   "end_time_l1c is not null AND " \
                   "satellite_name=\'{satellite}\' ORDER BY " \
                   "start_time_l1c".format(satellite=satellite)
        results = db.execute(get_data)
        for result in results:
            if result['start_time_l1c'] is not None:
                date_list.append(result['start_time_l1c'])
                ect_list.append(result['equator_crossing_time'])
        # if prime:
            # cut date_list and ect_list accordingly
        return date_list, ect_list

    else:
        # for: terra, aqua, envisat, ers-2
        cci = get_cci_sensors_dict()
        date_list, ect_list = create_date_ect_lists(satellite, 
                                cci[satellite]["start_date"], 
                                cci[satellite]["end_date"])
        return date_list, ect_list


def create_date_ect_lists(sat, sdt, edt):
    """
    Create date and ect lists for plot_avhrr_ect_ltan.py:
    terra, aqua, envisat, ers-2
    """
    dates = list()
    ects = list()
    # get hour and minute for satellite
    e = get_constant_ects(sat)
    # loop over days
    for dt in rrule(DAILY, dtstart=sdt, until=edt):
        dates.append(dt)
        ects.append(datetime.datetime(dt.year, dt.month, dt.day, e[0], e[1], 0))
    return dates, ects


def create_statistics_table(db):
    """
    run_pystat_add2sqlite.py: create statistics table.
    """
    act = "CREATE TABLE IF NOT EXISTS statistics ( " \
          "satelliteID INTEGER, date DATE, " \
          "channelID INTEGER, selectID INTEGER, " \
          "FOREIGN KEY (satelliteID) REFERENCES satellites (id), " \
          "FOREIGN KEY (channelID) REFERENCES channels (id), " \
          "FOREIGN KEY (selectID) REFERENCES selects (id), " \
          "PRIMARY KEY (satelliteID, date, channelID, selectID) )"
    db.execute(act)


def alter_statistics_table(db, belts):
    """
    run_pystat_add2sqlite.py: add statistics to existing table
    """
    glob_list = ('OrbitCount', 'GlobalMean', 'GlobalStdv', 'GlobalNobs')
    typ1_list = ('INTEGER', 'FLOAT', 'FLOAT', 'INTEGER')
    zona_list = ('ZonalMean', 'ZonalStdv', 'ZonalNobs')
    typ2_list = ('FLOAT', 'FLOAT', 'INTEGER')

    for glo, typ in zip(glob_list, typ1_list):
        act = "ALTER TABLE statistics ADD COLUMN " \
              "{0} {1}".format(glo, typ)
        db.execute(act)

    for zon, typ in zip(zona_list, typ2_list):
        for idx, lat in enumerate(belts):
            bel = zon + str(idx)
            act = "ALTER TABLE statistics ADD COLUMN " \
                  "{0} {1}".format(bel, typ)
            db.execute(act)


def create_id_name_table(db, table, lst):
    """
    run_pystat_add2sqlite.py: create new table.
    """
    if table is 'latitudes':
        act = "CREATE TABLE {0} " \
              "(id INTEGER PRIMARY KEY, belt FLOAT)".format(table)
        db.execute(act)
        for position, item in enumerate(lst):
            act = "INSERT OR ABORT INTO {0} " \
                  "VALUES ({1}, {2})".format(table, position, item)
            db.execute(act)
    else:
        act = "CREATE TABLE {0} " \
              "(id INTEGER PRIMARY KEY, name TEXT)".format(table)
        db.execute(act)
        for position, item in enumerate(lst):
            act = "INSERT OR ABORT INTO {0} " \
                  "VALUES ({1}, \'{2}\')".format(table, position, item)
            db.execute(act)


def cal_zonal_means(lat, tar, zone_size):
    """
    Calculation of daily zonal means.
    Called in run_pystat_add2sqlite.py
    S. Finkensieper, July 2014
    """
    # define latitudinal zone size :
    zone_rad = zone_size / 2.0

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

    return zonal_means, zonal_stdev, nobs


def set_fillvalue(fill_value, zonal_mean, zonal_stdv, zonal_nobs,
                  global_mean, global_stdv, global_nobs):
    """
    Set fill value in case element is masked or no data.
    """
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

    # noinspection PyUnresolvedReferences
    return (np.asscalar(glm), np.asscalar(gls),
            np.asscalar(gln.astype(int)),
            mean, stdv, nobs.astype(int))


def check_l1c_timestamps(dbfile, start_timestamp, end_timestamp, l1bfile):
    """
    After pygac and add2sqlite, check for start and end l1c timestamps.
    If necessary blacklist the orbit.
    """
    # convert start and end dates to unix timestamp
    d1_ts = time.mktime(start_timestamp.timetuple())
    d2_ts = time.mktime(end_timestamp.timetuple())

    # orbit duration in minutes
    orbit_duration = int(d2_ts - d1_ts) / 60
    max_orbit_duration = 120. # minutes

    logger.info("L1bFile : {0}".format(l1bfile))
    logger.info("L1COrbit: {0} -- {1}".format(start_timestamp, 
                                              end_timestamp))

    # start time lies behind end time, i.e. negative orbit length
    if start_timestamp > end_timestamp:
        txt = "negative_orbit_length"
        logger.info("Blacklist=1 this orbit due to {0}".format(txt))
        upd = "UPDATE orbits SET blacklist=1, " \
              "blacklist_reason=\'{blr}\' " \
              "WHERE filename=\'{fil}\'".format(fil=l1bfile, blr=txt)
        dbfile.execute(upd)
        return 1

    # orbit is longer than max_orbit_duration (length)
    elif orbit_duration > max_orbit_duration:
        txt = "orbit_length_too_long"
        logger.info("Blacklist=1 this orbit due to {0}".format(txt))
        upd = "UPDATE orbits SET blacklist=1, " \
              "blacklist_reason=\'{blr}\' " \
              "WHERE filename=\'{fil}\'".format(fil=l1bfile, blr=txt)
        dbfile.execute(upd)
        return 1

    # orbit length is OK
    else:
        logger.info("Orbit length is within valid range!")
        return 0


def get_l1c_timestamps(filename):
     # -- split filename
     split_string = split_filename(filename)
     start_date_string = split_string[5][0:-1]
     end_date_string = split_string[6][0:-1]

     # -- get timestamp of first scanline
     start_datetime_string = ''.join(start_date_string.split('T'))
     start_microseconds = int(start_datetime_string[-1]) * 1E5
     start_time_l1c_help = datetime.datetime.strptime(start_datetime_string[0:-1], '%Y%m%d%H%M%S')
     start_time_l1c = start_time_l1c_help + datetime.timedelta(microseconds=start_microseconds)

     # -- get timestamp of last scanline
     end_datetime_string = ''.join(end_date_string.split('T'))
     end_microseconds = int(end_datetime_string[-1]) * 1E5
     end_time_l1c_help = datetime.datetime.strptime(end_datetime_string[0:-1], '%Y%m%d%H%M%S')
     end_time_l1c = end_time_l1c_help + datetime.timedelta(microseconds=end_microseconds)

     return start_time_l1c, end_time_l1c


def get_monthly_ect_averages(satellite, datlst, ectlst): 
    """
    Calculate the monthly ECT averages using date_list and ect_list.
    """
    ects = list()   # ects in seconds
    datsec = list() # dates in seconds
    datobj = list() # dates as datetime objects

    logger.info("Calculate monthly ECT averages for {0}".format(satellite))

    # midnights for ectlst
    midnights = [datetime.datetime(ect.year, ect.month, ect.day, 0, 0) for ect in ectlst]
    # convert ectlst into seconds
    seconds = [(ect - m).total_seconds() for ect, m in zip(ectlst, midnights)]

    for mm in rrule(MONTHLY, dtstart=min(datlst), until=max(datlst)): 

        tmplst = list()
        ystr = mm.strftime('%Y')
        mstr = mm.strftime('%m')
        ndays = calendar.monthrange(int(ystr),int(mstr))[1]
        mindt = datetime.datetime(int(ystr), int(mstr), 1, 0, 0, 0)
        maxdt = datetime.datetime(int(ystr), int(mstr), ndays, 23, 59, 59)

        for idx,dt in enumerate(datlst):
            if mindt <= dt <= maxdt:
                tmplst.append(seconds[idx])

        if len(tmplst) > 0:
            date_in_sec = (mindt - datetime.datetime(1970, 1, 1, 0, 0)).total_seconds()
            monave = sum(tmplst) / float(len(tmplst))
            ects.append(monave)
            datsec.append(date_in_sec)
            datobj.append(mindt)

    return ects, datsec, datobj


def ect_convert_to_datetime(time_hours):
    # time_hours = 23.99431
    time_minutes = time_hours * 60
    time_seconds = time_minutes * 60

    hours_part = int(floor(time_hours))
    minutes_part = int(floor(time_minutes % 60))
    seconds_part = int(floor(time_seconds % 60))

    return hours_part, minutes_part, seconds_part


def get_ect_local_hour(lat, lon, start_time_l1c, verbose):
    """
    Calculates/estimates the equator crossing time of an orbit,
    based on the minimum of abs(lat).
    :rtype : datetime object
    """
    ect_lat_idx = None
    ect_lon_val = None
    ect_lat_val = None
    orbit = None
    oflag = None

    try:
        # find minimum of absolute latitude in the middle of the swath
        # avhrr swath: 409 pixels
        mid_pix = 204
        abs_lat = abs(lat)
        ori_lat = lat

        lat_mask = np.ma.mask_or(abs_lat > 1.,
                                 abs_lat > abs_lat.min() + 0.02)
        lat = np.ma.masked_where(lat_mask, lat)
        lon = np.ma.masked_where(lat_mask, lon)

        # noinspection PyUnresolvedReferences
        lat_min = lat[:, mid_pix].compressed().tolist()
        # noinspection PyUnresolvedReferences
        lon_min = lon[:, mid_pix].compressed().tolist()
        lat_idx = np.ma.where(abs(lat[:, mid_pix]) >= 0.)[0].tolist()

        if len(lat_min) == 0:
            logger.info("Lat_min is empty:{0} - "
                        "no match found over the equator".
                        format(len(lat_min)))
            return None

        if len(lat_min) != len(lat_idx):
            logger.info("FAILED: lat_min cnt != lat_idx cnt")
            return None

        for cnt, val in enumerate(lat_idx):
            lat_idx_next = val + 1
            lat_val_next = ori_lat[lat_idx_next, mid_pix]

            if isinstance(lat_val_next, np.ma.core.MaskedConstant):
                continue

            else:
                if lat_min[cnt] < lat_val_next:
                    oflag = "afternoon"
                    orbit = 'asc:{0:8.4f} < {1:8.4f}'.\
                        format(lat_min[cnt], lat_val_next)
                else:
                    oflag = "morning"
                    orbit = 'des:{0:8.4f} > {1:8.4f}'.\
                        format(lat_min[cnt], lat_val_next)

                if oflag == "afternoon":
                    ect_lat_idx = val
                    ect_lat_val = lat_min[cnt]
                    ect_lon_val = lon_min[cnt]
                    break

        if oflag:
            if oflag == "afternoon":
                # calculate equator crossing time (local time [hour])
                start_date = start_time_l1c.date()
                start_time = start_time_l1c.time()

                # beginning of the orbit
                start_time_hour = start_time.hour + \
                                  start_time.minute / 60. + \
                                  start_time.second / 3600.

                # 2 scanlines per second, 3600 seconds per hour
                scanline_over_equator_time = ect_lat_idx / 2. / 3600.

                # ect local hour over equator
                ect_local_hour = (start_time_hour + scanline_over_equator_time) + \
                                 (ect_lon_val / 15.)

                if ect_local_hour > 24.:
                    ect_local_hour -= 24.
                elif ect_local_hour < 0:
                    ect_local_hour += 24.

                (eh, em, es) = ect_convert_to_datetime(ect_local_hour)

                ect_datetime = datetime.datetime(start_date.year, start_date.month,
                                                 start_date.day, eh, em, es)

                if verbose:
                    logger.info("Local Time of Ascending Node (LTAN) [{4}]: "
                                "{0:8.4f} hour -> to {1} "
                                "for lat:{2:8.4f} and lon:{3:8.4f}".
                                format(ect_local_hour, ect_datetime,
                                       ect_lat_val, ect_lon_val, orbit))

                return ect_datetime

            else:
                logger.info("No afternoon orbit_flag: {0}".format(oflag))
                return None

        else:
            logger.info("No orbit_flag: {0}".format(oflag))
            return None

    except (IndexError, ValueError, RuntimeError, Exception) as err:
        logger.info("FAILED: {0}".format(err))



