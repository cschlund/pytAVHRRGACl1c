#
# subroutines for pystat, sqlite updating/writing/creating
#

import os
import sys
import fnmatch
import datetime
import string
import numpy as np
import logging
from math import floor

logger = logging.getLogger('root')


class ColumnError(Exception):
    pass


class NegativeScanline(Exception):
    pass


def str2upper(string_object):
    return string_object.upper()


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


def calc_midnight(stime, etime):
    """
    Check for midnight orbit & check if day has changed
    """
    # 2008-07-31 22:29:51.400000|2008-08-01 00:18:15.400000
    date_diff = abs(etime.date() - stime.date()).days
    if date_diff > 1:
        logger.info("SUSPECT: {0} days difference".format(date_diff))
        logger.info("  --> start_time_l1c={0}".format(stime))
        logger.info("  -->   end_time_l1c={0}".format(etime))

    # Max. 1 day difference
    if date_diff <= 1:

        # calculate how much time has passed
        # between start time and midnight
        midnight = datetime.datetime.strptime(
            str(etime.day * 1000000), '%d%H%M%S')
        midnight = midnight + datetime.timedelta(microseconds=0)
        midnight_diff = midnight - stime

        # calculate the orbit line under the
        # assumption of 2 scanlines/second
        midnight_diff_msec = midnight_diff.seconds + midnight_diff.microseconds / 1000000
        midnight_orbit_calc = midnight_diff_msec * 2

    else:

        # set midnight variable to -1 if the day hasn't changed
        # midnight_orbit_calc = None
        midnight_orbit_calc = ''

    return midnight_orbit_calc


def calc_overlap(stime, etime):
    """
    GAC_overlap.py: calculation of overlapping scanlines
    """

    # time difference between
    # start of next orbit and end of current orbit
    # assumption: 2 scanlines per second

    timediff = etime - stime
    timediff_msec = timediff.days * 24 * 60 * 60 + timediff.seconds + timediff.microseconds / 1000000
    overlap_rows = timediff_msec * 2

    if overlap_rows < 0:
        logger.info("overlap_rows = {0}".format(overlap_rows))
        raise NegativeScanline(' *** Negative Scanline Number ***')
    else:
        return overlap_rows


def get_new_cols():
    """
    These columns will be added to already existing database
    providing information about overlapping scanlines and
    midnight orbit scanline
    [ GAC_overlap.py ]
    """

    new_cols = ["start_scanline_begcut", "end_scanline_begcut",
                "start_scanline_endcut", "end_scanline_endcut",
                "midnight_scanline"]
    return new_cols


def update_db_without_midnight(vals, db):
    """
    GAC_overlap.py: update database
                    vals = [string, start_line, string, end_line,
                            stime, etime, satellite]
    """

    # noinspection PyProtectedMember
    sat_id = db._get_id_by_name(table='satellites', name=vals[6])

    act = "UPDATE orbits SET {0} = {1}, {2} = {3} " \
          "WHERE blacklist=0 AND " \
          "start_time_l1c=\'{4}\' AND end_time_l1c=\'{5}\' AND " \
          "satellite_id={6}".format(vals[0], vals[1], vals[2],
                                    vals[3], vals[4], vals[5],
                                    sat_id)

    total_changes_before = db.conn.total_changes
    db.execute(act)
    nchanges = db.conn.total_changes - total_changes_before
    if not nchanges == 1:
        logger.info("total_changes_before: {0}".format(total_changes_before))
        logger.info("nchanges now: {0}".format(nchanges))
        logger.info("db.execute(act): {0}".format(act))
        raise ColumnError(' *** DB UPDATE fishy ***')


def update_db_with_midnight(vals, db):
    """
    GAC_overlap.py: update database
                    vals = [string, start_line, string, end_line,
                            string, midnight_calc,
                            stime, etime, satellite]
    """
    # noinspection PyProtectedMember
    sat_id = db._get_id_by_name(table='satellites', name=vals[8])

    act = "UPDATE orbits SET {0} = {1}, {2} = {3}, " \
          "{4} = \'{5}\' " "WHERE blacklist=0 AND " \
          "start_time_l1c=\'{6}\' AND " "end_time_l1c=\'{7}\' AND " \
          "satellite_id={8}".format(vals[0], vals[1], vals[2], vals[3],
                                    vals[4], vals[5], vals[6], vals[7],
                                    sat_id)

    total_changes_before = db.conn.total_changes
    db.execute(act)
    nchanges = db.conn.total_changes - total_changes_before
    if not nchanges == 1:
        raise ColumnError(' *** DB UPDATE fishy ***')


def get_record_lists(satellite, db):
    """
    GAC_overlap.py: get start, end and along track information
    from a sqlite3 database: table orbits
    """

    start_dates = []
    end_dates = []
    data_along = []

    get_data = "SELECT start_time_l1c, end_time_l1c, " \
               "along_track FROM vw_std WHERE " \
               "blacklist=0 AND " \
               "start_time_l1c is not null AND " \
               "end_time_l1c is not null AND " \
               "satellite_name=\'{satellite}\' ORDER BY " \
               "start_time_l1c".format(satellite=satellite)

    results = db.execute(get_data)

    for result in results:
        if result['start_time_l1c'] is not None:
            start_dates.append(result['start_time_l1c'])
            end_dates.append(result['end_time_l1c'])
            data_along.append(result['along_track'])

    return start_dates, end_dates, data_along


def get_ect_records(satellite, db):
    """
    plot_avhrr_ect_ltan.py:
    get equator crossing time for given satellite
    from a sqlite3 database: table orbits
    and return ect list and date list
    """

    ect_list = []
    date_list = []

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

    return date_list, ect_list


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

    glob_list = ('GlobalMean', 'GlobalStdv', 'GlobalNobs')
    zona_list = ('ZonalMean', 'ZonalStdv', 'ZonalNobs')
    type_list = ('FLOAT', 'FLOAT', 'INTEGER')

    for glo, typ in zip(glob_list, type_list):
        act = "ALTER TABLE statistics ADD COLUMN " \
              "{0} {1}".format(glo, typ)
        db.execute(act)

    for zon, typ in zip(zona_list, type_list):
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


def get_cloudcci_l3u_products():
    """
    List of Cloud_cci L3U products in netCDF file.
    """
    l3u_products = ['cc_mask_asc', 'cc_mask_desc',
                    'cccot_asc', 'cccot_desc',
                    'cld_alb_ch1_asc', 'cld_alb_ch1_desc',
                    'cld_alb_ch2_asc', 'cld_alb_ch2_desc',
                    'cld_type_asc', 'cld_type_desc',
                    'cot_asc', 'cot_desc',
                    'cot_uncertainty_asc', 'cot_uncertainty_desc',
                    'cph_asc', 'cph_desc', 'cth_asc', 'cth_desc',
                    'cth_uncertainty_asc', 'cth_uncertainty_desc',
                    'ctp_asc', 'ctp_desc',
                    'ctp_uncertainty_asc', 'ctp_uncertainty_desc',
                    'ctt_asc', 'ctt_desc',
                    'ctt_uncertainty_asc', 'ctt_uncertainty_desc',
                    'cwp_asc', 'cwp_desc',
                    'cwp_uncertainty_asc', 'cwp_uncertainty_desc',
                    'illum_asc', 'illum_desc',
                    'npoints_l2b_asc', 'npoints_l2b_desc',
                    'qcflag_asc', 'qcflag_desc', 'ref_asc', 'ref_desc',
                    'ref_uncertainty_asc', 'ref_uncertainty_desc',
                    'relazi_asc', 'relazi_desc', 'satzen_asc', 'satzen_desc',
                    'solzen_asc', 'solzen_desc', 'stemp_asc', 'stemp_desc',
                    'stemp_uncertainty_asc', 'stemp_uncertainty_desc',
                    'time_asc', 'time_desc']
    return l3u_products

def get_cloudcci_l3c_products():
    """
    List of Cloud_cci L3C products in netCDF file.
    """
    l3c_products = ['cc_high', 'cc_low', 'cc_middle', 'cc_total',
                    'cc_total_day', 'cc_total_micro', 'cc_total_night',
                    'cc_total_std', 'cc_total_micro_std', 'cc_total_twl',
                    'cc_total_uncertainty', 'cc_total_uncertainty_std',
                    'cot', 'cot_ice', 'cot_liq', 'cot_log', 'cot_std',
                    'cot_std_ice', 'cot_std_liq', 'cot_uncertainty',
                    'cot_uncertainty_ice', 'cot_uncertainty_liq',
                    'cot_uncertainty_std', 'cot_uncertainty_std_ice',
                    'cot_uncertainty_std_liq', 'cph', 'cph_std', 'cth',
                    'cth_std', 'cth_uncertainty', 'cth_uncertainty_std',
                    'ctp', 'ctp_log', 'ctp_std', 'ctp_uncertainty',
                    'ctp_uncertainty_std', 'ctt', 'ctt_std', 'ctt_uncertainty',
                    'ctt_uncertainty_std', 'cwp', 'cwp_std', 'cwp_uncertainty',
                    'cwp_uncertainty_std', 'iwp', 'iwp_std', 'iwp_uncertainty',
                    'iwp_uncertainty_std', 'lwp', 'lwp_std', 'lwp_uncertainty',
                    'lwp_uncertainty_std', 'npoints_macro',
                    'npoints_macro_cct_clear', 'npoints_macro_cct_clear_raw',
                    'npoints_macro_cct_cloudy', 'npoints_macro_cct_cloudy_raw',
                    'npoints_macro_cct_day_clear', 'npoints_macro_cct_day_cloudy',
                    'npoints_macro_cct_high_cloudy', 'npoints_macro_cct_low_cloudy',
                    'npoints_macro_cct_middle_cloudy', 'npoints_macro_cct_night_clear',
                    'npoints_macro_cct_night_cloudy', 'npoints_macro_cct_twl_clear',
                    'npoints_macro_cct_twl_cloudy', 'npoints_macro_clear', 'npoints_macro_cloudy',
                    'npoints_macro_cloudy_ice', 'npoints_macro_cloudy_liq', 'npoints_micro',
                    'npoints_micro_cct_clear', 'npoints_micro_cct_cloudy', 'npoints_micro_clear',
                    'npoints_micro_cloudy', 'npoints_micro_cloudy_ice', 'npoints_micro_cloudy_liq',
                    'ref', 'ref_ice', 'ref_liq', 'ref_std', 'ref_std_ice', 'ref_std_liq',
                    'ref_uncertainty', 'ref_uncertainty_ice', 'ref_uncertainty_liq',
                    'ref_uncertainty_std', 'ref_uncertainty_std_ice', 'ref_uncertainty_std_liq']
    return l3c_products


def get_satellite_list():
    """
    SATELLITE list: sqlite3 nomenclature
    """

    satellites = ['NOAA7', 'NOAA9', 'NOAA11', 'NOAA12',
                  'NOAA14', 'NOAA15', 'NOAA16', 'NOAA17',
                  'NOAA18', 'NOAA19', 'METOPA', 'METOPB']
    return satellites


def get_channel_list():
    """
    List of AVHRR GAC channels.
    """

    channels = ['ch1', 'ch2', 'ch3b', 'ch4', 'ch5', 'ch3a']
    return channels


def get_select_list():
    """
    List of selected times in pystat calculation and
    later for the routines which are plotting statistics.
    """

    selects = ['day', 'night', 'twilight']
    return selects


def get_color_list():
    """
    List of colors used in the plotting routines.
    """

    colorlst = ['Magenta', 'DodgerBlue', 'DarkOrange', 'Lime',
                'Sienna', 'Red', 'DarkGreen', 'Turquoise',
                'DarkMagenta', 'Navy', 'Gold', 'Olive',
                'MediumSlateBlue', 'DimGray']
    return colorlst


def full_sat_name(sat):
    """
    List of satellite names occurring in L1b and L1c filenames,
    as well as in sqlite3 databases.
            name = for plotting routines (name on the PLOT)
            abbr = pygac nomenclature
            lite = sqlite3 nomenclature
    """

    m1_list = ["m01", "metopb", "metop01", "M1", "METOPB"]
    m2_list = ["m02", "metopa", "metop02", "M2", "METOPA"]
    nc_list = ["n07", "noaa7", "noaa07", "NC", "NOAA7"]
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
        message = "\n * The satellite name you've chosen is not " \
                  "available in the current list!\n"
        sys.exit(message)

    return name, abbr, lite


def full_cha_name(target):
    """
    Decoding of channel abbreviations.
    """

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
        message = "\n * Wrong target name! see help message !\n"
        sys.exit(message)

    return name


def datestring(dstr):
    """
    Convert date string containing '-' or '_' or '/'
    into date string without any character.
    """

    if '-' in dstr:
        correct_date_string = string.replace(dstr, '-', '')
    elif '_' in dstr:
        correct_date_string = string.replace(dstr, '_', '')
    elif '/' in dstr:
        correct_date_string = string.replace(dstr, '/', '')
    else:
        correct_date_string = dstr

    return correct_date_string


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


def lite_sat_string(sstr):
    return full_sat_name(sstr)[2]


def satstring(sstr):
    return full_sat_name(sstr)[1]


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