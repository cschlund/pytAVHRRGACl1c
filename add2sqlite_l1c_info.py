#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import os
import sys
import datetime
import argparse
import h5py
import read_avhrrgac_h5 as rh5
import subs_avhrrgac as subs
from pycmsaf.avhrr_gac.database import AvhrrGacDatabase
from pycmsaf.logger import setup_root_logger

logger = setup_root_logger(name='root')

parser = argparse.ArgumentParser(description=u'''{0:s}
reads pyGAC L1c output h5 orbit files (avhrr, qualflags)
and adds to L1b sqlite database, valuable L1c information for each orbit,
which has been processed by pyGAC (white-listed L1b).
Thus, this script adds the start and end time of measurement, i.e.
timestamp of first and last scanline, as well as the
along and across track dimension. This L1c information will be
later used for calculating the number of AVHRR GAC overlapping
scanlines of two consecutive orbits.'''.format(os.path.basename(__file__)))

parser.add_argument('-l1b', '--l1b_file', required=True, type=str,
                    help='e.g., NSS.GHRR.NJ.D96015.S0112.E0306.B0537071.WI.gz')

parser.add_argument('-l1c', '--l1c_file', required=True, type=str,
                    help='e.g., ECC_GAC_avhrr_noaa14_99999_19960115T0112111Z_19960115T0306281Z.h5')

parser.add_argument('-dir', '--l1c_path', required=True, type=str,
                    help='Directory where L1c files are located.')

parser.add_argument('-dbf', '--db_file', required=True, type=str,
                    help='''/path/to/AVHRR_GAC_archive_L1b_L1c.sqlite3,'''
                         '''which should be updated with L1c information''')

parser.add_argument('-ver', '--verbose', action="store_true",
                    help='increase output verbosity')

args = parser.parse_args()


# -- some screen output
if args.verbose:
    logger.info("Parameter passed")
    logger.info("L1bFile    : %s" % args.l1b_file)
    logger.info("L1cFile    : %s" % args.l1c_file)
    logger.info("Input Path : %s" % args.l1c_path)
    logger.info("Database   : %s" % args.db_file)
    logger.info("Verbose    : %s" % args.verbose)


# -- get full qualified L1c File and db satellite_name
sat_id = args.l1b_file.split(".")[2]
sat_name = subs.full_sat_name(sat_id)[2]
fil_name = os.path.join(args.l1c_path, args.l1c_file)

try:
    # -- get x and y dimension of orbit
    f = h5py.File(fil_name, "r+")
    fil_dim = rh5.get_data_size(f)
    f.close()

    if fil_dim is not None:
        data_across = fil_dim[1]
        data_along = fil_dim[0]
    else:
        logger.info("*** Skip %s -> no file dimensions!" % fil_name)
        sys.exit(0)

    # -- read quality flag file
    qfil = fil_name.replace("ECC_GAC_avhrr_", "ECC_GAC_qualflags_")
    q = h5py.File(qfil, "r+")
    (row, col, total_records, last_scanline, data) = rh5.read_qualflags(q)
    q.close()

    # -- look for missing scanlines
    if total_records == last_scanline:

        number_of_missing_scanlines = 0
        missing_scanlines = list()

    else:

        number_of_missing_scanlines = abs(total_records - last_scanline)
        missing_scanlines = rh5.find_scanline_gaps(0, last_scanline, data)

        if args.verbose:
            logger.info("File:{0}, Row:{1}, Col:{2}, TotalRecords:{3}, "
                        "Last_ScanLine:{4}, NumberOfMissingScanlines:{5}, "
                        "MissingScanlines:{6}".
                        format(os.path.basename(qfil), row, col, total_records,
                               last_scanline, number_of_missing_scanlines,
                               missing_scanlines))

    # -- split filename
    split_string = subs.split_filename(fil_name)
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

    # -- get equator crossing time
    ang_file = fil_name.replace("ECC_GAC_avhrr_", "ECC_GAC_sunsatangles_")
    f = h5py.File(fil_name, "r+")
    a = h5py.File(ang_file, "r+")
    (lat, lon, tar) = rh5.read_avhrrgac(f, a, 'day', 'ch1', False)
    f.close()
    a.close()
    ect = subs.get_ect_local_hour(lat, lon, start_time_l1c, args.verbose, logger)

    if args.verbose:
        logger.info("UPDATE {0}:".format(args.db_file))
        logger.info("L1bfile:{0}, start_time_l1c:{1}, end_time_l1c:{2}, "
                    "across_track:{3}, along_track:{4}, missing_scanlines:{5}, "
                    "equator_crossing_time:{6} " .
                    format(args.l1b_file, start_time_l1c, end_time_l1c,
                           data_across, data_along, missing_scanlines, ect))

    # connect to database
    db = AvhrrGacDatabase(dbfile=args.db_file, timeout=36000, exclusive=True)

    # -- add to sqlite
    db.add_l1c_fields(where_filename=args.l1b_file,
                      start_time_l1c=start_time_l1c, end_time_l1c=end_time_l1c,
                      across_track=data_across, along_track=data_along,
                      missing_scanlines=missing_scanlines,
                      equator_crossing_time=ect)

    # -- commit changes
    db.commit_changes()

except (IndexError, ValueError, RuntimeError, Exception) as err:
    logger.info("FAILED: {0}".format(err))

logger.info("{0:s} finished!".format(os.path.basename(__file__)))