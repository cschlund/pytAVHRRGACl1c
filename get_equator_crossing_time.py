#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#

import os
import datetime
import argparse
import h5py
import read_avhrrgac_h5 as rh5
import subs_avhrrgac as subs
from pycmsaf.logger import setup_root_logger

logger = setup_root_logger(name='root')

parser = argparse.ArgumentParser(description=u'''{0:s}
calculates the equator crossing time for each orbit
and adds the information to the AVHRR GAC archive
database.'''.format(os.path.basename(__file__)))

parser.add_argument('--date', required=True, type=str,
                    help='e.g., 19960115')

parser.add_argument('--l1c_path', required=True, type=str,
                    help='Directory where L1c files are located.')

parser.add_argument('--verbose', action="store_true",
                    help='increase output verbosity')

args = parser.parse_args()


# -- some screen output
if args.verbose:
    logger.info(" *** Parameter passed: {0} ***".format(os.path.basename(__file__)))
    logger.info(" Date       : %s" % args.date)
    logger.info(" Input Path : %s" % args.l1c_path)
    logger.info(" Verbose    : %s" % args.verbose)


# -- collect all available files for args.date
pattern = 'ECC_GAC_avhrr*' + args.date + '*.h5'
file_list = subs.find(pattern, args.l1c_path)
file_list.sort()

# -- loop over files
for fil in file_list:

    fil_name = fil
    ang_file = fil_name.replace("ECC_GAC_avhrr_", "ECC_GAC_sunsatangles_")

    # logger.info(" L1c_path:{0} ".format(args.l1c_path))
    logger.info(" L1c_file:{0} ".format(os.path.basename(fil_name)))

    # split filename
    split_string = subs.split_filename(fil_name)
    start_date_string = split_string[5][0:-1]
    if start_date_string[0:8] != args.date:
        continue

    # -- get timestamp of first scanline
    start_datetime_string = ''.join(start_date_string.split('T'))
    start_microseconds = int(start_datetime_string[-1]) * 1E5
    start_time_l1c_help = datetime.datetime.strptime(start_datetime_string[0:-1], '%Y%m%d%H%M%S')
    start_time_l1c = start_time_l1c_help + datetime.timedelta(microseconds=start_microseconds)

    # read file
    f = h5py.File(fil_name, "r+")
    a = h5py.File(ang_file, "r+")
    (lat, lon, tar) = rh5.read_avhrrgac(f, a, 'day', 'ch1', False)
    f.close()
    a.close()

    ect = subs.get_ect_local_hour(lat, lon, start_time_l1c, args.verbose)

logger.info(" *** {0:s} finished ***\n".format(os.path.basename(__file__)))