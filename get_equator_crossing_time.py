#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#

import os
import datetime
import argparse

import h5py
import matplotlib.pyplot as plt
from dateutil.rrule import rrule, DAILY

import read_avhrrgac_h5 as rh5
import subs_avhrrgac as subs
from pycmsaf.argparser import str2date
from pycmsaf.logger import setup_root_logger


logger = setup_root_logger(name='root')

parser = argparse.ArgumentParser(description=u'''{0:s}
calculates the equator crossing time for each orbit
for the ascending node, i.e. local time of ascending node
(LTAN).'''.format(os.path.basename(__file__)))

parser.add_argument('--start_date', required=True, type=str2date,
                    help='e.g., 19960115')

parser.add_argument('--end_date', required=True, type=str2date,
                    help='e.g., 19960121')

parser.add_argument('--l1c_path', required=True, type=str,
                    help='Directory where L1c files are located.')

parser.add_argument('--verbose', action="store_true",
                    help='increase output verbosity')

args = parser.parse_args()


# -- some screen output
if args.verbose:
    logger.info(" *** Parameter passed: {0} ***".
                format(os.path.basename(__file__)))
    logger.info(" Start_Date : %s" % args.start_date)
    logger.info(" End_Dat    : %s" % args.end_date)
    logger.info(" Input Path : %s" % args.l1c_path)
    logger.info(" Verbose    : %s" % args.verbose)

# -- for plotting
res_dict = dict()
color_list = subs.get_color_list()
all_sat_list = subs.get_satellite_list()
for sat in all_sat_list:
    res_dict[sat] = dict()

cnt = 0

# -- loop over date range
for dt in rrule(DAILY, dtstart=args.start_date, until=args.end_date):

    dstr = str(dt.strftime("%Y%m%d"))

    # -- collect all available files for current date
    pattern = 'ECC_GAC_avhrr*' + dstr + '*.h5'
    file_list = subs.find(pattern, args.l1c_path)
    file_list.sort()

    # -- loop over files
    for num, fil in enumerate(file_list):

        # -- get satellite name and add to list
        split_filename = subs.split_filename(fil)
        for sf in split_filename:
            if sf.lower().startswith("noaa") or \
                    sf.lower().startswith("metop"):
                sat = subs.full_sat_name(sf)[2]
                break

        # logger.info(" L1c_path:{0} ".format(args.l1c_path))
        logger.info(" L1c_file:{0} ".format(os.path.basename(fil)))

        # split filename
        split_string = subs.split_filename(fil)
        start_date_string = split_string[5][0:-1]
        if start_date_string[0:8] != dstr:
            continue

        # -- get timestamp of first scanline
        start_datetime_string = ''.join(start_date_string.split('T'))
        start_microseconds = int(start_datetime_string[-1]) * 1E5
        start_time_l1c_help = datetime.datetime.strptime(start_datetime_string[0:-1], '%Y%m%d%H%M%S')
        start_time_l1c = start_time_l1c_help + datetime.timedelta(microseconds=start_microseconds)

        # read file
        f = h5py.File(fil, "r+")
        lat, lon = rh5.read_latlon(f, False)
        f.close()

        ect = subs.get_ect_local_hour(lat, lon, start_time_l1c, args.verbose)
        if ect is not None:
            res_dict[sat][cnt] = dict()
            res_dict[sat][cnt]['date'] = dt
            res_dict[sat][cnt]['ect'] = ect
            cnt += 1

# # -- plot results
logger.info("Plot LTAN")
plt_title = "Equatorial Crossing Time of NOAA " \
            "Polar Satellites based on AVHRR\n"
x_title = "Date\n"
y_title = "Local Time of Ascending Node\n"
out_file = "AVHRR_LocalTime_AscendingNode.png"
cnt = 0
lwd = 2
date_min = datetime.datetime(1981, 1, 1)
date_max = datetime.datetime(2015, 12, 31)

fig = plt.figure()
ax = fig.add_subplot(111)

for satellite in all_sat_list:

    date_list = list()
    ect_list = list()

    for key_idx in res_dict[satellite]:
        date_list.append(res_dict[satellite][key_idx]['date'])
        ect_list.append(res_dict[satellite][key_idx]['ect'])

    if len(date_list) != 0:

        midnights = [datetime.datetime(ect.year, ect.month, ect.day, 0, 0)
                     for ect in ect_list]
        seconds = [(ect - m).total_seconds()
                   for ect, m in zip(ect_list, midnights)]

        ax.plot(date_list, seconds,
                label=satellite, color=color_list[cnt],
                linewidth=lwd)
        ax.plot(date_list, seconds,
                'o', color=color_list[cnt], linewidth=lwd)

    cnt += 1

# annotate plot
ax.set_title(plt_title)
ax.set_xlabel(x_title)
ax.set_ylabel(y_title)
# set y label
ax.set_ylim(0, 86400)
seconds_label = range(3600, 86400, 3600)
seconds_strings = [str(datetime.timedelta(seconds=s))[0:-3]
                   for s in seconds_label]
ax.yaxis.set_ticks(seconds_label)
ax.yaxis.set_ticklabels(seconds_strings)
# set x label
ax.set_xlim(date_min, date_max)
# year_label = range(1980, 2015 + 5, 5)
# year_string = [str(y) for y in year_label]
# ax.xaxis.set_ticks(year_label)
# ax.xaxis.set_ticklabels(year_string)
plt.gcf().autofmt_xdate()
# set grid
ax.grid()
# tight layout
plt.tight_layout()
leg = ax.legend(loc='best', fancybox=True)
# save and close plot
logger.info("Save: {0}".format(out_file))
plt.savefig(out_file)
plt.show()
plt.close()

logger.info(" *** {0:s} finished ***\n".format(os.path.basename(__file__)))