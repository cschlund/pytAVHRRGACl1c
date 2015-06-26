#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# C.Schlundt: November, 2014
#

import os
import sys
import argparse
import sqlite3 as lite

import subs_avhrrgac as mysub
import subs_plot_sql as psql

from pycmsaf.logger import setup_root_logger

logdir = os.path.join(os.getcwd(),'log')
logger = setup_root_logger(name='root', logdir=logdir, logfile=True)

chalist = '|'.join(mysub.get_channel_list())
sellist = '|'.join(mysub.get_pystat_select_list())
satlist = '|'.join(mysub.get_satellite_list())


def plot_results():
    for channel in cha_list:
        for select in sel_list:
            if args.target == 'global':

                try: 
                    check = cur.execute("SELECT OrbitCount FROM statistics")
                except Exception as e: 
                    if select == 'day_90sza':
                        continue

                psql.plot_time_series(sat_list, channel, select,
                                      start_date, end_date, args.outdir,
                                      cur, args.verbose, args.asciifiles,
                                      args.show_figure)

                if args.linfit:
                    psql.plot_time_series_linfit(sat_list, channel, select,
                                                 start_date, end_date, args.outdir,
                                                 cur, args.verbose, args.show_figure)

            else:

                psql.plot_zonal_results(sat_list, channel, select,
                                        start_date, end_date, args.outdir,
                                        cur, args.target, args.verbose,
                                        args.show_figure)

    return


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='''%s 
    displays pystat results, i.e. daily global and
    zonal means and standard deviations stored in a sqlite
    database.''' % os.path.basename(__file__))

    parser.add_argument('-db', '--dbfile', type=str, required=True,
                        help='String, e.g. /path/to/db.sqlite3')

    parser.add_argument('-out', '--outdir', type=str, required=True,
                        help='Path, e.g. /path/to/plot.png')

    parser.add_argument('-sd', '--sdate', type=mysub.datestring,
                        default='1980-01-01', help='Start Date, e.g. 2009-01-01')

    parser.add_argument('-ed', '--edate', type=mysub.datestring,
                        default='2016-01-01', help='End Date, e.g. 2012-12-31')

    parser.add_argument('-cha', '--channel', type=str,
                        help='Channel abbreviation, available: ' + chalist)

    parser.add_argument('-tim', '--time', type=str,
                        help='Time abbreviation, available: ' + sellist)

    parser.add_argument('-sats', '--satellites', type=mysub.str2upper, nargs='*',
                        help='Satellite, available: ' + satlist)

    parser.add_argument('-tar', '--target', type=str, default='global',
                        help='''Latitudinal (zonal, zonalall) 
                        or time series plot (default).
                        NOTE: if you select \'zonal\' choose
                        one day or a very small range because you will get
                        additionally one plot per day/satellite/channel/time.
                        If you select \'zonalall\' then you will get one
                        plot per day/channel/time including all available
                        satellites.''')

    parser.add_argument('-fit', '--linfit', action="store_true",
                        help='''If you want to plot a time series including a
                        linear regression (plot per satellite/channel/time).''')

    parser.add_argument('-ver', '--verbose',
                        help='increase output verbosity', action="store_true")

    parser.add_argument('-show', '--show_figure', action="store_true",
                        help='Show figure instead of saving saving')

    parser.add_argument('-asc', '--asciifiles', type=str,
                        help='read old pystat results stored in ascii files')

    args = parser.parse_args()

    # -- some settings
    start_date = mysub.str2date(args.sdate)
    end_date = mysub.str2date(args.edate)

    # -- channel selection
    if args.channel is None:
        cha_list = mysub.get_channel_list()
    else:
        cha_list = [args.channel]

    # -- time selection
    if args.time is None:
        sel_list = mysub.get_pystat_select_list()
    else:
        sel_list = [args.time]

    # -- satellite selection
    if args.satellites is None:
        sat_list = mysub.get_satellite_list()
    else:
        sat_list = args.satellites

    # -- target selection
    if args.target == 'global':
        target_plt_name = "Time Series Plot"
    else:
        target_plt_name = "Latitudinal Plot"

    # -- make output directory if necessary
    if not os.path.exists(args.outdir):
        os.makedirs(args.outdir)

    # -- summary of settings if verbose mode
    if args.verbose:
        logger.info("Parameter passed")
        logger.info("Input Path : %s" % args.dbfile)
        logger.info("Output Path: %s" % args.outdir)
        logger.info("Start Date : %s" % start_date)
        logger.info("End Date   : %s" % end_date)
        logger.info("Channel    : %s" % cha_list)
        logger.info("Time       : %s" % sel_list)
        logger.info("Satellite  : %s" % sat_list)
        logger.info("Target plot: %s" % target_plt_name)
        logger.info("TimeS.LinF : %s" % args.linfit)
        logger.info("Old asciiF : %s" % args.asciifiles)
        logger.info("Show fig   : %s" % args.show_figure)
        logger.info("Verbose    : %s\n" % args.verbose)

    try:
        dbfile = lite.connect(args.dbfile,
                              detect_types=lite.PARSE_DECLTYPES | lite.PARSE_COLNAMES)
        dbfile.row_factory = mysub.dict_factory
        cur = dbfile.cursor()

        plot_results()

        dbfile.close()

        if args.verbose:
            logger.info("{0} finished".format(sys.argv[0]))

    except lite.Error, e:
        logger.info("ERROR {0} ".format(e.args[0]))
        sys.exit(1)
