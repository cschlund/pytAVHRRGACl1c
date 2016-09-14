#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# C.Schlundt: November, 2014
#

import os
import sys
import argparse
import sqlite3 as lite
import matplotlib
#matplotlib.use('GTK2Agg')
import subs_avhrrgac as subs
import subs_plot_sql as psql

from pycmsaf.logger import setup_root_logger

logdir = os.path.join(os.getcwd(),'log')
logger = setup_root_logger(name='root', logdir=logdir, logfile=True)

satlist = '|'.join(subs.get_satellite_list())

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='''{{0}}
    displays AVHRR/NOAAs equator crossing time (local time of ascending node),
    which is stored in the AVHRR GAC archive database
    after L1c processing.'''.format(os.path.basename(__file__)))

    parser.add_argument('-db', '--dbfile', type=str, required=True,
                        help='String, i.e. /path/to/db.sqlite3')

    parser.add_argument('-out', '--outdir', type=str, required=True,
                        help='String, i.e. /path/to/plot_out/')

    parser.add_argument('-sd', '--sdate', type=subs.datestring,
                        default='1979-01-01', help='String, Start Date')

    parser.add_argument('-ed', '--edate', type=subs.datestring,
                        default='2017-01-01', help='String, End Date')

    parser.add_argument('-sats', '--satellites', type=subs.str2upper, nargs='*',
                        help='Satellite, available: ' + satlist)

    parser.add_argument('-ign', '--ignore', type=subs.str2upper, 
                        nargs='*', help='Satellite, which are ignored ')

    parser.add_argument('-cci', '--cci_sensors', action="store_true", 
                        help='Cloud_cci instruments')

    parser.add_argument('-pri', '--primes', action="store_true", 
                        help='Plot only prime time range')

    parser.add_argument('-ver', '--verbose', action="store_true",
                        help='increase output verbosity')

    parser.add_argument('-show', '--show_figure', action="store_true",
                        help='Show figure instead of saving saving')

    parser.add_argument('-leg', '--plot_legend', action="store_true",
                        help='Plot legend instead of inline text.')

    args = parser.parse_args()

    # -- some settings
    start_date = subs.str2date(args.sdate)
    end_date = subs.str2date(args.edate)

    # -- satellite selection
    if args.satellites is None:
        sat_list = subs.get_satellite_list()
    else:
        sat_list = args.satellites

    # -- cloud_cci settings
    if args.cci_sensors:
        sat_list = subs.get_cci_satellite_list()

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
        logger.info("Satellite  : %s" % sat_list)
        logger.info("Cloud_cci  : %s" % args.cci_sensors)
        logger.info("CCI primes : %s" % args.primes)
        logger.info("Don't plot : %s" % args.ignore)
        logger.info("Show fig   : %s" % args.show_figure)
        logger.info("Plot legend: %s" % args.plot_legend)
        logger.info("Verbose    : %s\n" % args.verbose)

    try:
        dbfile = lite.connect(args.dbfile,
                              detect_types=lite.PARSE_DECLTYPES | lite.PARSE_COLNAMES)

        dbfile.row_factory = subs.dict_factory

        cur = dbfile.cursor()

        psql.plot_avhrr_ect_results(dbfile, args.outdir,
                                    start_date, end_date,
                                    sat_list, args.ignore,
                                    args.cci_sensors,
                                    args.primes,
                                    args.verbose,
                                    args.show_figure,
                                    args.plot_legend)

        dbfile.close()

        if args.verbose:
            logger.info("{0} finished".format(sys.argv[0]))

    except lite.Error, e:
        logger.info("ERROR {0}".format(e.args[0]))
        sys.exit(1)
