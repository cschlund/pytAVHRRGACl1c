#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# C.Schlundt: November, 2014
#

import os
import sys
import argparse
import sqlite3 as lite

import subs_avhrrgac as subs
import subs_plot_sql as psql

from pycmsaf.logger import setup_root_logger

logger = setup_root_logger(name='root')

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
                        default='1981-01-01', help='String, Start Date')

    parser.add_argument('-ed', '--edate', type=subs.datestring,
                        default='2015-01-01', help='String, End Date')

    parser.add_argument('-sats', '--satellites', type=subs.str2upper, nargs='*',
                        help='Satellite, available: ' + satlist)

    parser.add_argument('-ver', '--verbose', action="store_true",
                        help='increase output verbosity')

    parser.add_argument('-show', '--show_figure', action="store_true",
                        help='Show figure before saving')

    args = parser.parse_args()

    # -- some settings
    start_date = subs.str2date(args.sdate)
    end_date = subs.str2date(args.edate)

    # -- satellite selection
    if args.satellites is None:
        sat_list = subs.get_satellite_list()
    else:
        sat_list = args.satellites

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
        logger.info("Show fig   : %s" % args.show_figure)
        logger.info("Verbose    : %s\n" % args.verbose)

    try:
        dbfile = lite.connect(args.dbfile,
                              detect_types=lite.PARSE_DECLTYPES | lite.PARSE_COLNAMES)

        dbfile.row_factory = subs.dict_factory

        cur = dbfile.cursor()

        psql.plot_avhrr_ect_results(dbfile, args.outdir,
                                    start_date, end_date,
                                    sat_list, args.verbose,
                                    args.show_figure)

        dbfile.close()

        if args.verbose:
            logger.info("{0} finished".format(sys.argv[0]))

    except lite.Error, e:
        logger.info("ERROR {0}".format(e.args[0]))
        sys.exit(1)