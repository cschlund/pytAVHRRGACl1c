#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# C.Schlundt: December, 2015
#

import os
import sys
import argparse
import sqlite3 as lite
import matplotlib
#matplotlib.use('GTK3Agg')
import subs_avhrrgac as subs
import subs_plot_sql as psql
from pycmsaf.avhrr_gac.database import AvhrrGacDatabase
from pycmsaf.logger import setup_root_logger

logdir = os.path.join(os.getcwd(),'log')
logger = setup_root_logger(name='root', logdir=logdir, 
                           append=True, logfile=True)

out_dir = os.path.join(os.getcwd(),'plots')
satlist = '|'.join(subs.get_satellite_list())

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='''{{0}}
    displays the missing scanlines per day per satellite. '''.
    format(os.path.basename(__file__)))

    parser.add_argument('-db', '--dbfile', type=str, required=True,
                        help='String, i.e. /path/to/db.sqlite3')

    parser.add_argument('-sat', '--satellites', type=subs.str2upper, 
                        nargs='*', help='Satellites: ' + satlist)

    parser.add_argument('-out', '--outdir', type=str, default=out_dir,
                        help='Default is: ' + out_dir)

    parser.add_argument('-sd', '--sdate', type=subs.datestring,
                        default='1978-01-01', 
                        help='Start Date, default is 1978-01-01')

    parser.add_argument('-ed', '--edate', type=subs.datestring,
                        default='2016-01-01', 
                        help='End Date, default is 2016-01-01')

    parser.add_argument('-ver', '--verbose', action="store_true",
                        help='increase output verbosity')

    parser.add_argument('-show', '--show_figure', action="store_true",
                        help='Show figure instead of saving saving')

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

    db = AvhrrGacDatabase(dbfile=args.dbfile) 

    for sat in sat_list: 
        psql.plot_miss_scls(db, args.outdir, start_date, end_date, 
                            sat, args.verbose, args.show_figure)

    logger.info("*** {0} succesfully finished\n\n".format(sys.argv[0]))
