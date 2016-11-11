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

logdir = os.path.join(os.getcwd(), 'log')
logger = setup_root_logger(name='root', logdir=logdir, append=True, logfile=True)


def plot_results(dbcursor, params, start_date, end_date):
    """
    Plot PySTAT results.
    :param dbcursor: sqlite cursor
    :param params: passed arguments
    :param start_date: first date to be considered
    :param end_date: last date to be considered
    :return:
    """
    if params.channel_difference:
        if len(params.channels) == 2:
            for select in params.times:
                logger.info("PySTAT channel difference between "
                            "{c1} and {c2} for {sat_list} and {sza_time}".format(c1=params.channels[0],
                                                                                 c2=params.channels[1],
                                                                                 sat_list=params.satellites,
                                                                                 sza_time=select))

                psql.pystat_channel_difference(cha_list=params.channels, sat_list=params.satellites,
                                               sza_time=select, cursor=dbcursor, out_path=params.outdir,
                                               sdate=start_date, edate=end_date,
                                               linesty=params.linestyle)
            return
        else:
            logger.info("Provide two channels using --channels argument")
            return
    else:
        for channel in params.channels:
            for select in params.times:
                if params.target == 'global':
                    if params.linfit:
                        psql.plot_time_series_linfit(sat_list=params.satellites,
                                                     channel=channel, select=select,
                                                     start_date=start_date, end_date=end_date,
                                                     outpath=params.outdir, cursor=dbcursor,
                                                     show_fig=params.show_figure)
                    else:
                        psql.plot_time_series(sat_list=params.satellites,
                                              channel=channel, select=select,
                                              start_date=start_date, end_date=end_date,
                                              outpath=params.outdir, cursor=dbcursor,
                                              verbose=params.verbose,
                                              show_fig=params.show_figure,
                                              linesty=params.linestyle)
                else:
                    psql.plot_zonal_results(sat_list=params.satellites,
                                            channel=channel, select=select,
                                            start_date=start_date, end_date=end_date,
                                            outpath=params.outdir, cur=dbcursor, target=params.target,
                                            verbose=params.verbose, show_fig=params.show_figure)
        return


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='''%s displays pystat results, i.e. daily global and
    zonal means and standard deviations stored in a sqlite database.''' % os.path.basename(__file__))

    parser.add_argument('-db', '--dbfile', type=str, required=True,
                        help='String, e.g. /path/to/db.sqlite3')

    parser.add_argument('-out', '--outdir', type=str, required=True,
                        help='Path, e.g. /path/to/plot.png')

    parser.add_argument('-sd', '--start_date', type=mysub.datestring, default='1979-01-01',
                        help='Start Date, e.g. 2009-01-01')

    parser.add_argument('-ed', '--end_date', type=mysub.datestring, default='2017-01-01',
                        help='End Date, e.g. 2012-12-31')

    parser.add_argument('-cha', '--channels', type=str, nargs='*', default=mysub.get_channel_list(),
                        help='Channel abbreviation, available: ' + '|'.join(mysub.get_channel_list()))

    parser.add_argument('-tim', '--times', type=str, nargs='*', default=mysub.get_pystat_select_list(),
                        help='Time abbreviation, available: ' + '|'.join(mysub.get_pystat_select_list()))

    parser.add_argument('-sat', '--satellites', type=mysub.str2upper, nargs='*',
                        default=mysub.get_satellite_list(),
                        help='Satellite, available: ' + '|'.join(mysub.get_satellite_list()))

    parser.add_argument('-tar', '--target', type=str, default='global',
                        help='''-tar global (default) plots global daily statistics (time series).
                        -tar zonal plots zonal daily statistics per date/channel/satellite/time selection.
                        -tar zonalall plots all available satellite zonal statistics per date/channel/time.''')

    parser.add_argument('-fit', '--linfit', action="store_true",
                        help='''If you want to plot a time series including a
                        linear regression (plot per satellite/channel/time).''')

    parser.add_argument('-ver', '--verbose', action="store_true", help='increase output verbosity')

    parser.add_argument('-show', '--show_figure', action="store_true", help='Show figure.')

    parser.add_argument('-cdiff', '--channel_difference', action="store_true",
                        help='e.g. -cdiff -cha ch4 ch5 [only for -tar global].')

    parser.add_argument('--linestyle', default='-', help='Default is \'-\' ')

    args = parser.parse_args()

    # -- some settings
    sdate = mysub.str2date(args.start_date)
    edate = mysub.str2date(args.end_date)

    # -- make output directory if necessary
    if not os.path.exists(args.outdir):
        os.makedirs(args.outdir)

    # -- some screen output if wanted
    if len(sys.argv[1:]) > 0: 
        logger.info("{0}\n".format(sys.argv[1:]))

    # -- open SQL file and plot data
    try:
        dbfile = lite.connect(args.dbfile, detect_types=lite.PARSE_DECLTYPES | lite.PARSE_COLNAMES)
        dbfile.row_factory = mysub.dict_factory
        cur = dbfile.cursor()

        plot_results(dbcursor=cur, params=args, start_date=sdate, end_date=edate)

        dbfile.close()
        logger.info("{0} successfully finished\n\n".format(sys.argv[0]))

    except lite.Error, e:
        logger.info("ERROR {0} \n\n".format(e.args[0]))
        sys.exit(1)
