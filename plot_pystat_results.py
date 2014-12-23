#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# C.Schlundt: November, 2014
#

import os
import sys
import argparse
import datetime
import numpy as np
import sqlite3 as lite
import subs_avhrrgac as mysub
import subs_plot_sql as psql

chalist = '|'.join(mysub.get_channel_list())
sellist = '|'.join(mysub.get_select_list())
satlist = '|'.join(mysub.get_satellite_list())


# -------------------------------------------------------------------
def plot_results(): 

    for channel in cha_list: 
        for select in sel_list: 
            if args.target == 'global': 

                psql.plot_time_series(sat_list, channel, select, 
                        start_date, end_date, args.outdir,
                        cur, args.verbose, args.asciifiles) 

                if args.linfit == True: 
                    psql.plot_time_series_linfit(sat_list, channel, 
                        select, start_date, end_date, args.outdir,
                        cur, args.verbose) 

            else: 

                    psql.plot_zonal_results(sat_list, channel, 
                        select, start_date, end_date, args.outdir,
                        cur, args.target, args.verbose) 

    return

# -------------------------------------------------------------------

if __name__ == '__main__': 

    parser = argparse.ArgumentParser(description='''%s 
    displays pystat results, i.e. daily global and
    zonal means and standard deviations stored in a sqlite
    database.''' % os.path.basename(__file__))
    
    parser.add_argument('-db', '--dbfile', type=str, required=True,
                        help='String, e.g. /path/to/db.sqlite3')
    parser.add_argument('-out', '--outdir', type=str, required=True,
                        help='Path, e.g. /path/to/plot.png')
    parser.add_argument('-sd',  '--sdate', type=mysub.datestring, 
                        required=True, help='Start Date, e.g. 2009-01-01')
    parser.add_argument('-ed',  '--edate', type=mysub.datestring, 
                        required=True, help='End Date, e.g. 2012-12-31')
    parser.add_argument('-cha', '--channel', type=str, 
                        help='Channel abbreviation, available: '+chalist)
    parser.add_argument('-tim', '--time', type=str, 
                        help='Time abbreviation, available: '+sellist)
    parser.add_argument('-sat', '--satellite', type=mysub.lite_sat_string, 
                        help='Satellite, available: '+satlist)
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
    parser.add_argument('-asc', '--asciifiles', type=str, 
                        help='read old pystat results stored in ascii files')
    
    args = parser.parse_args()
    

    # ---------------------------------------------------------------
    # -- some settings
    
    start_date = mysub.str2date(args.sdate)
    end_date   = mysub.str2date(args.edate)
    
    if args.channel == None:
        cha_list  = mysub.get_channel_list()
    else:
        cha_list = [args.channel]
    
    if args.time == None:
        sel_list  = mysub.get_select_list()
    else:
        sel_list = [args.time]
    
    if args.satellite == None:
        sat_list  = mysub.get_satellite_list()
    else:
        sat_list = [args.satellite]
    
    
    if args.target == 'global':
        target_plt_name = "Time Series Plot"
    else:
        target_plt_name = "Latitudinal Plot"
    
    
    if not os.path.exists(args.outdir):
        os.makedirs(args.outdir)
    

    # ---------------------------------------------------------------
    # -- summary of settings if verbose mode
    
    if args.verbose == True: 
        print ("\n *** Parameter passed" )
        print (" ---------------------- ")
        print ("   - Input Path : %s" % args.dbfile)
        print ("   - Output Path: %s" % args.outdir)
        print ("   - Start Date : %s" % start_date)
        print ("   - End Date   : %s" % end_date)
        print ("   - Channel    : %s" % cha_list)
        print ("   - Time       : %s" % sel_list)
        print ("   - Satellite  : %s" % sat_list)
        print ("   - Target plot: %s" % target_plt_name)
        print ("   - TimeS.LinF : %s" % args.linfit)
        print ("   - Old asciiF : %s" % args.asciifiles)
        print ("   - Verbose    : %s\n" % args.verbose)
    

    # ---------------------------------------------------------------
    # -- connect to database
    try:
        con = lite.connect(args.dbfile,
                detect_types=lite.PARSE_DECLTYPES|lite.PARSE_COLNAMES)
        con.row_factory = mysub.dict_factory
        cur = con.cursor()
    
        plot_results()
    
    except lite.Error, e:
        print ("\n *** Error %s ***\n" % e.args[0])
        sys.exit(1)
    
    finally:
        if con:
            con.close()
            if args.verbose == True: 
                print ( "\n *** %s finished \n" % (sys.argv[0]) )
    
    # -------------------------------------------------------------------
