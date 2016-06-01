#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import os
import sys
import argparse
from subs_plot_sql import blacklisting_histogram
from subs_avhrrgac import full_sat_name, get_satellite_list
from subs_avhrrgac import str2upper
from subs_avhrrgac import pre_blacklist_reasons
from subs_avhrrgac import proc_blacklist_reasons
from subs_avhrrgac import post_blacklist_reasons
from pycmsaf.avhrr_gac.database import AvhrrGacDatabase
from pycmsaf.logger import setup_root_logger

logdir = os.path.join(os.getcwd(),'log')
logger = setup_root_logger(name='root', logdir=logdir, 
                           append=True, logfile=True)


def collect_histo_information( db, btype, bdict, l1bs, xcnt, xlab, ylab, cols, 
                               satname=None ):

    logger.info("--------------------------------------------------------------")
    logger.info("Collect HIST info for {0} ".format(btype))
    logger.info("--------------------------------------------------------------")

    sum_cnt = 0.0

    if btype is 'pre': 
        color = 'Green'
    elif btype is 'proc': 
        color = 'Magenta'
    elif btype is 'post': 
        color = 'Blue'
    else:
        logger.info("{0} not defined".format(btype))
        sys.exit(0)

    # --- each kind of btype
    for key in sorted( bdict ): 
        cnt = 0.0 
        cnt = float( print_changes( db, bdict[key], satname ) )
        sum_cnt += cnt
        freq = 100.0 * ( cnt / l1bs )
        xlab.append(  bdict [key] )
        xcnt.append( int(cnt) )
        ylab.append( freq )
        cols.append( color )

    # --- total of btype
    total_freq = 100.0 * ( sum_cnt / l1bs )
    ylab.append( total_freq )
    xcnt.append( int(sum_cnt) )

    if btype is 'pre': 
        xlab.append( 'PRE-PROC' )
        cols.append( 'Green' )
    elif btype is 'proc': 
        xlab.append( 'PROC' )
        cols.append( 'Magenta' )
    elif btype is 'post': 
        xlab.append( 'POST-PROC' )
        cols.append( 'Blue' )
    else:
        logger.info("{0} not defined".format(btype))
        sys.exit(0)


def print_changes(db, reason, satname=None):
    """
    PRINT results to screen.
    """
    postdict = post_blacklist_reasons()
    sqltxt = "SELECT COUNT(*) FROM vw_std WHERE "
    logtxt = "{0:26s} -> {1:8d} orbits "

    if reason is 'all_l1b':
        sqltxt = sqltxt + "filename is not null "
    elif reason is 'all_blacklisted':
        sqltxt = sqltxt + "filename is not null AND blacklist=1 "
    elif reason is 'all_l1b_white':
        sqltxt = sqltxt + "filename is not null AND blacklist=0 "
    elif reason is 'all_l1c_white':
        sqltxt = sqltxt + "start_time_l1c is not null AND blacklist=0 "
    elif reason is 'all_l1c_missing':
        sqltxt = sqltxt + "start_time_l1c is null AND blacklist=0 "

    elif reason is 'pygac_failed':
        sqltxt = sqltxt + "( start_time_l1c is null AND blacklist=0 "
        if satname:
            sqltxt = sqltxt + "AND satellite_name=\'{sat}\' "
        for key in sorted(postdict):
            if 'wrong' in postdict[key]:
                continue
            if 'indexerror' in postdict[key]:
                continue
            if 'along_track' in postdict[key]:
                continue
            sqltxt = sqltxt + ") OR ( start_time_l1c is null AND "
            sqltxt = sqltxt + "blacklist_reason=\'{0}\' ".format(postdict[key])
            if satname:
                sqltxt = sqltxt + "AND satellite_name=\'{sat}\' "
        sqltxt = sqltxt + ") "

    elif reason is 'redundant':
        sqltxt = sqltxt + "redundant=1 AND blacklist=1 AND blacklist_reason LIKE 'NSS%' "
        logtxt = logtxt + "blacklisted "
    else:
        sqltxt = sqltxt + "blacklist_reason=\'{0}\' ".format(reason)
        logtxt = logtxt + "blacklisted "


    if sqltxt:
        if satname:
            sqltxt = sqltxt + "AND satellite_name=\'{sat}\'"
            logtxt = logtxt + "for {2}"
            cmd = sqltxt.format(sat=satname)
            res = db.execute(cmd)
            num = res[0]['COUNT(*)']
            logger.info(logtxt.format(reason,num,satname))
            return num
        else:
            res = db.execute(sqltxt)
            num = res[0]['COUNT(*)']
            logger.info(logtxt.format(reason,num))
            return num
    else:
        logger.info("{0} case is not yet defined!".format(reason))


if __name__ == '__main__':

    work_dir = os.getcwd()
    work_out = os.path.join(work_dir, 'figures')

    predict = pre_blacklist_reasons()
    procdict = proc_blacklist_reasons()
    postdict = post_blacklist_reasons()

    parser = argparse.ArgumentParser(
        description=('{0} reads the AVHRR GAC L1 SQL database. '
                     'See Usage for more information.').
                     format(os.path.basename(__file__)))

    parser.add_argument('-dbf', '--dbfile', required=True,
                        help='/path/to/database.sqlite3')

    parser.add_argument('-out', '--outputdir', help='default is '+work_out, 
                        default=work_out)

    parser.add_argument('-sat', '--satellite', type=str2upper, 
                        help='Select a specific satellite in combination '
                        'with --black* options. SatList: {0}'.
                        format(get_satellite_list()))

    parser.add_argument('-pre', '--black_pre', action="store_true",
                        help='Select all orbits which have been blacklisted '
                        'before the AVHRR GAC L1c processing due to {0}.'.
                        format(sorted(predict.values())))

    parser.add_argument('-proc', '--black_proc', action="store_true",
                        help='Select all orbits which have been blacklisted '
                        'during the AVHRR GAC L1c processing due to {0}.'.
                        format(sorted(procdict.values())))

    parser.add_argument('-post', '--black_post', action="store_true",
                        help='Select all orbits regarding {0}.'.
                        format(sorted(postdict.values())))

    args = parser.parse_args()

    # create output directory if not existing
    if not os.path.exists(args.outputdir):
        os.makedirs(args.outputdir)

    # -- some screen output if wanted
    if len(sys.argv[1:]) > 0: 
        logger.info("{0}\n".format(sys.argv[1:]))

    # -- connect to database
    dbfile = AvhrrGacDatabase(dbfile=args.dbfile,
                              timeout=36000, exclusive=True)

    # -- get total amount of l1b files
    cmd = "SELECT COUNT(*) FROM vw_std WHERE filename is not null"
    out = dbfile.execute(cmd)
    all_files = float( out[0]['COUNT(*)'] )

    # -- initialize lists for plotting
    x_cnts = list() # counts of blacklisted orbits
    x_axis = list() # frequencies in percentage
    y_axis = list() # types of blacklisting
    colors = list() # color list

    # -- first entry in lists
    x_axis.append('Total')
    y_axis.append(100.0)
    x_cnts.append(int(all_files))
    colors.append('Black')


    # -- collect information for plotting
    if args.black_pre: 
        collect_histo_information( dbfile, 'pre', predict, all_files, 
                x_cnts, x_axis, y_axis, colors, args.satellite )

    if args.black_proc: 
        collect_histo_information( dbfile, 'proc', procdict, all_files, 
                x_cnts, x_axis, y_axis, colors, args.satellite )

    if args.black_post: 
        collect_histo_information( dbfile, 'post', postdict, all_files, 
                x_cnts, x_axis, y_axis, colors, args.satellite )


    # -- last entry in lists
    btotal_cnt = 0
    btotal = 0.0
    for index in range(len(colors)): 
        if x_axis[index].endswith('PROC'):
            btotal += y_axis[index]
            btotal_cnt += x_cnts[index]

    x_axis.append( 'TOTAL' )
    y_axis.append( btotal )
    x_cnts.append( btotal_cnt )
    colors.append( 'Gray' )


    # -- print summary
    logger.info(" === Summery of HIST info === ")

    for index in range(len(colors)):
        logger.info("{0:30s} {1:12.4f} % -> {2:20s}".
                format(x_axis[index], y_axis[index], colors[index]))


    # -- plot blacklisting
    if args.satellite:
        sats = args.satellite
    else:
        sats = "allsats"


    # -- separate into 2 lists

    # (1) plot pre, proc, post histogram
    x1 = list()
    y1 = list()
    c1 = list()
    n1 = list()
    # (2) plot single blacklist reasons
    x2 = list() 
    y2 = list()
    c2 = list()
    n2 = list()

    for i in range(len(x_axis)):
        if x_axis[i].endswith('PROC') or x_axis[i].endswith('TOTAL'):
            x1.append(x_axis[i])
            y1.append(y_axis[i])
            c1.append(colors[i])
            n1.append(x_cnts[i])
        else:
            if x_axis[i].startswith('Total'):
                continue
            x2.append(x_axis[i])
            y2.append(y_axis[i])
            c2.append(colors[i])
            n2.append(x_cnts[i])


    if args.black_pre and args.black_proc and args.black_post: 
        outfile = os.path.join(args.outputdir, "HISTO_total-gac-blacklisting_"+sats+".png") 
        blacklisting_histogram( n1, x1, y1, c1, 0.8, outfile, args.satellite )

    if args.black_pre and not args.black_proc and not args.black_post: 
        outfile = os.path.join(args.outputdir, "HISTO_gac-pre-blacklisting_"+sats+".png")
    elif args.black_proc and not args.black_pre and not args.black_post: 
        outfile = os.path.join(args.outputdir, "HISTO_gac-proc-blacklisting_"+sats+".png")
    elif args.black_post and not args.black_pre and not args.black_proc: 
        outfile = os.path.join(args.outputdir, "HISTO_gac-post-blacklisting_"+sats+".png")
    else:
        outfile = os.path.join(args.outputdir, "HISTO_gac-all-blacklisting_"+sats+".png")

    blacklisting_histogram( n2, x2, y2, c2, 0.8, outfile, args.satellite )


    logger.info("%s finished\n\n" % os.path.basename(__file__))
