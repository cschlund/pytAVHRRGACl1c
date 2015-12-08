#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import os
import sys
import argparse
import datetime
import time
import post_blacklist as pb
from subs_avhrrgac import full_sat_name, get_satellite_list
from subs_avhrrgac import str2upper, lite_satstring
from subs_avhrrgac import pre_blacklist_reasons
from subs_avhrrgac import proc_blacklist_reasons
from subs_avhrrgac import post_blacklist_reasons
from pycmsaf.avhrr_gac.database import AvhrrGacDatabase
from pycmsaf.logger import setup_root_logger

logdir = os.path.join(os.getcwd(),'log')
logger = setup_root_logger(name='root', logdir=logdir, 
                           append=True, logfile=True)


def print_verbose(sqlres): 
    for r in sqlres:
        logger.info("Satellite     : {0}".format(r['satellite_name']))
        logger.info("L1b Filename  : {0}".format(r['filename']))
        logger.info("start_time_l1b: {0}".format(r['start_time_l1b']))
        logger.info("start_time_l1c: {0}".format(r['start_time_l1c']))
        logger.info("end_time_l1b  : {0}".format(r['end_time_l1b']))
        logger.info("end_time_l1c  : {0}".format(r['end_time_l1c']))
        logger.info("along_track   : {0}".format(r['along_track']))
        #logger.info("across_track  : {0}".format(r['across_track']))
        logger.info("blacklist     : {0}".format(r['blacklist']))
        logger.info("black_reason  : {0}\n".format(r['blacklist_reason']))


def print_changes(db, reason, satname=None):
    """
    PRINT results to screen.
    """
    sqltxt = "SELECT COUNT(*) FROM vw_std WHERE "
    logtxt = "{0:24s} -> {1:8d} orbits "

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
            for sat in satname:
                cmd = sqltxt.format(sat=sat)
                res = db.execute(cmd)
                num = res[0]['COUNT(*)']
                logger.info(logtxt.format(reason,num,sat))
        else:
            res = db.execute(sqltxt)
            num = res[0]['COUNT(*)']
            logger.info(logtxt.format(reason,num))
    else:
        logger.info("{0} case is not yet defined!".format(reason))



def blacklist_wrong_ydim(db, ver):
    """
    Diana Stein found these L1b orbits during CLARA-A2
    processing, where pyGAC provided L1c files where the
    along_track dimension is too large, i.e. ydim too long.
    """
    black_reason, blist = pb.list_along_track_too_long()
    
    for i in blist:
        upd = "UPDATE orbits SET blacklist=1, " \
              "blacklist_reason=\'{blr}\' " \
              "WHERE filename=\'{fil}\' and blacklist=0 "
        db.execute(upd.format(blr=black_reason, fil=i))

    if ver:
        cmd = "SELECT * FROM orbits WHERE " \
              "blacklist_reason=\'{blr}\' "
        res = db.execute(cmd.format(blr=black_reason))
        print_verbose(res) 

    print_changes(db, black_reason)
    logger.info("COMMIT CHANGES FOR \'{0}\'\n".format(black_reason))
    db.commit_changes()


def blacklist_pygac_indexerror(db, ver):
    """
    pyGAC resulted in IndexError: index out of bounds
    pyGAC failed on these orbits after pyGAC was updated
    w.r.t. NOAA-7 and NOAA-9 clock drift error correction
    has been enabled,
    i.e. pyGAC was successful on these orbits when
    clock drift error correction was disabled, which explains
    the existing L1c timestamps. 
    However, these L1c orbits are not in the ECFS archive,
    therefore, they have to be blacklisted!
    """
    black_reason, blist = pb.list_indexerror()

    for i in blist:
        upd = "UPDATE orbits SET blacklist=1, " \
              "blacklist_reason=\'{blr}\' " \
              "WHERE filename=\'{fil}\' and blacklist=0 "
        db.execute(upd.format(blr=black_reason, fil=i))

    if ver:
        cmd = "SELECT * FROM vw_std WHERE " \
              "blacklist_reason=\'{blr}\' "
        res = db.execute(cmd.format(blr=black_reason))
        print_verbose(res) 

    print_changes(db, black_reason)
    logger.info("COMMIT CHANGES FOR \'{0}\'\n".format(black_reason))
    db.commit_changes()


def blacklist_bad_l1c_quality(db, ver):
    """
    Blacklist all dates between sdate and edate for given satellite.
    See post_blacklistings.py for details.
    """
    black_reason, bdict = pb.list_bad_l1c_quality()

    for satkey in bdict.keys(): 

        sdate = bdict[satkey]["sdate"]
        edate = bdict[satkey]["edate"]
        satid = db._get_id_by_name(table='satellites', name=satkey)

        logger.info("Blacklist orbits between {0} & {1} for {2}".
                format(sdate, edate, satkey))

        upd = "UPDATE orbits SET blacklist=1, blacklist_reason=\'{blr}\' " \
              "WHERE satellite_id = \'{satid}\' AND " \
              "redundant=0 AND blacklist=0 AND " \
              "start_time_l1b BETWEEN \'{sdate}\' AND \'{edate}\' "
        db.execute(upd.format(blr=black_reason, satid=satid,
                              sdate=sdate, edate=edate))

        if ver: 
            cmd = "SELECT * from vw_std WHERE " \
                  "satellite_id = \'{satid}\' AND " \
                  "blacklist_reason=\'{blr}\' AND " \
                  "start_time_l1b BETWEEN \'{sdate}\' AND \'{edate}\' " \
                  "ORDER BY start_time_l1b"
            res = db.execute(cmd.format(satid=satid,blr=black_reason,
                                        sdate=sdate,edate=edate))
            print_verbose(res) 

        print_changes(db, black_reason, [satkey])
        logger.info("COMMIT CHANGES FOR \'{0}\'\n".format(black_reason))
        db.commit_changes()


def blacklist_ch3a_zero_reflectance(db, ver):
    """
    There are periods where channel 3a is active
    but contains only zero reflectances.
    """
    black_reason, bdict = pb.list_ch3a_zero_reflectance()

    for idx in bdict.keys():
        for satkey in bdict[idx]:
            sdate = bdict[idx][satkey]["sdate"]
            edate = bdict[idx][satkey]["edate"]
            satid = db._get_id_by_name(table='satellites', name=satkey)

            logger.info("Blacklist orbits between {0} & {1} for {2}".
                    format(sdate, edate, satkey))

            upd = "UPDATE orbits SET blacklist=1, blacklist_reason=\'{blr}\' " \
                  "WHERE satellite_id = \'{satid}\' AND " \
                  "redundant=0 AND blacklist=0 AND " \
                  "start_time_l1b BETWEEN \'{sdate}\' AND \'{edate}\' "
            db.execute(upd.format(blr=black_reason, satid=satid,
                                  sdate=sdate, edate=edate))

            if ver: 
                cmd = "SELECT * from vw_std WHERE " \
                      "satellite_id = \'{satid}\' AND " \
                      "blacklist_reason=\'{blr}\' AND " \
                      "start_time_l1b BETWEEN \'{sdate}\' AND \'{edate}\' " \
                      "ORDER BY start_time_l1b"
                res = db.execute(cmd.format(satid=satid,blr=black_reason,
                                            sdate=sdate,edate=edate))
                print_verbose(res) 

            print_changes(db, black_reason, [satkey])
            logger.info("COMMIT CHANGES FOR \'{0}\'\n".format(black_reason))
            db.commit_changes()



def blacklist_no_valid_l1c_data(db, ver):
    """
    Blacklist all orbits of days, where no valid L1c data available
    """
    black_reason, bdict, slist = pb.list_no_valid_l1c_data()

    for sat in slist:
        try:
            date_list = bdict[sat]
            sat_id = db._get_id_by_name(table='satellites', name=sat)

            for yyyymm in bdict[sat]:
                y = int(yyyymm[0:4])
                m = int(yyyymm[4:])
                days = bdict[sat][yyyymm]

                for d in days:
                    dt = datetime.date(y,m,d)
                    # blacklist all orbits between
                    # start_date <= start_time_l1c <= end_date
                    start_time = datetime.datetime(y, m, d, 0, 0, 0)
                    end_time = datetime.datetime(y, m, d, 23, 59, 59)

                    upd = "UPDATE orbits SET " \
                          "blacklist=1, blacklist_reason=\'{blr}\' " \
                          "WHERE blacklist=0 AND " \
                          "satellite_id=\'{sat_id}\' AND " \
                          "start_time_l1b BETWEEN " \
                          "\'{start_time}\' AND \'{end_time}\' "
                    db.execute(upd.format(blr=black_reason, sat_id=sat_id,
                                          start_time=start_time, 
                                          end_time=end_time))

                    # security check
                    cmd = "SELECT COUNT(*) from orbits " \
                          "WHERE satellite_id=\'{sat_id}\' AND " \
                          "start_time_l1b BETWEEN " \
                          "\'{start_time}\' AND \'{end_time}\' " \
                          "ORDER BY start_time_l1b"
                    res = db.execute(cmd.format(sat_id=sat_id,
                                                start_time=start_time,
                                                end_time=end_time,
                                                blr=black_reason))
                    numcheck = res[0]['COUNT(*)']
                    if numcheck == 0: 
                        logger.info("WARNING: No orbits={0} for {1} and {2}".
                                format(numcheck,dt,sat))

                    if ver:
                        cmd = "SELECT * from vw_std " \
                              "WHERE satellite_id=\'{sat_id}\' AND " \
                              "blacklist_reason=\'{blr}\' AND " \
                              "start_time_l1b BETWEEN " \
                              "\'{start_time}\' AND \'{end_time}\' " \
                              "ORDER BY start_time_l1b"
                        res = db.execute(cmd.format(sat_id=sat_id,
                                                    start_time=start_time,
                                                    end_time=end_time,
                                                    blr=black_reason))
                        print_verbose(res) 

            print_changes(db, black_reason,[sat])

        except KeyError:
            if ver:
                logger.info("Nothing to blacklist for {0}!".format(sat))
            pass

    print_changes(db, black_reason)
    logger.info("COMMIT CHANGES FOR \'{0}\'\n".format(black_reason))
    db.commit_changes()


def blacklist_wrong_l1c_timestamp(db, ver):
    """
    Black list single orbits.
    """
    black_reason, blist = pb.list_wrong_l1c_timestamp()

    for fil in blist:

        splits = fil.split('.')

        upd = "UPDATE orbits SET blacklist=1, " \
              "blacklist_reason=\'{blr}\' WHERE filename=\'{fil}\'"
        db.execute(upd.format(fil=fil, blr=black_reason))

        if ver:
            cmd = "SELECT * FROM vw_std WHERE filename=\'{fil}\' "
            res = db.execute(cmd.format(fil=fil))
            print_verbose(res) 

    print_changes(db, black_reason)
    logger.info("COMMIT CHANGES FOR \'{0}\'\n".format(black_reason))
    db.commit_changes()


if __name__ == '__main__':

    predict = pre_blacklist_reasons()
    procdict = proc_blacklist_reasons()
    postdict = post_blacklist_reasons()

    parser = argparse.ArgumentParser(
        description=('{0} reads the AVHRR GAC L1 SQL database. '
                     'See Usage for more information.').
                     format(os.path.basename(__file__)))

    parser.add_argument('-d', '--dbfile', required=True,
                        help='/path/to/database.sqlite3')

    parser.add_argument('-v', '--verbose', action="store_true",
                        help='increase output verbosity')

    parser.add_argument('-s', '--satellites', type=str2upper, nargs='*',
                        help='Select a specific satellite in combination '
                        'with --show* options. SatList: {0}'.
                        format(get_satellite_list()))

    parser.add_argument('-a', '--show_all', action="store_true",
                        help='SHOW all whitelisted L1b orbits. '
                        'USE ORIGINAL SQL: AVHRR_GAC_archive.sqlite3')

    parser.add_argument('-b', '--show_all_blacklisted', action="store_true",
                        help='SHOW all blacklisted L1b+L1c orbits. '
                        'USE ORIGINAL SQL: AVHRR_GAC_archive.sqlite3')

    parser.add_argument('-wb', '--show_l1b_whitelist', action="store_true",
                        help='SHOW all whitelisted L1b orbits. '
                        'USE ORIGINAL SQL: AVHRR_GAC_archive.sqlite3')

    parser.add_argument('-wc', '--show_l1c_whitelist', action="store_true",
                        help='SHOW all whitelisted L1c orbits.')

    parser.add_argument('-mc', '--show_l1c_missing', action="store_true",
                        help='SHOW all whitelisted L1c orbits which are missing '
                        'because pyGAC failed.')

    parser.add_argument('-pre', '--show_pre', action="store_true",
                        help='SHOW all orbits which have been blacklisted '
                        'before the AVHRR GAC L1c processing due to {0}.'.
                        format(sorted(predict.values())))

    parser.add_argument('-proc', '--show_proc', action="store_true",
                        help='SHOW all orbits which have been blacklisted '
                        'during the AVHRR GAC L1c processing due to {0}.'.
                        format(sorted(procdict.values())))

    parser.add_argument('-post', '--show_post', action="store_true",
                        help='SHOW all orbits regarding {0}.'.
                        format(sorted(postdict.values())))

    parser.add_argument('-ts', '--wrong_l1c_timestamp', action="store_true",
                        help='''Blacklist L1b files getting wrong l1c timestamp.
                             This must be done before GAC_overlap.py, otherwise 
                             it will be aborted due to ambiguous entries.''')

    parser.add_argument('-no', '--no_valid_l1c_data', action="store_true", 
                        help='''Blacklist days, where no valid l1c data is available. 
                             ATTENTION: only if you know what you are doing! 
                             The list_of_invalid_days changes from processing 
                             to processing due to pygac updates.''')

    parser.add_argument('-bad', '--bad_l1c_quality', action="store_true",
                        help='''Blacklist all days between specified sdate and 
                        edate for a specific satellite. 
                        See post_blacklistings.py for details.''')

    parser.add_argument('-ydim', '--along_track_too_long', action="store_true",
                        help='''Diana found during CLARA-A2 processing orbits, 
                        which are too long in the along_track dimension.''')

    parser.add_argument('-ie', '--pygac_indexerror', action="store_true",
                        help="pyGAC provided an IndexError, i.e no L1c orbit.")

    parser.add_argument('-ch3a', '--ch3a_zero_reflectance', action="store_true",
                        help="Channel 3a is active but has zero reflectances.")

    args = parser.parse_args()

    # -- consider all satellites
    if args.satellites:
        if 'ALL' in args.satellites:
            satlist = get_satellite_list()
        else:
            satlist = args.satellites
    else:
        satlist = args.satellites

    # -- some screen output if wanted
    if len(sys.argv[1:]) > 0: 
        logger.info("{0}\n".format(sys.argv[1:]))

    # -- connect to database
    dbfile = AvhrrGacDatabase(dbfile=args.dbfile,
                              timeout=36000, exclusive=True)

    # -- blacklisting
    if args.wrong_l1c_timestamp: 
        blacklist_wrong_l1c_timestamp(dbfile, args.verbose)
    if args.no_valid_l1c_data: 
        blacklist_no_valid_l1c_data(dbfile, args.verbose)
    if args.bad_l1c_quality: 
        blacklist_bad_l1c_quality(dbfile, args.verbose)
    if args.along_track_too_long: 
        blacklist_wrong_ydim(dbfile, args.verbose)
    if args.pygac_indexerror:
        blacklist_pygac_indexerror(dbfile, args.verbose)
    if args.ch3a_zero_reflectance:
        blacklist_ch3a_zero_reflectance(dbfile, args.verbose)


    # -- show total listing
    if args.show_all: 
        print_changes(dbfile, 'all_l1b', satlist)
    if args.show_all_blacklisted: 
        print_changes(dbfile, 'all_blacklisted', satlist)
    if args.show_l1b_whitelist: 
        print_changes(dbfile, 'all_l1b_white', satlist)
    if args.show_l1c_whitelist: 
        print_changes(dbfile, 'all_l1c_white', satlist)
    if args.show_l1c_missing: 
        print_changes(dbfile, 'all_l1c_missing', satlist) 

    # -- show blacklistings
    sumup = 0
    if args.show_pre: 
        for key in sorted(predict):
            print_changes(dbfile, predict[key], satlist)
    if args.show_proc: 
        for key in sorted(procdict):
            print_changes(dbfile, procdict[key], satlist)
    if args.show_post: 
        for key in sorted(postdict):
            print_changes(dbfile, postdict[key], satlist)

    logger.info("%s finished\n\n" % os.path.basename(__file__))
