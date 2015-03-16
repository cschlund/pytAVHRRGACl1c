#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import os
import argparse
import datetime
from subs_avhrrgac import full_sat_name, get_satellite_list
from pycmsaf.avhrr_gac.database import AvhrrGacDatabase
from pycmsaf.logger import setup_root_logger

logger = setup_root_logger(name='root')


def blacklist_days():
    """
    List of days, which did not provide any reasonable L1c orbits.
    """
    bdict = dict()
    bdict["blacklist_reason"] = "corrupt_l1b_data"

    bdict["NOAA7"] = [datetime.date(1983, 7, 27), datetime.date(1983, 7, 28),
                      datetime.date(1983, 7, 29), datetime.date(1983, 7, 30),
                      datetime.date(1983, 7, 31), datetime.date(1983, 8, 1),
                      datetime.date(1983, 8, 2), datetime.date(1983, 8, 6),
                      datetime.date(1983, 9, 21), datetime.date(1983, 9, 22),
                      datetime.date(1983, 9, 23), datetime.date(1983, 9, 24),
                      datetime.date(1983, 9, 25), datetime.date(1983, 9, 26),
                      datetime.date(1984, 7, 23)]

    # bdict["NOAA9"] nothing to blacklist

    bdict["NOAA11"] = [datetime.date(1989, 4, 19), datetime.date(1989, 4, 20),
                       datetime.date(1989, 4, 21), datetime.date(1989, 4, 22),
                       datetime.date(1989, 4, 23), datetime.date(1989, 4, 24),
                       datetime.date(1989, 4, 25), datetime.date(1989, 4, 26),
                       datetime.date(1989, 4, 27), datetime.date(1989, 4, 28),
                       datetime.date(1989, 4, 29), datetime.date(1989, 4, 30),
                       datetime.date(1989, 5, 1), datetime.date(1989, 5, 2),
                       datetime.date(1989, 5, 3), datetime.date(1989, 5, 4),
                       datetime.date(1989, 5, 5), datetime.date(1994, 7, 1),
                       datetime.date(1994, 7, 2), datetime.date(1994, 7, 3),
                       datetime.date(1994, 7, 4), datetime.date(1994, 7, 5),
                       datetime.date(1994, 7, 6), datetime.date(1994, 7, 7),
                       datetime.date(1994, 7, 8), datetime.date(1994, 7, 9),
                       datetime.date(1994, 7, 10), datetime.date(1994, 7, 11),
                       datetime.date(1994, 7, 12), datetime.date(1994, 7, 13),
                       datetime.date(1994, 7, 14), datetime.date(1994, 7, 15),
                       datetime.date(1994, 7, 16), datetime.date(1994, 7, 17),
                       datetime.date(1994, 7, 18), datetime.date(1994, 7, 19),
                       datetime.date(1994, 7, 20), datetime.date(1994, 9, 14),
                       datetime.date(1994, 9, 15), datetime.date(1994, 9, 16),
                       datetime.date(1994, 9, 17), datetime.date(1994, 9, 18),
                       datetime.date(1994, 9, 19), datetime.date(1994, 9, 20),
                       datetime.date(1994, 9, 25), datetime.date(1994, 9, 28)]

    bdict["NOAA12"] = [datetime.date(1993, 10, 13), datetime.date(1993, 10, 14),
                       datetime.date(1993, 10, 15), datetime.date(1993, 10, 16),
                       datetime.date(1993, 10, 17), datetime.date(1993, 10, 18),
                       datetime.date(1993, 10, 19)]

    bdict["NOAA14"] = [datetime.date(2001, 12, 17), datetime.date(2001, 12, 18),
                       datetime.date(2001, 12, 19), datetime.date(2001, 12, 20),
                       datetime.date(2001, 12, 21), datetime.date(2001, 12, 22),
                       datetime.date(2001, 12, 23), datetime.date(2001, 12, 24),
                       datetime.date(2001, 12, 25), datetime.date(2001, 12, 26),
                       datetime.date(2001, 12, 27), datetime.date(2001, 12, 28),
                       datetime.date(2001, 12, 29), datetime.date(2001, 12, 30),
                       datetime.date(2001, 12, 31), datetime.date(2002, 7, 28)]

    bdict["NOAA15"] = [datetime.date(2000, 7, 11), datetime.date(2000, 7, 23),
                       datetime.date(2000, 7, 24), datetime.date(2000, 7, 25),
                       datetime.date(2000, 7, 26), datetime.date(2000, 7, 27),
                       datetime.date(2000, 7, 29), datetime.date(2000, 7, 30),
                       datetime.date(2000, 8, 2), datetime.date(2000, 8, 3),
                       datetime.date(2000, 8, 4), datetime.date(2000, 8, 5),
                       datetime.date(2000, 8, 7), datetime.date(2000, 8, 9),
                       datetime.date(2000, 8, 11), datetime.date(2000, 8, 12),
                       datetime.date(2000, 8, 13), datetime.date(2000, 8, 14),
                       datetime.date(2000, 8, 16), datetime.date(2000, 8, 17),
                       datetime.date(2000, 8, 18), datetime.date(2000, 8, 19),
                       datetime.date(2000, 8, 20), datetime.date(2000, 8, 21),
                       datetime.date(2000, 8, 22), datetime.date(2000, 8, 23),
                       datetime.date(2000, 8, 24), datetime.date(2000, 8, 26),
                       datetime.date(2000, 8, 27), datetime.date(2000, 8, 28),
                       datetime.date(2000, 8, 31),
                       datetime.date(2000, 9, 4), datetime.date(2000, 9, 18),
                       datetime.date(2000, 9, 19), datetime.date(2000, 9, 20),
                       datetime.date(2000, 9, 21), datetime.date(2000, 9, 23),
                       datetime.date(2000, 9, 24), datetime.date(2000, 9, 25),
                       datetime.date(2000, 9, 26), datetime.date(2000, 9, 28),
                       datetime.date(2000, 9, 29),
                       datetime.date(2000, 10, 1), datetime.date(2000, 10, 3),
                       datetime.date(2000, 10, 4), datetime.date(2000, 10, 5),
                       datetime.date(2000, 10, 6), datetime.date(2000, 10, 7),
                       datetime.date(2000, 10, 8), datetime.date(2000, 10, 9),
                       datetime.date(2000, 10, 11), datetime.date(2000, 10, 12),
                       datetime.date(2000, 10, 13), datetime.date(2000, 10, 14),
                       datetime.date(2000, 10, 15), datetime.date(2000, 10, 16),
                       datetime.date(2000, 10, 17), datetime.date(2000, 10, 18),
                       datetime.date(2000, 10, 19), datetime.date(2000, 10, 20),
                       datetime.date(2000, 10, 21), datetime.date(2000, 10, 22),
                       datetime.date(2000, 10, 23), datetime.date(2000, 10, 24),
                       datetime.date(2000, 10, 25), datetime.date(2000, 10, 26),
                       datetime.date(2000, 10, 27), datetime.date(2000, 10, 28),
                       datetime.date(2000, 10, 29), datetime.date(2000, 10, 31),
                       datetime.date(2000, 11, 3), datetime.date(2000, 11, 4),
                       datetime.date(2000, 11, 5), datetime.date(2000, 11, 6),
                       datetime.date(2000, 11, 7), datetime.date(2000, 11, 8),
                       datetime.date(2000, 11, 9), datetime.date(2000, 11, 10),
                       datetime.date(2000, 11, 11), datetime.date(2000, 11, 12),
                       datetime.date(2000, 11, 13), datetime.date(2000, 11, 14),
                       datetime.date(2000, 11, 15), datetime.date(2000, 11, 16),
                       datetime.date(2000, 11, 17), datetime.date(2000, 11, 18),
                       datetime.date(2000, 11, 19), datetime.date(2000, 11, 20),
                       datetime.date(2000, 11, 23), datetime.date(2000, 11, 24),
                       datetime.date(2000, 11, 25), datetime.date(2000, 11, 28),
                       datetime.date(2000, 11, 30),
                       datetime.date(2000, 12, 1), datetime.date(2000, 12, 2),
                       datetime.date(2000, 12, 3), datetime.date(2000, 12, 4),
                       datetime.date(2000, 12, 5), datetime.date(2000, 12, 6),
                       datetime.date(2000, 12, 7), datetime.date(2000, 12, 9),
                       datetime.date(2000, 12, 10), datetime.date(2000, 12, 11),
                       datetime.date(2000, 12, 12), datetime.date(2000, 12, 14),
                       datetime.date(2000, 12, 15), datetime.date(2000, 12, 18),
                       datetime.date(2000, 12, 22), datetime.date(2000, 12, 23),
                       datetime.date(2000, 12, 25), datetime.date(2000, 12, 26),
                       datetime.date(2000, 12, 28),
                       datetime.date(2001, 1, 4), datetime.date(2001, 1, 6),
                       datetime.date(2001, 1, 7), datetime.date(2001, 1, 8),
                       datetime.date(2001, 1, 11), datetime.date(2001, 1, 12),
                       datetime.date(2001, 1, 13), datetime.date(2001, 1, 14),
                       datetime.date(2001, 1, 15), datetime.date(2001, 1, 16),
                       datetime.date(2001, 1, 17), datetime.date(2001, 1, 18),
                       datetime.date(2001, 1, 19), datetime.date(2001, 1, 20),
                       datetime.date(2001, 1, 21), datetime.date(2001, 1, 22),
                       datetime.date(2001, 1, 31),
                       datetime.date(2001, 2, 1), datetime.date(2001, 2, 9),
                       datetime.date(2001, 2, 10)]
    # due to KeyError
    # NOAA15/2007/20070214_20070221/maketarfile.1: * No L1c files for 20070216
    # NOAA15/2007/20070214_20070221/maketarfile.1: * No L1c files for 20070217
    # NOAA15/2007/20070222_20070301/maketarfile.1: * No L1c files for 20070301
    # NOAA15/2007/20070302_20070309/maketarfile.1: * No L1c files for 20070302
    # NOAA15/2007/20070302_20070309/maketarfile.1: * No L1c files for 20070303
    # NOAA15/2007/20070302_20070309/maketarfile.1: * No L1c files for 20070308
    # NOAA15/2007/20070302_20070309/maketarfile.1: * No L1c files for 20070309
    # NOAA15/2007/20070310_20070316/maketarfile.1: * No L1c files for 20070310

    # bdict["NOAA16"] nothing to blacklist

    bdict["NOAA17"] = [datetime.date(2002, 6, 25), datetime.date(2002, 6, 26),
                       datetime.date(2002, 6, 27), datetime.date(2002, 6, 28),
                       datetime.date(2002, 6, 29), datetime.date(2002, 6, 30),
                       datetime.date(2002, 7, 1), datetime.date(2002, 7, 2),
                       datetime.date(2002, 7, 3), datetime.date(2002, 7, 4),
                       datetime.date(2002, 7, 5), datetime.date(2002, 7, 6),
                       datetime.date(2002, 7, 7), datetime.date(2002, 7, 8),
                       datetime.date(2002, 7, 9),
                       datetime.date(2010, 10, 7), datetime.date(2010, 10, 8)]

    bdict["NOAA18"] = [datetime.date(2005, 5, 20), datetime.date(2005, 5, 21),
                       datetime.date(2005, 5, 22), datetime.date(2005, 5, 23),
                       datetime.date(2005, 5, 24), datetime.date(2005, 5, 25),
                       datetime.date(2005, 5, 26), datetime.date(2005, 5, 27),
                       datetime.date(2005, 5, 28), datetime.date(2005, 5, 29),
                       datetime.date(2005, 5, 30), datetime.date(2005, 5, 31),
                       datetime.date(2005, 6, 1), datetime.date(2005, 6, 2),
                       datetime.date(2005, 6, 3), datetime.date(2005, 6, 4)]

    bdict["NOAA19"] = [datetime.date(2009, 2, 6), datetime.date(2009, 2, 7),
                       datetime.date(2009, 2, 8), datetime.date(2009, 2, 9),
                       datetime.date(2009, 2, 10), datetime.date(2009, 2, 11),
                       datetime.date(2009, 2, 12), datetime.date(2009, 2, 13),
                       datetime.date(2009, 2, 14), datetime.date(2009, 2, 15),
                       datetime.date(2009, 2, 16), datetime.date(2009, 2, 17),
                       datetime.date(2009, 2, 18), datetime.date(2009, 2, 19),
                       datetime.date(2009, 2, 20), datetime.date(2009, 2, 21)]

    bdict["METOPA"] = [datetime.date(2007, 9, 18), datetime.date(2008, 3, 20)]

    # 2013: DOY 100 - 139 no data, i.e. 2013-04-10 until 2013-05-19
    # bdict["METOPB"] = [datetime.date(2013, 4, 10), datetime.date(2013, 4, 11),
    #                    datetime.date(2013, 4, 12), datetime.date(2013, 4, 13),
    #                    datetime.date(2013, 4, 14),
    #                    datetime.date(2013, 5, 17), datetime.date(2013, 5, 18),
    #                    datetime.date(2013, 5, 19)]

    return bdict


def blacklist_wrong_timestamp():
    """
    List of AVHRR GAC orbits, which got a wrong L1c timestamp from pygac.
    """
    blacklist_reason = "wrong_l1c_timestamp"

    blist = ['NSS.GHRR.NC.D83206.S0019.E0207.B1076667.GC.gz',
             'NSS.GHRR.NH.D92104.S1732.E1842.B1830304.WI.gz',
             'NSS.GHRR.ND.D95320.S0657.E0851.B0000000.WI.gz',
             'NSS.GHRR.ND.D95015.S1456.E1641.B0000000.GC.gz',
             'NSS.GHRR.NJ.D95035.S0905.E1046.B0000000.GC.gz',
             'NSS.GHRR.NJ.D01001.S0002.E0100.B3095253.GC.gz',
             'NSS.GHRR.NJ.D01001.S0050.E0231.B3095354.GC.gz',
             'NSS.GHRR.NJ.D01001.S0225.E0420.B3095455.WI.gz',
             'NSS.GHRR.NJ.D01001.S0414.E0609.B3095556.WI.gz',
             'NSS.GHRR.NJ.D01001.S0604.E0758.B3095657.WI.gz',
             'NSS.GHRR.NJ.D01001.S0754.E0940.B3095758.WI.gz',
             'NSS.GHRR.NJ.D01001.S0935.E1121.B3095859.WI.gz',
             'NSS.GHRR.NJ.D01001.S1116.E1311.B3095960.GC.gz',
             'NSS.GHRR.NJ.D01001.S1306.E1439.B3096061.GC.gz',
             'NSS.GHRR.NJ.D01001.S1434.E1628.B3096162.GC.gz',
             'NSS.GHRR.NJ.D01001.S1623.E1800.B3096263.GC.gz',
             'NSS.GHRR.NJ.D01001.S1805.E1940.B3096364.GC.gz',
             'NSS.GHRR.NJ.D01001.S1935.E2105.B3096465.WI.gz',
             'NSS.GHRR.NJ.D01001.S2100.E2254.B3096466.GC.gz',
             'NSS.GHRR.NJ.D01001.S2249.E0043.B3096667.GC.gz',
             'NSS.GHRR.NJ.D99286.S2145.E2333.B2467071.GC.gz',
             'NSS.GHRR.NJ.D99287.S1459.E1645.B2468081.GC.gz',
             'NSS.GHRR.NJ.D99287.S1640.E1834.B2468182.WI.gz']

    return blist, blacklist_reason


def blacklist_full_days(db, ver):
    """
    Blacklist all orbits of days, where no valid L1c data available
    """
    sat_list = get_satellite_list()
    black_dict = blacklist_days()
    black_reason = black_dict["blacklist_reason"]

    if ver:
        logger.info("*** BlackList Reason: \'{0}\'".format(black_reason))

    for sat in sat_list:

        try:
            date_list = black_dict[sat]
            # noinspection PyProtectedMember
            sat_id = db._get_id_by_name(table='satellites', name=sat)

            if ver:
                logger.info("{1}: {0} days".format(len(date_list), sat))

            for cnt, dt in enumerate(date_list):

                # blacklist all orbits between
                # start_date <= start_time_l1c <= end_date
                start_time = datetime.datetime(dt.year, dt.month,
                                               dt.day, 0, 0, 0)
                end_time = datetime.datetime(dt.year, dt.month,
                                             dt.day, 23, 59, 59)
                if ver:
                    logger.info("- ({1}) blacklist all l1b orbits on: {0}\n".
                                format(dt, cnt + 1))

                upd = "UPDATE orbits SET blacklist=1, blacklist_reason=\'{blr}\' " \
                      "WHERE blacklist = 0 AND satellite_id = \'{sat_id}\' AND " \
                      "start_time_l1b BETWEEN " \
                      "\'{start_time}\' AND \'{end_time}\' ".format(blr=black_reason,
                                                                    sat_id=sat_id,
                                                                    start_time=start_time,
                                                                    end_time=end_time)
                db.execute(upd)

                cmd = "SELECT * from orbits WHERE " \
                      "satellite_id = \'{sat_id}\' AND " \
                      "start_time_l1b BETWEEN \'{start_time}\' AND \'{end_time}\' " \
                      "ORDER BY start_time_l1b".format(sat_id=sat_id,
                                                       start_time=start_time,
                                                       end_time=end_time)
                res = db.execute(cmd)

                if ver:
                    for r in res:
                        logger.info("L1b Filename    : {0}".format(r['filename']))
                        logger.info("start_time_l1b  : {0}".format(r['start_time_l1b']))
                        # logger.info("start_time_l1c  : {0}".format(r['start_time_l1c']))
                        logger.info("end_time_l1b    : {0}".format(r['end_time_l1b']))
                        # logger.info("end_time_l1c    : {0}".format(r['end_time_l1c']))
                        # logger.info("equ_cross_time  : {0}".format(r['equator_crossing_time']))
                        logger.info("blacklist       : {0}".format(r['blacklist']))
                        logger.info("blacklist_reason: {0}\n".format(r['blacklist_reason']))

        except KeyError:
            if ver:
                logger.info("Nothing to blacklist for {0}!".format(sat))
            pass

    return black_reason


def blacklist_single_orbits(db, ver):
    """
    Black list single orbits.
    """

    black_list, black_reason = blacklist_wrong_timestamp()
    if ver:
        logger.info("*** BlackList Reason: {0} times \'{1}\'".
                    format(len(black_list), black_reason))

    for fil in black_list:

        splits = fil.split('.')
        satnam = full_sat_name(splits[2])[2]

        if ver:
            logger.info("* {0} L1bFile: {1}".format(satnam, fil))

        upd = "UPDATE orbits SET blacklist=1, " \
              "blacklist_reason=\'{blr}\' " \
              "WHERE filename=\'{fil}\'".format(fil=fil, blr=black_reason)
        db.execute(upd)

        cmd = "SELECT * FROM vw_std WHERE filename=\'{fil}\' AND " \
              "satellite_name=\'{satnam}\'".format(fil=fil, satnam=satnam)
        res = db.execute(cmd)

        if ver:
            for r in res:
                # logger.info("L1b Filename    : {0}".format(r['filename']))
                logger.info("start_time_l1b  : {0}".format(r['start_time_l1b']))
                logger.info("start_time_l1c  : {0}".format(r['start_time_l1c']))
                logger.info("end_time_l1b    : {0}".format(r['end_time_l1b']))
                logger.info("end_time_l1c    : {0}".format(r['end_time_l1c']))
                logger.info("equ_cross_time  : {0}".format(r['equator_crossing_time']))
                logger.info("blacklist       : {0}".format(r['blacklist']))
                logger.info("blacklist_reason: {0}\n".format(r['blacklist_reason']))

    return black_reason


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description=('{0} corrects AVHRR GAC archive database based on '
                     'logfile analysis, i.e. blacklist AVHRR GAC orbits'
                     'due to specific reason').format(os.path.basename(__file__)))

    parser.add_argument('-dbf', '--dbfile', required=True,
                        help='/path/to/database.sqlite3')

    parser.add_argument('-ver', '--verbose', action="store_true",
                        help='increase output verbosity')

    args = parser.parse_args()

    # -- some screen output if wanted
    logger.info("Parameter passed")
    logger.info("Verbose    : %s" % args.verbose)
    logger.info("DB_Sqlite3 : %s" % args.dbfile)

    # -- connect to database
    dbfile = AvhrrGacDatabase(dbfile=args.dbfile,
                              timeout=36000, exclusive=True)

    # -- wrong l1c timestamp conversion
    reason = blacklist_single_orbits(dbfile, args.verbose)
    ret = "SELECT COUNT(*) FROM vw_std WHERE " \
          "blacklist_reason=\'{reason}\'".format(reason=reason)
    num = dbfile.execute(ret)
    for i in num:
        logger.info("{0} orbits are blacklisted due to {1}".
                    format(i['COUNT(*)'], reason))

    # -- all l1b input corrupted on following dates
    reason = blacklist_full_days(dbfile, args.verbose)
    ret = "SELECT COUNT(*) FROM vw_std WHERE " \
          "blacklist_reason=\'{reason}\'".format(reason=reason)
    num = dbfile.execute(ret)
    for i in num:
        logger.info("{0} orbits are blacklisted due to {1}".
                    format(i['COUNT(*)'], reason))

    # -- commit changes
    logger.info("Commit all changes")
    dbfile.commit_changes()

    logger.info("%s finished" % os.path.basename(__file__))