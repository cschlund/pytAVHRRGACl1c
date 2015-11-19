#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import os
import sys
import argparse
import datetime
import time
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


def print_verbose(sqlres, sat=None): 
    for r in sqlres: 
        if sat: 
            logger.info("Satellite     : {0}".format(sat))
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
        sqltxt = sqltxt + "blacklist=1 "
    elif reason is 'all_l1b_white':
        sqltxt = sqltxt + "filename is not null AND blacklist=0 "
    elif reason is 'all_l1c_white':
        sqltxt = sqltxt + "start_time_l1c is not null AND blacklist=0 "
    elif reason is 'all_l1c_missing':
        sqltxt = sqltxt + "start_time_l1c is null AND blacklist=0 "
    elif reason is 'redundant':
        sqltxt = sqltxt + "redundant=1 "
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
    List of l1b files, which have a wrong y-dimension, i.e. along_track.
    """
    blist = list()
    black_reason = "along_track_too_long"

    blist = ["NSS.GHRR.NK.D09357.S2056.E2242.B6037880.WI.gz",
             "NSS.GHRR.NK.D10075.S0422.E0617.B6155152.WI.gz",
             "NSS.GHRR.NK.D10298.S2008.E2150.B6473637.WI.gz",
             "NSS.GHRR.NK.D11129.S1504.E1552.B6752424.WI.gz",
             "NSS.GHRR.NK.D12036.S1904.E2022.B7140001.WI.gz",
             "NSS.GHRR.NL.D07100.S0445.E0638.B3375152.WI.gz",
             "NSS.GHRR.NL.D08218.S0916.E1102.B4057273.WI.gz",
             "NSS.GHRR.NL.D11317.S0738.E0932.B5744243.WI.gz",
             "NSS.GHRR.NN.D06032.S0311.E0506.B0361920.GC.gz",
             "NSS.GHRR.NN.D09253.S0123.E0318.B2219293.WI.gz",
             "NSS.GHRR.NN.D12226.S0457.E0652.B3726061.WI.gz",
             "NSS.GHRR.NP.D14087.S0248.E0316.B2645555.SV.gz",
             "NSS.GHRR.NP.D14365.S2230.E2349.B3038989.GC.gz",
             "NSS.GHRR.NC.D84283.S0744.E0931.B1699899.WI.gz"]
    
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
    logger.info("COMMIT CHANGES FOR \'{0}\'".format(black_reason))
    db.commit_changes()



def bad_l1c_data(db, ver, start_time, end_time, sat_id):
    """
    Blacklist all dates between sdate and edate for given satellite.
    """
    black_reason = "bad_l1c_quality"

    upd = "UPDATE orbits SET blacklist=1, blacklist_reason=\'{blr}\' " \
          "WHERE satellite_id = \'{sat_id}\' AND " \
          "start_time_l1b BETWEEN \'{start_time}\' AND \'{end_time}\' "

    db.execute(upd.format(blr=black_reason, sat_id=sat_id,
                          start_time=start_time,end_time=end_time))

    cmd = "SELECT * from orbits WHERE satellite_id = \'{sat_id}\' AND " \
          "start_time_l1b BETWEEN \'{start_time}\' AND \'{end_time}\' " \
          "ORDER BY start_time_l1b"

    res = db.execute(cmd.format(sat_id=sat_id, start_time=start_time, 
                                end_time=end_time))

    if ver: 
        logger.info("Blacklist all L1b orbits between "
                    "{0} and {1}".format(start_time, end_time))
        print_verbose(res) 

    return black_reason


def blacklist_n17_data(db, ver):
    """
    Blacklist all dates between sdate and edate for NOAA17.
    AVHRR scan motor degraded and finally, stalled on 15 Oct 2010.
    """
    satna = 'NOAA17'
    sdate = datetime.datetime(2010, 3, 1, 0, 0, 0)
    edate = datetime.datetime(2012, 1, 1, 0, 0, 0)
    satid = db._get_id_by_name(table='satellites', name=satna)
    dueto = bad_l1c_data(db, ver, sdate, edate, satid)
    print_changes(db, dueto, [satna])
    logger.info("COMMIT CHANGES FOR \'{0}\'".format(dueto))
    db.commit_changes()


def blacklist_n6_data(db, ver):
    """
    Blacklist all dates between sdate and edate for NOAA6.
    PYSTAT results look not so good between 1981-08-14 & 1982-08-02.
    """
    satna = 'NOAA6'
    sdate = datetime.datetime(1981, 8, 14, 0, 0, 0)
    edate = datetime.datetime(1982, 8,  2, 0, 0, 0)
    satid = db._get_id_by_name(table='satellites', name=satna)
    dueto = bad_l1c_data(db, ver, sdate, edate, satid)
    print_changes(db, dueto, [satna])
    logger.info("COMMIT CHANGES FOR \'{0}\'".format(dueto))
    db.commit_changes()


def blacklist_n8_data(db, ver):
    """
    Blacklist all dates between sdate and edate for NOAA8.
    PYSTAT results look not so good between 1983-05-04 & 1983-09-19.
    """
    satna = 'NOAA8'
    sdate = datetime.datetime(1983, 5,  4, 0, 0, 0)
    edate = datetime.datetime(1983, 9, 19, 0, 0, 0)
    satid = db._get_id_by_name(table='satellites', name=satna)
    dueto = bad_l1c_data(db, ver, sdate, edate, satid)
    print_changes(db, dueto, [satna])
    logger.info("COMMIT CHANGES FOR \'{0}\'".format(dueto))
    db.commit_changes()


def blacklist_days():
    """
    List of days, which did not provide any reasonable L1c orbits.
    """
    bdict2 = dict()
    blacklist_reason = "no_valid_l1c_data"
    satlist = get_satellite_list()
    
    # initialize dictionary
    for s in satlist:
        bdict2[s] = dict()
    
    # fill dict with dates based on logfile analysis, where no L1c files have
    # been created during AVHRR GAC L1C procession VERSION 2
    bdict2["NOAA7"]["198205"] = [28,29,30,31]
    bdict2["NOAA7"]["198209"] = [25,26]
    bdict2["NOAA7"]["198307"] = [27,28,29,30,31]
    bdict2["NOAA7"]["198308"] = [1,2,6]
    bdict2["NOAA7"]["198309"] = [21,22,23,24,25,26]
    bdict2["NOAA7"]["198401"] = [14,15]
    bdict2["NOAA7"]["198404"] = [10]
    bdict2["NOAA7"]["198407"] = [23]
    bdict2["NOAA7"]["198412"] = [6]
    bdict2["NOAA9"]["198603"] = [14,15]
    bdict2["NOAA11"]["199409"] = [14,15,16,17,18,19,20,25,28]
    bdict2["NOAA11"]["199410"] = [6,8,9,10,11,12]
    bdict2["NOAA12"]["199310"] = [13,14,15,16,17,18,19]
    bdict2["NOAA14"]["200101"] = [1]
    bdict2["NOAA14"]["200105"] = [31]
    bdict2["NOAA14"]["200112"] = [17,18,19,20,21,22,23,24,25,26,27,28,29,30,31]
    bdict2["NOAA14"]["200207"] = [28]
    bdict2["NOAA15"]["200007"] = [11,23,24,25,26,27,29,30]
    bdict2["NOAA15"]["200008"] = [2,3,4,5,7,9,11,12,13,14,16,17,18,19,20,21,22,23,24,26,27,28,31]
    bdict2["NOAA15"]["200009"] = [4,18,19,20,21,23,24,25,26,28,29]
    bdict2["NOAA15"]["200010"] = [1,3,4,5,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,31]
    bdict2["NOAA15"]["200011"] = [4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,23,24,25,28]
    bdict2["NOAA15"]["200012"] = [1,2,3,4,5,6,7,9,10,11,12,14,15,18,22,23,25,26,27]
    bdict2["NOAA15"]["200101"] = [6,7,8,11,12,13,14,15,16,17,18,19,20,21,22,31]
    bdict2["NOAA15"]["200102"] = [1,9,10]
    bdict2["NOAA15"]["200702"] = [16,17]
    bdict2["NOAA15"]["200703"] = [1,2,3,8,9,10]
    bdict2["NOAA17"]["200206"] = [25,26,27,28,29,30]
    bdict2["NOAA17"]["200207"] = [1,2,3,4,5,6,7,8,9]
    bdict2["NOAA17"]["201010"] = [7,8]
    bdict2["NOAA18"]["200505"] = [20,21,22,23,24,25,26,27,28,29,30,31]
    bdict2["NOAA18"]["200506"] = [1,2,3,4]
    bdict2["NOAA19"]["200902"] = [6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21]
    bdict2["METOPA"]["200709"] = [18]
    bdict2["METOPA"]["200803"] = [20]
    bdict2["METOPB"]["201304"] = [10,11,12,13,14]
    bdict2["METOPB"]["201305"] = [17,18,19]
    bdict2["METOPB"]["201410"] = [17,24,25,26,27,28]
    bdict2["METOPB"]["201412"] = [31]

    return bdict2, blacklist_reason


def orbitlist_wrong_timestamp():
    """
    List of AVHRR GAC orbits, which got a wrong L1c timestamp from pygac.
    """
    blacklist_reason = "wrong_l1c_timestamp"

    # C. Schlundt: avhrrgac proc. version 2 [May/June 2015]
    #              (several pygac updates w.r.t. timestamp)
    bdict = dict()
    # detected during GAC_overlap.py
    # reason: two different l1bfiles produces the same l1cfile
    bdict["NSS.GHRR.NC.D83172.S1921.E2106.B1028384.WI.gz"] = "ECC_GAC_avhrr_noaa7_99999_19830621T2102005Z_19830621T2217055Z.h5"
    bdict["NSS.GHRR.NC.D83206.S0019.E0207.B1076667.GC.gz"] = "ECC_GAC_avhrr_noaa7_99999_19830726T0019507Z_19830726T0207452Z.h5"
    # based on logfile analysis
    bdict["NSS.GHRR.NH.D92104.S1732.E1842.B1830304.WI.gz"] = "ECC_GAC_avhrr_noaa11_99999_19910414T1732594Z_19910414T1842164Z.h5"
    bdict["NSS.GHRR.ND.D95015.S1456.E1641.B0000000.GC.gz"] = "ECC_GAC_avhrr_noaa12_99999_19970115T1456586Z_19970115T1641386Z.h5"
    bdict["NSS.GHRR.ND.D95320.S0657.E0851.B0000000.WI.gz"] = "ECC_GAC_avhrr_noaa12_99999_19961115T0657221Z_19961115T0851491Z.h5"
    bdict["NSS.GHRR.NJ.D95035.S0905.E1046.B0000000.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_19980204T0905006Z_19980204T1046101Z.h5"
    bdict["NSS.GHRR.NJ.D99286.S2145.E2333.B2467071.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20110511T1433055Z_20110511T1438440Z.h5"
    bdict["NSS.GHRR.NJ.D99287.S1640.E1834.B2468182.WI.gz"] = "ECC_GAC_avhrr_noaa14_99999_20140216T0741375Z_20140216T0837485Z.h5"
    bdict["NSS.GHRR.NJ.D99287.S1459.E1645.B2468081.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20140216T1110190Z_20140216T1123500Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S0002.E0100.B3095253.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T0002345Z_20020528T0100095Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S0050.E0231.B3095354.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T0050210Z_20020528T0231020Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S0225.E0420.B3095455.WI.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T0225425Z_20020528T0420190Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S0414.E0609.B3095556.WI.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T0414575Z_20020528T0609235Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S0604.E0758.B3095657.WI.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T0604180Z_20020528T0758475Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S0754.E0940.B3095758.WI.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T0754500Z_20020528T0940520Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S0935.E1121.B3095859.WI.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T0935540Z_20020528T1121535Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S1116.E1311.B3095960.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T1116520Z_20020528T1311180Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S1306.E1439.B3096061.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T1306130Z_20020528T1439125Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S1434.E1628.B3096162.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T1434125Z_20020528T1628120Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S1623.E1800.B3096263.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T1623060Z_20020528T1800340Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S1805.E1940.B3096364.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T1805230Z_20020528T1940235Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S1935.E2105.B3096465.WI.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T1935335Z_20020528T2105070Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S2100.E2254.B3096466.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T2100025Z_20020528T2254295Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S2249.E0043.B3096667.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T2249160Z_20020529T0043530Z.h5"
    bdict["NSS.GHRR.NK.D99144.S2359.E0153.B0535153.GC.gz"] = "ECC_GAC_avhrr_noaa15_99999_19990525T0000041Z_19990525T0153486Z.h5"
    bdict["NSS.GHRR.NK.D00257.S2303.E0048.B1214951.WI.gz"] = "ECC_GAC_avhrr_noaa15_99999_20000914T0047076Z_20000914T0048346Z.h5"

    blist = list()
    for key in bdict:
        blist.append(key)

    return blist, blacklist_reason


def blacklist_no_valid_l1c_data(db, ver):
    """
    Blacklist all orbits of days, where no valid L1c data available
    """
    sat_list = get_satellite_list()
    black_dict, black_reason = blacklist_days()

    for sat in sat_list:
        try:
            date_list = black_dict[sat]
            sat_id = db._get_id_by_name(table='satellites', name=sat)

            for yyyymm in black_dict[sat]:
                y = int(yyyymm[0:4])
                m = int(yyyymm[4:])
                days = black_dict[sat][yyyymm]

                for d in days:
                    dt = datetime.date(y,m,d)
                    # blacklist all orbits between
                    # start_date <= start_time_l1c <= end_date
                    start_time = datetime.datetime(y, m, d, 0, 0, 0)
                    end_time = datetime.datetime(y, m, d, 23, 59, 59)

                    upd = "UPDATE orbits SET blacklist=1, blacklist_reason=\'{blr}\' " \
                          "WHERE blacklist=0 AND satellite_id=\'{sat_id}\' AND " \
                          "start_time_l1b BETWEEN \'{start_time}\' AND \'{end_time}\' "
                    db.execute(upd.format(blr=black_reason, sat_id=sat_id,
                                          start_time=start_time, end_time=end_time))

                    cmd = "SELECT * from orbits WHERE satellite_id=\'{sat_id}\' AND " \
                          "blacklist_reason=\'{blr}\' AND " \
                          "start_time_l1b BETWEEN \'{start_time}\' AND \'{end_time}\' " \
                          "ORDER BY start_time_l1b"
                    res = db.execute(cmd.format(sat_id=sat_id,start_time=start_time,
                                                end_time=end_time,blr=black_reason))
                    if ver:
                        print_verbose(res,sat) 

            print_changes(db, black_reason,[sat])

        except KeyError:
            if ver:
                logger.info("Nothing to blacklist for {0}!".format(sat))
            pass

    print_changes(db, black_reason)
    logger.info("COMMIT CHANGES FOR \'{0}\'".format(black_reason))
    db.commit_changes()


def blacklist_wrong_l1c_timestamp(db, ver):
    """
    Black list single orbits.
    """
    black_list, black_reason = orbitlist_wrong_timestamp()

    for fil in black_list:

        splits = fil.split('.')
        satnam = full_sat_name(splits[2])[2]

        upd = "UPDATE orbits SET blacklist=1, " \
              "blacklist_reason=\'{blr}\' WHERE filename=\'{fil}\'"
        db.execute(upd.format(fil=fil, blr=black_reason))

        cmd = "SELECT * FROM vw_std WHERE filename=\'{fil}\' AND " \
              "satellite_name=\'{satnam}\'"
        res = db.execute(cmd.format(fil=fil, satnam=satnam))
        if ver:
            print_verbose(res) 

    print_changes(db, black_reason)
    logger.info("COMMIT CHANGES FOR \'{0}\'".format(black_reason))
    db.commit_changes()



if __name__ == '__main__':

    prelist = pre_blacklist_reasons()
    proclist = proc_blacklist_reasons()
    postlist = post_blacklist_reasons()

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
                        format(prelist))

    parser.add_argument('-proc', '--show_proc', action="store_true",
                        help='SHOW all orbits which have been blacklisted '
                        'during the AVHRR GAC L1c processing due to {0}.'.
                        format(proclist))

    parser.add_argument('-post', '--show_post', action="store_true",
                        help='SHOW all orbits regarding {0}.'.
                        format(postlist))

    parser.add_argument('-ts', '--wrong_l1c_timestamp', action="store_true",
                        help='''Blacklist L1b files getting wrong l1c timestamp.
                             This must be done before GAC_overlap.py, otherwise 
                             it will be aborted due to ambiguous entries.''')

    parser.add_argument('-no', '--no_valid_l1c_data', action="store_true", 
                        help='''Blacklist days, where no valid l1c data is available. 
                             ATTENTION: only if you know what you are doing! 
                             The list_of_invalid_days changes from processing 
                             to processing due to pygac updates.''')

    parser.add_argument('-n17', '--bad_n17_data', action="store_true",
                        help='''Blacklist all days between 2010-03-01 and
                        2011-12-31 of NOAA17 because data show problems. AVHRR
                        scan motor stalled on 15 Oct 2010.''')

    parser.add_argument('-n6', '--bad_n6_data', action="store_true",
                        help='''Blacklist all days between 1981-08-14 and
                        1982-08-02 of NOAA6 because data show problems.
                        See PYSTAT results.''')

    parser.add_argument('-n8', '--bad_n8_data', action="store_true",
                        help='''Blacklist all days between 1983-05-04 and
                        1983-09-19 of NOAA8 because data show problems.
                        See PYSTAT results.''')

    parser.add_argument('-ydim', '--along_track_too_long', action="store_true",
                        help='''Diana found during CLARA-A2 processing orbits, 
                        which are too long in the along_track dimension.''')

    args = parser.parse_args()

    # -- consider all satellites
    if 'ALL' in args.satellites:
        satlist = get_satellite_list()
    else:
        satlist = args.satellites

    # -- some screen output if wanted
    if len(sys.argv[1:]) > 0: 
        logger.info("{0}".format(sys.argv[1:]))

    # -- connect to database
    dbfile = AvhrrGacDatabase(dbfile=args.dbfile,
                              timeout=36000, exclusive=True)

    # -- blacklisting
    if args.wrong_l1c_timestamp: 
        blacklist_wrong_l1c_timestamp(dbfile, args.verbose)
    if args.no_valid_l1c_data: 
        blacklist_no_valid_l1c_data(dbfile, args.verbose)
    if args.bad_n17_data: 
        blacklist_n17_data(dbfile, args.verbose)
    if args.bad_n6_data: 
        blacklist_n6_data(dbfile, args.verbose)
    if args.bad_n8_data: 
        blacklist_n8_data(dbfile, args.verbose)
    if args.along_track_too_long: 
        blacklist_wrong_ydim(dbfile, args.verbose)

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
        for i in prelist: 
            print_changes(dbfile, i, satlist)
    if args.show_proc: 
        for i in proclist: 
            print_changes(dbfile, i, satlist)
    if args.show_post: 
        for i in postlist: 
            print_changes(dbfile, i, satlist)

    logger.info("%s finished\n\n" % os.path.basename(__file__))
