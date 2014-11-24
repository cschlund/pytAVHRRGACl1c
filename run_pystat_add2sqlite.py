#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# C.Schlundt: Oct., 2014
#

import numpy as np
import h5py
import sqlite3
import datetime
import os, sys, getopt
import argparse
import subs_avhrrgac as mysub
import read_avhrrgac_h5 as rh5
from multiprocessing import Pool

# -------------------------------------------------------------------
# read level 1c files

def readfiles(tup):
    idx, fil = tup #tuple

    global zone_size
    global nzones

    # initialize global mean, stdv, nobs parameters
    # saving output for each orbit
    gmean = dict()
    gstdv = dict()
    gnobs = dict()
    zmean = dict()
    zstdv = dict()
    znobs = dict()

    for cha in cha_list:
        gmean[cha] = dict()
        gstdv[cha] = dict()
        gnobs[cha] = dict()
        zmean[cha] = dict()
        zstdv[cha] = dict()
        znobs[cha] = dict() 

        for sel in sel_list: 
            gmean[cha][sel] = 0.
            gstdv[cha][sel] = 0.
            gnobs[cha][sel] = 0.
            zmean[cha][sel] = np.ma.zeros(nzones)
            zstdv[cha][sel] = np.ma.zeros(nzones)
            znobs[cha][sel] = np.ma.zeros(nzones)

            if cha is 'ch1' or cha is 'ch2' or cha is 'ch3a': 
                break


    # get angles file for ahvrr file
    afil = fil.replace("ECC_GAC_avhrr_", "ECC_GAC_sunsatangles_")
    
    if args.verbose == True and args.test == True: 
        print ("   * %s = %s/%s" % 
                (idx, os.path.basename(fil),os.path.basename(afil)))
    
    # open H5 files
    f = h5py.File(fil, "r+")
    a = h5py.File(afil, "r+")


    # cha_list  = ['ch1', 'ch2', 'ch3b', 'ch4', 'ch5', 'ch3a']
    for channel in cha_list:

        # sel_list  = ['day', 'night', 'twilight']
        for select in sel_list:

            try: 
                check = global_mean[channel][select] 

                if args.verbose == True and args.test == True: 
                    print ("   * %s = %s (%s)" % 
                          (idx, mysub.full_cha_name(channel), select)) 

                try: 
                    (lat, lon, tar) = rh5.read_avhrrgac(f, a, select, channel, False) 
                    #(lat, lon, tar) = rh5.read_avhrrgac(f, a, select, channel, args.verbose)

                    # check is channel is filled with measurements
                    if np.ma.count(tar) == 0: 
                        break

                    # global statistics
                    gn = tar.count()
                    gm = tar.mean(dtype=np.float64)
                    gs = tar.std(dtype=np.float64)

                    #zonal statistics
                    (zm, zs, zn) = mysub.cal_zonal_means(lat, tar, zone_size)

                    if zn.sum() != gn: 
                        print (" --- FAILED: Input is fishy due to: %s(zonal nobs) != %s (global nobs) " % (int(zn.sum()), gn) ) 
                        print ("        Fil: %s" % fil) 
                        print ("       Afil: %s" % afil) 
                        print ("    Cha/Sel: %s/%s " % (channel,select)) 
                        return None

                    gmean[channel][select] = gm
                    gstdv[channel][select] = gs
                    gnobs[channel][select] = gn

                    zmean[channel][select] = zm
                    zstdv[channel][select] = zs
                    znobs[channel][select] = zn

                    # clear variables
                    del(gm, gs, gn, zm, zs, zn)

                except (IndexError, ValueError, RuntimeError, Exception) as err: 
                    print (" --- FAILED: %s" % err)
                    print ("        Fil: %s" % fil)
                    print ("       Afil: %s" % afil)
                    print ("    Cha/Sel: %s/%s " % (channel,select))
                    return None

            except KeyError: 
                break

        # select loop
        #break

    # channel loop
    #break

    # close H5 files
    a.close()
    f.close()

    # return pro orbit=file
    return (idx, gmean, gstdv, gnobs, zmean, zstdv, znobs)


# -------------------------------------------------------------------
# -- main 
# -------------------------------------------------------------------

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='''%s
    calculates statistics (daily zonal and global means) of 
    AVHRR GAC L1c data processed in the framework of Cloud_cci (pyGAC).
    For the VIS channels, statistics is based on daytime observations only,
    i.e. SZA less than 80. For the IR channels day/twilight/night 
    observations are considered. Statistics are stored in a sqlite db.
    Orbits are processed in parallel mode.''' % os.path.basename(__file__))

    parser.add_argument('-d', '--date', type=mysub.datestring, 
            help='Date String, e.g. 20090126', required=True)
    parser.add_argument('-s', '--satellite', type=mysub.satstring, 
            help='Satellite, e.g. metop02', required=True)
    parser.add_argument('-i', '--inpdir', 
            help='Path, e.g. /path/to/input', required=True)
    parser.add_argument('-g', '--gsqlite', 
            help='/path/to/AVHRR_GAC_L1c_pystat.sqlite3', required=True)
    parser.add_argument('-b', '--binsize', 
            help='Define binsize for latitudinal belts', default=5)
    parser.add_argument('-t', '--test', 
            help='Run test with reduced channel and select list', 
            action="store_true")
    parser.add_argument('-v', '--verbose', 
            help='increase output verbosity', action="store_true")

    args = parser.parse_args()

    # -------------------------------------------------------------------
    # -- make some screen output if wanted

    if args.verbose == True:
        print ("\n *** Parameter passed" )
        print (" ---------------------- ")
        print ("   - TEST       : %s" % args.test)
        print ("   - Date       : %s" % args.date)
        print ("   - Satellite  : %s" % args.satellite)
        print ("   - Input Path : %s" % args.inpdir)
        print ("   - Binsize    : %s" % args.binsize)
        print ("   - Verbose    : %s" % args.verbose)
        print ("   - DB_Sqlite3 : %s" % args.gsqlite)

    # -------------------------------------------------------------------
    # -- some settings

    fill_value = -9999.
    pattern    = 'ECC_GAC_avhrr*'+args.satellite+'*'+args.date+'T*'
    fil_list   = mysub.find(pattern, args.inpdir)
    nfiles     = len(fil_list)
    message    = "*** No files available for "+args.date+", "+args.satellite
    qflag      = True # quality flag if input data is not fishy

    if nfiles == 0:
        print message
        sys.exit(0)
    else:
        fil_list.sort()
        #for f in fil_list:
        #    print f

    # -------------------------------------------------------------------
    # -- lists for generating total arrays

    if args.test is True:
        cha_list = ['ch1']
        sel_list = ['day']
    else:
        cha_list = mysub.get_channel_list()
        sel_list = mysub.get_select_list()

    # -------------------------------------------------------------------
    # -- define latitudinal zone size :

    global zone_size
    global nzones

    zone_size = float(args.binsize)
    zone_rad = zone_size/2.0

    # -------------------------------------------------------------------
    # -- determine zone centers:

    zone_centers = np.arange(-90 + zone_rad, 90 + zone_rad, zone_size)
    nzones = len(zone_centers)

    # -------------------------------------------------------------------
    # -- initialize global mean, stdv, nobs parameters

    # saving output for each orbit
    global_mean = dict()
    global_stdv = dict()
    global_nobs = dict()
    zonal_mean = dict()
    zonal_stdv = dict()
    zonal_nobs = dict()

    # saving mean values based on all orbits/day
    all_global_mean = dict()
    all_global_stdv = dict()
    all_global_nobs = dict()
    all_zonal_mean = dict()
    all_zonal_stdv = dict()
    all_zonal_nobs = dict()

    for cha in cha_list:
        global_mean[cha] = dict()
        global_stdv[cha] = dict()
        global_nobs[cha] = dict()
        zonal_mean[cha] = dict()
        zonal_stdv[cha] = dict()
        zonal_nobs[cha] = dict()

        all_global_mean[cha] = dict()
        all_global_stdv[cha] = dict()
        all_global_nobs[cha] = dict()
        all_zonal_mean[cha] = dict()
        all_zonal_stdv[cha] = dict()
        all_zonal_nobs[cha] = dict()

        for sel in sel_list:
            global_mean[cha][sel] = np.ma.zeros(nfiles)
            global_stdv[cha][sel] = np.ma.zeros(nfiles)
            global_nobs[cha][sel] = np.ma.zeros(nfiles)
            zonal_mean[cha][sel] = np.ma.zeros((nfiles,nzones))
            zonal_stdv[cha][sel] = np.ma.zeros((nfiles,nzones))
            zonal_nobs[cha][sel] = np.ma.zeros((nfiles,nzones))

            all_global_mean[cha][sel] = 0.
            all_global_stdv[cha][sel] = 0.
            all_global_nobs[cha][sel] = 0.
            all_zonal_mean[cha][sel] = np.ma.zeros(nzones)
            all_zonal_stdv[cha][sel] = np.ma.zeros(nzones)
            all_zonal_nobs[cha][sel] = np.ma.zeros(nzones)

            if cha is 'ch1' or cha is 'ch2' or cha is 'ch3a': 
                break

    # -------------------------------------------------------------------
    # -- creating jobs as tuple

    arglist = []
    for pos, fil in enumerate(fil_list): 
        arglist.append((pos,fil))

    pool = Pool(processes=nfiles)
    results = pool.map(func=readfiles, iterable=arglist)

    for pos,out in enumerate(results): 
        if out is None: 
            print (" --- FAILED: %s. Input is fishy for %s --> %s" %
                    (pos, args.date, fil_list[pos]))
            qflag = False
        else:
            for cha in cha_list:
                for sel in sel_list:
                    try:
                        check = global_mean[cha][sel]
                        global_mean[cha][sel][out[0]] = out[1][cha][sel]
                        global_stdv[cha][sel][out[0]] = out[2][cha][sel]
                        global_nobs[cha][sel][out[0]] = out[3][cha][sel]
                        zonal_mean[cha][sel][out[0],:] = out[4][cha][sel]
                        zonal_stdv[cha][sel][out[0],:] = out[5][cha][sel]
                        zonal_nobs[cha][sel][out[0],:] = out[6][cha][sel]
                    except KeyError:
                        break

    # -------------------------------------------------------------------
    # -- only store good data
    if qflag is True:

        # -- create lists of mean, stdv, nobs for globa/zonal
        global_list = [global_mean, global_stdv, global_nobs]
        zonal_list  = [zonal_mean, zonal_stdv, zonal_nobs]

        all_global_list = [all_global_mean, all_global_stdv, all_global_nobs]
        all_zonal_list  = [all_zonal_mean, all_zonal_stdv, all_zonal_nobs]


        # -- Global means/stdv/nobs
        for position, item in enumerate(global_list): 
            for chakey in item: 
                for selkey,selval in item[chakey].items(): 
                    # mask zeros 
                    mask = np.ma.equal(item[chakey][selkey], 0.) 
                    data = np.ma.masked_where(mask, item[chakey][selkey]) 
                    #print chakey , "=>", selkey, " : ", selval, " ; ", data 

                    if position is 2: 
                        ave = np.sum(data) 
                    else: 
                        ave = data.mean() 

                    try: 
                        all_item = all_global_list[position] 
                        check = all_item[chakey][selkey] 
                        all_item[chakey][selkey] = ave 
                        del ave
                    except KeyError: 
                        break


        # -- Zonal means/stdv/nobs 
        for position, item in enumerate(zonal_list): 
            for chakey in item: 
                for selkey,selval in item[chakey].items(): 
                    # mask zeros 
                    mask = np.ma.equal(item[chakey][selkey], 0.) 
                    data = np.ma.masked_where(mask, item[chakey][selkey]) 
                    #print chakey , "=>", selkey, " : ", selval, " ; ", data 

                    if position is 2: 
                        ave = np.sum(data, axis=0) 
                    else: 
                        ave = data.mean(axis=0)

                    try:
                        all_item = all_zonal_list[position]
                        check = all_item[chakey][selkey]
                        all_item[chakey][selkey][:] = ave
                        del ave
                    except KeyError:
                        break


        # -- save output 
        # -- Sqlite database for global statistics (all satellites)  

        if args.verbose == True: 
            print ("\n   *** Write global/zonal "
                   "output into %s " % args.gsqlite)

        lite_datstr = datetime.datetime.strptime(args.date, '%Y%m%d').date()
        lite_satstr = mysub.full_sat_name(args.satellite)[2]

        all_satellites = mysub.get_satellite_list()
        all_channels   = mysub.get_channel_list()
        all_selects    = mysub.get_select_list()

        tab_sat = 'satellites'
        tab_cha = 'channels'
        tab_sel = 'selects'
        tab_lat = 'latitudes'
        tab_sta = 'statistics'

        try: 
            db = sqlite3.connect(args.gsqlite, 
              detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES,
              timeout=36000)

            db.row_factory = mysub.dict_factory

            db.isolation_level = 'EXCLUSIVE'
            db.execute('BEGIN EXCLUSIVE')

            cursor = db.cursor()

            # -- create table for satellites
            res = mysub.check_if_table_exists(cursor, tab_sat)
            if res is 0:
                mysub.create_id_name_table(db, tab_sat, all_satellites)

            # -- create table for channels  
            res = mysub.check_if_table_exists(cursor, tab_cha)
            if res is 0:
                mysub.create_id_name_table(db, tab_cha, all_channels)

            # -- create table for selected times
            res = mysub.check_if_table_exists(cursor, tab_sel)
            if res is 0:
                mysub.create_id_name_table(db, tab_sel, all_selects)

            # -- create table for latitudinal belts
            res = mysub.check_if_table_exists(cursor, tab_lat)
            if res is 0:
                mysub.create_id_name_table(db, tab_lat, zone_centers)

            # -- create table for statistics
            res = mysub.check_if_table_exists(cursor, tab_sta)
            if res is 0:
                mysub.create_statistics_table(db)
                mysub.alter_statistics_table(db, zone_centers)


            # -- get satID
            get_id = "SELECT id FROM {0} "\
                     "WHERE name = \'{1}\'".format(tab_sat, lite_satstr)
            results = db.execute(get_id)
            for item in results:
                #print lite_satstr, item["id"]
                sat_id = item["id"]


            for chakey in cha_list: 
                for selkey in sel_list: 
                    try: 
                        check = all_zonal_mean[chakey][selkey]

                        if np.ma.count(check) == 0:
                            continue

                        if args.verbose == True: 
                            print ("      + %s (%s) "  
                                    % (mysub.full_cha_name(chakey), selkey) )

                        zm = all_zonal_list[0][chakey][selkey]
                        zn = all_zonal_list[2][chakey][selkey]
                        gn = all_global_list[2][chakey][selkey]
                        gmean_check = np.ma.dot(zm, zn)/gn

                        if args.verbose == True: 
                            print ("        - Global mean based "
                                   "on zonal means: %f = %f (global)" % 
                                    (gmean_check, all_global_list[0][chakey][selkey]) ) 
                            print ("        - Global nobs based "
                                   "on zonal nobs: %d = %d (global)" % (np.sum(zn), gn))


                        # -- get chaID
                        get_id = "SELECT id FROM {0} "\
                                 "WHERE name = \'{1}\'".format(tab_cha, chakey)
                        results = db.execute(get_id)
                        for item in results: 
                            cha_id = item["id"]


                        # -- get selID
                        get_id = "SELECT id FROM {0} "\
                                 "WHERE name = \'{1}\'".format(tab_sel, selkey)
                        results = db.execute(get_id)
                        for item in results:
                            sel_id = item["id"]


                        # -- set bad records to fill_value
                        (glm, gls, gln, 
                         mean, stdv, nobs) = mysub.set_fillvalue( fill_value, 
                                           all_zonal_list[0][chakey][selkey], 
                                           all_zonal_list[1][chakey][selkey], 
                                           all_zonal_list[2][chakey][selkey],
                                           all_global_list[0][chakey][selkey],
                                           all_global_list[1][chakey][selkey],
                                           all_global_list[2][chakey][selkey] )


                        # -- convert numpy arrays to lists
                        zonal_mean_list = mean.tolist()
                        zonal_stdv_list = stdv.tolist()
                        zonal_nobs_list = nobs.tolist()


                        # -- add statistics to args.gsqlite db
                        prim_list = [sat_id, lite_datstr, cha_id, sel_id]
                        base_list = prim_list + [glm, gls, gln]
                        full_list = base_list + zonal_mean_list + \
                                    zonal_stdv_list + zonal_nobs_list
                        tuple_len = len(full_list)
                        holders   = ','.join('?' * tuple_len)
                        sql_query = "INSERT OR ABORT INTO %s "\
                                    "VALUES({0})".format(holders) % tab_sta
                        #sql_query = "INSERT OR REPLACE INTO %s "\
                        #            "VALUES({0})".format(holders) % tab_sta
                        db.execute(sql_query, full_list)


                    except KeyError:
                        break


        except sqlite3.Error, e: 
            if db: 
                db.rollback()
                print "\n *** Error: %s" % e.args[0]
                print (" *** Query %s for list %s" % (sql_query, full_list))
                sys.exit(1)


        finally: 
            if db: 
                db.commit()
                db.close()


    # -------------------------------------------------------------------
    else: 
        print (" --- FAILED: No output due to fishy input !")
    # -------------------------------------------------------------------


    # -------------------------------------------------------------------
    print ( "\n *** %s finished for %s and %s\n" % 
            (sys.argv[0], args.satellite, args.date) )
    # -------------------------------------------------------------------

