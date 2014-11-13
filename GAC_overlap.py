#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Heidrun Hoeschen, Oct. 2014
# C. Schlundt, Nov. 2014, minor changes
#

import sqlite3
import h5py
import os, sys
import argparse
import datetime, time
import read_avhrrgac_h5 as rh5
import subs_avhrrgac as subs
import numpy as np


if __name__ == '__main__': 

    satlist  = '|'.join(subs.get_satellite_list())
    add_cols = subs.get_new_cols()

    parser = argparse.ArgumentParser(description='''%s
    calculates the number of overlapping rows. 
    Five columns are added: \'%s\' and \'%s\'
    in case the beginning of the orbits will be cut, 
    \'%s\' and \'%s\' in case the end of the orbit will be cut, 
    and \'%s\' giving the midnight orbit scan line.''' % 
    (os.path.basename(__file__),add_cols[0], add_cols[1],
        add_cols[2], add_cols[3], add_cols[4]))

    parser.add_argument('-s', '--sat', type=subs.satstring,
            help='Available are: '+satlist+', default: use all')
    parser.add_argument('-g', '--sqlcomp',
            help='/path/to/sqlitefile.sqlite3', required=True)
    parser.add_argument('-v', '--verbose',
            help='increase output verbosity', action="store_true")

    args = parser.parse_args()

    # ----------------------------------------------------------------
    # -- some screen output if wanted

    if args.verbose == True: 
        print ("\n *** Parameter passed" )
        print (" ---------------------- ")
        print ("   - Satellite  : %s" % args.sat)
        print ("   - Verbose    : %s" % args.verbose)
        print ("   - DB_Sqlite3 : %s\n" % args.sqlcomp)

    # ----------------------------------------------------------------
    # -- either use full sat list or only one

    if args.sat == None:
        sat_list = subs.get_satellite_list()
    else:
        sat_list = [args.sat]

    # ----------------------------------------------------------------
    # -- settings for sqlite  

    try:
        # -- connect to database
        db = sqlite3.connect(args.sqlcomp, 
                detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)    
        db.row_factory=subs.dict_factory
        cursor = db.cursor()

        for i in add_cols: 
            try: 
                act1 = "ALTER TABLE orbits ADD COLUMN "+i+" INTEGER"
                db.execute(act1) 
            except: 
                pass


        if args.verbose == True: 
            print ("   + Read %s " % args.sqlcomp)


        # -- loop over satellite list
        for sate in sat_list: 
            satellite = subs.full_sat_name(sate)[2] 

            if args.verbose == True: 
                print ("\n      - Get records for: %s" % satellite)

            (start_dates, end_dates, 
             data_along) = subs.get_record_lists(satellite,db)

            if not start_dates and not end_dates and not data_along:
                record_flag = False
            else:
                record_flag = True


            if record_flag == True:

                if args.verbose == True: 
                    print ("      - Checking for midnight orbit "\
                           "and number of overlapping lines")
 
                counter = 0
                endcut_helper = 0

                # -- loop over end dates
                for etime in end_dates: 

                    # ***********************************************************
                    #                                                           #
                    #       if start of next orbit is earlier                   #
                    #       than end of current orbit                           #
                    #                                                           #
                    # ***********************************************************

                    if (counter+1)<len(end_dates) and counter>0: 

                        # -- start and end date/time of current orbit
                        stime_safe = start_dates[counter]
                        etime_safe = end_dates[counter]


                        # -- check for midnight orbit & check if day has changed
                        if stime_safe.day < etime_safe.day: 
                            
                            # calculate how much time has passed 
                            # between start time and midnight 
                            midnight = datetime.datetime.strptime( 
                                        str(etime_safe.day*1000000), '%d%H%M%S')
                            midnight = midnight + datetime.timedelta(microseconds=0)
                            midnight_diff = midnight-stime_safe
                            
                            # calculate the orbit line under the 
                            # assumption of 2 scanlines/second
                            midnight_diff_msec  = midnight_diff.seconds+\
                                                  midnight_diff.microseconds/1000000
                            midnight_orbit_calc = midnight_diff_msec*2

                        else:
                            
                            # set midnight variable to -1 if the day hasn't changed 
                            midnight_orbit_calc = -1


                        # -- check for overlap
                        if etime >= start_dates[counter+1]:
                            
                            # time difference between 
                            # start of next orbit and end of current orbit
                            # assumption: 2 scanlines per second
                            timediff      = etime-start_dates[counter+1]
                            timediff_msec = timediff.days*24*60*60+timediff.seconds\
                                            +timediff.microseconds/1000000
                            overlap_rows  = timediff_msec*2
                            
                            # CUT ORBIT AT THE BEGINNING of next orbit
                            start_begcut = overlap_rows
                            end_begcut   = data_along[counter+1]

                            # CUT ORBIT AT THE END of current orbit
                            start_endcut = 0
                            end_endcut   = data_along[counter] - overlap_rows

                            # -- update database
                            val_list = [start_begcut, end_begcut,
                                        start_endcut, end_endcut,
                                        midnight_orbit_calc, 
                                        stime_safe, etime_safe, satellite]

                            subs.update_database(add_cols, val_list, db)
                  

                        # -- if no overlap was found: no cutting !
                        else: 

                            # -- update database
                            val_list = [0, data_along[counter+1],
                                        0, data_along[counter+1],
                                        midnight_orbit_calc, 
                                        stime_safe, etime_safe, satellite]

                            subs.update_database(add_cols, val_list, db)


                    # ***********************************************************
                    #                                                           #
                    #   Special case if very first orbit: counter == 0          #
                    #   important if the end of each orbit will be cut.         #
                    #                                                           #
                    #   (The last orbit is no special case, since the program   #
                    #   was originally written as 1. calculating overlap,       #
                    #                             2. finding new beginning)     #
                    #                                                           #
                    # ***********************************************************

                    elif counter == 0: 

                        # -- start and end date/time of next orbit
                        stime_safe = start_dates[counter+1] 
                        etime_safe = end_dates[counter+1] 


                        # -- check for midnight orbit, details see above 
                        if stime_safe.day<etime_safe.day: 

                            midnight = datetime.datetime.strptime(
                                        str(etime_safe.day*1000000), '%d%H%M%S')
                            midnight = midnight + datetime.timedelta(microseconds=0)

                            midnight_diff       = midnight-stime_safe
                            midnight_diff_msec  = midnight_diff.seconds+\
                                                  midnight_diff.microseconds/1000000
                            midnight_orbit_calc = midnight_diff_msec*2 

                        else:

                            midnight_orbit_calc = -1


                        # -- calculating overlapping scanlines, details see above
                        timediff      = end_dates[counter]-start_dates[counter+1]
                        timediff_msec = timediff.days*24*60*60+timediff.seconds+\
                                        timediff.microseconds/1000000
                        overlap_rows  = timediff_msec*2

                        # CUT ORBIT AT THE BEGINNING of next orbit
                        start_begcut = 0
                        end_begcut   = data_along[counter]

                        # CUT ORBIT AT THE END of current orbit
                        start_endcut = 0
                        end_endcut   = data_along[counter] - overlap_rows

                        # -- update database
                        val_list = [start_begcut, end_begcut,
                                    start_endcut, end_endcut,
                                    midnight_orbit_calc, 
                                    start_dates[counter], end_dates[counter], 
                                    satellite]

                        subs.update_database(add_cols, val_list, db)

                  
                    # end of if (counter+1)<len(end_dates) and counter>0: 
                    # elif counter == 0: 

                    # -- pick next orbits
                    counter=counter+1 

                # end of for loop: etime in end_dates

            # end if record_flag == True:
            else:
                print ("      ! No data records found for %s" % satellite)

        # end of for loop: sate in sat_list

    # ----------------------------------------------------------------

    except sqlite3.Error, e:
        if db: 
            db.rollback()

        print "\n *** Error %s ***\n" % e.args[0]
        sys.exit(1)
    
    # ----------------------------------------------------------------
    
    finally:
        if db: 
            if args.verbose == True: 
                print ("\n   + Commit and close %s\n" % 
                        args.sqlcomp)
    
            db.commit()
            db.close()
    
    # ----------------------------------------------------------------

# end of main code
