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
from pycmsaf.avhrr_gac.database import AvhrrGacDatabase

# -------------------------------------------------------------------
def update_database(db): 

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
 

            # -- loop over end dates
            for position, end_time in enumerate(end_dates): 

                # -- start and end date/time of current orbit
                stime_current = start_dates[position]
                etime_current = end_dates[position]

                # -- check for midnight
                midnight_orbit_current = subs.calc_midnight(stime_current,
                                                            etime_current)

                # -- range: first orbit until last but one
                if (position+1) < len(end_dates):

                    # -- start time of next orbit
                    stime_next = start_dates[position+1]
                    etime_next = end_dates[position+1]


                    # -- very first orbit: 
                    #    no cutting at the beginning of orbit
                    #    i. e. start = 0 and end = along_track_dimension
                    if position == 0:
                        val_list = [add_cols[0], 0, 
                                    add_cols[1], data_along[position],
                                    stime_current, etime_current, satellite]

                        subs.update_db_without_midnight(val_list, db)


                    # -- check for overlap
                    if etime_current >= stime_next:
                        
                        # number of overlapping scanlines
                        overlap_rows = subs.calc_overlap(stime_next,
                                                         etime_current)
                        
                        # CUT ORBIT AT THE BEGINNING of next orbit
                        # write to db corresponding to next orbit
                        start_next = overlap_rows
                        end_next   = data_along[position+1]

                        # CUT ORBIT AT THE END of current orbit
                        # write to db corrsp. to current orbit
                        start_current = 0
                        end_current   = data_along[position] - \
                                        overlap_rows

                        # -- update database next orbit
                        val_list = [add_cols[0], start_next, 
                                    add_cols[1], end_next,
                                    stime_next, etime_next, satellite]

                        subs.update_db_without_midnight(val_list, db)
              
                        # -- update database current orbit
                        val_list = [add_cols[2], start_current, 
                                    add_cols[3], end_current,
                                    add_cols[4], midnight_orbit_current,
                                    stime_current, etime_current, satellite]

                        subs.update_db_with_midnight(val_list, db)


                    # -- if no overlap was found: no cutting, i.e.
                    #    start = 0, end = along_track_dimension
                    else: 

                        # -- update database next orbit
                        val_list = [add_cols[0], 0, 
                                    add_cols[1], data_along[position+1],
                                    stime_next, etime_next, satellite]

                        subs.update_db_without_midnight(val_list, db)
              
                        # -- update database current orbit
                        val_list = [add_cols[2], 0, 
                                    add_cols[3], data_along[position],
                                    add_cols[4], midnight_orbit_current,
                                    stime_current, etime_current, satellite]

                        subs.update_db_with_midnight(val_list, db)


                # -- very last orbit 
                else: 

                    val_list = [add_cols[2], 0, 
                                add_cols[3], data_along[position],
                                add_cols[4], midnight_orbit_current,
                                stime_current, etime_current, satellite]

                    subs.update_db_with_midnight(val_list, db)

                # end of if position < len(end_dates) 

            # end of for loop: etime in end_dates

        # end if record_flag == True:
        else:
            print ("      ! No data records found for %s" % satellite)

    # end of for loop: sate in sat_list

# -------------------------------------------------------------------

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


    # -- some screen output if wanted
    if args.verbose == True: 
        print ("\n *** Parameter passed" )
        print (" ---------------------- ")
        print ("   - Satellite  : %s" % args.sat)
        print ("   - Verbose    : %s" % args.verbose)
        print ("   - DB_Sqlite3 : %s\n" % args.sqlcomp)

    
    # -- either use full sat list or only one
    if args.sat == None:
        sat_list = subs.get_satellite_list()
    else:
        sat_list = [args.sat]

    
    # -- settings for sqlite  
    db = AvhrrGacDatabase(dbfile=args.sqlcomp, timeout=36000, exclusive=True)

    if args.verbose == True: 
        print ("   + Read %s " % args.sqlcomp)

    update_database(db)

    db.commit_changes()

# end of main code
