#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Heidrun Hoeschen, Oct. 2014: read_data.py written
# C. Schlundt, Oct. 2014: modified to add2sqlite.py
# C. Schlundt, Nov. 2014: added read_qualflags file and added
#                         information about missing scanlines
#
# -------------------------------------------------------------------

import sqlite3 as lite
import h5py
import os, sys
import datetime
import argparse
import read_avhrrgac_h5 as rh5
import subs_avhrrgac as subs

# -------------------------------------------------------------------

parser = argparse.ArgumentParser(description='''%s
reads pyGAC L1c output h5 orbit files (avhrr, qualflags) 
and adds to L1b sqlite database, 
valuable L1c information for each orbit, 
which has been processed by pyGAC (white-listed L1b). 
Thus, this script adds the start and end time of measurement, i.e.
timestamp of first and last scanline, as well as the 
along and across track dimension. This L1c information will be 
later used for calculating the number of AVHRR GAC overlapping 
scanlines of two consecutive orbits.''' % os.path.basename(__file__))


parser.add_argument('-dat', '--date', type=subs.datestring, 
      help='String, e.g. 20000101, 2000-01-01', required=True)
parser.add_argument('-sat', '--satellite', type=subs.satstring, 
      help='String, e.g. noaa19, NOAA19', required=True)
parser.add_argument('-inp', '--inpdir', 
      help='String, e.g. /path/to/input/files.h5', required=True)
parser.add_argument('-sql', '--sqlite', 
      help='''/path/to/AVHRR_GAC_archive_L1b_L1c.sqlite3,
      which should be updated with L1c information''', 
      required=True)
parser.add_argument('-ver', '--verbose', \
      help='increase output verbosity', action="store_true")
      
args = parser.parse_args()

# -------------------------------------------------------------------

if args.verbose == True:
    print ("\n *** Parameter passed" )
    print (" ---------------------- ")
    print ("   - Date       : %s" % args.date)
    print ("   - Satellite  : %s" % args.satellite)
    print ("   - Input Path : %s" % args.inpdir)
    print ("   - Verbose    : %s" % args.verbose)
    print ("   - DB_Sqlite3 : %s" % args.sqlite)
  
# -------------------------------------------------------------------

# qual_flag: 0 (no scanlines missing), 1 (scanlines missing)
pattern    = 'ECC_GAC_avhrr*'+args.satellite+'*'+args.date+'T*'
fil_list   = subs.find(pattern, args.inpdir)
nfiles     = len(fil_list)
message    = "*** No files available for "+\
             args.date+", "+args.satellite

if nfiles == 0:
    print message
    sys.exit(0)
else:
    fil_list.sort()


# -------------------------------------------------------------------
# -- sqlite database containing L1b information updated with L1c inf.
try:
    # connect to database
    con = lite.connect(args.sqlite, timeout=36000)
    con.isolation_level = 'EXCLUSIVE'
    con.execute('BEGIN EXCLUSIVE')
    cur = con.cursor() 

    # new columns to be added
    alist = [ "ALTER TABLE orbits ADD COLUMN start_time_l1c TIMESTAMP ", 
              "ALTER TABLE orbits ADD COLUMN end_time_l1c TIMESTAMP ", 
              "ALTER TABLE orbits ADD COLUMN across_scanline INTEGER ", 
              "ALTER TABLE orbits ADD COLUMN along_scanline INTEGER ", 
              "ALTER TABLE orbits ADD COLUMN number_of_missing_scanlines INTEGER ", 
              "ALTER TABLE orbits ADD COLUMN missing_scanlines TEXT " ] 

    for act in alist: 
        try:
            cur.execute(act)
        except:
            pass # if already existing
      
    
    # -- loop over file list
    for fil in fil_list:
        qfil = None 

        # -- get x and y dimension of orbit
        f = h5py.File(fil, "r+")
        fil_dim = rh5.get_data_size(f)
        f.close()
        
        if fil_dim != None:
            data_across = fil_dim[1]
            data_along  = fil_dim[0]
            qfil = fil.replace("ECC_GAC_avhrr_", "ECC_GAC_qualflags_")
        else:
            print (" * Skip %s - fishy!" % fil)
            break

        # -- read quality flag file
        qfil = fil.replace("ECC_GAC_avhrr_", "ECC_GAC_qualflags_")
        q = h5py.File(qfil, "r+")
        #rh5.show_properties(q)
        (row, col, total_records, 
         last_scanline, data) = rh5.read_qualflags(q)
        q.close()

        if total_records == last_scanline:
            number_of_missing_scanlines = 0
            missing_scanlines = None
        else:
            number_of_missing_scanlines = abs(total_records - last_scanline)
            missing_scanlines = rh5.find_scanline_gaps(0, last_scanline, data)
            if args.verbose == True:
                print ("   * File:%s, Row:%s, Col:%s, TotalRecords:%s, "
                       "Last_ScanLine:%s, NumberOfMissingScanlines:%s, "
                       "MissingScanlines:%s" % 
                       (os.path.basename(qfil),row, col, total_records, 
                        last_scanline, number_of_missing_scanlines, 
                        missing_scanlines))

        # -- split filename
        split_string      = subs.split_filename(fil)
        satellite         = subs.full_sat_name(split_string[3])[2]
        start_date_string = split_string[5][0:-1]
        end_date_string   = split_string[6][0:-1]
        
        # -- get timestamp of first scanline
        datetimestr1   = ''.join(start_date_string.split('T'))
        microseconds1  = int(datetimestr1[-1])*1E5
        stime_l1c_help = datetime.datetime.strptime(datetimestr1[0:-1], '%Y%m%d%H%M%S')
        stime_query    = datetime.datetime.strptime(datetimestr1[0:-3], '%Y%m%d%H%M')
        stime_l1c      = stime_l1c_help + \
                          datetime.timedelta(microseconds=microseconds1)
                  
        # -- get timestamp of last scanline
        datetimestr2   = ''.join(end_date_string.split('T'))
        microseconds2  = int(datetimestr2[-1])*1E5
        etime_l1c_help = datetime.datetime.strptime(datetimestr2[0:-1], '%Y%m%d%H%M%S')
        etime_query    = datetime.datetime.strptime(datetimestr2[0:-3], '%Y%m%d%H%M')
        etime_l1c      = etime_l1c_help + \
                          datetime.timedelta(microseconds=microseconds2)
        
        # -- add to sqlite
        act = "update orbits set " \
          "start_time_l1c = \'{stime_l1c}\', end_time_l1c = \'{etime_l1c}\', "\
          "across_scanline = {data_across}, along_scanline = {data_along}, " \
          "number_of_missing_scanlines = {number_of_missing_scanlines}, " \
          "missing_scanlines = \'{missing_scanlines}\' WHERE blacklist=0 AND "\
          "start_time=\'{stime_query}\' AND end_time=\'{etime_query}\' AND " \
          "sat=\'{satellite}\'".format(
                  stime_l1c=stime_l1c, etime_l1c=etime_l1c,
                  data_across=data_across, data_along=data_along,
                  number_of_missing_scanlines=number_of_missing_scanlines,
                  missing_scanlines=missing_scanlines,
                  stime_query=stime_query, etime_query=etime_query,
                  satellite=satellite)

        cur.execute(act)
      
    # -- end of loop over file list: now commit update
    con.commit()
    
    if args.verbose == True:
        print "   * Number of rows updated: %d" % cur.rowcount
  
# -------------------------------------------------------------------
except lite.Error, e:
    if con: 
        con.rollback()

    print " *** Error %s:" % e.args[0]
    sys.exit(1)
# -------------------------------------------------------------------
finally:
    if con:
        con.close()
# -------------------------------------------------------------------
