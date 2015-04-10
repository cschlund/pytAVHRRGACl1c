#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import os
import argparse
import subs_avhrrgac as subs
from pycmsaf.avhrr_gac.database import AvhrrGacDatabase
from pycmsaf.logger import setup_root_logger

logger = setup_root_logger(name='root')


def update_database(db):
    for sate in sat_list:

        satellite = subs.full_sat_name(sate)[2]

        logger.info("Get records for: %s" % satellite)

        (start_dates, end_dates, data_along) = subs.get_record_lists(satellite, db)

        if not start_dates and not end_dates and not data_along:
            record_flag = False
        else:
            record_flag = True

        if record_flag:

            if args.verbose:
                logger.info("Checking for midnight orbit and "
                            "number of overlapping lines\n")

            # -- loop over end dates
            for position, end_time in enumerate(end_dates):

                # -- start and end date/time of current orbit
                stime_current = start_dates[position]
                etime_current = end_dates[position]

                # -- check for midnight
                midnight_orbit_current = subs.calc_midnight(stime_current,
                                                            etime_current)

                # -- range: first orbit until last but one
                if (position + 1) < len(end_dates):

                    # -- start time of next orbit
                    stime_next = start_dates[position + 1]
                    etime_next = end_dates[position + 1]

                    # -- very first orbit:
                    # no cutting at the beginning of orbit
                    # i. e. start = 0 and end = along_track_dimension
                    if position == 0:
                        val_list = [add_cols[0], 0,
                                    add_cols[1], data_along[position],
                                    stime_current, etime_current, satellite]

                        if args.verbose:
                            logger.info("UPDATE db (first orbit): "
                                        "{0}".format(val_list))

                        subs.update_db_without_midnight(val_list, db)

                    # -- check for overlap
                    if etime_current >= stime_next:

                        # number of overlapping scanlines
                        overlap_rows = subs.calc_overlap(stime_next,
                                                         etime_current)

                        # CUT ORBIT AT THE BEGINNING of next orbit
                        # write to db corresponding to next orbit
                        start_next = overlap_rows
                        end_next = data_along[position + 1]

                        # CUT ORBIT AT THE END of current orbit
                        # write to db corrsp. to current orbit
                        start_current = 0
                        end_current = data_along[position] - overlap_rows

                        # -- update database next orbit
                        val_list = [add_cols[0], start_next,
                                    add_cols[1], end_next,
                                    stime_next, etime_next, satellite]

                        if args.verbose:
                            logger.info("UPDATE db (next orbit): "
                                        "{0}".format(val_list))

                        subs.update_db_without_midnight(val_list, db)

                        # -- update database current orbit
                        val_list = [add_cols[2], start_current,
                                    add_cols[3], end_current,
                                    add_cols[4], midnight_orbit_current,
                                    stime_current, etime_current, satellite]

                        if args.verbose:
                            logger.info("UPDATE db (current orbit): "
                                        "{0}".format(val_list))

                        subs.update_db_with_midnight(val_list, db)

                    # -- if no overlap was found: no cutting, i.e.
                    #    start = 0, end = along_track_dimension
                    else:

                        # -- update database next orbit
                        last_scanline_next = data_along[position + 1] - 1

                        val_list = [add_cols[0], 0,
                                    add_cols[1], last_scanline_next,
                                    stime_next, etime_next, satellite]

                        if args.verbose:
                            logger.info("UPDATE db (next orbit, no overlap): "
                                        "{0}".format(val_list))

                        subs.update_db_without_midnight(val_list, db)

                        # -- update database current orbit
                        last_scanline_current = data_along[position] - 1

                        val_list = [add_cols[2], 0,
                                    add_cols[3], last_scanline_current,
                                    add_cols[4], midnight_orbit_current,
                                    stime_current, etime_current, satellite]

                        if args.verbose:
                            logger.info("UPDATE db (current orbit, no overlap): "
                                        "{0}".format(val_list))

                        subs.update_db_with_midnight(val_list, db)

                # -- very last orbit 
                else:

                    val_list = [add_cols[2], 0,
                                add_cols[3], data_along[position],
                                add_cols[4], midnight_orbit_current,
                                stime_current, etime_current, satellite]

                    if args.verbose:
                        logger.info("UPDATE db (last orbit): "
                                    "{0}".format(val_list))

                    subs.update_db_with_midnight(val_list, db)

        else:

            logger.info("No data records found for %s\n" % satellite)

# -------------------------------------------------------------------

if __name__ == '__main__':

    satlist = '|'.join(subs.get_satellite_list())
    add_cols = subs.get_new_cols()

    parser = argparse.ArgumentParser(
        description=('{0} calculates the number of overlapping rows. '
                     'Five columns are added: \'{1}\' and \'{2}\' in '
                     'case the beginning of the orbits will be cut, \'{3}\' '
                     'and \'{4}\' in case the end of the orbit will be cut, '
                     'and \'{5}\' giving the midnight orbit scan line.').
        format(os.path.basename(__file__), add_cols[0], add_cols[1],
               add_cols[2], add_cols[3], add_cols[4]))

    parser.add_argument('-s', '--sat', type=subs.satstring,
                        help='Available are: ' + satlist + ', default: use all')
    parser.add_argument('-g', '--sqlcomp',
                        help='/path/to/sqlitefile.sqlite3', required=True)
    parser.add_argument('-v', '--verbose',
                        help='increase output verbosity', action="store_true")

    args = parser.parse_args()

    # -- some screen output if wanted
    logger.info("Parameter passed")
    logger.info("Satellite  : %s" % args.sat)
    logger.info("Verbose    : %s" % args.verbose)
    logger.info("DB_Sqlite3 : %s" % args.sqlcomp)

    # -- either use full sat list or only one
    if args.sat is None:
        sat_list = subs.get_satellite_list()
    else:
        sat_list = [args.sat]

    # -- connect to database
    dbfile = AvhrrGacDatabase(dbfile=args.sqlcomp, timeout=36000,
                              exclusive=True)

    logger.info("Read {0} ".format(args.sqlcomp))
    logger.info("Update database")
    update_database(dbfile)

    logger.info("Commit changes")
    dbfile.commit_changes()

    logger.info("%s finished" % os.path.basename(__file__))