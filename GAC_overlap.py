#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import os
import argparse
from pycmsaf.avhrr_gac.database import AvhrrGacDatabase
from pycmsaf.logger import setup_root_logger

logdir = os.path.join(os.getcwd(),'log')
logger = setup_root_logger(name='root', logdir=logdir, 
                           append=True, logfile=True)


def get_new_cols():
    """
    These columns will be added to already existing database
    providing information about overlapping scanlines and
    midnight orbit scanline.
    """
    return [#"start_scanline_begcut", "end_scanline_begcut",
            "start_scanline_endcut", "end_scanline_endcut",
            "midnight_scanline"]


if __name__ == '__main__':

    add_cols = get_new_cols()

    # -- arguments
    parser = argparse.ArgumentParser(
        description=('{0} calculates the number of overlapping rows. '
                     '3 columns are added: \'{1}\' and \'{2}\' and \'{3}\' .').
        format(os.path.basename(__file__), add_cols[0], add_cols[1], add_cols[2]))

    parser.add_argument('--sqlfile', help='/path/to/database.sqlite3', required=True)

    args = parser.parse_args()

    # -- connect to database
    dbfile = AvhrrGacDatabase(dbfile=args.sqlfile, timeout=36000, exclusive=True)

    logger.info("Compute overlap {0} ".format(args.sqlfile))
    dbfile.compute_overlap()

    logger.info("Commit changes")
    dbfile.commit_changes()

    logger.info("%s finished" % os.path.basename(__file__))
