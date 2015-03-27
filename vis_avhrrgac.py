#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# how to use the script: 
# > python script.py -h
#
# C.Schlundt: March, 2015
#

import os
import sys
import h5py
import argparse
import regionslist as rl
import subs_avhrrgac as mysub
import subs_mapping as myplt
import read_avhrrgac_h5 as rh5
from pycmsaf.logger import setup_root_logger

logger = setup_root_logger(name='root')

work_dir = os.getcwd()
work_out = os.path.join(work_dir, 'maps')
avail = sorted(rl.REGIONS.keys())
defin = ', '.join(map(str, avail))
chalist = '|'.join(mysub.get_channel_list())
sellist = '|'.join(mysub.get_select_list())


def get_file_list(subargs):
    """
    Verify file list and return it.
    """
    if subargs.files:
        file_list = subargs.files

    elif subargs.datestring and subargs.inputdir and subargs.satellite:
        pattern = 'ECC_GAC_avhrr*' + subargs.satellite.lower() + \
                  '*' + subargs.datestring + 'T*'
        file_list = mysub.find(pattern, subargs.inputdir)
        message = "No files available for "
        if not file_list:
            logger.info(message + pattern)
            sys.exit(0)

    else:
        logger.info("Use either --files or --datestring "
                    "in combination with --inputdir and --satellite.")
        sys.exit(0)

    return file_list


def map_cci(args_cci):
    """
    Visualize AVHRR GAC cloud_cci results.
    """
    # set file list
    fil_list = get_file_list(args_cci)

    # create output directory if not existing
    if not os.path.exists(args_cci.outputdir):
        os.makedirs(args_cci.outputdir)

    for f in fil_list:
        myplt.map_cloud_cci(f, args_cci.product, args_cci.region,
                            args_cci.outputdir,
                            args_cci.basemap_background)

    return

def map_l1c(args_l1c):
    """
    Visualize AVHRR GAC L1c data. 1 file per orbit.
    """
    # set file list
    fil_list = get_file_list(args_l1c)

    # create output directory if not existing
    if not os.path.exists(args_l1c.outputdir):
        os.makedirs(args_l1c.outputdir)

    # loop over file list
    for fil in fil_list:

        logger.info("Read: {0}".format(fil))

        afil = fil.replace("ECC_GAC_avhrr_", "ECC_GAC_sunsatangles_")
        f = h5py.File(fil, "r+")
        a = h5py.File(afil, "r+")
        (latitude, longitude, target) = rh5.read_avhrrgac(f, a, args_l1c.time,
                                                          args_l1c.channel,
                                                          # args_l1c.verbose)
                                                          False)
        a.close()
        f.close()

        logger.info("Map AVHRR GAC L1c: "
                    "{0}, {1}, {2}".format(args_l1c.channel, args_l1c.time,
                                           args_l1c.region))

        myplt.map_avhrrgac_l1c(fil, args_l1c.channel, args_l1c.region,
                               args_l1c.time, args_l1c.outputdir,
                               longitude, latitude, target,
                               args_l1c.basemap_background)

    return


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='''{0} displays AVHRR GAC data (pygac: l1c;
        cloud_cci: l2, l3u, l3c, l3s).'''.format(os.path.basename(__file__)))

    # add main arguments
    parser.add_argument('-reg', '--region', help=defin, default='glo')
    parser.add_argument('-out', '--outputdir', help='/path/to/mapdir', default=work_out)
    parser.add_argument('-bmb', '--basemap_background', help='bluemarble/shaderelief/etopo')
    parser.add_argument('-ver', '--verbose', help='increase output verbosity', action="store_true")

    # define subcommands
    subparsers = parser.add_subparsers(help="Select a Subcommand")

    # plot l1c data
    map_l1c_parser = subparsers.add_parser('map_l1c', description="Map pygac results.")
    map_l1c_parser.add_argument('-cha', '--channel', help=chalist + ', default is ch1', default='ch1')
    map_l1c_parser.add_argument('-fil', '--files', nargs='*', help='List of full qualified files.')
    map_l1c_parser.add_argument('-dat', '--datestring', type=mysub.datestring, help='e.g. 2008-01-01, 2009/06/02')
    map_l1c_parser.add_argument('-sat', '--satellite', type=str, help='e.g. NOAA18, metopb')
    map_l1c_parser.add_argument('-inp', '--inputdir', help='/path/to/l1c/files')
    map_l1c_parser.add_argument('-tim', '--time', default='all', help=sellist + ', default is all')
    map_l1c_parser.set_defaults(func=map_l1c)

    # plot cloud cci data
    map_cci_parser = subparsers.add_parser('map_cci', description="Map cloud cci results.")
    map_cci_parser.add_argument('-pro', '--product', required=True)
    map_cci_parser.add_argument('-fil', '--files', nargs='*', help='List of full qualified files.')
    map_cci_parser.add_argument('-dat', '--datestring', type=mysub.datestring, help='e.g. 2008-01-01, 2009/06/02')
    map_cci_parser.add_argument('-sat', '--satellite', type=str, help='e.g. NOAA18, metopb')
    map_cci_parser.add_argument('-inp', '--inputdir', help='/path/to/cci/files')
    map_cci_parser.set_defaults(func=map_cci)

    # Parse arguments
    args = parser.parse_args()

    # Call function associated with the selected subcommand
    logger.info("*** {0} start for {1}".format(sys.argv[0], args))
    args.func(args)

    logger.info("*** {0} succesfully finished".format(sys.argv[0]))