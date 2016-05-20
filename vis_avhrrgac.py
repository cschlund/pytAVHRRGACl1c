#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# how to use the script: 
# > python script.py -h
#
# C.Schlundt: March, 2015
#

import matplotlib
#matplotlib.use('GTK3Agg')
import os
import sys
import argparse
import regionslist as rl
import subs_avhrrgac as mysub
import subs_mapping as myplt
from pycmsaf.logger import setup_root_logger

logdir = os.path.join(os.getcwd(),'log')
logger = setup_root_logger(name='root', logdir=logdir, 
                           append=True, logfile=True)

work_dir = os.getcwd()
work_out = os.path.join(work_dir, 'maps')
avail = sorted(rl.REGIONS.keys())
defin = ', '.join(map(str, avail))
chalist = '|'.join(mysub.get_channel_list())
sellist = '|'.join(mysub.get_select_list())


def get_file_list(sargs):
    """
    Verify file list and return it.
    """
    if sargs.files:
        file_list = sargs.files
        file_list.sort()
        return file_list

    elif sargs.date and sargs.inputdir:
        pattern = 'ECC_GAC_avhrr_*'+sargs.date+'*'
        file_list = mysub.find(pattern, sargs.inputdir)
        message = "No files available for "
        if not file_list:
            logger.info(message+sargs.date+' in '+sargs.inputdir)
            sys.exit(0)
        else:
            file_list.sort()
            return file_list
      
    else:
        logger.info("Option 1: use -fil=filenames ")
        logger.info("Option 2: use -inp=inpdir -dat=date.")
        sys.exit(0)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='''{0} displays AVHRR GAC data (pygac: l1c).
        Note: the orbits are simply plotted onto the map without
        any averaging.'''.format(os.path.basename(__file__)))

    # add main arguments
    parser.add_argument('-dbf', '--dbfile', help='/path/to/dbfile', required=True)
    parser.add_argument('-reg', '--region', help=defin, default='glo')
    parser.add_argument('-out', '--outputdir', help='/pwd/maps', default=work_out)
    parser.add_argument('-bmb', '--background', help='bluemarble/shaderelief/etopo',
                        default=None)
    parser.add_argument('-ver', '--verbose', help='increase output verbosity', action="store_true")
    parser.add_argument('-cha', '--channel', help=chalist + ', default is ch1', default='ch1')
    parser.add_argument('-fil', '--files', nargs='*', help='List of full qualified files.')
    parser.add_argument('-dat', '--date', type=mysub.datestring, help='2008-07-01')
    parser.add_argument('-inp', '--inputdir', help='/path/to/l1c/files')
    parser.add_argument('-tim', '--time', default='all', help=sellist + ', default is all')
    parser.add_argument('-off', '--overlap_off', action="store_true",
                        help='Overlap is not taken into account.')
    parser.add_argument('-mid', '--midnight', action="store_true",
                        help='Consider midnight as last scanline.')
    parser.add_argument('-qfl', '--qflag', action="store_true",
                        help='Plot qflag file.')
                        
    # Parse arguments
    args = parser.parse_args()

    # Call function associated with the selected subcommand
    logger.info("*** {0} start for {1}".format(sys.argv[0], args))

    # get file list
    fil_list = get_file_list(args)
    
    # create output directory if not existing
    if not os.path.exists(args.outputdir):
        os.makedirs(args.outputdir)
    
    myplt.map_avhrrgac_l1c(fil_list, args)

    logger.info("*** {0} succesfully finished\n\n".format(sys.argv[0]))
