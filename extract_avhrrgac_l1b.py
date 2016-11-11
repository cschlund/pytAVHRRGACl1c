#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import os
import random
import subprocess
from pycmsaf.logger import setup_root_logger

logdir = os.path.join(os.getcwd(), 'log')
logger = setup_root_logger(name='root', logdir=logdir, append=True, logfile=True)


def extract_files(args, filelist):
    """
    Extract files from tarfiles using input and output arguments.
    """
    for f in filelist:
        split_tarfile = f['tarfile_name'].split('/')
        tar = split_tarfile[-1:][0]
        sub = split_tarfile[-2:][0]
        target = os.path.join(sub,f['filename'])

        # copy source file because it is already extracted
        avhrr_1_list = ['NOAA5', 'NOAA6', 'NOAA8', 'NOAA10']
        if f['satellite_name'] in avhrr_1_list:
            new_subdir = os.path.join(args.output_path,sub)
            if not os.path.exists(new_subdir):
                os.makedirs(new_subdir)
            source = os.path.join(args.input_path,f['satellite_name'],f['filename'])
            logger.info("Copy L1b: {0}".format(f['filename']))
            c1 = ["cp", source, new_subdir]

        # extract L1b file to "-C inp", different location
        else:
            source = os.path.join(args.input_path,f['satellite_name'],tar)
            logger.info("Get L1b from tarfile: {0}".format(f['filename']))
            c1 = ["tar", "xf", source, "-C", args.output_path, target]

        p1 = subprocess.Popen(c1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p1.communicate()
        if stdout: 
            logger.info("STDOUT:{0}".format(stdout))
        if stderr: 
            logger.info("STDERR:{0}".format(stderr))


def get_file_list_from_sql(args, db): 
    """
    Extract information from SQLite database.
    """
    # blacklisted orbits
    if args.blacklist_reason: 
        if args.satellite: 
            cmd = "SELECT satellite_name, filename, tarfile_name "\
                  "FROM vw_std WHERE blacklist_reason=\'{blr}\' "\
                  "and satellite_name=\'{sat}\' "\
                  "order by start_time_l1b"
            res = db.execute(cmd.format(blr=args.blacklist_reason,
                                        sat=args.satellite)) 
        else: 
            cmd = "SELECT satellite_name, filename, tarfile_name "\
                  "FROM vw_std WHERE blacklist_reason=\'{blr}\' "\
                  "order by start_time_l1b"
            res = db.execute(cmd.format(blr=args.blacklist_reason)) 

    # whitelisted orbits
    else:
        if args.satellite: 
            cmd = "SELECT satellite_name, filename, tarfile_name "\
                  "FROM vw_std WHERE blacklist=0 AND "\
                  "start_time_l1c is not null AND "\
                  "satellite_name=\'{sat}\' order by start_time_l1b"
            res = db.execute(cmd.format(sat=args.satellite)) 
        else: 
            cmd = "SELECT satellite_name, filename, tarfile_name "\
                  "FROM vw_std WHERE blacklist=0 AND "\
                  #"number_of_missing_scanlines>12000 AND "\
                  "start_time_l1c is not null order by start_time_l1b"
            res = db.execute(cmd) 

    res_length = len(res)
    if args.number_of_files > res_length:
        num = res_length
    else:
        num = args.number_of_files

    choices = []
    while len(choices) < num: 
        selection = random.choice(res) 
        if selection not in choices: 
            choices.append(selection) 

    return choices


if __name__ == '__main__':

    sqlfile="AVHRR_GAC_archive_v2_201603_post_overlap.sqlite3"
    inppath="/scratch/ms/de/sf1/AVHRR_GAC_L1B_tarfiles"
    outpath="/scratch/ms/de/sf7/cschlund/tmp"

    predict = pre_blacklist_reasons()
    procdict = proc_blacklist_reasons()
    postdict = post_blacklist_reasons()
    blr_dict = dict(predict.items() + procdict.items() + postdict.items())
    blr_list = list()
    for key, value in blr_dict.iteritems(): 
        blr_list.append(value)
    blacklisting = ' | '.join(blr_list)

    parser = argparse.ArgumentParser(
        description=('{0} reads the AVHRR GAC L1 SQL database and extracts L1b files to $SCRATCH. '
                     'See Usage for more information.').format( os.path.basename(__file__)))

    parser.add_argument('-dbf', '--dbfile', default=sqlfile, help='Default is: {0}'.format(sqlfile))

    parser.add_argument('-blr', '--blacklist_reason', help='Select specific blacklist reason: '+blacklisting)

    parser.add_argument('-num', '--number_of_files', type=int, default=5, help='Number of files to be extracted.')

    parser.add_argument('-inp', '--input_path', default=inppath, help='Default is: {0}'.format(inppath))

    parser.add_argument('-out', '--output_path', default=outpath, help='Default is: {0}'.format(outpath))

    parser.add_argument('-sat', '--satellite', type=str2upper, help='Select a specific satellite.')

    args = parser.parse_args()

    # -- settings
    logger.info("SQLite File     : {0}".format(args.dbfile))
    logger.info("Blacklist reason: {0}".format(args.blacklist_reason))
    logger.info("Number of files : {0}".format(args.number_of_files))
    logger.info("Satellite       : {0}".format(args.satellite))
    logger.info("Input path      : {0}".format(args.input_path))
    logger.info("Output path     : {0}".format(args.output_path))

    # -- create output directory if not already existing
    if not os.path.exists(args.output_path):
        os.makedirs(args.output_path)

    # -- connect to database
    db = AvhrrGacDatabase(dbfile=args.dbfile, timeout=36000, exclusive=True)

    # -- get file list
    file_list = get_file_list_from_sql(args, db)
    logger.info("{0} files will be extracted.".format(len(file_list)))

    # -- extract data from tarfile
    extract_files(args, file_list)

    logger.info("%s finished\n\n" % os.path.basename(__file__))