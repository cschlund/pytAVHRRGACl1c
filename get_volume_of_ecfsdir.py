#!/usr/bin/env python

import os
import math
import subprocess
import datetime
import time
import argparse
import numpy as np
from pycmsaf.logger import setup_root_logger

logdir = os.path.join(os.getcwd(), 'log')
logger = setup_root_logger(name='root', logdir=logdir, append=True, logfile=True)


def convertSize(size): 
    size_name = ("KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size,1024)))
    p = math.pow(1024,i)
    s = round(size/p,2)
    if s > 0:
        return '%s %s' % (s,size_name[i])
    else: 
        return '0B'


def collect_info(pwd, pattern):
    # define lists
    ecfsdir_list = list()
    tarball_list = list()
    tarsize_list = list()
    # get data record from archive
    pathstring = pwd
    cmd = ["els", "-Rl", 'ec:'+pwd]
    logger.info("Working on: {0}".format(cmd))
    pmd = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = pmd.communicate()
    lines = stdout.split("\n")
    for line in lines:
        if line.startswith(pwd):
            pathstring = line
        if pattern in line:
            line_list = line.split()
            tarsize = line_list[4]
            tarball = line_list[-1]
            if tarball and tarsize and pathstring:
                tarball_list.append(tarball)
                tarsize_list.append(tarsize)
                ecfsdir_list.append("ec:"+pathstring)
    # return lists
    return ecfsdir_list, tarball_list, tarsize_list


def convert_tarball_sizes(tarsize_list):
    tarsizes = [int(i)/1024 for i in tarsize_list]
    tarsizes_converted = [convertSize(i) for i in tarsizes]
    tarsizes_array = np.asarray(tarsizes)
    total_size = tarsizes_array.sum()
    return convertSize(total_size), tarsizes_converted


def write2file(output_list):
    ts = datetime.datetime.fromtimestamp(time.time())
    timestamp = ts.strftime("%Y-%m-%d %H:%M:%S")
    colsrange = range(0,len(output_list[0]),1)
    f = open(outf, mode="w")
    f.write("# Created: {0}\n".format(timestamp))
    f.write("# Total size of data: {0}\n".format(totalsize))
    f.write("# Columns: ecfs_path_to_file  ")
    f.write("satellite_monthly_tarball_filename  ")
    f.write("tarball_size [Byte]  ")
    f.write("tarball_size_converted\n")
    for i in output_list:
        for c in colsrange:
            f.write(i[c]+"  ")
        f.write("\n")
    f.close()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='''{0}
    calculates the data volume of specified ECFS directory.'''.format( os.path.basename(__file__)))

    parser.add_argument('-e', '--ecfs_basepath', type=str,
                        required=True, help="/user/path/to/data")

    parser.add_argument('-p', '--pattern', type=str,
                        required=True, help='''Search pattern, i.e. --pattern=tar''')

    args = parser.parse_args()

    # output file summarizing results
    outf = 'ECFS' + args.ecfs_basepath.replace("/", "_") + '.txt'

    # some output for logfile
    logger.info("*** PARAMETER passed:")
    logger.info("ECFS_BASEPATH: {0}".format(args.ecfs_basepath))
    logger.info("SEARCH_PATTERN: {0}".format(args.pattern))
    logger.info("OUTPUT_FILENAME: {0}".format(outf))

    # collect information
    (dirs, balls, sizes) = collect_info(args.ecfs_basepath, args.pattern)

    # convert sizes
    (totalsize, sizelist_converted) = convert_tarball_sizes(sizes)
    logger.info("Total size of data: {0}".format(totalsize))

    # write information to file
    logger.info("WRITE2FILE: {0}".format(outf))
    write2file(zip(dirs, balls, sizes, sizelist_converted))

    logger.info("*** {0} finished\n".format(os.path.basename(__file__)))