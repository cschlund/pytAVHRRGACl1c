#!/usr/bin/env python

import os
import sys
import math
import subprocess
import datetime
import time
import argparse
import numpy as np
from pycmsaf.logger import setup_root_logger

logdir = os.path.join(os.getcwd(),'log')
logger = setup_root_logger(name='root', logdir=logdir, 
                           append=True, logfile=True)


def remove_files(pwd, pattern):
    cnt = 0
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
            file2delete = os.path.join('ec:'+pathstring,line_list[-1])
            logger.info("{0}".format(file2delete))
            cnt += 1
            cmd2 = ["erm", file2delete]
            pmd2 = subprocess.Popen(cmd2, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout2, stderr2 = pmd2.communicate()
            logger.info("STDOUT: {0}".format(stdout2))
            logger.info("STDERR: {0}".format(stderr2))
    return cnt


def remove_subdir(pwd, sdir):
    full_subdir = os.path.join('ec:'+pwd, sdir)
    cmd = ["ermdir", full_subdir]
    pmd = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = pmd.communicate()
    logger.info("STDOUT: {0}".format(stdout))
    logger.info("STDERR: {0}".format(stderr))
    return full_subdir


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='''{0} deletes data
            from ECFS archive depending on user
            request.'''.format(os.path.basename(__file__)))
    parser.add_argument('-e', '--ecfs_basepath', type=str, required=True, 
                        help="/user/path/to/data")
    parser.add_argument('-p', '--pattern', type=str, 
                        help='''Search pattern, i.e. --pattern=tar, --pattern=NOAA7''')
    parser.add_argument('-s','--subdir', type=str, nargs='*', 
                        help='''Delete this subdirectory.''')
    args = parser.parse_args()

# some output for logfile 
logger.info("*** PARAMETER passed:")
logger.info("ECFS_BASEPATH: {0}".format(args.ecfs_basepath))
logger.info("SEARCH_PATTERN: {0}".format(args.pattern))
logger.info("SUBDIRECTORY: {0}".format(args.subdir))
# remove files and directory
if args.pattern: 
    num = remove_files(args.ecfs_basepath, args.pattern)
    logger.info("{0} files have been removed from ECFS archive".format(num))
elif args.subdir:
    for i in args.subdir: 
        ret = remove_subdir(args.ecfs_basepath, i) 
        logger.info("{0} has been removed from ECFS archive".format(ret))
else:
    logger.info('''Please tell me what to do: ''' 
                '''remove files via --pattern or ''' 
                '''remove directory via --subdir''')
logger.info("*** {0} finished\n".format(os.path.basename(__file__)))
