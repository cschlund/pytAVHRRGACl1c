#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# testing pygac on local machine
#

import subprocess
import subs_avhrrgac as subs
import quick_l1c_analysis as quick

from config_run_pygac import *

from pycmsaf.logger import setup_root_logger
logdir = os.path.join(os.getcwd(), 'log')
logger = setup_root_logger(name='root', logdir=logdir, append=False, logfile=True)


def def_pygac_cfg(fil, out):
    """
    Define config file for pygac.
    :param fil: config file name
    :param out: ouput directory
    :return:
    """
    try:
        f = open(fil, mode="w")
        f.write("[tle]\n")
        f.write("tledir = " + pygac_tle_dir + "\n")
        f.write("tlename = " + pygac_tle_txt + "\n")
        f.write("\n")
        f.write("[output]\n")
        f.write("output_dir = " + out + "\n")
        f.write("output_file_prefix = " + pygac_prefix + "\n")
        f.close()
    except Exception as e:
        logger.info("FAILED: {0}".format(e))


def call_pygac(file_list):
    """
    Run PyGAC over each file in list.
    Collect information about logfile and L1c File content for later analysis.
    :param file_list: L1b file list
    :return:
    """
    # -- collect failed L1b orbits
    failed_l1b_orbits = list()

    # -- create pygac config file
    cfg_file = os.path.join(out_path, "run_pygac.cfg")
    def_pygac_cfg(cfg_file, out_path)
    os.putenv('PYGAC_CONFIG_FILE', cfg_file)

    logger.info("call {0}".format(os.path.basename(pygac_runtool)))

    # -- loop over files
    for ifile in file_list:
        logger.info("Working on {0}".format(ifile))

        c3 = ["python", pygac_runtool, ifile, "0", "0"]
        p3 = subprocess.Popen(c3, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p3.communicate()
        stderr_lines = stderr.split("\n")

        logger.info("STDOUT:{0}".format(stdout.strip()))
        logger.info("STDERR:")
        for line in stderr_lines:
            print line

        logger.info("collect information out of STDERR")
        ifile_l1c = None
        pygac_took = None
        p_warnings = list()
        p_errors = list()
        for line in stderr_lines:
            if "warning" in line.lower():
                p_warnings.append(line)
            elif "error" in line.lower():
                p_errors.append(line)
            elif "Filename: "+pygac_prefix+"_avhrr" in line:
                line_list = line.split()
                ret = filter(lambda x: '.h5' in x, line_list)[0]
                ifile_l1c = os.path.join(out_path, ret)
            elif "pygac took" in line.lower():
                ll = line.split()
                from datetime import datetime, timedelta
                t = datetime.strptime(ll[-1], "%H:%M:%S.%f")
                pygac_took = timedelta(hours=t.hour, minutes=t.minute, seconds=t.second,
                                       microseconds=t.microsecond).total_seconds()
            else:
                continue

        logger.info("L1c File: {0}".format(ifile_l1c))
        logger.info("Errors  : {0}".format(p_errors))
        logger.info("Warnings: {0}".format(p_warnings))
        logger.info("RunTime : {0}".format(pygac_took))

        logger.info("Collect records for quick L1c analysis\n")
        quick.collect_records(l1b_file=ifile+'.gz', l1c_file=ifile_l1c,
                              sql_file=sql_quick_output, pygac_version=pygac_commit,
                              pygac_took=pygac_took, pygac_errors=p_errors, pygac_warnings=p_warnings)

        if ifile_l1c is None:
            failed_l1b_orbits.append(ifile)

    if len(failed_l1b_orbits) > 0:
        logger.info("PYGAC FAILED {0} time(s):".format(len(failed_l1b_orbits)))
        for failed in failed_l1b_orbits:
            logger.info("{0}".format(failed))


if __name__ == '__main__':

    logger.info("{0} started for ".format(os.path.basename(__file__)))

    logger.info("PyGAC Version: {0}".format(pygac_commit))
    logger.info("PyGAC Input  : {0}".format(inp_path))
    logger.info("PyGAC Output : {0}".format(out_path))
    logger.info("SQL database : {0}".format(sql_quick_output))

    # -- Get AVHRR GAC l1b file list
    file_list = subs.find("NSS*", inp_path)
    logger.info("{0} files found".format(len(file_list)))

    # -- Call PyGAC and make quick analysis of each orbit
    call_pygac(file_list)

    logger.info("{0} successfully finished\n\n".format(os.path.basename(__file__)))


