#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import sys
import sqlite3 as lite
import collections
import subs_avhrrgac as mysub
import numpy as np
import matplotlib.pyplot as plt
from pylab import *
from subs_plot_sql import plot_time_series
from config_plot_quick_l1c_analysis import *
from pycmsaf.logger import setup_root_logger

logger = setup_root_logger(name='root')


def init_pygacloc_plot():
    """
    Initialize plot for pygaclog.
    :return:
    """

    fig = plt.figure(figsize=(16, 9))

    fig_runtime = fig.add_subplot(211)
    fig_errors = fig.add_subplot(212)

    plt.rcParams['font.size'] = 20.0
    plt.rcParams['legend.fontsize'] = 16
    plt.rcParams['axes.titlesize'] = 20
    plt.rcParams['axes.labelsize'] = 18
    plt.rcParams['xtick.labelsize'] = 16
    plt.rcParams['ytick.labelsize'] = 16
    plt.rcParams['lines.linewidth'] = 2

    return fig_runtime, fig_errors


def finalize_pygaclog_plot(upper, lower):
    """
    Finalize pygaclog plot: annotate and legend
    :param upper: figure showing runtimes of pygac
    :param lower: figure showing errors of pygac
    :return:
    """
    # annotate plot
    lower.set_xlabel("Number of pygac errors")
    upper.set_xlabel("Number of processed orbits")
    upper.set_ylabel("PyGAC Runtime [sec]")

    # make legend
    leg_lower = lower.legend(loc='best', fancybox=True)
    leg_lower.get_frame().set_alpha(0.5)
    leg_upper = upper.legend(loc='best', fancybox=True)
    leg_upper.get_frame().set_alpha(0.5)
    plt.tight_layout(rect=(0.02, 0.02, 0.98, 0.98))


def get_bhar_yticks(list_of_keys):
    list_of_yticks = list()
    for k in list_of_keys:
        if 'IndexError' in k:
            list_of_yticks.append('IndexError')
        elif 'Stop processing' in k:
            list_of_yticks.append('CorruptData')
        elif 'Value error' in k:
            list_of_yticks.append('Value error')
        else:
            list_of_yticks.append('Undefined')

    return list_of_yticks


def make_pygac_error_histogram(figobj, pdict, colors, proc_versions):
    """
    Create histogram for pygac errors for different processing versions.
    :param pdict:
    :return:
    """
    ind = np.arange(len(pdict))
    height = 0.25
    cnt = 0
    rect_list = list()

    for x in pdict:
        if cnt == 0:
            shift = 0.
        else:
            shift += height

        yticknames = pdict[x]["pygac_error_key"]
        ytickvalues = pdict[x]["pygac_error_value"]
        rects = figobj.barh(ind + shift, ytickvalues, height, color=colors[cnt],
                            align='center', alpha=0.75, label=proc_versions[cnt])
        rect_list.append(rects)
        autolabel(rects, figobj)
        cnt += 1

    short_ticknames = get_bhar_yticks(yticknames)
    figobj.set_yticks(ind + height)
    figobj.set_yticklabels(short_ticknames)


def autolabel(rects, ax):
    for rect in rects:
        width = rect.get_width()
        if width < 5:
            factor = 20.0
        elif 4 < width < 100:
            factor = 3.0
        else:
            factor = 0.96
        ax.text(factor * rect.get_width(), rect.get_y() + 0.4 * rect.get_height(),
                '%d' % int(width), ha='center', va='center')


def plot_pygac_runtime(cursor):
    """
    Read the pygac runtime of each processing version and plot it.
    :param cursor:
    :return:
    """
    colors = ['DimGray', 'Red', 'DarkGreen', 'Blue']

    # initialize plot
    (figrun, figerr) = init_pygacloc_plot()

    logger.info("Read all available pygac processing versions")

    query1 = "SELECT * FROM pygac_versions"
    cursor.execute(query1)
    pygac_versions = cursor.fetchall()
    pygac_dict = dict()
    proc_versions = list()

    for pv in pygac_versions:
        pv_id = pv['id']
        pv_name = pv['name']
        pv_info = pv['metadata']

        proc_versions.append(pv_name)

        logger.info("Working on: {0}, i.e. {1}".format(pv_name, pv_info))

        # get pygac_runtimes
        query2 = "SELECT pygac_runtime FROM procs WHERE " \
                 "pygac_version_id={pv_id} AND " \
                 "pygac_runtime is not null"

        cursor.execute(query2.format(pv_id=pv_id))
        runtimes = cursor.fetchall()
        if runtimes:
            rts = list()
            for rt in runtimes:
                rts.append(rt['pygac_runtime'])

            x = range(len(rts))
            ave = sum(rts)/float(len(rts))
            label = "{0: >6s} {1:4.2f}s  {2: <55s} ".format(" mean:", ave, pv_info)
            figrun.plot(x, rts, 'o', label=label,
                        color=colors[pv_id-1], alpha=0.75, markersize=5)
        else:
            continue

        # get PyGAC errors
        pygac_dict[pv_id] = dict()
        for x in ("pygac_error_key", "pygac_error_value"):
            pygac_dict[pv_id][x] = 0

        query3 = "SELECT pygac_errors FROM procs WHERE " \
                 "pygac_version_id={pv_id} AND " \
                 "pygac_errors is not null"
        cursor.execute(query3.format(pv_id=pv_id))
        p_errors = cursor.fetchall()
        if p_errors:
            perr = list()
            for p in p_errors:
                perr.append(p['pygac_errors'])

        # count errors
        counter = collections.Counter(perr)
        pygac_dict[pv_id]["pygac_error_key"] = counter.keys()
        pygac_dict[pv_id]["pygac_error_value"] = counter.values()

        # # get PyGAC warnings
        # query4 = "SELECT pygac_warnings FROM procs WHERE " \
        #          "pygac_version_id={pv_id} AND " \
        #          "pygac_warnings is not null"
        # cursor.execute(query4.format(pv_id=pv_id))
        # p_warns = cursor.fetchall()
        # if p_warns:
        #     pwar = list()
        #     for p in p_warns:
        #         pwar.append(p['pygac_warnings'])
        # print len(set(pwar))

    make_pygac_error_histogram(figerr, pygac_dict, colors=colors,
                               proc_versions=proc_versions)
    finalize_pygaclog_plot(upper=figrun, lower=figerr)
    # plt.show()
    plt.savefig(os.path.join(png_path, procs_png))
    logger.info("Done {0}".format(os.path.join(png_path, procs_png)))
    plt.close()


def plot_pystat_results():
    """
    Plot the different pystat results.
    :return:
    """
    cnt = 0
    for sql in pystat_list:
        cnt += 1
        base = os.path.basename(sql)
        splt = base.split("_")
        pystat_version = "_" + str(cnt) + '_' + splt[-1].split(".")[0]

        try:
            db = lite.connect(sql, detect_types=lite.PARSE_DECLTYPES | lite.PARSE_COLNAMES)
            db.row_factory = mysub.dict_factory
            cur = db.cursor()

            for channel in channel_list:
                if channel == 'ch1' or channel == 'ch2' or channel == 'ch3a':
                    mean_range = [0.0, 0.25]
                    stdv_range = [0.0, 0.40]
                else:
                    mean_range = [240., 300.]
                    stdv_range = [5.0, 40.]

                for select in select_list:
                    plot_time_series(sat_list=satellite_list, channel=channel, select=select,
                                     start_date=start_date, end_date=end_date,
                                     outpath=png_path, cursor=cur,
                                     verbose=False, show_fig=False, linesty='-',
                                     pystat_version=pystat_version,
                                     mean_range=mean_range,
                                     stdv_range=stdv_range)

            db.close()

        except lite.Error, e:
            logger.info("ERROR {0} \n\n".format(e.args[0]))
            sys.exit(1)


if __name__ == '__main__':

    if not os.path.exists(png_path):
        os.makedirs(png_path)

    # -- open pygaclog SQL file
    try:
        pygaclog = lite.connect(sql_pygac_log,
                                detect_types=lite.PARSE_DECLTYPES | lite.PARSE_COLNAMES)
        pygaclog.row_factory = mysub.dict_factory
        cur_log = pygaclog.cursor()

        # plot_pygac_runtime(cursor=cur_log)
        plot_pystat_results()

        pygaclog.close()
        logger.info("{0} successfully finished".format(os.path.basename(sys.argv[0])))

    except lite.Error, e:
        logger.info("ERROR {0} \n\n".format(e.args[0]))
        sys.exit(1)
