#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
import sys
import time
import sqlite3 as lite
import subs_avhrrgac as mysub
import numpy as np
import matplotlib.pyplot as plt
from pylab import *
from subs_plot_sql import plot_time_series
from config_plot_quick_l1c_analysis import *
from pycmsaf.logger import setup_root_logger

logger = setup_root_logger(name='root')

colors = ['DimGray', 'Red', 'DarkGreen', 'Blue']


def figure_settings():
    """
    Set fontsizes and linewidths
    :return:
    """
    plt.rcParams['font.size'] = 18
    plt.rcParams['legend.fontsize'] = 16
    plt.rcParams['axes.titlesize'] = 20
    plt.rcParams['axes.labelsize'] = 18
    plt.rcParams['xtick.labelsize'] = 16
    plt.rcParams['ytick.labelsize'] = 16
    plt.rcParams['lines.linewidth'] = 2
    return


def init_timestamp_plot():
    """
    Initialize plot for l1c timestamp analysis.
    :return:
    """
    fig = plt.figure(figsize=(16, 9))
    fig_times = fig.add_subplot(311)
    fig_zoom1 = fig.add_subplot(312)
    fig_zoom2 = fig.add_subplot(313)
    figure_settings()
    return fig, fig_times, fig_zoom1, fig_zoom2


def finalize_timestamp_plot(fig, obj1, obj2, obj3):
    """
    Finalize pygaclog plot: annotate and legend
    :param upper: figure showing runtimes of pygac
    :param lower: figure showing errors of pygac
    :return:
    """
    # set y format
    obj1.yaxis.get_major_formatter().set_powerlimits((0, 1))
    obj2.yaxis.get_major_formatter().set_powerlimits((0, 1))
    obj3.yaxis.get_major_formatter().set_powerlimits((0, 1))
    # set limits
    obj3.set_ylim(-50, 150.)
    obj2.set_ylim(-3000., 3000.)
    # annotate plot
    # fig.text(0.5, 0.02, 'Number of processed orbits', ha='center', va='center')
    obj3.set_xlabel("Number of processed orbits")
    fig.text(0.02, 0.5, 'Orbit Length [minutes]', ha='center', va='center', rotation='vertical')
    # make legend
    leg = obj1.legend(loc='lower center', fancybox=True)
    leg.get_frame().set_alpha(0.5)
    plt.tight_layout(rect=(0.02, 0.02, 0.98, 0.98))
    return


def init_pygacloc_plot():
    """
    Initialize plot for pygaclog.
    :return:
    """
    fig = plt.figure(figsize=(16, 9))
    fig_runtime = fig.add_subplot(211)
    fig_errors = fig.add_subplot(212)
    figure_settings()
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
    return


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
    return


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
    return


def plot_orbit_durations(data, num_runs, num_orbits, info):
    """
    Plot orbit durations of each proc. version run.
    :param data: dictionary holding timestamps, orbit length, orbit names
    :param info: metadata of proc. versions
    :return:
    """
    cnt = 1
    (fig, fobj, fzoom1, fzoom2) = init_timestamp_plot()

    # collect list of orbits
    orbits = list()
    for run in data:
        for orbit in data[run]:
            orbits.append(orbit)
    orbs = list(set(orbits))
    orbs.sort()
    x = range(num_orbits)

    # collect corresponding durations and plot it
    while cnt <= num_runs:
        durations = list()
        for fil in orbs:
            durations.append(data[cnt][fil]["duration"])
        # min/max
        min_val = min(durations)
        max_val = max(durations)
        label = "{0:4s} {1:6.1f} {2:4s} {3:6.1f}   {4:65s} ".\
                format("min:", min_val, "max:", max_val, info[cnt - 1])
        # plot list
        fobj.plot(x, durations, '-', label=label, color=colors[cnt-1], alpha=0.75)
        fzoom1.plot(x, durations, '-', color=colors[cnt-1], alpha=0.75)
        fzoom2.plot(x, durations, '--', color=colors[cnt-1], alpha=0.75)
        cnt += 1

    finalize_timestamp_plot(fig=fig, obj1=fobj, obj2=fzoom1, obj3=fzoom2)
    plt.savefig(os.path.join(png_path, times_png))
    logger.info("Done {0}".format(os.path.join(png_path, times_png)))
    plt.close()


def get_pygac_versions_dict(cursor):
    cursor.execute("SELECT * FROM pygac_versions ORDER BY id")
    return cursor.fetchall()


def calc_orbit_duration(start_timestamp, end_timestamp):
    """
    Calculate the orbith length (duration) based on start and end L1c timestamps.
    max_orbit_duration = 120 minutes
    :param start_timestamp:
    :param end_timestamp:
    :return: orbit_duration [minutes]
    """
    # convert start and end dates to unix timestamp
    d1_ts = time.mktime(start_timestamp.timetuple())
    d2_ts = time.mktime(end_timestamp.timetuple())
    # orbit duration in minutes
    orbit_duration = int(d2_ts - d1_ts) / 60
    return orbit_duration


def plot_pygac_l1c_timestamps(cursor):
    """
    Read pygac l1c timestamps from table procs.
    :param cursor:
    :return:
    """
    logger.info("Read all available pygac processing versions")
    pygac_versions = get_pygac_versions_dict(cursor=cursor)

    # list of processing versions
    p_vers_list = list()
    # dict of orbit lengths of different proc. versions
    orbit_dict = dict()

    for pv in pygac_versions:
        pv_id = pv['id']
        pv_name = pv['name']
        pv_info = pv['metadata']
        p_vers_list.append(pv_info)

        logger.info("Working on: {0}, i.e. {1}".format(pv_name, pv_info))

        logger.info("Read L1c timestamps from table procs")

        cmd = "SELECT satellite_name, orbit_name, orbit_id, " \
              "start_time_l1c, end_time_l1c FROM vw_procs WHERE " \
              "pygac_version_id={pv_id} AND " \
              "start_time_l1c is not null AND " \
              "end_time_l1c is not null " \
              "ORDER BY orbit_name"
        cursor.execute(cmd.format(pv_id=pv_id))
        timestamps = cursor.fetchall()

        orbit_names = list()
        orbit_lengths = list()
        start_times = list()
        end_times = list()

        if timestamps:
            for ts in timestamps:
                ret = calc_orbit_duration(start_timestamp=ts['start_time_l1c'],
                                          end_timestamp=ts['end_time_l1c'])
                orbit_names.append(ts['orbit_name'])
                orbit_lengths.append(ret)
                start_times.append(ts['start_time_l1c'])
                end_times.append(ts['end_time_l1c'])

        # initialize dictionary
        orbit_dict[pv_id] = dict()
        for o in orbit_names:
            orbit_dict[pv_id][o] = dict()
            for x in ("start", "end", "duration"):
                orbit_dict[pv_id][o][x] = 0
        # fill dictionary
        for idx, orbit in enumerate(orbit_names):
            orbit_dict[pv_id][orbit]["duration"] = orbit_lengths[idx]
            orbit_dict[pv_id][orbit]["start"] = start_times[idx]
            orbit_dict[pv_id][orbit]["end"] = end_times[idx]

    # check dictionary
    num_of_runs = 0
    num_of_orbs = 0
    for run in orbit_dict:
        num_of_runs = len(orbit_dict)
        num_of_orbs = len(orbit_dict[run])
        logger.info("Number of proc. versions is {0}".format(num_of_runs))
        logger.info("Number of orbits is {0}".format(num_of_orbs))
        break

    # plot orbit durations
    plot_orbit_durations(data=orbit_dict, num_runs=num_of_runs,
                         num_orbits=num_of_orbs, info=p_vers_list)

    return


def plot_pygac_runtime_and_errors(cursor):
    """
    Read the pygac runtime of each processing version and plot it.
    :param cursor:
    :return:
    """
    import collections

    # initialize plot
    (figrun, figerr) = init_pygacloc_plot()

    logger.info("Read all available pygac processing versions")
    pygac_versions = get_pygac_versions_dict(cursor=cursor)

    errors_dict = dict()
    p_vers_list = list()

    for pv in pygac_versions:
        pv_id = pv['id']
        pv_name = pv['name']
        pv_info = pv['metadata']
        p_vers_list.append(pv_name)

        logger.info("Working on: {0}, i.e. {1}".format(pv_name, pv_info))

        # get pygac_runtimes
        query2 = "SELECT pygac_runtime FROM vw_procs WHERE " \
                 "pygac_version_id={pv_id} AND " \
                 "pygac_runtime is not null " \
                 "ORDER BY orbit_name"

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
        errors_dict[pv_id] = dict()
        for x in ("pygac_error_key", "pygac_error_value"):
            errors_dict[pv_id][x] = 0

        query3 = "SELECT pygac_errors FROM vw_procs WHERE " \
                 "pygac_version_id={pv_id} AND " \
                 "pygac_errors is not null " \
                 "ORDER BY orbit_name"
        cursor.execute(query3.format(pv_id=pv_id))
        p_errors = cursor.fetchall()
        if p_errors:
            perr = list()
            for p in p_errors:
                perr.append(p['pygac_errors'])

        # count errors
        counter = collections.Counter(perr)
        errors_dict[pv_id]["pygac_error_key"] = counter.keys()
        errors_dict[pv_id]["pygac_error_value"] = counter.values()

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

    make_pygac_error_histogram(figobj=figerr, pdict=errors_dict,
                               colors=colors, proc_versions=p_vers_list)
    finalize_pygaclog_plot(upper=figrun, lower=figerr)
    # plt.show()
    plt.savefig(os.path.join(png_path, procs_png))
    logger.info("Done {0}".format(os.path.join(png_path, procs_png)))
    plt.close()
    return


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
    return


if __name__ == '__main__':

    if not os.path.exists(png_path):
        os.makedirs(png_path)

    if vis_pystat:
        plot_pystat_results()

    if vis_pygaclog or vis_timestamps:
        try:
            pygaclog = lite.connect(sql_pygac_log, detect_types=lite.PARSE_DECLTYPES | lite.PARSE_COLNAMES)
            pygaclog.row_factory = mysub.dict_factory
            cur_log = pygaclog.cursor()
            if vis_pygaclog:
                plot_pygac_runtime_and_errors(cursor=cur_log)
            if vis_timestamps:
                plot_pygac_l1c_timestamps(cursor=cur_log)
            pygaclog.close()

        except lite.Error, e:
            logger.info("ERROR {0} \n\n".format(e.args[0]))
            sys.exit(1)
