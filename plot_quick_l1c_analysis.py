#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
import sys
import re
import time
import sqlite3 as lite
# import numpy as np
# import matplotlib.pyplot as plt
from subs_avhrrgac import lite_satstring
from subs_avhrrgac import get_channel_list
from subs_avhrrgac import get_pystat_select_list
from subs_avhrrgac import dict_factory
from subs_avhrrgac import full_cha_name
from subs_avhrrgac import get_channel_unit
from pylab import *
from subs_plot_sql import plot_time_series, calc_date_formatter
from config_plot_quick_l1c_analysis import *
from pycmsaf.logger import setup_root_logger

logger = setup_root_logger(name='root')

# """
# Set fontsizes and linewidths
# """
plt.rcParams['font.size'] = 18
plt.rcParams['legend.fontsize'] = 14
plt.rcParams['axes.titlesize'] = 20
plt.rcParams['axes.labelsize'] = 18
plt.rcParams['xtick.labelsize'] = 16
plt.rcParams['ytick.labelsize'] = 16
plt.rcParams['lines.linewidth'] = 2
# set tick width
mpl.rcParams['xtick.major.size'] = 10
mpl.rcParams['xtick.major.width'] = 2
mpl.rcParams['xtick.minor.size'] = 5
mpl.rcParams['xtick.minor.width'] = 1


def extract_orbit_attrs(l1bfile):
    """
    Extract orbit attributes from the given l1b filename.
    @type l1bfile: str
    @return: Orbit attributes.
    @rtype: dict
    @raise ValueError: If the filename does not match the expected format.
    """
    fname_pattern_read = '^NSS\.GHRR\.' \
        '(?P<sat_short>[A-Z0-9]{2})\.' \
        'D(?P<year>\d{2})(?P<doy>\d{3})\.' \
        'S(?P<start_time_l1b>\d{4})\.' \
        'E(?P<end_time_l1b>\d{4})\.' \
        'B(?P<orbit_number_offset>\d{3})' \
        '(?P<start_orbit_number>\d{2})' \
        '(?P<end_orbit_number>\d{2})\.' \
        '(?P<ground_station>[A-Z]{2})\.' \
        'gz(?P<old>\.v\d+){0,1}$'
    fname_regex = re.compile(fname_pattern_read)
    match = fname_regex.match(l1bfile)
    if match:
        # Extract attributes
        grp = match.group  # is a method
        sat_short = grp('sat_short')
        year = datetime.datetime.strptime(grp('year'), '%y').year
        doy = int(grp('doy'))  # beginning at 1!
        tstart = strptime_hhmm(grp('start_time_l1b'))
        tend = strptime_hhmm(grp('end_time_l1b'))
        orbit_number_offset = int(grp('orbit_number_offset'))
        start_orbit_number = int(grp('start_orbit_number'))
        end_orbit_number = int(grp('end_orbit_number'))
        ground_station = grp('ground_station')
        old = int(bool(grp('old')))

        # Assemble datetime objects
        date = datetime.date(year=year, month=1, day=1) + \
               datetime.timedelta(days=doy - 1)
        start = datetime.datetime.combine(date=date, time=tstart)
        if tend < tstart:
            end = datetime.datetime.combine(
                date=date + datetime.timedelta(days=1), time=tend)
        else:
            end = datetime.datetime.combine(date=date, time=tend)

        # Store attributes in dictionary
        orbit_attrs = dict(
            satellite=lite_satstring(sat_short),
            start_time_l1b=start, end_time_l1b=end,
            orbit_number_offset=orbit_number_offset,
            start_orbit_number=start_orbit_number,
            end_orbit_number=end_orbit_number,
            ground_station=ground_station, old=old)
        return orbit_attrs
    else:
        raise ValueError('No match in filename: {0}'.format(l1bfile))


def strptime_hhmm(timestr):
    """
    Convert a time string C{hhmm}to a datetime.time object.

    The python module datetime.datetime.strptime() does not enforce leading
    zeros for the C{%H%M} format specifier. This function does.

    @param timestr: Time-string to be converted.
    @type timestr: str
    """
    match = re.match(pattern='^(?P<hh>\d\d)(?P<mm>\d\d)$', string=timestr)
    if match:
        return datetime.datetime(year=1900, month=1, day=1,  # dummy date
                                 hour=int(match.group('hh')),
                                 minute=int(match.group('mm'))).time()
    else:
        raise ValueError('String \'{0}\' does not match %H%M format.'
                         .format(timestr))


def date2str(dateobject):
    """
    Create a date string from a given datetime.date object.
    """
    return dateobject.strftime("%Y/%m/%d")


def date2filestr(dateobject):
    """
    Create a date string from a given datetime.date object.
    """
    return dateobject.strftime("%Y%m%d")


def datetime2str(dateobject):
    """
    Create a date string from a given datetime.date object.
    """
    return dateobject.strftime("%Y/%m/%d %H:%M")


def init_tab_stats_plot_tsm():
    """
    Initialize plot for table stats (tsm check).
    """
    fig = plt.figure(figsize=(16, 9))
    fig_normal = fig.add_subplot(211)
    fig_zoom = fig.add_subplot(212)
    return fig, fig_normal, fig_zoom


def init_tab_stats_plot():
    """
    Initialize plot for table stats (channel information).
    """
    fig = plt.figure(figsize=(16, 9))
    fig_mean = fig.add_subplot(311)
    fig_valid = fig.add_subplot(312)
    fig_masked = fig.add_subplot(313)
    return fig, fig_mean, fig_valid, fig_masked


def finalize_tab_stats_plot(obj1, obj2, obj3, ptitle, sdt, edt, obj1_y_range):
    """
    Finalize table stats plot: annotate and legend
    """
    # title
    obj1.set_title(ptitle)
    # y axis range
    obj1.set_ylim(obj1_y_range[0], obj1_y_range[1])
    # x axis range
    for obj in [obj1, obj2, obj3]:
        obj.set_xlim(sdt, edt)
    # modify x axis
    delta_days = (edt - sdt).days
    (minor_loc, major_loc, major_fmt, date_label) = calc_date_formatter(delta_days, "Date")
    for obj in [obj1, obj2, obj3]:
        obj.xaxis.set_major_locator(major_loc)
        obj.xaxis.set_minor_locator(minor_loc)
        obj.xaxis.set_major_formatter(major_fmt)
        obj.grid(which='both')
    # fancy date labeling
    plt.gcf().autofmt_xdate(rotation=20)
    # set y format
    obj2.yaxis.get_major_formatter().set_powerlimits((0, 1))
    obj3.yaxis.get_major_formatter().set_powerlimits((0, 1))
    # annotate plot
    obj1.set_ylabel("Means")
    obj2.set_ylabel("Valid obs.")
    obj3.set_ylabel("Masked obs.")
    obj3.set_xlabel(date_label)
    # make legend
    # leg_box_pos = (0.8, 4.20)
    leg_box_pos = (0.5, 1.3)
    leg = obj3.legend(ncol=2, loc='upper center',
                      bbox_to_anchor=leg_box_pos, fancybox=True)
    leg.get_frame().set_alpha(0.9)
    plt.tight_layout(rect=(0.01, 0.01, 0.99, 0.99))
    # plt.tight_layout(h_pad=2.2)

    return


def finalize_tab_stats_plot_tsm(obj1, obj2, ptitle, sdt, edt):
    """
    Finalize table stats plot tsm-check: annotate and legend
    """
    # title
    obj1.set_title(ptitle)
    # y axis range
    obj2.set_ylim(-5000, 500)
    # x axis range
    for obj in [obj1, obj2]:
        obj.set_xlim(sdt, edt)
    # modify x axis
    delta_days = (edt - sdt).days
    (minor_loc, major_loc, major_fmt, date_label) = calc_date_formatter(delta_days, "Date")
    for obj in [obj1, obj2]:
        obj.xaxis.set_major_locator(major_loc)
        obj.xaxis.set_minor_locator(minor_loc)
        obj.xaxis.set_major_formatter(major_fmt)
        obj.grid(which='both')
    # fancy date labeling
    plt.gcf().autofmt_xdate(rotation=20)
    # set y format
    obj1.yaxis.get_major_formatter().set_powerlimits((0, 1))
    obj2.yaxis.get_major_formatter().set_powerlimits((0, 1))
    # annotate plot
    obj1.set_ylabel("# masked obs.")
    obj2.set_ylabel("# masked obs.")
    obj2.set_xlabel(date_label)
    # make legend
    leg = obj1.legend(ncol=2, loc='best', fancybox=True)
    leg.get_frame().set_alpha(0.9)
    plt.tight_layout(rect=(0.01, 0.01, 0.99, 0.99))
    # plt.tight_layout(h_pad=2.2)
    return


def init_timestamp_plot():
    """
    Initialize plot for l1c timestamp analysis.
    """
    fig = plt.figure(figsize=(16, 9))
    fig_times = fig.add_subplot(311)
    fig_zoom1 = fig.add_subplot(312)
    fig_zoom2 = fig.add_subplot(313)
    return fig, fig_times, fig_zoom1, fig_zoom2


def finalize_timestamp_plot(fig, obj1, obj2, obj3, ptitle, sdt, edt):
    """
    Finalize pygaclog plot: annotate and legend
    """
    obj1.set_title(ptitle, loc='left')
    # x axis range
    for obj in [obj1, obj2, obj3]:
        obj.set_xlim(sdt, edt)
    # modify x axis
    delta_days = (edt - sdt).days
    (minor_loc, major_loc, major_fmt, date_label) = calc_date_formatter(delta_days, "Date")
    for obj in [obj1, obj2, obj3]:
        obj.xaxis.set_major_locator(major_loc)
        obj.xaxis.set_minor_locator(minor_loc)
        obj.xaxis.set_major_formatter(major_fmt)
    plt.gcf().autofmt_xdate(rotation=20)
    # set y format
    obj1.yaxis.get_major_formatter().set_powerlimits((0, 1))
    obj2.yaxis.get_major_formatter().set_powerlimits((0, 1))
    obj3.yaxis.get_major_formatter().set_powerlimits((0, 1))
    # set limits
    obj3.set_ylim(-50, 150.)
    obj2.set_ylim(-3000., 3000.)
    # annotate plot
    obj3.set_xlabel(date_label + " (" + date2str(sdt) + " - " + date2str(edt) + ")")
    fig.text(0.03, 0.5, 'Orbit Length [minutes]',
             ha='center', va='center', rotation='vertical')
    # make legend
    leg1 = obj1.legend(ncol=2, loc='upper right', bbox_to_anchor=(1.03, 1.50),
                       fancybox=True, fontsize=13)
    leg2 = obj2.legend(ncol=2, loc='upper right', bbox_to_anchor=(1.03, 1.40),
                       fancybox=True, fontsize=13)
    leg1.get_frame().set_alpha(0.5)
    leg2.get_frame().set_alpha(0.5)
    plt.tight_layout(rect=(0.02, 0.02, 0.98, 0.98))
    return


def init_pygacloc_plot():
    """
    Initialize plot for pygaclog.
    """
    fig = plt.figure(figsize=(16, 9))
    fig_runtime = fig.add_subplot(211)
    fig_errors = fig.add_subplot(212)
    return fig_runtime, fig_errors


def finalize_pygaclog_plot(upper, lower, ptitle, sdt, edt):
    """
    Finalize pygaclog plot: annotate and legend
    """
    # horizontal line
    upper.plot((sdt, edt), (0, 0), 'g-')
    # ticks
    upper.minorticks_on()
    lower.minorticks_on()
    # upper.tick_params('both', length=20, width=2, which='major')
    # upper.tick_params('both', length=10, width=1, which='minor')
    upper.tick_params('x', length=20, width=2, which='major')
    upper.tick_params('x', length=10, width=1, which='minor')
    lower.tick_params('x', length=10, width=2, which='major')
    lower.tick_params('x', length=5, width=1, which='minor')
    lower.tick_params('y', length=0, width=0, which='major')
    lower.tick_params('y', length=0, width=0, which='minor')
    # plot title
    upper.set_title(ptitle)
    # x axis range
    upper.set_xlim(sdt, edt)
    # modify x axis
    delta_days = (edt - sdt).days
    (minor_loc, major_loc, major_fmt, date_label) = calc_date_formatter(delta_days, "Date")
    upper.xaxis.set_major_locator(major_loc)
    upper.xaxis.set_minor_locator(minor_loc)
    upper.xaxis.set_major_formatter(major_fmt)
    labels = upper.get_xticklabels()
    plt.setp(labels, rotation=20)
    # annotate plot
    lower.set_xlabel("Number of pygac errors")
    upper.set_xlabel(date_label)
    upper.set_ylabel("PyGAC Runtime [sec]")
    # set limit
    upper.set_ylim(-50., 250.)
    # make legend
    leg_lower = lower.legend(loc='best', fancybox=True)
    leg_lower.get_frame().set_alpha(0.5)
    leg_upper = upper.legend(loc='upper right', fancybox=True)
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
        elif 'Timestamp mismatch' in k:
            list_of_yticks.append('No TS Corr.')
        else:
            list_of_yticks.append('Undefined')

    return list_of_yticks


def make_pygac_error_histogram(figobj, pdict, colors, proc_versions):
    """
    Create histogram for pygac errors for different processing versions.
    """
    height = 0.4
    cnt = 0
    rect_list = list()

    # find pversion of maximum amount of errors occurred in the processing
    number_of_errors = list()
    for x in pdict:
        number_of_errors.append(len(pdict[x]["pygac_error_key"]))
    max_index = number_of_errors.index(max(number_of_errors))
    y_axis = sorted(pdict[max_index+1]["pygac_error_key"])
    y_rang = np.arange(len(y_axis))*2

    for x in pdict:

        if cnt == 0:
            shift = 0.
        else:
            shift += height

        xtickvalues = pdict[x]["pygac_error_value"]
        yticknames = pdict[x]["pygac_error_key"]

        (x, y) = sort_results(y_axis, yticknames, xtickvalues)
        legend_label = proc_versions[cnt]

        rects = figobj.barh(y_rang + shift, x, height, color=colors[cnt],
                            align='center', alpha=0.75, label=legend_label)

        rect_list.append(rects)
        autolabel(rects, figobj, colors[cnt])

        cnt += 1

    short_ticknames = get_bhar_yticks(y_axis)
    figobj.set_yticks(y_rang + height)
    figobj.set_yticklabels(short_ticknames)

    return


def sort_results(ref_list, names, values):
    xval = list()
    ystr = list()
    for ref in ref_list:
        ystr.append(ref)
        flag = False
        for n, v in zip(names, values):
            if n == ref:
                xval.append(v)
                flag = True
                break
        if flag is False:
            xval.append(0)
    return xval, ystr


def autolabel(rects, ax, col):
    for rect in rects:
        width = rect.get_width()
        xmin, xmax = ax.get_xlim()
        offset = xmax/40.
        ax.text(offset + width, rect.get_y() + 0.4 * rect.get_height(),
                '%d' % int(width), ha='center', va='center',
                fontsize=10, color=col)
    return


def plot_orbit_durations(data, num_runs, info, satellite):
    """
    Plot orbit durations of each proc. version run.
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

    # collect corresponding durations and plot it
    while cnt <= num_runs:
        durations = list()
        start_time_l1b = list()
        cnt_negatives = 0
        cnt_toolongs = 0

        for fil in orbs:
            try:
                attr_dict = extract_orbit_attrs(fil)
                start_time_l1b.append(attr_dict['start_time_l1b'])
                durations.append(data[cnt][fil]["duration"])
                # negative orbit length
                if data[cnt][fil]["duration"] < 0:
                    cnt_negatives += 1
                # too_long_orbit_length
                if data[cnt][fil]["duration"] > 120:
                    cnt_toolongs += 1
            except KeyError, e:
                print 'I got a KeyError - reason "%s"' % str(e)

        # min/max
        min_val = min(durations)
        max_val = max(durations)

        # USE ax.text() option instead of legend!!!
        # label = "{0:4s} {1:6.1f} {2:4s} {3:6.1f}   {4:65s} ".\
        #         format("min:", min_val, "max:", max_val, info[cnt - 1])
        label = info[cnt - 1]
        label2 = "{0:<15}{1:<15}".format("Neg. Orbits: " + str(cnt_negatives),
                                         "Long Orbits: " + str(cnt_toolongs))
        # plot list
        (x, y) = slice_data(dates=start_time_l1b, values=durations)
        fobj.plot(x, y, '-', label=label, color=colors[cnt-1], alpha=0.75)
        fzoom1.plot(x, y, '-', label=label2, color=colors[cnt-1], alpha=0.75)
        fzoom2.plot(x, y, '--', color=colors[cnt-1], alpha=0.75)
        cnt += 1

    (sdt, edt) = check_dates_limits(start_time_l1b)

    # add labels and legend
    # ptitle = satellite + ": " + date2str(sdt) + " - " + date2str(edt) + "\n"
    ptitle = satellite + "\n"
    finalize_timestamp_plot(fig=fig, obj1=fobj, obj2=fzoom1, obj3=fzoom2,
                            ptitle=ptitle, sdt=sdt, edt=edt)

    # save plot as png file
    dat_str = date2filestr(sdt) + '_' + date2filestr(edt)
    png_fil = times_png + "_" + dat_str + '_' + satellite + ".png"
    outfile = os.path.join(png_path, png_fil)
    plt.savefig(outfile)
    logger.info("Done {0}".format(outfile))
    plt.close()


def get_pygac_versions_dict(cursor):
    cursor.execute("SELECT * FROM pygac_versions ORDER BY id")
    return cursor.fetchall()


def readout_satellites(cursor):
    satellites = list()
    cursor.execute("SELECT satellite_name FROM vw_procs order by satellite_id")
    ret = cursor.fetchall()
    for r in ret:
        satellites.append(str(r['satellite_name']))
    return list(set(satellites))


def calc_orbit_duration(start_timestamp, end_timestamp):
    """
    Calculate the orbith length (duration) based on start and end L1c timestamps.
    max_orbit_duration = 120 minutes
    """
    # convert start and end dates to unix timestamp
    d1_ts = time.mktime(start_timestamp.timetuple())
    d2_ts = time.mktime(end_timestamp.timetuple())
    # orbit duration in minutes
    orbit_duration = int(d2_ts - d1_ts) / 60
    return orbit_duration


def plot_pygac_l1c_timestamps(cursor, satellite):
    """
    Read pygac l1c timestamps from table procs.
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

        cmd = "SELECT orbit_name, orbit_id, " \
              "start_time_l1c, end_time_l1c FROM vw_procs WHERE " \
              "satellite_name=\'{satellite}\' AND " \
              "pygac_version_id={pv_id} AND " \
              "start_time_l1c is not null AND " \
              "end_time_l1c is not null " \
              "ORDER BY orbit_name"
        cursor.execute(cmd.format(satellite=satellite, pv_id=pv_id))
        timestamps = cursor.fetchall()

        orbit_names = list()
        orbit_lengths = list()
        start_times = list()
        end_times = list()

        if timestamps:
            for ts in timestamps:
                ret = calc_orbit_duration(start_timestamp=ts['start_time_l1c'],
                                          end_timestamp=ts['end_time_l1c'])
                orbit_names.append(str(ts['orbit_name']))
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
                         info=p_vers_list, satellite=satellite)

    return


def plot_pygac_runtime_and_errors(cursor, satellite):
    """
    Read the pygac runtime of each processing version and plot it.
    """
    import collections

    # initialize plot
    (figrun, figerr) = init_pygacloc_plot()

    logger.info("Read all available pygac processing versions")
    pygac_versions = get_pygac_versions_dict(cursor=cursor)

    l1b_dates = list()
    errors_dict = dict()
    p_vers_list = list()

    for pv in pygac_versions:
        pv_id = pv['id']
        pv_name = pv['name']
        pv_info = pv['metadata']
        p_vers_list.append(pv_name)

        logger.info("Working on: {0}, i.e. {1}".format(pv_name, pv_info))

        # get pygac_runtimes
        query2 = "SELECT orbit_name, pygac_runtime FROM vw_procs WHERE " \
                 "satellite_name=\'{satellite}\' AND " \
                 "pygac_version_id={pv_id} " \
                 "ORDER BY orbit_name"
        cursor.execute(query2.format(satellite=satellite, pv_id=pv_id))
        runtimes = cursor.fetchall()
        if runtimes:
            start_time_l1b = list()
            rts = list()
            rts_without_Nones = list()
            for rt in runtimes:
                attr_dict = extract_orbit_attrs(rt['orbit_name'])
                start_time_l1b.append(attr_dict['start_time_l1b'])
                if rt['pygac_runtime'] is None:
                    rts.append(-20.0)
                else:
                    rts.append(rt['pygac_runtime'])
                    rts_without_Nones.append(rt['pygac_runtime'])

            # calculate average pygac runtime
            ave = sum(rts_without_Nones)/float(len(rts_without_Nones))
            label = "{0: >6s} {1:4.2f}s  {2: <55s} ".format(" mean:", ave, pv_info)
            # collect start_time_l1b
            for dt in start_time_l1b:
                l1b_dates.append(dt)
            # plot data
            (x, y) = slice_data(dates=start_time_l1b, values=rts)
            figrun.plot(x, y, 'o', label=label, color=colors[pv_id - 1],
                        alpha=0.75, markersize=5)
        else:
            continue

        # get PyGAC errors
        errors_dict[pv_id] = dict()
        for x in ("pygac_error_key", "pygac_error_value"):
            errors_dict[pv_id][x] = 0

        query3 = "SELECT pygac_errors FROM vw_procs WHERE " \
                 "satellite_name=\'{satellite}\' AND " \
                 "pygac_version_id={pv_id} AND " \
                 "pygac_errors is not null " \
                 "ORDER BY orbit_name"
        cursor.execute(query3.format(satellite=satellite, pv_id=pv_id))
        p_errors = cursor.fetchall()
        if p_errors:
            perr = list()
            for p in p_errors:
                perr.append(p['pygac_errors'])
        # count errors
        counter = collections.Counter(perr)
        errors_dict[pv_id]["pygac_error_key"] = counter.keys()
        errors_dict[pv_id]["pygac_error_value"] = counter.values()

    # create histogram plot
    make_pygac_error_histogram(figobj=figerr, pdict=errors_dict,
                               colors=colors, proc_versions=p_vers_list)

    # find date limits
    (sdt, edt) = check_dates_limits(l1b_dates)

    # add labels and legend
    ptitle = satellite + ": " + date2str(sdt) + " - " + date2str(edt) + "\n"
    finalize_pygaclog_plot(upper=figrun, lower=figerr, ptitle=ptitle, sdt=sdt, edt=edt)

    # save plot as png file
    dat_str = date2filestr(sdt) + '_' + date2filestr(edt)
    png_fil = procs_png + "_" + dat_str + "_" + satellite + ".png"
    outfile = os.path.join(png_path, png_fil)
    plt.savefig(outfile)
    logger.info("Done {0}".format(outfile))
    plt.close()
    return


def plot_pystat_results(satellite, pygaclog_cursor):
    """
    Plot the different pystat results.
    """
    cnt = 0
    channel_list = get_channel_list()
    select_list = get_pystat_select_list()

    for sql in pystat_list:
        cnt += 1

        # get file suffix w.r.t. pygac version
        base = os.path.basename(sql)
        splt = base.split("_")
        commit = splt[-1].split(".")[0]
        commit_id = commit.split("-")[-1]
        pystat_version = "_" + str(cnt) + '_' + commit

        # get metadata w.r.t. pygac version
        pv_metadata = None
        pygac_versions = get_pygac_versions_dict(cursor=pygaclog_cursor)
        for pv in pygac_versions:
            if commit_id in pv['name']:
                pv_metadata = pv['metadata']
                break
        if not pv_metadata:
            logger.info("No match found for {0} in {1}\n".format(commit_id, sql_pygac_log))
            sys.exit(1)

        try:
            db = lite.connect(sql, detect_types=lite.PARSE_DECLTYPES | lite.PARSE_COLNAMES)
            db.row_factory = dict_factory
            cur = db.cursor()

            for channel in channel_list:
                if channel == 'ch1' or channel == 'ch2' or channel == 'ch3a':
                    mean_range = [0.0, 0.25]
                    stdv_range = [0.0, 0.40]
                else:
                    mean_range = [240., 300.]
                    stdv_range = [5.0, 40.]

                for select in select_list:
                    plot_time_series(sat_list=[satellite], channel=channel, select=select,
                                     start_date=start_date, end_date=end_date,
                                     outpath=png_path, cursor=cur,
                                     verbose=False, show_fig=False, linesty='-',
                                     pystat_version=pystat_version,
                                     pystat_metadata=pv_metadata,
                                     mean_range=mean_range,
                                     stdv_range=stdv_range)
            db.close()

        except lite.Error, e:
            logger.info("ERROR {0} \n\n".format(e.args[0]))
            sys.exit(1)
    return


def plot_table_stats(cursor, satellite, sdate, edate):
    """
    Plot per satellite, per channel from vw_stats
        - mean_val
        - number_of_masked_obs
        - number_of_valid_obs
    """
    channel_list = get_channel_list()

    for channel in channel_list:
    # for channel in ['ch1']:

        if channel == 'ch1' or channel == 'ch2' or channel == 'ch3a':
            mean_range = [0.0, 0.30]
        else:
            mean_range = [230., 300.]

        logger.info("Working on {0}:{1}".format(satellite, channel))
        logger.info("Read all available pygac processing versions")
        pygac_versions = get_pygac_versions_dict(cursor=cursor)

        p_vers_list = list()
        l1b_dates = list()

        for pv in pygac_versions:
            pv_id = pv['id']
            pv_name = pv['name']
            pv_info = pv['metadata']
            p_vers_list.append(pv_name)

            logger.info("Working on: {0}, i.e. {1}".format(pv_name, pv_info))
            # each version into a separate png: initialize plot (3 panels)
            (fig1, fig_mean, fig_valid, fig_masked) = init_tab_stats_plot()

            l1b_filenames = list()
            means = list()
            valids = list()
            masked = list()

            # get statistics
            cmd = "SELECT orbit_name, mean_val, number_of_valid_obs, number_of_masked_obs " \
                  "FROM vw_stats WHERE " \
                  "satellite_name=\'{satellite}\' AND " \
                  "channel_name=\'{channel}\' AND " \
                  "pygac_version_id={pv_id} AND " \
                  "number_of_total_obs is not null " \
                  "ORDER BY orbit_name"

            cursor.execute(cmd.format(satellite=satellite, channel=channel,
                                      pv_id=pv_id, sdate=sdate, edate=edate))
            channel_stat = cursor.fetchall()
            if channel_stat:
                for cs in channel_stat:
                    l1b_filenames.append(cs['orbit_name'])
                    means.append(cs['mean_val'])
                    valids.append(cs['number_of_valid_obs'])
                    masked.append(cs['number_of_masked_obs'])
            else:
                continue

            start_time_l1b = list()
            for orbit in l1b_filenames:
                odict = extract_orbit_attrs(orbit)
                start_time_l1b.append(odict['start_time_l1b'])

            # collect start_time_l1b
            for dt in start_time_l1b:
                l1b_dates.append(dt)

            # plot data
            (x1, y1) = slice_data(dates=start_time_l1b, values=means)
            (x2, y2) = slice_data(dates=start_time_l1b, values=valids)
            (x3, y3) = slice_data(dates=start_time_l1b, values=masked)
            fig_mean.plot(x1, y1, '-', color=colors[pv_id-1], alpha=0.75, linewidth=1.)
            fig_valid.plot(x2, y2, 'o', color=colors[pv_id-1], alpha=0.75, markersize=3)
            fig_masked.plot(x3, y3, 'o', label=pv_info, color=colors[pv_id-1], alpha=0.75, markersize=3)

            (sdt, edt) = check_dates_limits(l1b_dates)

            fc = full_cha_name(target=channel)
            unit = get_channel_unit(target=channel)
            ptitle = satellite + ": " + fc + " " + unit + " " + \
                     date2str(sdt) + " - " + date2str(edt) + "\n"

            finalize_tab_stats_plot(obj1=fig_mean, obj2=fig_valid, obj3=fig_masked,
                                    ptitle=ptitle, sdt=sdt, edt=edt,
                                    obj1_y_range=mean_range)

            # save plot as png file
            pstring = str(pv_id) + "-" + pv_name
            dat_str = date2filestr(sdt) + '_' + date2filestr(edt)
            png_fil = stats_png + "_" + dat_str + '_' + channel + '_' + satellite + "_" + pstring + ".png"
            outfile = os.path.join(png_path, png_fil)
            fig1.savefig(outfile)
            logger.info("Done {0}".format(outfile))
            plt.close()

    return


def plot_table_stats_all_in_one(cursor, satellite, sdate, edate):
    """
    Plot per satellite, per channel from vw_stats
        - mean_val
        - number_of_masked_obs
        - number_of_valid_obs
    """
    channel_list = get_channel_list()

    for channel in channel_list:

        if channel == 'ch1' or channel == 'ch2' or channel == 'ch3a':
            mean_range = [0.0, 0.30]
        else:
            mean_range = [230., 300.]

        # (1) all in one: initialize plot (3 panels)
        (fig1, fig_mean, fig_valid, fig_masked) = init_tab_stats_plot()

        logger.info("Working on {0}:{1}".format(satellite, channel))
        logger.info("Read all available pygac processing versions")
        pygac_versions = get_pygac_versions_dict(cursor=cursor)

        p_vers_list = list()
        l1b_dates = list()

        for pv in pygac_versions:
            pv_id = pv['id']
            pv_name = pv['name']
            pv_info = pv['metadata']
            p_vers_list.append(pv_name)

            logger.info("Working on: {0}, i.e. {1}".format(pv_name, pv_info))

            l1b_filenames = list()
            means = list()
            valids = list()
            masked = list()

            # get statistics
            cmd = "SELECT orbit_name, mean_val, number_of_valid_obs, number_of_masked_obs " \
                  "FROM vw_stats WHERE " \
                  "satellite_name=\'{satellite}\' AND " \
                  "channel_name=\'{channel}\' AND " \
                  "pygac_version_id={pv_id} AND " \
                  "number_of_total_obs is not null " \
                  "ORDER BY orbit_name"

            cursor.execute(cmd.format(satellite=satellite, channel=channel,
                                      pv_id=pv_id, sdate=sdate, edate=edate))
            channel_stat = cursor.fetchall()
            if channel_stat:
                for cs in channel_stat:
                    l1b_filenames.append(cs['orbit_name'])
                    means.append(cs['mean_val'])
                    valids.append(cs['number_of_valid_obs'])
                    masked.append(cs['number_of_masked_obs'])
            else:
                continue

            start_time_l1b = list()
            for orbit in l1b_filenames:
                odict = extract_orbit_attrs(orbit)
                start_time_l1b.append(odict['start_time_l1b'])

            # collect start_time_l1b
            for dt in start_time_l1b:
                l1b_dates.append(dt)

            # plot data
            (x1, y1) = slice_data(dates=start_time_l1b, values=means)
            (x2, y2) = slice_data(dates=start_time_l1b, values=valids)
            (x3, y3) = slice_data(dates=start_time_l1b, values=masked)
            fig_mean.plot(x1, y1, '-', color=colors[pv_id-1], alpha=0.75, linewidth=1.)
            fig_valid.plot(x2, y2, 'o', color=colors[pv_id-1], alpha=0.75, markersize=3)
            fig_masked.plot(x3, y3, 'o', label=pv_info, color=colors[pv_id-1], alpha=0.75, markersize=3)

        (sdt, edt) = check_dates_limits(l1b_dates)

        fc = full_cha_name(target=channel)
        unit = get_channel_unit(target=channel)
        ptitle = satellite + ": " + fc + " " + unit + " " + \
                 date2str(sdt) + " - " + date2str(edt) + "\n"

        finalize_tab_stats_plot(obj1=fig_mean, obj2=fig_valid, obj3=fig_masked,
                                ptitle=ptitle, sdt=sdt, edt=edt,
                                obj1_y_range=mean_range)

        # save plot as png file
        dat_str = date2filestr(sdt) + '_' + date2filestr(edt)
        png_fil = stats_png + "_" + dat_str + '_' + channel + '_' + satellite + ".png"
        outfile = os.path.join(png_path, png_fil)
        fig1.savefig(outfile)
        logger.info("Done {0}".format(outfile))
        plt.close()

    return


def check_dates_limits(dates):
    set_xmin = start_date.date()
    set_xmax = end_date.date()
    if min(dates).date() > start_date.date():
        set_xmin = min(dates).date()
    if max(dates).date() < end_date.date():
        set_xmax = max(dates).date()
    return set_xmin, set_xmax


def slice_data(dates, values):
    dat_list = list()
    val_list = list()
    for idx, dt in enumerate(dates):
        if (dt >= start_date) and (dt <= end_date):
            dat_list.append(dt)
            val_list.append(values[idx])
    return dat_list, val_list


def plot_tsm_check(cursor, satellite, sdate, edate):
    """
    Plot number_of_masked_obs before and after TSM correction.
    TSM: temporary scan motor issue
    """
    channel_list = get_channel_list()

    for channel in channel_list:

        # initialize plot
        (fig, fig_normal, fig_zoom) = init_tab_stats_plot_tsm()

        logger.info("Working on {0}:{1}".format(satellite, channel))
        logger.info("Read all available pygac processing versions")
        pygac_versions = get_pygac_versions_dict(cursor=cursor)

        p_vers_list = list()
        l1b_dates = list()
        # number_of_masked_obs from two different runs
        tsm_masked_1 = list()
        tsm_masked_2 = list()
        l1b_filenames_1 = list()
        l1b_filenames_2 = list()

        for pv in pygac_versions:
            pv_id = pv['id']
            if pv_id is not tsm_id_1 and pv_id is not tsm_id_2:
                continue
            pv_name = pv['name']
            pv_info = pv['metadata']
            p_vers_list.append(pv_info)

            logger.info("Working on: {0}, i.e. {1}".format(pv_name, pv_info))

            # get statistics
            cmd = "SELECT orbit_name, number_of_masked_obs " \
                  "FROM vw_stats WHERE " \
                  "satellite_name=\'{satellite}\' AND " \
                  "channel_name=\'{channel}\' AND " \
                  "pygac_version_id={pv_id} AND " \
                  "number_of_total_obs is not null " \
                  "ORDER BY orbit_name"

            cursor.execute(cmd.format(satellite=satellite, channel=channel,
                                      pv_id=pv_id, sdate=sdate, edate=edate))
            channel_stat = cursor.fetchall()
            if channel_stat:
                for cs in channel_stat:
                    if pv_id == tsm_id_1:
                        l1b_filenames_1.append(cs['orbit_name'])
                        tsm_masked_1.append(cs['number_of_masked_obs'])
                    if pv_id == tsm_id_2:
                        l1b_filenames_2.append(cs['orbit_name'])
                        tsm_masked_2.append(cs['number_of_masked_obs'])
            else:
                continue

        if len(l1b_filenames_1) != len(l1b_filenames_2) and \
            len(tsm_masked_1) != len(tsm_masked_2):
                logger.info("Something is wrong here! lists are not of same length")
                sys.exit(1)

        start_time_l1b = list()
        for orbit in l1b_filenames_1:
            odict = extract_orbit_attrs(orbit)
            start_time_l1b.append(odict['start_time_l1b'])

        # collect start_time_l1b
        for dt in start_time_l1b:
            l1b_dates.append(dt)

        # plot data
        (x1, y1) = slice_data(dates=start_time_l1b, values=tsm_masked_1)
        (x2, y2) = slice_data(dates=start_time_l1b, values=tsm_masked_2)

        ydiff = list()
        for i, j in zip(y1, y2):
            ydiff.append(i-j)

        label = p_vers_list[0] + " MINUS " + p_vers_list[1]
        fig_normal.plot(x1, ydiff, '-', label=label, color='Navy', alpha=0.75, linewidth=1.)
        fig_zoom.plot(x1, ydiff, '-', color='Navy', alpha=0.75, linewidth=1.)

        # finalize plot
        (sdt, edt) = check_dates_limits(l1b_dates)
        fc = full_cha_name(target=channel)
        ptitle = satellite + ": " + fc + " " + date2str(sdt) + " - " + date2str(edt) + "\n"
        finalize_tab_stats_plot_tsm(obj1=fig_normal, obj2=fig_zoom, ptitle=ptitle, sdt=sdt, edt=edt)

        # save plot as png file
        dat_str = date2filestr(sdt) + '_' + date2filestr(edt)
        png_fil = stats_png + "_" + dat_str + '_' + channel + '_' + satellite + "_tsm-check.png"
        outfile = os.path.join(png_path, png_fil)
        fig.savefig(outfile)
        logger.info("Done {0}".format(outfile))
        plt.close()
    return


if __name__ == '__main__':

    if not os.path.exists(png_path):
        os.makedirs(png_path)

    try:
        pygaclog = lite.connect(sql_pygac_log,
                                detect_types=lite.PARSE_DECLTYPES | lite.PARSE_COLNAMES)
        pygaclog.row_factory = dict_factory
        cur_log = pygaclog.cursor()

        # get list of satellites in database
        sat_list = readout_satellites(cursor=cur_log)
        logger.info("These satellites are in the database: {0}".format(sat_list))

        # make plot per satellite
        if vis_pystat:
            plot_pystat_results(satellite=sat, pygaclog_cursor=cur_log)

        if vis_pygaclog:
            plot_pygac_runtime_and_errors(cursor=cur_log, satellite=sat)

        # select statements using start and end date from config file TBD
        if vis_timestamps:
            plot_pygac_l1c_timestamps(cursor=cur_log, satellite=sat)

        if vis_tab_stats:
            plot_table_stats(cursor=cur_log, satellite=sat,
                             sdate=start_date, edate=end_date)
            plot_table_stats_all_in_one(cursor=cur_log, satellite=sat,
                                        sdate=start_date, edate=end_date)

        if vis_tsm_check:
            plot_tsm_check(cursor=cur_log, satellite=sat,
                           sdate=start_date, edate=end_date)

        # close dbfile
        pygaclog.close()

    except lite.Error, e:
        logger.info("ERROR {0} \n\n".format(e.args[0]))
        sys.exit(1)
