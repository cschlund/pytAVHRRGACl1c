#
# subroutines for plotting AVHRR GAC L1c data (input = sql db)
#

import os
import numpy as np
import datetime
import math
import matplotlib.pyplot as plt
import subs_avhrrgac as subs
from scipy import stats
from matplotlib import gridspec
from matplotlib.ticker import FormatStrFormatter
from matplotlib.dates import MONDAY, DayLocator
from matplotlib.dates import YearLocator, MonthLocator, WeekdayLocator, DateFormatter
from mpl_toolkits.axes_grid1 import host_subplot
import mpl_toolkits.axisartist as aa
from dateutil.rrule import rrule, DAILY
from datetime import timedelta
from numpy import array
import warnings
import logging

logger = logging.getLogger('root')
warnings.filterwarnings("ignore")


def get_id(table, column, value, sql):
    """
    Get value from a table and column.
    """
    get = "SELECT {0} FROM {1} WHERE name = \'{2}\'".format(column, table, value)
    res = sql.execute(get)

    for item in res:
        str_id = item["id"]

    return str_id


def get_lat_belts(table, sql):
    """
    Read table containing the latitudinal belt information.
    """

    # latitudes (id INTEGER PRIMARY KEY, belt FLOAT);
    idx_list = list()
    val_list = list()

    act = "SELECT * FROM {0}".format(table)
    result = sql.execute(act)

    for item in result:
        idx_list.append(item['id'])
        val_list.append(item['belt'])

    return idx_list, val_list


def read_zonal_stats(sat, cha, sel, dt, sql):
    """
    Read sqlite database (sql):
    return daily zonal statistics for a given satellite (sat),
    channel (cha), time selection (sel) and date (dt).
    """

    mean_list = list()
    stdv_list = list()
    nobs_list = list()

    zonal_list = ("ZonalMean", "ZonalStdv", "ZonalNobs")

    sat_id = get_id("satellites", "id", sat, sql)
    cha_id = get_id("channels", "id", cha, sql)
    sel_id = get_id("selects", "id", sel, sql)

    (lat_id, lats) = get_lat_belts("latitudes", sql)

    mean_str = list()
    stdv_str = list()
    nobs_str = list()

    for idx in lat_id:
        mean_str.append(zonal_list[0] + str(idx))
        stdv_str.append(zonal_list[1] + str(idx))
        nobs_str.append(zonal_list[2] + str(idx))

    full_list = mean_str + stdv_str + nobs_str
    get_cols = ', '.join(full_list)
    sql_query = "SELECT {0} FROM statistics WHERE " \
                "satelliteID={1} AND channelID={2} AND " \
                "selectID={3} AND date=\'{4}\'".format(get_cols, sat_id,
                                                       cha_id, sel_id, dt)
    results = sql.execute(sql_query)

    for item in results:
        for i in full_list:
            if i.startswith(zonal_list[0]):
                mean_list.append(item[i])
            if i.startswith(zonal_list[1]):
                stdv_list.append(item[i])
            if i.startswith(zonal_list[2]):
                nobs_list.append(item[i])

    return mean_list, stdv_list, nobs_list, lats


def read_global_newstats(sat, cha, sel, sd, ed, sql):
    """
    Read sqlite database (sql): 
    return global statistics for a given satellite (sat), 
    channel (cha), time selection (sel) between 
    start_date (sd) and end_date (ed).
    """

    # reflectance
    if cha == "ch1" or cha == "ch2" or cha == "ch3a":
        minval = 0.
        maxval = 1.5
    # brightness temperature
    else:
        minval = 140.
        maxval = 350.

    glob_list = ("OrbitCount", "GlobalMean", "GlobalStdv", "GlobalNobs")
    mean_list = list()
    stdv_list = list()
    nobs_list = list()
    date_list = list()
    orbs_cnts = list()

    sat_id = get_id("satellites", "id", sat, sql)
    cha_id = get_id("channels", "id", cha, sql)
    sel_id = get_id("selects", "id", sel, sql)

    get_data = "SELECT date, {0}, {1}, {2}, {3} " \
               "FROM statistics WHERE satelliteID={4} AND " \
               "channelID={5} AND selectID={6} AND " \
               "date>=\'{7}\' AND date<=\'{8}\' AND " \
               "GlobalMean >= {9} AND GlobalMean <= {10} " \
               "ORDER BY date".format(glob_list[0], glob_list[1],
                                      glob_list[2], glob_list[3],
                                      sat_id, cha_id, sel_id, sd, ed,
                                      minval, maxval)
    # print get_data
    results = sql.execute(get_data)

    for result in results:
        if result['date'] is not None:
            date_list.append(result['date'])
            orbs_cnts.append(result[glob_list[0]])
            mean_list.append(result[glob_list[1]])
            stdv_list.append(result[glob_list[2]])
            nobs_list.append(result[glob_list[3]])
        else:
            return None

    return date_list, mean_list, stdv_list, nobs_list, orbs_cnts


def get_number_of_orbits_per_day(satellite, date_list, db):
    """
    get number of valid orbits per day for a specific
    satellite from the archive database containing
    the L1B and L1C information of AVHRR GAC.
    """
    orbit_cnt_list = []

    for sdt in date_list:
        edt = sdt + datetime.timedelta(days=1)

        get_data = "SELECT COUNT(*) " \
                   "FROM vw_std WHERE blacklist=0 AND " \
                   "satellite_name=\'{satellite}\' AND " \
                   "start_time_l1c BETWEEN " \
                   "\'{sdt}\' AND \'{edt}\' ".format(satellite=satellite,
                                                     sdt=sdt, edt=edt)

        for row in db.execute(get_data):
            if row['COUNT(*)']:
                nums = int(row['COUNT(*)'])
                orbit_cnt_list.append(nums)

    return orbit_cnt_list


def pystat_channel_difference(cha_list, sat_list, sza_time, cursor,
                              out_path, sdate, edate, linesty):
    """
    Plot the difference of two channels based on PySTAT results.
    :param cha_list: list of channels to be used for difference calculation ([0] - [1])
    :param sat_list: list of satellites plotted into figure
    :param sza_time: day/night/twilight
    :param cursor: SQL cursor
    :param out_path: output directory for png file
    :param sdate: start date
    :param edate: end date
    :param linesty: defines the line style used in the plot
    :return:
    """
    isdata_cnt = 0
    sat_cnt = 0

    if sza_time == "day_90sza":
        stime = "day"
    elif sza_time == "day":
        # stime = "day_80sza"
        return
    else:
        stime = sza_time

    (cha1_label, nobs_label, date_label, orbit_label, mean_label, stdv_label) = get_pystat_labels(cha_list[0])
    (cha2_label, nobs_label, date_label, orbit_label, mean_label, stdv_label) = get_pystat_labels(cha_list[1])

    mean_label = r'$\Delta$' + mean_label
    stdv_label = r'$\Delta$' + stdv_label

    (plot_label, c1_name, c2_name) = get_channel_difference_plot_title(label1=cha1_label, label2=cha2_label,
                                                                       selected_time=stime, channel_list=cha_list)

    (ax_val, ax_std, ax_rec) = init_pystat_plot()

    # -- loop over satellites
    for satellite in sat_list:

        # get color for satellite
        satcolor = subs.color_satstring(satellite)

        # read pystat results
        (datelst_1, meanlst_1, stdvlst_1,
         nobslst_1, orb_cnts_lst_1) = read_global_newstats(satellite, cha_list[0],
                                                           sza_time, sdate, edate, cursor)
        (datelst_2, meanlst_2, stdvlst_2,
         nobslst_2, orb_cnts_lst_2) = read_global_newstats(satellite, cha_list[1],
                                                           sza_time, sdate, edate, cursor)

        collocate = False
        if len(datelst_1) != len(datelst_2):
            logger.info(" --- Both date lists do not have the same length.")
            logger.info(" ====> collocate both data sets!")
            collocate = True
        else:
            for cnt, idx in enumerate(datelst_1):
                if datelst_1[cnt] != datelst_2[cnt]:
                    logger.info(" --- Both date lists have the same length but do not match.")
                    logger.info("     Date List 1: {0}".format(datelst_1[cnt]))
                    logger.info("     Date List 2: {0}".format(datelst_2[cnt]))
                    logger.info(" ====> collocate both data sets!")
                    collocate = True
                    break

        if collocate:
            (dates, mean1, mean2, stdv1, stdv2,
             nobs1, nobs2) = collocte_pystat_lists(datelst_1, meanlst_1, stdvlst_1, nobslst_1,
                                                   datelst_2, meanlst_2, stdvlst_2, nobslst_2)
        else:
            dates = datelst_1
            nobs1 = nobslst_1
            nobs2 = nobslst_2
            mean1 = meanlst_1
            mean2 = meanlst_2
            stdv1 = stdvlst_1
            stdv2 = stdvlst_2

        if len(dates) > 1:
            isdata_cnt += 1
            sat_cnt += 1
            dat = np.asarray(dates)
            ob1 = np.asarray(nobs1)
            ob2 = np.asarray(nobs2)
            ave = np.asarray(mean1) - np.asarray(mean2)
            std = np.asarray(stdv1) - np.asarray(stdv2)
            ax_val.plot(dat, ave, linesty, label=satellite, color=satcolor, alpha=0.8, markersize=5)
            ax_std.plot(dat, std, linesty, label=satellite, color=satcolor, alpha=0.8, markersize=5)
            if sat_cnt <= 1:
                ax_rec.plot(dat, ob1, '--o', label=c1_name, markersize=5, alpha=0.5, color='orange')
                ax_rec.plot(dat, ob2, '--o', label=c2_name, markersize=5, alpha=0.5, color='gray')
            else:
                ax_rec.plot(dat, ob1, '--o', markersize=5, alpha=0.5, color='orange')
                ax_rec.plot(dat, ob2, '--o', markersize=5, alpha=0.5, color='gray')
        else:
            continue
    # -- end ofloop over satellites

    if isdata_cnt > 0:
        if len(sat_list) == 1:
            min_x_date = min(dates)
            max_x_date = max(dates)
            delta_days = (max_x_date - min_x_date).days
            sdate_str = subs.date2str(min_x_date)
            edate_str = subs.date2str(max_x_date)
            sname = subs.full_sat_name(sat_list[0])[2]
            fbase = 'Plot_TimeSeries_' + sdate_str + '_' + edate_str + \
                    '_' + c1_name + '-' + c2_name + '_' + stime + '_' + sname + '.png'
        else:
            delta_days = (edate - sdate).days
            min_x_date = sdate
            max_x_date = edate
            sdate_str = subs.date2str(sdate)
            edate_str = subs.date2str(edate)
            fbase = 'Plot_TimeSeries_' + sdate_str + '_' + edate_str + \
                    '_' + c1_name + '-' + c2_name + '_' + stime + '.png'

        ofile = os.path.join(out_path, fbase)

        # annotate plot
        label_pystat_plot(ave=ax_val, std=ax_std, obs=ax_rec,
                          min_x_date=min_x_date, max_x_date=max_x_date,
                          delta_days=delta_days,
                          plt_label=plot_label, dat_label=date_label,
                          ave_label=mean_label, std_label=stdv_label, obs_label=nobs_label)

        # make legend
        if len(sat_list) > 8:
            num_of_sats = int(math.ceil(len(sat_list) / 2.))
        else:
            num_of_sats = len(sat_list)

        leg = ax_std.legend(ncol=num_of_sats, loc='best', fancybox=True)
        leg2 = ax_rec.legend(loc='best', fancybox=True)
        plt.tight_layout(rect=(0.02, 0.02, 0.98, 0.98))
        leg.get_frame().set_alpha(0.5)

        # save and close plot
        plt.savefig(ofile)
        logger.info("Done {0}".format(ofile))
        plt.close()

    else:
        plt.close()

    return


def plot_time_series(sat_list, channel, select, start_date, end_date, outpath,
                     cursor, verbose, show_fig, linesty):
    """
    Plot time series based on pystat results.
    """
    isdata_cnt = 0

    if select == "day_90sza":
        stime = "day"
    elif select == "day":
        # stime = "day_80sza"
        return
    else:
        stime = select

    (chan_label, nobs_label, date_label,
     orbit_label, mean_label, stdv_label) = get_pystat_labels(channel)

    plot_label = "Daily global statistics for AVHRR " + chan_label + " (" + stime + ")\n"

    (ax_val, ax_std, ax_rec) = init_pystat_plot()

    # -- loop over satellites
    for satellite in sat_list:

        # get color for satellite
        satcolor = subs.color_satstring(satellite)

        # read pystat results
        (datelst, meanlst, stdvlst,
         nobslst, orb_cnts_lst) = read_global_newstats(satellite, channel, select,
                                                       start_date, end_date, cursor)
        if len(datelst) > 1:
            isdata_cnt += 1
            ax_val.plot(datelst, meanlst, linesty, label=satellite, color=satcolor, alpha=0.8, markersize=5)
            ax_std.plot(datelst, stdvlst, linesty, label=satellite, color=satcolor, alpha=0.8, markersize=5)
            ax_rec.plot(datelst, nobslst, '--o', label=satellite, markersize=5, alpha=0.8, color=satcolor)
        else:
            continue
    # -- end ofloop over satellites

    if isdata_cnt > 0:
        if len(sat_list) == 1:
            min_x_date = min(datelst)
            max_x_date = max(datelst)
            delta_days = (max_x_date - min_x_date).days
            sdate_str = subs.date2str(min_x_date)
            edate_str = subs.date2str(max_x_date)
            sname = subs.full_sat_name(sat_list[0])[2]
            fbase = 'Plot_TimeSeries_' + sdate_str + '_' + edate_str + \
                    '_' + channel + '_' + stime + '_' + sname + '.png'
        else:
            delta_days = (end_date - start_date).days
            min_x_date = start_date
            max_x_date = end_date
            sdate_str = subs.date2str(start_date)
            edate_str = subs.date2str(end_date)
            fbase = 'Plot_TimeSeries_' + sdate_str + '_' + edate_str + \
                    '_' + channel + '_' + stime + '.png'

        ofile = os.path.join(outpath, fbase)

        # get number of valid orbits per day
        if len(sat_list) == 1:
            plot_orbits_per_day(ax_rec, sat_list[0], orb_cnts_lst, datelst, orbit_label)

        # annotate plot
        label_pystat_plot(ave=ax_val, std=ax_std, obs=ax_rec,
                          min_x_date=min_x_date, max_x_date=max_x_date,
                          delta_days=delta_days,
                          plt_label=plot_label, dat_label=date_label,
                          ave_label=mean_label, std_label=stdv_label, obs_label=nobs_label)

        # make legend
        if len(sat_list) > 8:
            num_of_sats = int(math.ceil(len(sat_list) / 2.))
        else:
            num_of_sats = len(sat_list)

        leg = ax_std.legend(ncol=num_of_sats, loc='best', fancybox=True)
        plt.tight_layout(rect=(0.02, 0.02, 0.98, 0.98))
        leg.get_frame().set_alpha(0.5)

        # save and close plot
        plt.savefig(ofile)
        if show_fig:
            plt.show()
            logger.info("Shown: {0} ".format(os.path.basename(ofile)))
        logger.info("Done {0}".format(ofile))
        plt.close()

    else:
        plt.close()

    return


def plot_time_series_linfit(sat_list, channel, select, start_date, end_date,
                            outpath, cursor, show_fig):
    """
    Plot Time Series based on pystat results for each satellite
    including linear regression.
    """
    if select == "day_90sza":
        stime = "day"
    elif select == "day":
        # stime = "day_80sza"
        return
    else:
        stime = select

    sdate = subs.date2str(start_date)
    edate = subs.date2str(end_date)

    (chan_label, nobs_label, date_label,
     orbit_label, mean_label, stdv_label) = get_pystat_labels(channel)

    # -- loop over satellites
    for satellite in sat_list:

        # read statistics from SQL file
        (datelst, meanlst, stdvlst,
         nobslst, orb_cnts_lst) = read_global_newstats(satellite, channel, select,
                                                       start_date, end_date, cursor)

        if len(datelst) > 10:
            plot_label = "Daily global statistics for AVHRR " + satellite + \
                         ' ' + chan_label + " (" + stime + ")\n"
            prefix = "Plot_TimeSeries_LinFit_"
            fbase = prefix + sdate + '_' + edate + '_' + satellite + '_' + \
                    channel + '_' + stime + '.png'
            ofile = os.path.join(outpath, fbase)

            # initialize plot
            (ax_val, ax_std, ax_rec) = init_pystat_plot()

            # list to array
            ave = np.asarray(meanlst)
            std = np.asarray(stdvlst)

            # convert date list to a set of numbers counting
            # the number of days having passed from the first day of the file
            x = [(e - min(datelst)).days for e in datelst]

            # linear regression
            (slope, intercept, r_value, p_value, std_err) = stats.linregress(x, ave)
            (slope2, intercept2, r_value2, p_value2, std_err2) = stats.linregress(x, std)
            yp = np.polyval([slope, intercept], x)
            yp2 = np.polyval([slope2, intercept2], x)

            # plot data and linfit
            ax_val.plot(datelst, meanlst, color='DarkGreen')
            ax_val.plot(datelst, yp, '--', color='Red',
                        label="Linear fit: y = %.5f * x + %.5f" % (slope, intercept))
            ax_std.plot(datelst, stdvlst, color='DarkBlue')
            ax_std.plot(datelst, yp2, '--', color='Red',
                        label="Linear fit: y = %.5f * x + %.5f" % (slope2, intercept2))
            ax_rec.plot(datelst, nobslst, color='DimGray')

            # get number of valid orbits per day
            if len(sat_list) == 1:
                plot_orbits_per_day(ax_rec, satellite, orb_cnts_lst, datelst, orbit_label)

            # set x range regarding date list
            min_x_date = min(datelst)
            max_x_date = max(datelst)
            delta_days = (max_x_date - min_x_date).days

            # annotate plot
            label_pystat_plot(ave=ax_val, std=ax_std, obs=ax_rec,
                              min_x_date=min_x_date, max_x_date=max_x_date,
                              delta_days=delta_days,
                              plt_label=plot_label, dat_label=date_label,
                              ave_label=mean_label, std_label=stdv_label, obs_label=nobs_label)

            # add legend
            leg = ax_val.legend(loc='best', fancybox=True)
            leg.get_frame().set_alpha(0.5)
            leg = ax_std.legend(loc='best', fancybox=True)
            leg.get_frame().set_alpha(0.5)

            # tight layout
            plt.tight_layout(rect=(0.02, 0.02, 0.98, 0.98))

            # save and close plot
            plt.savefig(ofile)
            if show_fig:
                plt.show()
                logger.info("Shown: {0} ".format(os.path.basename(ofile)))
            logger.info("Done {0}".format(ofile))
            plt.close()

        else:
            continue

    return


def plt_zonal_means(zonal_mean, zonal_nobs, global_mean, zone_size,
                    ofil_name, fill_value, date_str, chan_str, plat_str,
                    sel_str, show_fig):
    """
    plot global and zonal means.
    s. finkensieper, july 2014
    """

    # set_xlim
    xmin = -90
    xmax = 90
    # xaxis.set_tick
    start = xmin + (zone_size / 2.0)
    end = xmax + (zone_size / 2.0)
    major_ticks = np.arange(start, end, zone_size * 3)
    minor_ticks = np.arange(start, end, zone_size)

    plot_title = date_str + " AVHRR/" + plat_str + " GAC L1C " + chan_str + " (" + sel_str + ")\n"

    glo_mask = np.ma.equal(global_mean, fill_value)
    zon_mask = np.ma.equal(zonal_mean, fill_value)

    glm = np.ma.masked_where(glo_mask, global_mean)

    zonal_means = np.ma.masked_where(zon_mask, zonal_mean)
    nobs = np.ma.masked_where(zon_mask, zonal_nobs)

    if np.ma.count(zonal_means) == 0:
        return

        # define latitudinal zone size :
    zone_rad = zone_size / 2.0

    # determine zone centers:
    zone_centers = np.arange(-90 + zone_rad, 90 + zone_rad, zone_size)
    nzones = len(zone_centers)

    # create one host axes object and one child axes object which share their
    # x-axis, but have individual y-axes:
    fig = init_pystat_zonal_figure()
    ax_means = host_subplot(111, axes_class=aa.Axes)
    ax_nobs = ax_means.twinx()

    # plot zonal mean into the host axes:
    width = zone_rad * 0.5
    ax_means.bar(zone_centers, zonal_means,
                 label='Zonal Mean using lat. zone size of {0} degrees'.format(zone_size),
                 width=width, color='darkorange')
    ax_means.set_xlim(xmin, xmax)
    ax_means.set_xticks(major_ticks)
    ax_means.set_xticks(minor_ticks, minor=True)

    # plot number of observations into the child axes:
    ax_nobs.bar(zone_centers + width, nobs,
                label='# of Observations (total: ' + format(int(nobs.sum())) + ')',
                width=width, color='g')

    # plot global mean on top of them:
    ax_means.plot(zone_centers, np.ma.ones(nzones) * glm, 'b--', lw=2.0,
                  label='Global Mean: ' + format('%.4f' % glm))

    # set axes labels:
    ax_means.set_ylabel('zonal mean')
    ax_nobs.set_ylabel('# of observations')
    ax_means.set_xlabel('\nlatitudinal zone center [degrees]')
    ax_nobs.yaxis.get_major_formatter().set_powerlimits((0, 1))

    # set axes range:
    ax_means.set_ylim(0, 1.2 * np.ma.max(zonal_means))
    ax_nobs.set_ylim(0, 1.2 * np.ma.max(nobs))

    # add title & legend:
    plt.title(plot_title)
    plt.legend(loc='upper center')

    # ensure 'tight layout' (prevents the axes labels from being placed outside
    # the figure):
    plt.tight_layout(rect=(0.02, 0.02, 0.98, 0.98))

    # save figure to file:
    # plt.savefig('zonal_means.png', bbox_inches='tight')
    with np.errstate(all='ignore'):
        plt.savefig(ofil_name)
        if show_fig:
            plt.show()
            logger.info("Shown: {0} ".format(os.path.basename(ofil_name)))
        logger.info("Done {0}".format(ofil_name))
        plt.close()

    return


def plt_zonal_mean_stdv(zonal_mean, zonal_stdv, zonal_nobs,
                        zone_centers, fill_value, zone_size, ofil_name,
                        date_str, chan_str, plat_str, sel_str,
                        show_fig):
    """
    plot zonal means and standard deviations.
    """

    plot_title = date_str + " AVHRR/" + plat_str + " GAC L1C " + chan_str + " (" + sel_str + ")\n"

    # set_xlim
    xmin = -90
    xmax = 90
    # xaxis.set_tick
    start = xmin + (zone_size / 2.0)
    end = xmax + (zone_size / 2.0)
    major_ticks = np.arange(start, end, zone_size * 3)
    minor_ticks = np.arange(start, end, zone_size)

    zon_mask = np.ma.equal(zonal_mean, fill_value)

    avearr = np.ma.masked_where(zon_mask, zonal_mean)
    devarr = np.ma.masked_where(zon_mask, zonal_stdv)
    cntarr = np.ma.masked_where(zon_mask, zonal_nobs)
    belarr = np.ma.masked_where(zon_mask, zone_centers)

    xlabel = 'Latitude using zone size of {0} degrees'.format(zone_size)
    mean_label = 'zonal mean'
    stdv_label = 'zonal standard deviation'

    if np.ma.count(avearr) == 0:
        return

    fig = init_pystat_zonal_figure()
    gs = gridspec.GridSpec(2, 1, height_ratios=[3, 1])
    ax_val = fig.add_subplot(gs[0])
    ax_rec = fig.add_subplot(gs[1])

    y1 = avearr + devarr
    y2 = avearr - devarr

    allcnt = int(zonal_nobs.sum())
    maxcnt = int(zonal_nobs.max())

    # plot zonal mean & stdv
    ax_val.plot(belarr, avearr, 'o', color='red')
    ax_val.plot(belarr, avearr, color='red', linewidth=2)
    ax_val.fill_between(belarr, y1, y2, facecolor='SkyBlue', alpha=0.5)
    ax_val.set_xlim(xmin, xmax)
    ax_val.set_xticks(major_ticks)
    ax_val.set_xticks(minor_ticks, minor=True)
    ax_val.set_title(plot_title)
    ax_val.set_ylabel('zonal mean and stdv')
    ax_val.grid(which='both')
    # ax_val.grid(which='minor', alpha=0.2)
    # ax_val.grid(which='major', alpha=0.5)

    # plot number of observations / lat. zone
    ax_rec.plot(belarr, cntarr, 'o', color='black')
    ax_rec.plot(belarr, cntarr, color='black', linewidth=2,
                label='total records = ' + format(allcnt))
    ax_rec.set_xlim(xmin, xmax)
    ax_rec.set_xticks(major_ticks)
    ax_rec.set_xticks(minor_ticks, minor=True)
    ax_rec.set_ylabel('# of obs.')
    ax_rec.set_xlabel(xlabel)
    ax_rec.grid(which='both')

    # set axes range:
    ax_rec.yaxis.get_major_formatter().set_powerlimits((0, 1))
    ax_rec.set_ylim(0, 1.1 * maxcnt)

    # plot legend for mean & stdv
    m = plt.Line2D((0, 1), (1, 1), c='red', lw=2)
    s = plt.Rectangle((0, 0), 1, 1, fc='SkyBlue')
    leg = ax_val.legend([m, s], [mean_label, stdv_label],
                        loc='best', fancybox=True)
    leg.get_frame().set_alpha(0.5)

    # plot legend for observations
    # leg2 = ax_rec.legend(loc='upper center', fancybox=True)
    leg.get_frame().set_alpha(0.5)

    # ensure 'tight layout' (prevents the axes labels from being 
    # placed outside the figure):
    plt.tight_layout(rect=(0.02, 0.02, 0.98, 0.98))

    plt.savefig(ofil_name)
    if show_fig:
        plt.show()
        logger.info("Shown: {0} ".format(os.path.basename(ofil_name)))
    logger.info("Done {0}".format(ofil_name))
    plt.close()

    return


def plt_all_sat_zonal(outfile, mean, stdv, nobs, lats, cols, sats,
                      date_label, chan_label, sel_label, zone_size, fill_value,
                      show_fig):
    """
    Plot all zonal results of all satellites into one plot.
    """
    xmin = -90
    xmax = 90
    start = xmin + (zone_size / 2.0)
    end = xmax + (zone_size / 2.0)
    major_ticks = np.arange(start, end, zone_size * 3)
    minor_ticks = np.arange(start, end, zone_size)

    (ax_val, ax_std, ax_rec) = init_pystat_plot(zonal=True)

    mean_max = list()
    stdv_max = list()
    nobs_max = list()
    mean_min = list()
    stdv_min = list()

    for pos, item in enumerate(mean):
        # mask fill_values
        mask = np.ma.equal(np.array(mean[pos]), fill_value)
        zm = np.ma.masked_where(mask, np.array(mean[pos]))
        zs = np.ma.masked_where(mask, np.array(stdv[pos]))
        zr = np.ma.masked_where(mask, np.array(nobs[pos]))
        be = np.ma.masked_where(mask, np.array(lats[pos]))
        # find max values
        mean_max.append(np.ma.max(zm))
        stdv_max.append(np.ma.max(zs))
        nobs_max.append(np.ma.max(zr))
        mean_min.append(np.ma.min(zm))
        stdv_min.append(np.ma.min(zs))
        # plot zonal mean & stdv & records
        ax_val.plot(be, zm, color=cols[pos], label=sats[pos])
        ax_std.plot(be, zs, color=cols[pos], label=sats[pos])
        ax_rec.plot(be, zr, color=cols[pos], label=sats[pos])

    # label plot
    ax_val.set_title(date_label + " AVHRR GAC L1C " + chan_label + " (" + sel_label + ")\n")
    ax_val.set_ylabel('zonal mean')
    ax_std.set_ylabel('zonal stdv')
    ax_rec.set_ylabel('# of pixels')
    ax_rec.set_xlabel('Latitude using zone size of {0} degrees'.format(zone_size))
    ax_rec.yaxis.get_major_formatter().set_powerlimits((0, 1))

    # legend
    leg = ax_val.legend(loc='best', fontsize=11)
    leg.get_frame().set_alpha(0.5)

    # set xticks and make grid
    ax_val.set_xlim(xmin, xmax)
    ax_val.set_xticks(major_ticks)
    ax_val.set_xticks(minor_ticks, minor=True)
    ax_val.set_xticklabels([])
    ax_std.set_xlim(xmin, xmax)
    ax_std.set_xticks(major_ticks)
    ax_std.set_xticks(minor_ticks, minor=True)
    ax_std.set_xticklabels([])
    ax_rec.set_xlim(xmin, xmax)
    ax_rec.set_xticks(major_ticks)
    ax_rec.set_xticks(minor_ticks, minor=True)

    # make grid
    ax_val.grid(which='major', alpha=0.9)
    ax_val.grid(which='minor', alpha=0.4)
    ax_std.grid(which='major', alpha=0.9)
    ax_std.grid(which='minor', alpha=0.4)
    ax_rec.grid(which='major', alpha=0.9)
    ax_rec.grid(which='minor', alpha=0.4)

    # save and close plotfile
    plt.savefig(outfile)
    if show_fig:
        plt.show()
        logger.info("Shown: {0} ".format(os.path.basename(outfile)))
    logger.info("Done {0}".format(outfile))
    plt.close()

    return


def plot_zonal_results(sat_list, channel, select, start_date, end_date,
                       outpath, cur, target, verbose=None, show_fig=None):
    """
    plotting daily zonal means and standard deviation.
    c. schlundt, june 2014
    """

    fill_value = -9999.0
    chan_label = subs.full_cha_name(channel)
    cnt = 0

    # -- loop over days
    for dt in rrule(DAILY, dtstart=start_date, until=end_date):

        pdate = dt.strftime("%Y-%m-%d")
        fdate = subs.date2str(dt.date())

        # lists for all in one plot
        mlist = list()  # mean
        slist = list()  # stdv
        rlist = list()  # nobs
        blist = list()  # belt
        plist = list()  # platform
        clist = list()  # color

        # -- loop over satellites
        for satellite in sat_list:

            satcolor = subs.color_satstring(satellite)
            sat_label = subs.plot_satstring(satellite)

            (datelst, meanlst, stdvlst,
             nobslst, orb_cnts_lst) = read_global_newstats(satellite, channel, select,
                                                           dt.date(), dt.date(), cur)

            if meanlst:
                global_mean = meanlst.pop()

            (zmean, zstdv, znobs, belts) = read_zonal_stats(satellite, channel, select, dt.date(), cur)

            zone_size = 180. / len(belts)

            if len(zmean) > 0:

                # ---------------------------------------------------
                # zonalall: save results for all satellites
                # ---------------------------------------------------
                mlist.append(zmean)
                slist.append(zstdv)
                rlist.append(znobs)
                blist.append(belts)
                clist.append(satcolor)
                plist.append(satellite)

                # ---------------------------------------------------
                # one plot per day/satellite/channel/select
                # ---------------------------------------------------
                if target == 'zonal':
                    # zonal histogram plot, one per satellite
                    fbase = 'Plot_ZonalResult1_' + satellite + '_' + \
                            fdate + '_' + channel + '_' + select + '.png'
                    ofile = os.path.join(outpath, fbase)

                    plt_zonal_means(np.array(zmean), np.array(znobs),
                                    global_mean, zone_size, ofile, fill_value,
                                    pdate, chan_label, sat_label, select,
                                    show_fig)

                    if verbose is True:
                        logger.info("%s done!" % ofile)

                    # latitudinal plot: one per satellite
                    fbase = 'Plot_ZonalResult2_' + satellite + '_' + \
                            fdate + '_' + channel + '_' + select + '.png'
                    ofile = os.path.join(outpath, fbase)

                    plt_zonal_mean_stdv(np.array(zmean), np.array(zstdv),
                                        np.array(znobs), np.array(belts), fill_value,
                                        zone_size, ofile, pdate, chan_label,
                                        sat_label, select, show_fig)

                    if verbose is True:
                        logger.info("%s done!" % ofile)

            # set new color for next satellite
            cnt += 1

        # -- end of loop over satellites

        # final plot
        if len(mlist) > 0:
            filebase = 'Plot_ZonalResults_ALLSAT_' + fdate + '_' + \
                       channel + '_' + select + '.png'
            outfilen = os.path.join(outpath, filebase)

            plt_all_sat_zonal(outfilen, mlist, slist, rlist, blist,
                              clist, plist, pdate, chan_label, select,
                              zone_size, fill_value, show_fig)

            if verbose is True:
                logger.info("%s done!" % outfilen)

    # -- end of loop over days

    return


def plot_avhrr_ect_results(dbfile, outdir, sdate, edate, sat_list, ignore_sats, cci_sensors,
                           primes, verbose, show_fig, make_legend):
    """
    Plot AVHRR / NOAAs equator crossing time
    """
    if verbose:
        logger.info("Plot LTAN")

    # morning satellites shift by minus 12 hours
    am_sats = ['NOAA6', 'NOAA8', 'NOAA10', 'NOAA12', 'NOAA15', 'NOAA17', 'METOPA', 'METOPB']

    # settings for CCI, primes
    base_title = "Equatorial Crossing Time of "
    if cci_sensors:
        sdate = datetime.datetime(1981, 1, 1)
        file_prefix = "Plot_Cloudcci_"
        # plt_title = base_title + "Satellites used by ESA Cloud_cci"
        if primes:
            file_prefix = "Plot_Cloudcci_primes_"
            # plt_title = base_title + "Prime Satellites used by ESA Cloud_cci"
    else:
        file_prefix = "Plot_AVHRR_"
        # plt_title = base_title + "AVHRR's on-board NOAA/MetOp Polar Satellites"

    # initialize plot
    fig = plt.figure(figsize=(15, 7))
    ax = fig.add_subplot(111)

    # output file
    ofile = file_prefix + "equat_cross_time_" + subs.date2str(sdate) + "_" + subs.date2str(edate)
    outfile = os.path.join(outdir, ofile + '.png')

    if not cci_sensors:
        txtfile = os.path.join(outdir, ofile + '.txt')
        f = open(txtfile, mode="w")
        f.write('# Equatorial Crossing Time of AVHRRs onboard NOAA and METOP satellites\n')
        f.write("# 1.Satellite  2.YearMonth 3.Local Time Ascending Node (LTAN)\n")
        f.close()

    x_title = "Date"
    y_title = "Local Time (hour)"

    # count for satellite color
    cnt = 0

    # collect first day for each satellite
    tleg_inline = list()
    xleg_inline = list()
    yleg_inline = list()
    cleg_inline = list()

    # loop over satellites
    for satellite in sat_list:

        # check if satellite is in ignore_list
        if ignore_sats and satellite in ignore_sats:
            continue

        # get color for satellite
        if primes:
            satcolor = subs.color_satstring_cci(satellite)
        else:
            satcolor = subs.color_satstring(satellite)

        # get records for satellite
        date_list, ect_list = subs.get_ect_records(satellite, dbfile, primes)
        if not date_list:
            continue

        if primes:
            cci = subs.get_cci_sensors_dict()
            zip_filtered = filter(lambda x:
                                  x[0] >= cci[satellite]["start_date"] and
                                  x[1] <= cci[satellite]["end_date"] + timedelta(days=1),
                                  zip(date_list, ect_list))
            date_list = map(lambda x: x[0], zip_filtered)
            ect_list = map(lambda x: x[1], zip_filtered)

        logger.info("{0}: {1} -- {2}".format(satellite, min(date_list), max(date_list)))

        if len(date_list) != 0:

            # midnights for ect_list
            midnights = [datetime.datetime(ect.year, ect.month, ect.day, 0, 0) for ect in ect_list]
            # convert ect_list into seconds
            seconds = [(ect - m).total_seconds() for ect, m in zip(ect_list, midnights)]

            # get dates from date_list without time
            dates = [datetime.datetime(dt.year, dt.month, dt.day, 0, 0) for dt in date_list]
            # convert dates into seconds
            date_seconds = [(dt - datetime.datetime(1970, 1, 1, 0, 0)).total_seconds() for dt in dates]

            # convert to numpy arrays
            sec_arr = array(seconds)
            dat_arr = array(date_seconds)

            # minus 12 hours if morning satellite
            if satellite in am_sats:
                logger.info("{0} is a morning satellite".format(satellite))
                sec_arr -= 12 * 60 * 60

            # collect x,y,text values for inline
            tleg_inline.append(satellite)
            xleg_inline.append(date_seconds[0])
            yleg_inline.append(sec_arr[0])
            cleg_inline.append(satcolor)

            # count number of days from unique dat_arr
            total_bins = len(set(dat_arr)) / 30
            bins = np.linspace(min(dat_arr), max(dat_arr), total_bins)
            bin_delta = bins[1] - bins[0]
            idx = np.digitize(dat_arr, bins)
            running_mean = [np.median(sec_arr[idx == k])
                            for k in range(total_bins)]

            # linewidth
            linew = 4
            lines = 'solid'
            if satellite == "ERS-2":
                linew = 6
            if satellite == "TERRA":
                linew = 3

            # plot x and y
            ax.plot(bins - bin_delta / 2., running_mean, color=satcolor,
                    ls=lines, lw=linew, alpha=.9, label=satellite)

            # ENVISAT: once for CC4CL and once for FAME-C
            if primes and satellite == "ENVISAT":
                ax.plot(bins - bin_delta / 2., running_mean, color='DarkViolet',
                        ls='dashed', lw=linew, alpha=.9)

            if not cci_sensors:
                # write monthly ect averages into txt file
                (ectmean, date_in_seconds,
                 date_as_dtobject) = subs.get_monthly_ect_averages(satellite, date_list, ect_list)
                yearmonth = [d.strftime('%Y%m') for d in date_as_dtobject]
                ect_in_seconds = [e / 3600. for e in ectmean]

                f = open(txtfile, mode="a")
                for idx, val in enumerate(ectmean):
                    line = '{0:8s}{1:8s}{2:10.4f}\n'.format(satellite, yearmonth[idx],
                                                            ect_in_seconds[idx])
                    f.write(line)
                f.close()

                # plot shows bumps
                # ax.plot(date_in_seconds, ectmean,
                #        color=satcolor, lw=4, alpha=.9, label=satellite)

        # next satellite
        cnt += 1

    # annotate plot
    # ax.set_title(plt_title, fontsize=20)
    ax.set_xlabel(x_title, fontsize=20)
    ax.set_ylabel(y_title, fontsize=20)
    ax.tick_params(axis='both', which='major', labelsize=16)
    ax.tick_params(axis='both', which='minor', labelsize=0)

    # modify y axis
    if primes:
        hour_start = 5 * 3600
        hour_end = 18 * 3600
    else:
        hour_start = 4 * 3600
        hour_end = 20 * 3600
    ax.set_ylim(hour_end, hour_start)
    major_seconds_label = range(hour_start + 3600, hour_end, 2 * 3600)
    minor_seconds_label = range(hour_start + 3600, hour_end, 3600)
    seconds_strings = [str(datetime.timedelta(seconds=s))[0:-3]
                       for s in major_seconds_label]
    ax.yaxis.set_ticks(major_seconds_label)
    ax.yaxis.set_ticklabels(seconds_strings)
    ax.yaxis.set_ticks(minor_seconds_label, minor=True)

    # modify x axis
    # -- start -- S. Finkensieper 2015-05-19
    # Calculate number of years between sdate and edate (approximately) 
    nyears = edate.year - sdate.year
    # Create datetime objects for every first day of the year 
    fdoys = [datetime.datetime(year=sdate.year + iyear, month=1, day=1) for iyear in range(nyears)]
    # fdoys2 = [fdoys[i] for i in range(1, nyears, 2)]
    # Convert dates to seconds since 1970-1-1 
    origin = datetime.datetime(1970, 1, 1)
    start_sec = int((datetime.datetime(sdate.year, sdate.month, sdate.day) - origin).total_seconds())
    end_sec = int((datetime.datetime(edate.year, edate.month, edate.day) - origin).total_seconds())
    fdoys_sec = [(fdoy - origin).total_seconds() for fdoy in fdoys]
    fdoys2_sec = [fdoys_sec[i] for i in range(1, nyears, 2)]
    # Set xaxis properties
    ax.set_xlim(start_sec, end_sec)
    fdoys_str = [fdoy.strftime('%Y-%m-%d') for fdoy in fdoys]
    fdoys2_str = [fdoys_str[i] for i in range(1, nyears, 2)]
    ax.xaxis.set_ticks(fdoys2_sec)
    ax.xaxis.set_ticklabels(fdoys2_str)
    ax.xaxis.set_ticks(fdoys_sec, minor=True)
    plt.gcf().autofmt_xdate()
    # --end -- S. Finkensieper 2015-05-19

    # set grid
    ax.grid(which='minor', alpha=0.3)
    ax.grid(which='major', alpha=0.7)

    # satellite names inline
    for cnt, satname in enumerate(tleg_inline):
        xoff, yoff = get_offset_values(satname, cci_sensors)
        plt.text(xleg_inline[cnt] + xoff,
                 yleg_inline[cnt] + yoff,
                 tleg_inline[cnt],
                 color=cleg_inline[cnt],
                 fontsize=20)

    # legend
    if make_legend:
        if cnt < 13:
            num_of_sats = int(math.ceil(cnt / 2.))
        else:
            num_of_sats = int(math.ceil(cnt / 3.))
        leg = ax.legend(ncol=num_of_sats, loc='lower left', fancybox=True, fontsize=18)
        leg.get_frame().set_alpha(0.5)

    # special legend box for CCI
    if primes:
        xmin = 0.02
        ymax = 0.95
        strings = ["AVHRR-PM", "AVHRR-AM", "MODIS-Terra", "MODIS-Aqua", "ATSR2-AATSR", "MERIS+AATSR"]
        scolors = ['Navy', 'DodgerBlue', 'DarkGreen', 'LimeGreen', 'DarkOrange', 'DarkViolet']
        # not so fancy box
        props = dict(boxstyle='round,pad=0.5', facecolor='white', alpha=1)
        ax.text(xmin, ymax, 'Cloud_cci datasets:\n\n\n\n\n\n\n\n', fontweight='bold',
                transform=ax.transAxes, fontsize=18, verticalalignment='top', bbox=props)
        # add text
        for c, i in enumerate(strings):
            ax.text(xmin, ymax - (c + 1) * 0.07, strings[c], transform=ax.transAxes, fontsize=18,
                    fontweight='bold', verticalalignment='top', color=scolors[c])

    plt.tight_layout(rect=(0.02, 0.02, 0.98, 0.98))

    # save and close plot
    # plt.savefig(outfile, bbox_inches='tight')
    plt.savefig(outfile)
    if show_fig:
        plt.show()
        logger.info("Shown: {0} ".format(os.path.basename(outfile)))
    logger.info("Done {0}".format(outfile))
    plt.close()


def plot_miss_scls(dbfile, outdir, sdate, edate, satellite, verbose, show_fig):
    """
    Plot missing scanlines per day per satellite.
    """
    # plot settings
    satcol = subs.color_satstring(satellite)
    satstr = subs.plot_satstring(satellite)
    ptitle = satstr + " / AVHRR GAC L1c"
    ytitle = "# of scan lines (dots)"
    ytitle2 = "# of missing scan lines"
    xtitle = "Date"

    tick_labelsize = 16
    plt.rcParams['xtick.labelsize'] = tick_labelsize
    plt.rcParams['ytick.labelsize'] = tick_labelsize
    title_fontsize = 20
    label_fontsize = 20

    # get data records
    (gaps, dates, counts, endline, alongtrack) = subs.get_datagaps_records(satellite, dbfile)

    if len(dates) == 0:
        logger.info("! No dates available for {0} !".format(satellite))
        return

    sd = min(dates)
    ed = max(dates)
    mindt = sd.strftime('%Y-%m-%d')
    maxdt = ed.strftime('%Y-%m-%d')
    sdstr = sd.strftime('%Y/%m/%d')
    edstr = ed.strftime('%Y/%m/%d')
    ptitle = ptitle + ' (' + sdstr + ' - ' + edstr + ')'

    a1 = min(alongtrack)
    a2 = max(alongtrack)
    astring = ': min=' + str(a1) + '; max=' + str(a2)
    c1 = min(counts)
    c2 = max(counts)
    cstring = ': min=' + str(c1) + '; max=' + str(c2)

    # convert dates into seconds
    origin = datetime.datetime(1970, 1, 1, 0, 0, 0, 0)
    seconds = [(dt - origin).total_seconds() for dt in dates]

    # initialize plot
    base = plt.figure(figsize=(14, 7))
    fig = base.add_subplot(111)

    # plot along_track dimension
    fig.plot(seconds, alongtrack, 'o', color='yellow', lw=2, alpha=0.4, markersize=4)

    # plot missing scanlines
    fig2 = fig.twinx()
    fig2.vlines(seconds, 0, counts, colors=satcol, linestyle='solid', lw=2)
    for tl in fig2.get_yticklabels():
        tl.set_color(satcol)

    # modify x labels
    nyears = ed.year + 1 - sd.year
    fdoys = [datetime.datetime(year=sd.year + iyear, month=1, day=1,
                               hour=0, minute=0, second=0, microsecond=0) for iyear in range(nyears + 1)]
    fdoys_sec = [(fdoy - origin).total_seconds() for fdoy in fdoys]
    fdoys_str = [fdoy.strftime('%Y-%m-%d') for fdoy in fdoys]
    minor = [datetime.datetime(year=sd.year + iyear, month=7, day=1,
                               hour=0, minute=0, second=0, microsecond=0) for iyear in range(nyears + 1)]
    minor_sec = [(m - origin).total_seconds() for m in minor]
    start_sec = int((datetime.datetime(sd.year, 1, 1, 0, 0, 0, 0) - origin).total_seconds())
    end_sec = int((datetime.datetime(ed.year + 1, 1, 1, 0, 0, 0, 0) - origin).total_seconds())

    # limits
    fig.xaxis.set_ticks(fdoys_sec)
    fig.xaxis.set_ticklabels(fdoys_str)
    fig.xaxis.set_ticks(minor_sec, minor=True)
    fig.set_ylim(0.0, 15000.0)
    fig2.set_ylim(0.0, 15000.0)
    fig.set_xlim(start_sec, end_sec)
    plt.gcf().autofmt_xdate()

    # annotate plot
    fig.set_title(ptitle, fontsize=title_fontsize)
    fig.set_xlabel(xtitle, fontsize=label_fontsize)
    fig.set_ylabel(ytitle, fontsize=label_fontsize)
    fig2.set_ylabel(ytitle2, fontsize=label_fontsize, color=satcol)

    # set grid
    fig.grid(which='minor', alpha=0.3)
    fig.grid(which='major', alpha=0.7)

    # png settings
    dtstr = mindt + '_' + maxdt + '_'
    fbase = 'Plot_missing_scanlines_' + dtstr + satellite + '.png'
    ofile = os.path.join(outdir, fbase)

    # save and show figure
    plt.savefig(ofile)
    if show_fig:
        plt.show()
    logger.info("Done: {0}".format(ofile))
    plt.close()

    return


def autolabel(rects, ax, fts, fmt, offset=None, counts=None, colors=None):
    for idx, rect in enumerate(rects):
        height = rect.get_height()
        if counts:
            axhour_start.text(rect.get_x() + rect.get_width() / 2.,
                              height + offset, fmt % int(counts[idx]),
                              ha='center', va='bottom', fontsize=fts,
                              color=colors[idx])
        else:
            ax.text(rect.get_x() + rect.get_width() / 2.,
                    offset * height, fmt % float(height),
                    ha='center', va='bottom', fontsize=fts)


def blacklisting_histogram(x_cnts, x_axis, y_axis, colors, width, outfile,
                           satellite=None):
    """
    Plot the statistics regarding blacklisting.
    """
    if 'total' in outfile:
        rotate = 0
        fts = 22
        tick_labelsize = 22
        ymax = int(math.ceil(max(y_axis) + 0.5))
        bar_format = '%.2f'
        off_text = 0.5
    else:
        rotate = 90
        fts = 16
        tick_labelsize = 16
        bar_format = '%.3f'
        off_text = 0.15
        ymax = 2.0
        if 'pre' in outfile:
            ymax = 1.4
            off_text = 0.1
        if 'proc' in outfile:
            ymax = 0.5
            off_text = 0.05

    plt.rcParams['ytick.labelsize'] = tick_labelsize
    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(111)
    ytitle = 'AVHRR GAC Blacklisting [%]'

    pos = np.arange(len(x_axis))
    rects = ax.bar(pos, y_axis, width, color=colors)
    autolabel(rects, ax, fts, bar_format, offset=1.01)
    autolabel(rects, ax, fts, '%d', offset=off_text,
              counts=x_cnts, colors=colors)

    if satellite:
        ax.text(0.1, 0.9, satellite, ha='center', va='center',
                transform=ax.transAxes, fontsize=fts)

    ax.set_xlim(-width / 2, len(pos) + width / 2)
    ax.set_ylim(0.0, ymax)
    ax.set_ylabel(ytitle, fontsize=fts)
    ax.set_xticks(pos + (width / 2))
    ax.yaxis.set_major_formatter(FormatStrFormatter('%.1f'))

    xtickNames = ax.set_xticklabels(x_axis)
    plt.setp(xtickNames, rotation=rotate, fontsize=fts)
    plt.tight_layout(rect=(0.02, 0.02, 0.98, 0.98))

    plt.savefig(outfile)
    # plt.show()
    logger.info("Shown: {0} ".format(os.path.basename(outfile)))
    logger.info("Done {0}".format(outfile))
    plt.close()

    return


def get_offset_values(satname, cci=None):
    """
    return x_offset, y_offset
    """
    year_fac = 12. * 31. * 24. * 3600.
    hour_fac = 60. * 60.

    # afternoon satellites
    if satname == "NOAA7":
        return -0.3 * year_fac, -0.3 * hour_fac
    elif satname == "NOAA9":
        return -0.3 * year_fac, -0.3 * hour_fac
    elif satname == "NOAA11":
        return -0.3 * year_fac, -0.3 * hour_fac
    elif satname == "NOAA14":
        return -0.3 * year_fac, -0.3 * hour_fac
    elif satname == "NOAA16":
        return -0.3 * year_fac, 1.0 * hour_fac
    elif satname == "NOAA18":
        return -0.3 * year_fac, -0.7 * hour_fac
    elif satname == "NOAA19":
        return 1.5 * year_fac, -0.7 * hour_fac
    elif satname == "AQUA":
        return -0.3 * year_fac, -0.3 * hour_fac
    # morning satellites
    elif satname == "ENVISAT":
        return 10.0 * year_fac, +0.3 * hour_fac
    elif satname == "ERS-2":
        return 0.0 * year_fac, +1.0 * hour_fac
    elif satname == "TERRA":
        return 0.0 * year_fac, +1.0 * hour_fac
    elif satname == "NOAA6":
        return -0.5 * year_fac, -0.3 * hour_fac
    elif satname == "NOAA8":
        return -0.5 * year_fac, -0.3 * hour_fac
    elif satname == "NOAA10":
        return -0.1 * year_fac, -0.3 * hour_fac
    elif satname == "NOAA12":
        return +1.5 * year_fac, -0.9 * hour_fac
    elif satname == "NOAA15":
        return +0.0 * year_fac, +1.0 * hour_fac
    elif satname == "NOAA17":
        return -0.5 * year_fac, -0.3 * hour_fac
    elif satname == "METOPA" and cci is False:
        return 0. * year_fac, -0.6 * hour_fac
    elif satname == "METOPA" and cci is True:
        return 4.3 * year_fac, -0.3 * hour_fac
    elif satname == "METOPB":
        return 0. * year_fac, -0.3 * hour_fac
    else:
        return 0., 0.


def calc_date_formatter(delta_days, date_label):
    nyears = int(delta_days / 365)

    # modify x axis
    alldays = DayLocator()
    allyears = YearLocator()
    allmonths = MonthLocator()
    mondays = WeekdayLocator(MONDAY)
    yearsFormatter = DateFormatter('%Y-%m-%d')
    # monthsFormatter = DateFormatter('%b %Y')
    # weekFormatter = DateFormatter("%b %d '%y")
    # dayFormatter = DateFormatter('%d')

    if delta_days <= 90:
        minor_loc = alldays
        major_loc = mondays
        major_loc_str = " (major ticks: Mondays)"
        date_label += major_loc_str
        # major_fmt = weekFormatter
        major_fmt = yearsFormatter
    elif delta_days < 90 or nyears <= 1:
        minor_loc = mondays
        minor_loc_str = " (minor ticks: Mondays)"
        date_label += minor_loc_str
        major_loc = allmonths
        major_fmt = yearsFormatter
    elif 1 < nyears <= 5:
        minor_loc = allmonths
        major_loc = MonthLocator(range(1, 13), bymonthday=1, interval=3)
        major_fmt = yearsFormatter
    elif 5 < nyears <= 8:
        minor_loc = allmonths
        major_loc = MonthLocator(range(1, 13), bymonthday=1, interval=6)
        major_fmt = yearsFormatter
    elif 8 < nyears <= 12:
        minor_loc = allmonths
        major_loc = allyears
        major_fmt = yearsFormatter
    elif 12 < nyears <= 16:
        minor_loc = YearLocator(1, month=7, day=1)
        major_loc = allyears
        major_fmt = yearsFormatter
    else:
        minor_loc = allyears
        major_loc = YearLocator(2, month=1, day=1)
        major_fmt = yearsFormatter

    return minor_loc, major_loc, major_fmt, date_label


def get_pystat_labels(channel):

    chan_label = subs.full_cha_name(channel)
    nobs_label = "# of pixels"
    date_label = "Date"
    orbit_label = "orbits/day"

    if channel == "ch1" or channel == "ch2" or channel == "ch3a":
        mean_label = "mean"
        stdv_label = "stddev"
    else:
        mean_label = "mean [K]"
        stdv_label = "stddev [K]"

    return chan_label, nobs_label, date_label, orbit_label, mean_label, stdv_label


def init_pystat_plot(zonal=None):

    if zonal:
        fig = init_pystat_zonal_figure()
    else:
        fig = init_pystat_global_figure()

    ave = fig.add_subplot(311)
    std = fig.add_subplot(312)
    rec = fig.add_subplot(313)

    return ave, std, rec


def init_pystat_global_figure():

    plt.rcParams['font.size'] = 20.0
    plt.rcParams['legend.fontsize'] = 14
    plt.rcParams['axes.titlesize'] = 20
    plt.rcParams['axes.labelsize'] = 18
    plt.rcParams['xtick.labelsize'] = 16
    plt.rcParams['ytick.labelsize'] = 16
    plt.rcParams['lines.linewidth'] = 2

    return plt.figure(figsize=(17, 9))


def init_pystat_zonal_figure():

    plt.rcParams['font.size'] = 18.0
    plt.rcParams['legend.fontsize'] = 14
    plt.rcParams['axes.titlesize'] = 16
    plt.rcParams['axes.labelsize'] = 18
    plt.rcParams['xtick.labelsize'] = 14
    plt.rcParams['ytick.labelsize'] = 14
    plt.rcParams['lines.linewidth'] = 2

    return plt.figure(figsize=(12, 8))


def label_pystat_plot(ave, std, obs, min_x_date, max_x_date, delta_days,
                      plt_label, dat_label, ave_label, std_label, obs_label):

    # x axis range
    ave.set_xlim(min_x_date, max_x_date)
    std.set_xlim(min_x_date, max_x_date)
    obs.set_xlim(min_x_date, max_x_date)

    # y axis range
    obs.yaxis.get_major_formatter().set_powerlimits((0, 1))
    ymin, ymax = obs.get_ylim()
    obs.set_ylim(0, ymax + ymax * 0.05)

    # modify x axis
    (minor_loc, major_loc, major_fmt, date_label) = calc_date_formatter(delta_days, dat_label)
    ave.xaxis.set_major_locator(major_loc)
    std.xaxis.set_major_locator(major_loc)
    obs.xaxis.set_major_locator(major_loc)
    ave.xaxis.set_minor_locator(minor_loc)
    std.xaxis.set_minor_locator(minor_loc)
    obs.xaxis.set_minor_locator(minor_loc)
    ave.xaxis.set_major_formatter(major_fmt)
    std.xaxis.set_major_formatter(major_fmt)
    obs.xaxis.set_major_formatter(major_fmt)
    plt.gcf().autofmt_xdate(rotation=20)

    # label axes
    ave.set_title(plt_label)
    ave.set_ylabel(ave_label)
    std.set_ylabel(std_label)
    obs.set_ylabel(obs_label)
    obs.set_xlabel(dat_label)

    # make grid
    ave.grid(which='major', alpha=0.9)
    ave.grid(which='minor', alpha=0.4)
    std.grid(which='major', alpha=0.9)
    std.grid(which='minor', alpha=0.4)
    obs.grid(which='major', alpha=0.9)
    obs.grid(which='minor', alpha=0.4)

    return


def plot_orbits_per_day(sub_obs, satellite, orb_cnts_lst, datelst, orbit_label):

    if satellite == 'NOAA15':
        satcol = 'b'
    else:
        satcol = 'r'

    max_cnts = max(orb_cnts_lst)

    ax2 = sub_obs.twinx()
    ax2.plot(datelst, orb_cnts_lst, '--o', color=satcol, linewidth=1.5, markersize=5, alpha=0.8)
    ax2.set_ylabel(orbit_label, color=satcol, fontsize=20)
    ax2.set_ylim(0, max_cnts + 10)
    ax2.grid(which='major', alpha=0.8, color=satcol)

    for tl in ax2.get_yticklabels():
        tl.set_color(satcol)

    return


def get_channel_difference_plot_title(label1, label2, selected_time, channel_list):
    """
    Create plot title.
    :param label1: full channel 1 name
    :param label2: full channel 2 name
    :param selected_time: time w.r.t. sza
    :param channel_list: list of channels passed via arguments
    :return:
    """
    ch_list = label1.split(" ") + label2.split(" ")
    if len(set(ch_list)) > 4:
        check = ' - '.join(channel_list)
        logger.info(" --- Check your --channels arguments! Are you sure about this: {0}".format(check))
        return

    ch1_name = label1.split(" ")[0]
    ch2_name = label2.split(" ")[0]
    c_suffix = ' '.join(label1.split(" ")[1:])
    p_title = "AVHRR GAC daily global statistics: " + r'$\Delta$' + \
              "(" + ch1_name + "," + ch2_name + ") " + c_suffix + " (" + selected_time + ")\n"

    return p_title, ch1_name, ch2_name


def collocte_pystat_lists(d1, m1, s1, n1,
                          d2, m2, s2, n2):
    """
    Returns collocated lists (1 and 2) for calculating pystat channel differences.
    d = list of date
    m = list of mean values
    s = list of standard deviation values
    n = list of number of observations
    :return:
    """
    dates = list()
    nobs1 = list()
    nobs2 = list()
    mean1 = list()
    mean2 = list()
    stdv1 = list()
    stdv2 = list()

    if len(d1) > len(d2):

        for i1, date1 in enumerate(d1):
            for i2, date2 in enumerate(d2):
                if date1 == date2:
                    dates.append(date1)
                    # list 1
                    nobs1.append(n1[i1])
                    mean1.append(m1[i1])
                    stdv1.append(s1[i1])
                    # list 2
                    nobs2.append(n2[i2])
                    mean2.append(m2[i2])
                    stdv2.append(s2[i2])

        return dates, mean1, mean2, stdv1, stdv2, nobs1, nobs2

    else:
        for i2, date2 in enumerate(d2):
            for i1, date1 in enumerate(d1):
                if date1 == date2:
                    dates.append(date1)
                    # list 1
                    nobs1.append(n1[i1])
                    mean1.append(m1[i1])
                    stdv1.append(s1[i1])
                    # list 2
                    nobs2.append(n2[i2])
                    mean2.append(m2[i2])
                    stdv2.append(s2[i2])

        return dates, mean1, mean2, stdv1, stdv2, nobs1, nobs2
