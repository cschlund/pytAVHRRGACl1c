#
# subroutines for plotting AVHRR GAC L1c data (input = sql db)
#

import os, sys
import numpy as np
import datetime
import matplotlib.pyplot as plt
import subs_avhrrgac as mysub
from scipy import stats
from matplotlib import gridspec
from mpl_toolkits.axes_grid1 import host_subplot
import mpl_toolkits.axisartist as AA
from dateutil.rrule import rrule, DAILY
  
# -------------------------------------------------------------------
def get_id(table, column, value, sql):
    """
    Get value from a table and column.
    """
    get = "SELECT {0} FROM {1} "\
          "WHERE name = \'{2}\'".format(column, table, value)

    res = sql.execute(get)

    for item in res:
        str_id = item["id"]

    return str_id

# -------------------------------------------------------------------
def get_lat_belts( table, sql):
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

    return (idx_list, val_list)

# -------------------------------------------------------------------
def read_zonal_stats( sat, cha, sel, dt, sql ):
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
        mean_str.append(zonal_list[0]+str(idx))
        stdv_str.append(zonal_list[1]+str(idx))
        nobs_str.append(zonal_list[2]+str(idx))

    full_list = mean_str + stdv_str + nobs_str
    get_cols  = ', '.join(full_list)
    sql_query = "SELECT {0} FROM statistics WHERE "\
                "satelliteID={1} AND channelID={2} "\
                "AND selectID={3} AND "\
                "date=\'{4}\'".format( get_cols,
                        sat_id, cha_id, sel_id, dt)
    results   = sql.execute(sql_query)

    for item in results:
        for i in full_list:
            if i.startswith(zonal_list[0]):
                mean_list.append(item[i]) 
            if i.startswith(zonal_list[1]):
                stdv_list.append(item[i]) 
            if i.startswith(zonal_list[2]):
                nobs_list.append(item[i]) 

    return (mean_list, stdv_list, nobs_list, lats)

# -------------------------------------------------------------------
def read_global_stats( sat, cha, sel, sd, ed, sql ):
    """
    Read sqlite database (sql): 
    return global statistics for a given satellite (sat), 
    channel (cha), time selection (sel) between 
    start_date (sd) and end_date (ed).
    """

    glob_list  = ("GlobalMean", "GlobalStdv", "GlobalNobs")

    mean_list = list() 
    stdv_list = list()
    nobs_list = list()
    date_list = list()

    sat_id = get_id("satellites", "id", sat, sql)
    cha_id = get_id("channels", "id", cha, sql)
    sel_id = get_id("selects", "id", sel, sql)

    get_data = "SELECT date, {0}, {1}, {2} "\
               "FROM statistics WHERE satelliteID={3} AND "\
               "channelID={4} AND selectID={5} AND "\
               "date>=\'{6}\' AND date<=\'{7}\' "\
               "ORDER BY date".format(glob_list[0],
                       glob_list[1], glob_list[2],
                       sat_id, cha_id, sel_id, sd, ed)
    #print get_data
    results = sql.execute(get_data)

    for result in results: 
        if result['date'] != None: 
            date_list.append(result['date']) 
            mean_list.append(result[glob_list[0]]) 
            stdv_list.append(result[glob_list[1]])
            nobs_list.append(result[glob_list[2]])
        else:
            return None

    return (date_list, mean_list, stdv_list, nobs_list)

# -------------------------------------------------------------------
def plot_time_series(sat_list, channel, select, start_date,
                     end_date, outpath, cursor, verbose, ascinpdir): 
    """
    Plot time series based on pystat results.
    """

    isdata_cnt = 0
    chan_label = mysub.full_cha_name(channel)
    plot_label = "AVHRR GAC L1C Time Series (MODc6 calib.): "+\
                 chan_label+" ("+select+")\n"
    mean_label = "Global Mean\n"
    stdv_label = "Standard Deviation\n"
    nobs_label = "# of Observations\n"
    date_label = "\nTime"

    color_list = mysub.get_color_list()
    cnt = 0
    lwd = 2

    sdate = mysub.date2str(start_date)
    edate = mysub.date2str(end_date)
    if len(sat_list) == 1: 
        sname = mysub.full_sat_name(sat_list[0])[2] 
        slist = mysub.get_satellite_list()
        colid = slist.index(sname)
        cnt = colid
        fbase = 'Plot_TimeSeries_'+sdate+'_'+edate+\
                '_'+channel+'_'+select+'_'+\
                sname+'.png'
    else:
        fbase = 'Plot_TimeSeries_'+sdate+'_'+edate+\
                '_'+channel+'_'+select+'.png'
    ofile = os.path.join( outpath, fbase)

    fig    = plt.figure()
    ax_val = fig.add_subplot(311)
    ax_std = fig.add_subplot(312)
    ax_rec = fig.add_subplot(313)


    # -- loop over satellites
    for satellite in sat_list: 


        # if ascii files inpdir is given
        if ascinpdir != None: 
            satname = mysub.full_sat_name(satellite)[1] 
            ifile   = os.path.join( ascinpdir, 
                      "Global_statistics_AVHRRGACl1c_"+satname+".txt" )

            if os.path.isfile(ifile) == True: 
                (asc_datelst,asc_meanlst,
                 asc_stdvlst,asc_nobslst) = read_globstafile(ifile, 
                         channel, select, start_date, end_date)


        ( datelst, meanlst, stdvlst, nobslst ) = read_global_stats( 
                satellite, channel, select, start_date, end_date, 
                cursor )

        if not datelst:
            pass
        else: 
            if len(datelst) > 10:
                isdata_cnt += 1

                # date vs. global mean
                #ax_val.plot(datelst, meanlst, 'o', color=color_list[cnt])
                ax_val.plot(datelst, meanlst, label=satellite, 
                            color=color_list[cnt], linewidth=lwd)
                if ascinpdir != None and len(asc_datelst) > 10: 
                    ax_val.plot(asc_datelst, asc_meanlst, 'o', 
                            color=color_list[cnt], linewidth=lwd)

                # date vs. global stdv
                #ax_std.plot(datelst, stdvlst, 'o', color=color_list[cnt])
                ax_std.plot(datelst, stdvlst, label=satellite, 
                            color=color_list[cnt], linewidth=lwd)
                if ascinpdir != None and len(asc_datelst) > 10: 
                    ax_std.plot(asc_datelst, asc_stdvlst, 'o', 
                            color=color_list[cnt], linewidth=lwd)
                
                # date vs. global nobs
                #ax_rec.plot(datelst, nobslst, 'o', color=color_list[cnt])
                ax_rec.plot(datelst, nobslst, label=satellite, 
                            color=color_list[cnt], linewidth=lwd)
                if ascinpdir != None and len(asc_datelst) > 10: 
                    ax_rec.plot(asc_datelst, asc_nobslst, 'o', 
                            color=color_list[cnt], linewidth=lwd)

        # set new color for next satellite
        cnt += 1

    # -- end ofloop over satellites

    if isdata_cnt > 0:
        # label axes
        ax_val.set_title(plot_label)
        ax_val.set_ylabel(mean_label)
        ax_std.set_ylabel(stdv_label)
        ax_rec.set_ylabel(nobs_label)
        ax_rec.set_xlabel(date_label)

        # beautify the x-labels
        plt.gcf().autofmt_xdate()

        # make grid
        ax_val.grid()
        ax_std.grid()
        ax_rec.grid()

        # make legend
        if cnt > 2:
            leg = ax_val.legend(bbox_to_anchor=(1.125, 1.05), 
                                fontsize=11)
        else:
            plt.tight_layout()
            leg = ax_val.legend(loc='best', fancybox=True)

        leg.get_frame().set_alpha(0.5)

        # save figure
        plt.savefig(ofile)
        plt.close()

        if verbose == True: 
            print ("   + %s done!" % ofile)
    else:
        plt.close()

    return

# -------------------------------------------------------------------
def plot_time_series_linfit(sat_list, channel, select, start_date,
                     end_date, outpath, cursor, verbose): 
    """
    Plot Time Series based on pystat results for each satellite
    including linear regression.
    """

    if select is 'twilight':
        min_nobs = 0.4e7
    else:
        min_nobs = 2.5e7

    sdate = mysub.date2str(start_date)
    edate = mysub.date2str(end_date)

    chan_label = mysub.full_cha_name(channel)
    mean_label = "Global Mean\n"
    stdv_label = "Standard Deviation\n"
    nobs_label = "# of Observations\n"
    date_label = "\nTime"

    color_list = mysub.get_color_list()
    cnt = 0
    lwd = 2


    # -- loop over satellites
    for satellite in sat_list: 

        ( datelst, meanlst, stdvlst, nobslst ) = read_global_stats( 
                satellite, channel, select, start_date, end_date, 
                cursor )

        if not datelst:
            #if verbose == True:
            #    print ("     ! No data record found for %s %s %s" %
            #            (satellite, channel, select))
            pass
        else: 
            if len(datelst) > 10:
                #if verbose == True: 
                #    print ("     - %s records found for %s %s %s" % 
                #            (len(datelst), satellite, channel, select)) 

                plot_label = "AVHRR GAC L1C time series: "+\
                             satellite+' '+chan_label+" ("+select+")\n"
                prefix = "Plot_TimeSeries_LinFit_"
                fbase  = prefix+sdate+'_'+edate+'_'+satellite+\
                         '_'+channel+'_'+select+'.png'
                ofile  = os.path.join( outpath, fbase)

                # initialize plot
                fig    = plt.figure()
                ax_val = fig.add_subplot(311)
                ax_std = fig.add_subplot(312)
                ax_rec = fig.add_subplot(313)
                
                # list to array
                ave = np.asarray(meanlst)
                std = np.asarray(stdvlst)

                # convert date list to a set of numbers counting 
                # the number of days having passed from the first 
                # day of the file
                x = [(e - min(datelst)).days for e in datelst] 

                # linear regression
                (slope, intercept, r_value, 
                 p_value, std_err) = stats.linregress(x, ave)
                (slope2, intercept2, r_value2,
                 p_value2, std_err2) = stats.linregress(x, std)
                yp  = np.polyval([slope,intercept],x)
                yp2 = np.polyval([slope2,intercept2],x)

                # plot data and linfit
                # date vs. global mean
                #ax_val.plot(datelst, meanlst, 'o', color='DarkGreen')
                ax_val.plot(datelst, meanlst, color='DarkGreen', linewidth=lwd)
                ax_val.plot(datelst,yp, '--', color='Red',
                            label="Linear fit: y = %.5f * x + %.5f" % 
                            (slope, intercept), lw=lwd)
                # date vs. global stdv
                #ax_std.plot(datelst, stdvlst, 'o', color='DarkBlue')
                ax_std.plot(datelst, stdvlst, color='DarkBlue', linewidth=lwd)
                ax_std.plot(datelst,yp2, '--', color='Red',
                            label="Linear fit: y = %.5f * x + %.5f" % 
                            (slope2, intercept2), lw=lwd)
                # date vs. global nobs
                #ax_rec.plot(datelst, nobslst, 'o', color='DimGray')
                ax_rec.plot(datelst, nobslst, color='DimGray', linewidth=lwd)

                # label axes
                ax_val.set_title(plot_label)

                ax_val.set_ylabel(mean_label)
                leg = ax_val.legend(loc='best', fancybox=True)
                leg.get_frame().set_alpha(0.5)

                ax_std.set_ylabel(stdv_label)
                leg = ax_std.legend(loc='best', fancybox=True)
                leg.get_frame().set_alpha(0.5)

                ax_rec.set_ylabel(nobs_label)
                ax_rec.set_xlabel(date_label)

                # beautify the x-labels
                plt.gcf().autofmt_xdate()

                # make grid
                ax_val.grid()
                ax_std.grid()
                ax_rec.grid()

                # save figure
                plt.tight_layout()
                plt.savefig(ofile)
                plt.close()

                if verbose == True: 
                    print ("   + %s done!" % ofile)
    # -- end ofloop over satellites
    return

# -------------------------------------------------------------------
def plt_zonal_means(zonal_mean, zonal_nobs, global_mean, zone_size, 
        ofil_name, fill_value, date_str, chan_str, plat_str, 
        sel_str):
    """
    plot global and zonal means.
    s. finkensieper, july 2014
    """

    # set_xlim
    xmin     = -90
    xmax     = 90
    # xaxis.set_tick
    start    = xmin+(zone_size/2.0)
    end      = xmax+(zone_size/2.0)
    major_ticks = np.arange(start, end, zone_size*3)
    minor_ticks = np.arange(start, end, zone_size)

    plot_title = date_str+" AVHRR/"+plat_str+" GAC L1C "+\
                 chan_str+" ("+sel_str+")\n"
    
    glo_mask = np.ma.equal(global_mean, fill_value)
    zon_mask = np.ma.equal(zonal_mean, fill_value)
    
    glm = np.ma.masked_where(glo_mask, global_mean)
    
    zonal_means = np.ma.masked_where(zon_mask, zonal_mean)
    nobs = np.ma.masked_where(zon_mask, zonal_nobs)

    if np.ma.count(zonal_means) == 0:
        return 
    
    # define latitudinal zone size :
    zone_rad = zone_size/2.0

    # determine zone centers:
    zone_centers = np.arange(-90 + zone_rad, 90 + zone_rad, zone_size)
    nzones = len(zone_centers)

    # create one host axes object and one child axes object which share their
    # x-axis, but have individual y-axes:
    ax_means = host_subplot(111, axes_class=AA.Axes)
    ax_nobs = ax_means.twinx()

    # plot zonal mean into the host axes:
    width = zone_rad*0.5
    ax_means.bar(zone_centers, zonal_means, 
            label='Zonal Mean using lat. zone size of {0} degrees'.format(zone_size),
            width=width, color='darkorange')
    ax_means.set_xlim(xmin,xmax)
    ax_means.set_xticks(major_ticks)
    ax_means.set_xticks(minor_ticks, minor=True)

    # plot number of observations into the child axes:
    ax_nobs.bar(zone_centers + width, nobs, 
            label='# of Observations (total: '+format(int(nobs.sum()))+')',
            width=width, color='g')

    # plot global mean on top of them:
    ax_means.plot(zone_centers, np.ma.ones(nzones)*glm, 'b--', lw=2.0,
      	label='Global Mean: '+format('%.4f' % glm))

    # set axes labels:
    ax_means.set_ylabel('Zonal Mean')
    ax_nobs.set_ylabel('# of Observations')
    ax_means.set_xlabel('Latitudinal Zone Center [degrees]')

    # set axes range:
    ax_means.set_ylim(0, 1.2*np.ma.max(zonal_means))
    ax_nobs.set_ylim(0, 1.2*np.ma.max(nobs))

    # add title & legend:
    plt.title(plot_title)
    plt.legend(loc='upper center')

    # ensure 'tight layout' (prevents the axes labels from being placed outside
    # the figure):
    plt.tight_layout()

    # save figure to file:
    #plt.savefig('zonal_means.png', bbox_inches='tight')
    with np.errstate(all='ignore'): 
        plt.savefig(ofil_name)
        plt.close()
    return

# -------------------------------------------------------------------
def plt_zonal_mean_stdv(zonal_mean, zonal_stdv, zonal_nobs, 
        zone_centers, fill_value, zone_size, ofil_name, 
        date_str, chan_str, plat_str, sel_str):
    """
    plot zonal means and standard deviations.
    """

    plot_title = date_str+" AVHRR/"+plat_str+" GAC L1C "+\
                 chan_str+" ("+sel_str+")\n"

    # set_xlim
    xmin     = -90
    xmax     = 90
    # xaxis.set_tick
    start    = xmin+(zone_size/2.0)
    end      = xmax+(zone_size/2.0)
    major_ticks = np.arange(start, end, zone_size*3)
    minor_ticks = np.arange(start, end, zone_size)

    zon_mask = np.ma.equal(zonal_mean, fill_value)
    
    avearr = np.ma.masked_where(zon_mask, zonal_mean)
    devarr = np.ma.masked_where(zon_mask, zonal_stdv)
    cntarr = np.ma.masked_where(zon_mask, zonal_nobs)
    belarr = np.ma.masked_where(zon_mask, zone_centers)
    
    xlabel = 'Latitude using zone size of {0} degrees'.format(zone_size)
    mean_label = 'Zonal Mean'
    stdv_label = 'Zonal Standard Deviation'
    
    if np.ma.count(avearr) == 0:
        return 
    
    fig = plt.figure()
    gs = gridspec.GridSpec(2, 1, height_ratios=[3,1])
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
    ax_val.set_xlim(xmin,xmax)
    ax_val.set_xticks(major_ticks)
    ax_val.set_xticks(minor_ticks, minor=True)
    ax_val.set_title(plot_title)
    ax_val.set_ylabel('Zonal Mean and Standard Deviation')
    ax_val.grid(which='both')
    #ax_val.grid(which='minor', alpha=0.2)
    #ax_val.grid(which='major', alpha=0.5)

    # plot number of observations / lat. zone
    ax_rec.plot(belarr, cntarr, 'o', color='black')
    ax_rec.plot(belarr, cntarr, color='black', linewidth=2, 
            label='total records = '+format(allcnt))
    ax_rec.set_xlim(xmin,xmax)
    ax_rec.set_xticks(major_ticks)
    ax_rec.set_xticks(minor_ticks, minor=True)
    ax_rec.set_ylabel('# of Observations')
    ax_rec.set_xlabel(xlabel)
    ax_rec.grid(which='both')

    # set axes range:
    ax_rec.set_ylim(0, 1.1*maxcnt)

    # plot legend for mean & stdv
    m = plt.Line2D((0,1), (1,1), c='red', lw=2)
    s = plt.Rectangle((0,0), 1, 1, fc='SkyBlue')
    leg = ax_val.legend([m, s], [mean_label, stdv_label], 
            loc='best', fancybox=True)
    leg.get_frame().set_alpha(0.5)
    
    # plot legend for observations
    leg2 = ax_rec.legend(loc='upper center', fancybox=True)
    leg.get_frame().set_alpha(0.5)
    
    # ensure 'tight layout' (prevents the axes labels from being 
    # placed outside the figure):
    plt.tight_layout()
    plt.savefig(ofil_name)
    plt.close()

    return

# -------------------------------------------------------------------
def plt_all_sat_zonal(outfile, mean, stdv, nobs, lats, cols, sats,
        date_label, chan_label, sel_label, zone_size, fill_value):
    """
    Plot all zonal results of all satellites into one plot.
    """

    plot_title  = date_label+" AVHRR GAC L1C "+\
                  chan_label+" ("+sel_label+")\n"
    mean_ytitle = 'Zonal Mean\n'
    stdv_ytitle = 'Zonal Standard Deviation\n'
    nobs_ytitle = '# of Observations\n'
    nobs_xtitle = '\nLatitude using zone size of {0} degrees'.\
                  format(zone_size)

    xmin     = -90
    xmax     = 90
    start    = xmin+(zone_size/2.0)
    end      = xmax+(zone_size/2.0)
    major_ticks = np.arange(start, end, zone_size*3)
    minor_ticks = np.arange(start, end, zone_size)

    fig    = plt.figure()
    ax_val = fig.add_subplot(311)
    ax_std = fig.add_subplot(312)
    ax_rec = fig.add_subplot(313)

    mean_max = list()
    stdv_max = list()
    nobs_max = list()
    mean_min = list()
    stdv_min = list()

    for pos,item in enumerate(mean):
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
        ax_val.plot(be, zm, color=cols[pos], lw=2,
                label=sats[pos])
        ax_std.plot(be, zs, color=cols[pos], lw=2,
                label=sats[pos])
        ax_rec.plot(be, zr, color=cols[pos], lw=2,
                label=sats[pos])

    # label plot
    ax_val.set_title(plot_title)
    ax_val.set_ylabel(mean_ytitle)
    ax_std.set_ylabel(stdv_ytitle)
    ax_rec.set_ylabel(nobs_ytitle)
    ax_rec.set_xlabel(nobs_xtitle)

    # legend
    leg = ax_val.legend(bbox_to_anchor=(1.125, 1.05),
            fontsize=11)
    leg.get_frame().set_alpha(0.5)

    # set xticks and make grid
    ax_val.set_xlim(xmin,xmax)
    ax_val.set_xticks(major_ticks)
    ax_val.set_xticks(minor_ticks, minor=True)
    ax_val.grid(which='both')
    ax_std.set_xlim(xmin,xmax)
    ax_std.set_xticks(major_ticks)
    ax_std.set_xticks(minor_ticks, minor=True)
    ax_std.grid(which='both')
    ax_rec.set_xlim(xmin,xmax)
    ax_rec.set_xticks(major_ticks)
    ax_rec.set_xticks(minor_ticks, minor=True)
    ax_rec.grid(which='both')
    
    # save and close plotfile
    #plt.show()
    plt.savefig(outfile)
    plt.close() 

    return

# -------------------------------------------------------------------
def plot_zonal_results(sat_list, channel, select, start_date, 
        end_date, outpath, cur, target, verbose):
    """
    plotting daily zonal means and standard deviation.
    c. schlundt, june 2014
    """

    fill_value = -9999.0
    chan_label = mysub.full_cha_name(channel)
    color_list = mysub.get_color_list()
    cnt = 0

    # -- loop over days
    for dt in rrule(DAILY, dtstart=start_date, until=end_date): 

        pdate = dt.strftime("%Y-%m-%d")
        fdate = mysub.date2str(dt.date())

        # lists for all in one plot
        mlist = list() #mean
        slist = list() #stdv
        rlist = list() #nobs
        blist = list() #belt
        plist = list() #platform
        clist = list() #color

        # -- loop over satellites
        for satellite in sat_list: 

            sat_label = mysub.full_sat_name(satellite)[0]

            ( datelst, meanlst, 
              stdvlst, nobslst ) = read_global_stats( satellite, 
                                    channel, select, dt.date(), 
                                    dt.date(), cur )
            if meanlst: 
                global_mean = meanlst.pop()

            ( zmean, zstdv, 
              znobs, belts ) = read_zonal_stats( satellite, 
                                channel, select, dt.date(), cur )
        
            zone_size = 180./len(belts)


            if len(zmean) > 0:
                
                # ---------------------------------------------------
                # zonalall: save results for all satellites
                # ---------------------------------------------------
                mlist.append(zmean)
                slist.append(zstdv)
                rlist.append(znobs)
                blist.append(belts)
                clist.append(color_list[cnt])
                plist.append(satellite)

                # ---------------------------------------------------
                # one plot per day/satellite/channel/select
                # ---------------------------------------------------
                if target == 'zonal':
                    # zonal histogram plot, one per satellite
                    fbase = 'Plot_ZonalResult1_'+satellite+'_'+\
                            fdate+'_'+channel+'_'+select+'.png'
                    ofile = os.path.join( outpath, fbase)

                    plt_zonal_means(np.array(zmean), np.array(znobs), 
                            global_mean, zone_size, ofile, fill_value, 
                            pdate, chan_label, sat_label, select)

                    if verbose == True: 
                        print ("   + %s done!" % ofile)

                    # latitudinal plot: one per satellite
                    fbase = 'Plot_ZonalResult2_'+satellite+'_'+\
                            fdate+'_'+channel+'_'+select+'.png'
                    ofile = os.path.join( outpath, fbase)

                    plt_zonal_mean_stdv(np.array(zmean), np.array(zstdv),
                            np.array(znobs), np.array(belts), fill_value,
                            zone_size, ofile, pdate, chan_label, 
                            sat_label, select)

                    if verbose == True: 
                        print ("   + %s done!" % ofile)


            # set new color for next satellite
            cnt += 1

        # -- end of loop over satellites

        # final plot
        if len(mlist) > 0:
            filebase = 'Plot_ZonalResults_ALLSAT_'+fdate+'_'+\
                       channel+'_'+select+'.png'
            outfilen = os.path.join( outpath, filebase)

            plt_all_sat_zonal(outfilen, mlist, slist, rlist, blist,
                    clist, plist, pdate, chan_label, select, 
                    zone_size, fill_value)

            if verbose == True: 
                print ("   + %s done!" % outfilen)

    # -- end of loop over days

    return

# -------------------------------------------------------------------
# read Global_statistics_AVHRRGACl1c_*.txt
# Global statistics for AVHRR GAC on NOAA-15
# channel | date | time | mean | stdv | nobs
def read_globstafile(fil,cha,sel,sdate,edate):

    obj = open(fil, mode="r")
    lines = obj.readlines()
    obj.close()

    # Global statistics for AVHRR GAC on NOAA-15
    # channel | date | time | mean | stdv | nobs
    lstar = []
    lsdat = []
    lstim = []
    lsave = []
    lsstd = []
    lsrec = []
    
    for ll in lines: 
        line = ll.strip('\n')
      
        if '#' in line:
            continue
        if '-9999.0000' in line:
            continue
        
        string = line.split( )
        
        if string[0] == cha: 
            if string[2] == sel: 

                date = datetime.datetime.strptime(string[1], '%Y%m%d').date()

                if date < sdate or date > edate:
                    continue
                else:
                    lstar.append(string[0])
                    lsdat.append(date)
                    lstim.append(string[2])
                    lsave.append(float(string[3]))
                    lsstd.append(float(string[4]))
                    lsrec.append(int(string[5]))
      
    #return (lstar,lsdat,lstim,lsave,lsstd,lsrec)
    return (lsdat,lsave,lsstd,lsrec)
# -------------------------------------------------------------------

