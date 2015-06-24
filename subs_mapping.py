
import os
import numpy as np
from numpy import copy
import matplotlib.pyplot as plt
from matplotlib import colors
import regionslist as rl
import subs_avhrrgac as subs
from netCDF4 import Dataset
from mpl_toolkits.basemap import Basemap

import warnings
import logging

logger = logging.getLogger('root')
warnings.filterwarnings("ignore")


def get_colorbar(param, data):

    if 'cc_mask' in param or 'cloudmask' in param:
        colmap = colors.ListedColormap(['blue', 'white'])
        bounds = [0, 0.5, 1]
        colnorm = colors.BoundaryNorm(bounds, colmap.N)
        ticks = [0.25, 0.75]
        labels = ['clear', 'cloudy']

    elif 'cld_type' in param or 'cldtype' in param:

        if np.max(data) == 9:
            colmap = colors.ListedColormap(['DimGray', 'Peru', 'Navy', 'Lime',
                                            'OrangeRed', 'Cyan', 'Magenta', 'Yellow'])
            bounds = [0, 2, 3, 4, 6, 7, 8, 9, 10]
            colnorm = colors.BoundaryNorm(bounds, colmap.N)
            ticks = [1, 2.5, 3.5, 5, 6.5, 7.5, 8.5, 9.5]
            labels = ['clear', 'fog', 'water', 'super-cooled',
                      'opaque ice', 'cirrus', 'overlap', 'prob. opa. ice']

        if np.max(data) == 8:
            colmap = colors.ListedColormap(['DimGray', 'Peru', 'Navy', 'Lime',
                                            'OrangeRed', 'Cyan', 'Magenta'])
            bounds = [0, 2, 3, 4, 6, 7, 8, 9]
            colnorm = colors.BoundaryNorm(bounds, colmap.N)
            ticks = [1, 2.5, 3.5, 5, 6.5, 7.5, 8.5]
            labels = ['clear', 'fog', 'water', 'super-cooled',
                      'opaque ice', 'cirrus', 'overlap']

    else:
        logger.info("No special colorbar for {0}".format(param))
        return False, False, False, False, False

    return colmap, colnorm, bounds, ticks, labels


def map_cloud_cci(filename, product, region, outputdir, background):
    """
    Map Cloud CCI results: L2, L3U, L3S.
    :return:
    """

    logger.info("Read: {0}".format(filename))
    fh = Dataset(filename, mode='r')
    longitudes = fh.variables['lon'][:]
    latitudes = fh.variables['lat'][:]
    target = fh.variables[product][:]
    try:
        units = fh.variables[product].units
    except:
        units = False
        pass
    longname = fh.variables[product].long_name
    fh.close()

    logger.info("Target: {0}, Min: {1}, Max: {2}".
                format(longname, np.min(target), np.max(target)))

    # get color bar stuff for specific products
    (cmap, norm, interval,
     cticks, clabels) = get_colorbar(product, target)

    basfil = os.path.basename(filename)
    basout = os.path.splitext(basfil)[0]
    fsplit = basfil.split('-')
    fildat = fsplit[0]
    esacci = fsplit[1]
    protyp = fsplit[2].split('_')[0]
    # cldpro = fsplit[3]

    if protyp == "L2":
        instyp = fsplit[4]
        sattyp = fsplit[5]
    else:
        instyp = fsplit[4].split('_')[0]
        sattyp = fsplit[4].split('_')[1] + '-' + fsplit[5]

    outfil = basout + '_' + product + '_' + region + '.png'
    ofilen = os.path.join(outputdir, outfil)
    otitle = fildat + ' ' + esacci + ' CLD ' + protyp + ' ' + \
             instyp + '/' + sattyp

    # initialize figure
    fig = plt.figure()
    ax = fig.add_subplot(111)

    # create basemap
    logger.info("Draw basemap")
    m = Basemap(**rl.REGIONS[region]["geo"])

    # basemap background
    if background:
        if background.lower() == "bluemarble":
            m.bluemarble()
        elif background.lower() == "shaderelief":
            m.shadedrelief()
        elif background.lower() == "etopo":
            m.etopo()
        else:
            m.drawlsmask(land_color='coral', ocean_color='lightblue',
                         lakes=True)
    else:
        m.drawlsmask(land_color='coral', ocean_color='lightblue',
                     lakes=True)

    # Plot data
    logger.info("Plot {0} data onto map".format(protyp))
    if protyp == "L2":
        wmask = longitudes > 5   # cut all values where lon > 5
        emask = longitudes < -5  # cut all values where lon < -5

        for mask in (wmask, emask):
            # mask lat, lon & data arrays:
            mlon = np.ma.masked_where(mask, longitudes)
            mlat = np.ma.masked_where(mask, latitudes)
            mtar = np.ma.masked_where(mask, target)
            # find x,y values of map projection grid:
            x, y = m(mlon, mlat)
            if not cmap:
                cs = m.pcolor(x, y, mtar, cmap='jet',
                              vmin=np.min(target), vmax=np.max(target))
            else:
                cs = m.pcolor(x, y, mtar, cmap=cmap, norm=norm,
                              vmin=np.min(target), vmax=np.max(target))
    else:
        lon, lat = np.meshgrid(longitudes, latitudes)
        xi, yi = m(lon, lat)

        if not cmap:
            cs = m.pcolor(xi, yi, np.squeeze(target), cmap='jet',
                          vmin=np.min(target), vmax=np.max(target))
        else:
            cs = m.pcolor(xi, yi, np.squeeze(target), cmap=cmap, norm=norm,
                          vmin=np.min(target), vmax=np.max(target))

    # add grid lines
    logger.info("Finalize and save map: {0}".format(otitle))
    lons = np.arange(*rl.REGIONS[region]["mer"])
    lats = np.arange(*rl.REGIONS[region]["par"])
    # m.drawparallels(lats, labels=[True, False, False, False], fontsize=10)
    m.drawparallels(lats, labels=[True, False, False, False])
    # m.drawmeridians(lons, labels=[False, False, True, True], fontsize=10)
    m.drawmeridians(lons, labels=[False, False, True, True])

    # Add Coastlines, States, and Country Boundaries
    m.drawcoastlines()
    m.drawstates()
    m.drawcountries()

    # Add Colorbar
    if not interval:
        cbar = m.colorbar(cs, pad="2%")
        # cbar = m.colorbar(cs, loacation='bottom', pad="10%")
    else:
        cbar = m.colorbar(cs, cmap=cmap, norm=norm, boundaries=interval,
                          ticks=cticks, pad="2%")
        cbar.set_ticklabels(clabels)

    if not units:
        cbar.set_label("\n" + longname)
    else:
        cbar.set_label("\n" + longname + ' [' + units + ']')

    # add title:
    ax.set_title(otitle + "\n\n")

    # save to file:
    fig.savefig(ofilen, bbox_inches='tight')
    plt.close()
    logger.info("Done: {0}".format(ofilen))

    return


def map_avhrrgac_l1c(filename, channel, region, time, outputdir,
                     lon, lat, tar, background):
    """
    Mapping subroutine for AVHRR GAC L1c derived from pygac.
    :return:
    """
    sft = "%Y/%m/%d %H:%M:%S"
    sdt, edt = subs.get_l1c_timestamps(filename)
    strlst = subs.split_filename(filename)
    platform = strlst[3]
    strsdate = strlst[5][0:8]
    date_str = sdt.strftime(sft) + " - " + edt.strftime(sft)
    avhrrstr = "AVHRR GAC L1c / " + subs.full_sat_name(platform)[0]
    basfil = os.path.basename(filename)
    bastxt = os.path.splitext(basfil)[0]
    outfil = bastxt + '_' + channel + '_' + region + '_' + time + '.png'
    ofilen = os.path.join(outputdir, outfil)
    outtit = avhrrstr + " - " + rl.REGIONS[region]["nam"] + \
             " (" + time + ")" 

    # initialize figure
    fig = plt.figure()
    ax = fig.add_subplot(111)

    # create basemap
    logger.info("Draw basemap")
    m = Basemap(**rl.REGIONS[region]["geo"])

    # basemap background
    if background:
        if background.lower() == "bluemarble":
            m.bluemarble()
        elif background.lower() == "shaderelief":
            m.shadedrelief()
        elif background.lower() == "etopo":
            m.etopo()
        else:
            m.drawlsmask(land_color='coral', ocean_color='lightblue',
                         lakes=True)
    else:
        m.drawlsmask(land_color='coral', ocean_color='lightblue',
                     lakes=True)

    logger.info("Plot {0} data onto map".format(avhrrstr))
    # Split dataset west-east at the prime meridian in order to avoid misplaced
    # polygons produced by pcolor when lon crosses the dateline (i.e. jumps from
    # 180 to -180 or vice versa). Use 5 degrees of overlap to avoid polygon gaps
    # at lon=0.
    wmask = lon > 5  # cut all values where lon > 5
    emask = lon < -5  # cut all values where lon < -5

    for mask in (wmask, emask):
        # mask lat, lon & data arrays:
        mlon = np.ma.masked_where(mask, lon)
        mlat = np.ma.masked_where(mask, lat)
        mtar = np.ma.masked_where(mask, tar)

        # find x,y values of map projection grid:
        x, y = m(mlon, mlat)

        # Plot data. Note the vmin and vmax arguments. They have to be identical
        # for both the east- and west-plot in order to assure an identical colorbar
        # scaling. Here, vmin & vmax are set to the global minimum and maximum of
        # the data, respectively.
        # pcolor = m.pcolor(x, y, mtar, cmap='jet', vmin=0.0, vmax=1.0)
        from matplotlib import cm
        cmap = cm.get_cmap('jet')
        cmap.set_bad('grey')
        # pcolor = m.pcolor(x, y, mtar, cmap=cmap, vmin=np.min(tar), vmax=np.max(tar))
        pcolor = m.scatter(x, y, c=mtar, s=1.0, edgecolor='none', cmap=cmap, vmin=np.min(tar), vmax=np.max(tar))

    # add grid lines
    logger.info("Finalize and save map: {0}".format(outtit))
    lons = np.arange(*rl.REGIONS[region]["mer"])
    lats = np.arange(*rl.REGIONS[region]["par"])
    m.drawparallels(lats, labels=[True, False, False, False])
    m.drawmeridians(lons, labels=[False, False, True, True])

    # Add Coastlines, States, and Country Boundaries
    m.drawcoastlines()
    m.drawstates()
    m.drawcountries()

    # add colorbar with units:
    # noinspection PyUnboundLocalVariable
    cbar = m.colorbar(pcolor, pad="2%")
    cbar.set_label(subs.full_cha_name(channel))

    # add title:
    ax.set_title(outtit + "\n\n")

    # save to file:
    fig.savefig(ofilen, bbox_inches='tight')
    plt.close()
    logger.info("Done: {0}".format(ofilen))

    return


def plot_avhrrgac_qualflags2(filename, outputdir, 
                             qrow, qcol, recs, lastline, data):
    """
    Mapping subroutine for AVHRR GAC L1c derived from pygac: quality flags.
    :return:
    """
    color_list = ['DimGray', 'Red', 'Blue', 'Lime', 
                  'Magenta', 'DodgerBlue', 'Orange']
    label_list = ["Scan Line Number",
                  "fatal error flag", "insufficient data for calibration",
                  "insufficient data for navigation",
                  "solar contamination of blackbody occurred in Channel 3",
                  "solar contamination of blackbody occurred in Channel 4",
                  "solar contamination of blackbody occurred in Channel 5"]

    sft = "%Y/%m/%d %H:%M:%S"
    sdt, edt = subs.get_l1c_timestamps(filename)
    strlst = subs.split_filename(filename)
    platform = strlst[3]
    platname = subs.full_sat_name(platform)[0]
    strsdate = strlst[5][0:8]
    date_str = sdt.strftime(sft) + " - " + edt.strftime(sft)
    outtit = "Orbit length: " + date_str + '\n'
    basfil = os.path.basename(filename)
    bastxt = os.path.splitext(basfil)[0] + '.png'
    ofilen = os.path.join(outputdir, bastxt)
    ytitle = 'Quality Flag\n'
    xtitle = '\n' + label_list[0] + ' of AVHRR/' + platname

    # initialize figure
    fig = plt.figure(figsize=(17,10))
    ax = fig.add_subplot(211)
    ay = fig.add_subplot(212)

    # plot data
    for i in range(1, 4, 1):
        ax.plot(data[:,0], data[:,i], label=label_list[i], 
                color=color_list[i], linewidth=2)

    for i in range(4, 7, 1):
        ay.plot(data[:,0], data[:,i], label=label_list[i], 
                color=color_list[i], linewidth=2)

    # set limits
    x_range = range(0, lastline+1, 1)
    major_xticks = range(0, lastline+1, 1000)
    minor_xticks = range(0, lastline+1, 500)
    ymax = np.max(data[:,1:6])*1.3
    ax.set_xlim(0, lastline)
    ay.set_xlim(0, lastline)
    ay.set_ylim(-0.1, ymax)
    ax.set_ylim(-0.1, ymax)
    ax.set_xticks(major_xticks)
    ax.set_xticks(minor_xticks, minor=True)
    ay.set_xticks(major_xticks)
    ay.set_xticks(minor_xticks, minor=True)

    # legend
    axleg = ax.legend(ncol=3, loc='best', fancybox=True)
    axleg.get_frame().set_alpha(0.5)
    ayleg = ay.legend(ncol=2, loc='best', fancybox=True)
    ayleg.get_frame().set_alpha(0.5)

    # set labels
    ax.set_title(outtit)
    ax.set_ylabel(ytitle)
    ay.set_ylabel(ytitle)
    #ax.set_xlabel(xtitle)
    ay.set_xlabel(xtitle)
    ax.grid(which='both', alpha=0.8)
    ay.grid(which='both', alpha=0.8)

    # save to file:
    fig.savefig(ofilen, bbox_inches='tight')
    plt.close()
    logger.info("Done: {0}".format(ofilen))

    return


def plot_avhrrgac_qualflags(filename, outputdir, 
                            qrow, qcol, recs, lastline, data):
    """
    Mapping subroutine for AVHRR GAC L1c derived from pygac: quality flags.
    :return:
    """
    color_list = ['DimGray', 'Red', 'Navy', 'forestgreen', 
                  'Magenta', 'DodgerBlue', 'Orange']
    syms_list1 = ['.', '.', '.', '.', '.', '.', '.']
    #syms_list1 = [',', '.', '1', 'x', '2', '+', '3']
    syms_list2 = ['o', 'D', '8', '^', 'p', 's', '*']
    label_list = ["Scan Line Number",
                  "fatal error flag", "insufficient data for calibration",
                  "insufficient data for navigation",
                  "solar contamination of blackbody occurred in Channel 3",
                  "solar contamination of blackbody occurred in Channel 4",
                  "solar contamination of blackbody occurred in Channel 5"]

    sft = "%Y/%m/%d %H:%M:%S"
    sdt, edt = subs.get_l1c_timestamps(filename)
    strlst = subs.split_filename(filename)
    platform = strlst[3]
    platname = subs.full_sat_name(platform)[0]
    strsdate = strlst[5][0:8]
    date_str = sdt.strftime(sft) + " - " + edt.strftime(sft)
    outtit = "Orbit length: " + date_str + '\n'
    basfil = os.path.basename(filename)
    bastxt = os.path.splitext(basfil)[0] + '.png'
    ofilen = os.path.join(outputdir, bastxt)
    ytitle = 'Quality Flag\n'
    xtitle = '\n' + label_list[0] + ' of AVHRR/' + platname

    # initialize figure
    fig = plt.figure(figsize=(17,10))
    ax = fig.add_subplot(111)
    #ax = fig.add_subplot(211)
    #ay = fig.add_subplot(212)
    x_max = lastline + 1
    x_min = 0
    y_max = 0.4
    y_step = 0.05

    # just for plotting issue
    new_arr = copy(data).astype('float')
    new_arr[:,1:][new_arr[:,1:] == 1.0] = 0.3

    # plot data
    for i in range(1, 7, 1):
        #ax.plot(data[:,0], new_arr[:,i]+(i*0.014), 
        #        syms_list1[i], markersize=10, 
        #        color=color_list[i], label=label_list[i])
        ax.scatter(data[:,0], new_arr[:,i]+(i*0.014), 
                   color=color_list[i], alpha=.9, s=1,
                   label=label_list[i])
        #ay.plot(data[:,0], data[:,i], label=label_list[i], 
        #        color=color_list[i], linewidth=0.8)

    # set limits
    x_range = range(0, x_max, 1)
    major_xticks = range(x_min, x_max, 1000)
    minor_xticks = range(x_min, x_max, 500)
    major_yticks = np.arange(0, y_max, y_step)
    ax.set_xlim(x_min, x_max)
    ax.set_ylim(0, y_max)
    ax.set_xticks(major_xticks)
    ax.set_yticks(major_yticks)
    ax.set_xticks(minor_xticks, minor=True)

    # legend
    axleg = ax.legend(ncol=1, loc='center', 
                      fancybox=True, fontsize=22, markerscale=5)
    axleg.get_frame().set_alpha(0.5)

    # set labels
    ylabel_str = list()
    for i in major_yticks:
        if i == y_step:
            ylabel_str.append('VALID\n')
        elif i == y_max - y_step:
            ylabel_str.append('INVALID\n')
        else:
            ylabel_str.append('')

    ax.yaxis.set_ticklabels(ylabel_str, rotation=90)
    ax.set_title(outtit)
    ax.set_ylabel(ytitle)
    ax.set_xlabel(xtitle)
    ax.xaxis.grid(which='both', alpha=0.8)

    # color good and bad areas
    ax.axhspan(0.0, 0.1, facecolor='yellow', alpha=0.2)
    ax.axhspan(0.3, 0.4, facecolor='grey', alpha=0.2)

    # save to file:
    fig.savefig(ofilen, bbox_inches='tight')
    #plt.show()
    plt.close()
    logger.info("Done: {0}".format(ofilen))

    return
