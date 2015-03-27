
import os
import numpy as np
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


def get_colorbar(param):

    if 'cc_mask' in param:
        colmap = colors.ListedColormap(['blue', 'white'])
        bounds = [0, 0.5, 1]
        colnorm = colors.BoundaryNorm(bounds, colmap.N)
        ticks = [0.25, 0.75]
        labels = ['clear', 'cloudy']

    elif 'cld_type' in param:
        colmap = colors.ListedColormap(['DimGray', 'MediumSlateBlue', 'Navy',
                                        'Cyan', 'DarkOrange',
                                        'Lime', 'Magenta'])
        bounds = [0, 2, 3, 4, 6, 7, 8, 9]
        colnorm = colors.BoundaryNorm(bounds, colmap.N)
        ticks = [1, 2.5, 3.5, 5, 6.5, 7.5, 8.5]
        labels = ['clear', 'fog', 'water', 'super-cooled',
                  'opaque ice', 'cirrus', 'prob. opa. ice']

    else:
        logger.info("No special colorbar for {0}".format(param))
        return False, False, False, False, False

    return colmap, colnorm, bounds, ticks, labels


def map_cloud_cci(filename, product, region, outputdir, background):
    """
    Map Cloud CCI results: L2, L3U, L3S.
    :return:
    """

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

    # get color bar stuff for specific products
    (cmap, norm, interval,
     cticks, clabels) = get_colorbar(product)

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
             instyp + '/' + sattyp + "\n\n"

    # initialize figure
    fig = plt.figure()
    ax = fig.add_subplot(111)

    # create basemap
    logger.info("Draw basemap for {0}".format(ofilen))
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
            cs = m.pcolor(x, y, mtar, cmap='jet',
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
    ax.set_title(otitle)

    # save to file:
    fig.savefig(ofilen, bbox_inches='tight')
    plt.close()

    return


def map_avhrrgac_l1c(filename, channel, region, time, outputdir,
                     lon, lat, tar, background):
    """
    Mapping subroutine for AVHRR GAC L1c derived from pygac.
    :return:
    """

    strlst = subs.split_filename(filename)
    platform = strlst[3]
    strsdate = strlst[5][0:8]
    # stredate = strlst[6][0:8]
    avhrrstr = strsdate + ": AVHRR GAC L1c / " + subs.full_sat_name(platform)[0]
    basfil = os.path.basename(filename)
    bastxt = os.path.splitext(basfil)[0]
    outfil = bastxt + '_' + channel + '_' + region + '_' + time + '.png'
    ofilen = os.path.join(outputdir, outfil)
    outtit = avhrrstr + " - " + rl.REGIONS[region]["nam"] + " (" + time + ")\n\n"

    # initialize figure
    fig = plt.figure()
    ax = fig.add_subplot(111)

    # create basemap
    logger.info("Draw basemap for {0}".format(ofilen))
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
        pcolor = m.pcolor(x, y, mtar, cmap='jet', vmin=np.min(tar), vmax=np.max(tar))

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
    ax.set_title(outtit)

    # save to file:
    fig.savefig(ofilen, bbox_inches='tight')
    plt.close()