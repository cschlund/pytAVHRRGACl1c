
import os
import h5py
import warnings
import logging
import numpy as np
import matplotlib.pyplot as plt
import regionslist as rl
import subs_avhrrgac as subs
import read_avhrrgac_h5 as rh5
from pycmsaf.avhrr_gac.database import AvhrrGacDatabase
from mpl_toolkits.basemap import Basemap
from numpy import copy
from matplotlib import colors

logger = logging.getLogger('root')
warnings.filterwarnings("ignore")


def get_minmax_target(args):
    if args.channel is 'ch1' or \
            args.channel is 'ch2' or \
            args.channel is 'ch3a':
        return 0., 1.
    else:
        return 185., 330.


def get_background(mmap, args):
    if args.background:
        if args.background.lower() == "bluemarble":
            mmap.bluemarble()
        elif args.background.lower() == "shaderelief":
            mmap.shadedrelief()
        elif args.background.lower() == "etopo":
            mmap.etopo()
        else:
            mmap.drawlsmask(land_color='coral', 
                            ocean_color='lightblue', lakes=True)
    else: 
        mmap.drawlsmask(land_color='coral', 
                        ocean_color='lightblue', lakes=True)


def get_overlap_info(dbfile, date, satellite): 
    db = AvhrrGacDatabase(dbfile=dbfile) 
    records = db.get_scanlines(satellite=satellite, date=date)
    return records


def read_scanlines(ifile, records): 
    sdt, edt = subs.get_l1c_timestamps(ifile)
    for rec in records:
        if sdt == rec['start_time_l1c'] and edt == rec['end_time_l1c']:
            sl = rec['start_scanline_endcut']
            el = rec['end_scanline_endcut']
            return sl, el


def get_plot_info(flist, args):
    filename  = flist[0]
    splitstr  = subs.split_filename(filename)
    platform  = subs.lite_satstring(splitstr[3])
    date_str  = splitstr[5][0:8]
    date_obj  = subs.str2date(date_str)
    avhrrstr  = "AVHRR GAC L1c / " + subs.full_sat_name(platform)[0]
    basefile  = os.path.basename(filename)

    if len(flist) > 1: 
        cutfil = basefile.find(date_str+"T") 
        bastxt = basefile[0:cutfil]+date_str
    else:
        bastxt = os.path.splitext(basefile)[0]

    if args.overlap_off:
        over = '_overlap_off'
    else:
        over = ''

    pngfil = bastxt + '_' + args.channel + '_' + \
             args.region + '_' + args.time + over + '.png'
    outfil = os.path.join(args.outputdir, pngfil)
    title  = avhrrstr + " - " + rl.REGIONS[args.region]["nam"] + \
             " (" + args.time + ") for " + date_str 

    records = get_overlap_info(args.dbfile, date_obj, platform)

    return outfil, title, records


def map_avhrrgac_l1c(flist, args):
    """
    Mapping subroutine for AVHRR GAC L1c derived from pygac.
    """
    # some required information
    logger.info("Get plotting information")
    ofilen, outtit, recs = get_plot_info(flist, args)

    # initialize figure
    fig = plt.figure(figsize=(17,10))
    ax = fig.add_subplot(111)
    
    # create basemap
    logger.info("Draw basemap")
    m = Basemap(**rl.REGIONS[args.region]["geo"])

    # basemap background
    logger.info("Get basemap background")
    get_background(m, args)

    # which channel
    tarmin, tarmax = get_minmax_target(args)

    cnt = 0
    # loop over file list
    for fil in flist:

        # get scanlines
        sl, el = read_scanlines(fil, recs)
        logger.info("Start_Scanline: {0}".format(sl))
        logger.info("End_Scanline  : {0}".format(el))

        # read file
        afil = fil.replace("ECC_GAC_avhrr_", "ECC_GAC_sunsatangles_")
        f = h5py.File(fil, "r+")
        a = h5py.File(afil, "r+")
        (la, lo, ta) = rh5.read_avhrrgac(f, a, args.time, args.channel, 
                                         # args.verbose) 
                                         False)
        a.close()
        f.close()

        if args.overlap_off:
            lon = lo[:,:]
            lat = la[:,:]
            tar = ta[:,:]
            #if cnt == 0:
            #    lon = lo[6000:,:]
            #    lat = la[6000:,:]
            #    tar = ta[6000:,:]
            #    cnt = cnt + 1
            #else:
            #    lon = lo[0:6000,:]
            #    lat = la[0:6000,:]
            #    tar = ta[0:6000,:]
            #    cnt = cnt + 1
        else:
            lon = lo[sl:el+1,:]
            lat = la[sl:el+1,:]
            tar = ta[sl:el+1,:]
            #if cnt == 0:
            #    lon = lo[6000:el+1,:]
            #    lat = la[6000:el+1,:]
            #    tar = ta[6000:el+1,:]
            #    cnt = cnt + 1
            #else:
            #    lon = lo[sl:6000,:]
            #    lat = la[sl:6000,:]
            #    tar = ta[sl:6000,:]
            #    cnt = cnt + 1


        logger.info("Original target shape : {0}".format(ta.shape))
        logger.info("Truncated target shape: {0}".format(tar.shape))

        if args.qflag:
            # plot qflag file as additional information
            qfil = fil.replace("ECC_GAC_avhrr_", "ECC_GAC_qualflags_")
            q = h5py.File(qfil, "r+")
            (row, col, total, last, data) = rh5.read_qualflags(q)
            q.close()
            logger.info("Map AVHRR GAC L1c qualflag file")
            plot_avhrrgac_qualflags(qfil, args.outputdir,row, col, total, last, data)
    
        logger.info("Plot {0} data onto map".format(fil))
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
            pcolor = m.scatter(x, y, c=mtar, s=1.0, edgecolor='none', alpha=0.3,
                               cmap=cmap, vmin=tarmin, vmax=tarmax)


    # add grid lines
    logger.info("Finalize and save map: {0}".format(outtit))
    lons = np.arange(*rl.REGIONS[args.region]["mer"])
    lats = np.arange(*rl.REGIONS[args.region]["par"])
    m.drawparallels(lats, labels=[True, False, False, False])
    m.drawmeridians(lons, labels=[False, False, True, True])

    # Add Coastlines, States, and Country Boundaries
    m.drawcoastlines()
    m.drawstates()
    m.drawcountries()
    
    # add colorbar with units:
    cbar = m.colorbar(pcolor, pad="2%")
    cbar.set_label(subs.full_cha_name(args.channel))
    
    # add title:
    ax.set_title(outtit + "\n\n")

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
    plt.close()
    logger.info("Done: {0}".format(ofilen))

    return
