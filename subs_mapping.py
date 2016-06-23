
import os, sys
import h5py
import datetime
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


def slice_data(longitude, latitude, target, xdim, ydim, filecount, 
               halforbit, sline, eline, region, overlap_off=None): 

    start_x = 0
    end_x = xdim

    # without overlap correction
    if overlap_off:
        start_y = 0
        end_y = ydim
        # plot second half of first orbit and 
        #      first half of next orbit
        if region.startswith('over'): 
            if isEven(filecount) == False: 
                start_y = halforbit
            else:
                end_y = halforbit

    # with overlap correction
    else:
        start_y = sline
        end_y = eline+1
        # plot second half of first orbit and 
        #      first half of next orbit
        if region.startswith('over'): 
            if isEven(filecount) == False: 
                start_y = halforbit
            else:
                end_y = halforbit

    logger.info("FileCount = {0}".format(filecount))
    logger.info("Slice data along x-axis {0}:{1}".format(start_x, end_x))
    logger.info("Slice data along y-axis {0}:{1}".format(start_y, end_y))

    lon = longitude[start_y:end_y, start_x:end_x]
    lat = latitude[start_y:end_y, start_x:end_x]
    tar = target[start_y:end_y, start_x:end_x]

    return lon, lat, tar


def isEven(number): 
    return number % 2 == 0


def get_minmax_target(args):
    if args.channel == 'ch1' or args.channel == 'ch2' or args.channel == 'ch3a':
        return 0.0, 1.0
    else:
        return 180., 330.


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


def get_overlap_info(args, date, satellite): 
    if args.midnight: 
        records = db.get_scanlines(satellite=satellite, date=date)
        return records
    else:
        dt1 = date
        dt2 = date + datetime.timedelta(days=1)
        db = AvhrrGacDatabase(dbfile=args.dbfile) 
        query = "SELECT start_time_l1c, end_time_l1c, " \
                "start_scanline_endcut, end_scanline_endcut, " \
                "along_track, across_track FROM vw_std " \
                "WHERE satellite_name=\'{sat}\' AND " \
                "start_time_l1c BETWEEN \'{dt1}\' AND \'{dt2}\' ".format(
                        dt1=dt1, dt2=dt2, sat=satellite)
        records = db.execute(query)
        return records


def read_scanlines(ifile, records): 
    sdt, edt = subs.get_l1c_timestamps(ifile)
    for rec in records:
        if sdt == rec['start_time_l1c'] and edt == rec['end_time_l1c']:
            sl = rec['start_scanline_endcut']
            el = rec['end_scanline_endcut']
            yd = rec['along_track']
            xd = rec['across_track']
            return sl, el, xd, yd

    logger.info("No match found for {0} - {1}".
            format(sdt, edt))


def get_date_sat_from_filename(filename):
    splitstr  = subs.split_filename(filename)
    satellite = subs.lite_satstring(splitstr[3])
    date_string = splitstr[5][0:8]
    yy = splitstr[5][0:4]
    mm = splitstr[5][4:6]
    dd = splitstr[5][6:8]
    date_string_title = yy+'-'+mm+'-'+dd
    date_object = subs.str2date(date_string)
    return satellite, date_object, date_string, date_string_title


def get_records_from_dbfile(args, filename):
    sat, dt_obj, dt_str, dt_tit = get_date_sat_from_filename(filename)
    records = get_overlap_info(args, dt_obj, sat)
    return records


def get_plot_info(flist, args):
    platform, date_obj, date_str, date_tit = get_date_sat_from_filename(flist[0])
    avhrrstr  = "AVHRR GAC / " + subs.full_sat_name(platform)[0]
    basefile  = os.path.basename(flist[0])

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
             " (" + args.time + ") for " + date_tit
    short_title = avhrrstr

    date_list = list()
    if len(flist) > 1:
        for f in flist: 
            sdt, edt = subs.get_l1c_timestamps(f)
            dates = sdt.strftime("%Y/%m/%d %H:%M:%S") + ' -- ' \
                    + edt.strftime("%Y/%m/%d %H:%M:%S")
            date_list.append(dates)

    return outfil, title, date_list, short_title


def map_avhrrgac_l1c(flist, args):
    """
    Mapping subroutine for AVHRR GAC L1c derived from pygac.
    """
    # some required information
    logger.info("Get plotting information")
    ofilen, outtit, dates, outtit_short = get_plot_info(flist, args)

    tick_labelsize = 16
    plt.rcParams['xtick.labelsize'] = tick_labelsize
    plt.rcParams['ytick.labelsize'] = tick_labelsize
    title_fontsize=18
    label_fontsize=16
    latlon_fontsize=16

    # initialize figure
    fig = plt.figure(figsize=(17,10))
    ax = fig.add_subplot(111)
    # [left, bottom, width, height]
    #ax = fig.add_axes([0.4,0.4,0.8,0.8]) 
    
    # create basemap
    logger.info("Draw basemap")
    m = Basemap(**rl.REGIONS[args.region]["geo"])

    # basemap background
    logger.info("Get basemap background")
    get_background(m, args)

    # which channel
    tarmin, tarmax = get_minmax_target(args)

    cnt = 0
    cut = 6000

    # loop over file list
    for fil in flist:

        # file counter
        cnt += 1

        # get records
        recs = get_records_from_dbfile(args, fil)

        # get scanlines and dimension of orbit
        sl, el, xdim, ydim = read_scanlines(fil, recs)
        if sl is None:
            sl = 0
            #sl = 1000
        if el is None:
            el = ydim-1
            #el = 2000
        logger.info("Start -- End Scanlines: {0}:{1}".format(sl,el))
        logger.info("Across & Along Track  : {0}:{1}".format(xdim, ydim))
        logger.info("Overlapping scanlines : {0}".format(ydim-el))

        ## test
        #el = el - 1
        #el = el + 1

        # halforbit if overlap option
        cut = int(el / 2.)

        # read file
        afil = fil.replace("ECC_GAC_avhrr_", "ECC_GAC_sunsatangles_")
        f = h5py.File(fil, "r+")
        a = h5py.File(afil, "r+")
        (la, lo, ta) = rh5.read_avhrrgac(f, a, args.time, args.channel, 
                                         # args.verbose) 
                                         False)
        a.close()
        f.close()

        # slice data
        lon, lat, tar = slice_data(lo, la, ta, xdim, ydim, 
                                   cnt, cut, sl, el, 
                                   args.region, args.overlap_off) 


        logger.info("Original target shape : {0}".format(ta.shape))
        logger.info("Truncated target shape: {0}".format(tar.shape))


        # plot qflag file as additional information
        if args.qflag:
            qfil = fil.replace("ECC_GAC_avhrr_", "ECC_GAC_qualflags_")
            q = h5py.File(qfil, "r+")
            (row, col, total, last, data) = rh5.read_qualflags(q)
            q.close()
            logger.info("Quality flag: row:{0}, col:{1}, total:{2}, last:{3}".
                    format(row, col, total, last))
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
            cmap.set_under('gray')
            #pcolor = m.pcolor(x, y, mtar.filled(tarmin-1), cmap=cmap, vmin=np.min(tar), vmax=np.max(tar))
            pcolor = m.scatter(x, y, c=mtar.filled(tarmin-1), s=1.0, edgecolor='none', alpha=0.5, cmap=cmap, vmin=tarmin, vmax=tarmax)


    # add grid lines
    logger.info("Finalize and save map: {0}".format(outtit))
    lons = np.arange(*rl.REGIONS[args.region]["mer"])
    lats = np.arange(*rl.REGIONS[args.region]["par"])
    m.drawparallels(lats, labels=[True, False, False, False], fontsize=latlon_fontsize)
    m.drawmeridians(lons, labels=[False, False, True, False], fontsize=latlon_fontsize)

    # Add Coastlines, States, and Country Boundaries
    m.drawcoastlines()
    m.drawstates()
    m.drawcountries()
    
    # add colorbar with units:
    if args.channel == 'ch1' or args.channel == 'ch2' or args.channel == 'ch3a':
        unit = ''
    else:
        unit = ' [K]'

    cbar = m.colorbar(pcolor, pad="2%", location='bottom')
    cbar.set_label(outtit_short + " " + subs.full_cha_name(args.channel) + unit, 
                   fontsize=label_fontsize)
    
    # add title:
    #ax.set_title(outtit+"\n\n", fontsize=title_fontsize)

    ## annotate plot with dates used for plotting
    #left, right = ax.get_xlim()
    #low, high = ax.get_ylim()
    ##print left, right, low, high
    ## 0.0 5000000.0 0.0 4000000.0 [meter]
    #scale = 0.05
    #xi = (right - left) / 3.5
    #yi = right*scale
    #for dt in dates:
    #    plt.text(xi, yi, dt, fontsize=16, 
    #             ha='center', va='center', color='black')
    #    yi += right*scale

    # save to file:
    fig.savefig(ofilen, bbox_inches='tight')
    #plt.tight_layout()
    #fig.savefig(ofilen)
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
    outtit = "Orbit length: " + date_str
    basfil = os.path.basename(filename)
    bastxt = os.path.splitext(basfil)[0] + '.png'
    ofilen = os.path.join(outputdir, bastxt)
    ytitle = 'Quality Flag'
    xtitle = label_list[0] + ' of AVHRR/' + platname + ' (along_track: '+str(qrow)+')'

    fts = 18
    plt.rcParams['xtick.labelsize'] = fts
    plt.rcParams['ytick.labelsize'] = fts

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
    if lastline > 15000:
        major_step =  5000
        minor_step =  1000
    else:
        major_step =  1000
        minor_step =  500

    x_range = range(0, x_max, 1)
    major_xticks = range(x_min, x_max, major_step)
    minor_xticks = range(x_min, x_max, minor_step)
    major_yticks = np.arange(0, y_max, y_step)
    ax.set_xlim(x_min, x_max)
    ax.set_ylim(0, y_max)
    ax.set_xticks(major_xticks)
    ax.set_yticks(major_yticks)
    ax.set_xticks(minor_xticks, minor=True)

    # legend
    axleg = ax.legend(ncol=1, loc='center', fancybox=True, fontsize=22, markerscale=5)
    axleg.get_frame().set_alpha(0.5)

    # set labels
    ylabel_str = list()
    for i in major_yticks:
        if i == y_step:
            ylabel_str.append('VALID')
        elif i == y_max - y_step:
            ylabel_str.append('INVALID')
        else:
            ylabel_str.append('')

    ax.yaxis.set_ticklabels(ylabel_str, rotation=90, fontsize=fts)
    ax.set_title(outtit, fontsize=fts)
    #ax.set_ylabel(ytitle, fontsize=fts)
    ax.set_xlabel(xtitle, fontsize=fts)
    ax.xaxis.grid(which='both', alpha=0.8)

    # color good and bad areas
    ax.axhspan(0.0, 0.1, facecolor='yellow', alpha=0.2)
    ax.axhspan(0.3, 0.4, facecolor='grey', alpha=0.2)

    # save to file:
    fig.savefig(ofilen, bbox_inches='tight')
    plt.close()
    logger.info("Done: {0}".format(ofilen))

    return
