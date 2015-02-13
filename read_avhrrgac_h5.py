#
# reading avhrrgac level 1c H5 files
# C. Schlundt, July 2014
# H. Hoeschen, Oct. 2014, added get_data_size
# C. Schlundt, Nov. 2014, added read_qualflags
# 

import numpy as np
import numpy.ma as ma


def read_qualflags(fil):
    global data, last_scanline, total_records, dcol, drow
    try:
        groups = fil.keys()
        for i in groups:
            g = fil['/' + i + '/']
            data = fil[g.name + '/data'].value
            dcol = data.shape[1]
            drow = data.shape[0]
            # dsiz = data.size
            # dtyp = data.dtype
            # gain = g.attrs["gain"]
            # offs = g.attrs["offset"]
            # noda = g.attrs["nodata"]
            # miss = g.attrs["missingdata"]
            # name = g.attrs["dataset_name"]
            # sdat = g.attrs["startdate"]
            # edat = g.attrs["enddate"]
            # stim = g.attrs["starttime"]
            # etim = g.attrs["endtime"]
            total_records = g.attrs["total_number_of_data_records"]
            last_scanline = g.attrs["last_scan_line_number"]

    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)

    finally:
        return drow, dcol, int(total_records), int(last_scanline), data


# first column in data is Scan Line Number, starting with 1 (not 0)
def find_scanline_gaps(col, rows, qdata):
    gap_list = list()
    scanline_list = np.array(qdata[:, col].tolist())

    for i in range(rows):
        i += 1
        if i not in scanline_list:
            gap_list.append(i)

    return gap_list


def get_data_size(fil):
    groups = fil.keys()
    for item in groups:
        g = fil['/' + item + '/']
        for key in g:
            if key == 'data':
                xy = g[key].shape
                x = xy[0]
                y = xy[1]
                return x, y


def show_properties(fil):
    print ("   + Available KEYS: ")

    groups = fil.keys()

    for item in groups:

        print ("   - %s" % item)
        g = fil['/' + item + '/']

        for key in g:
            print ("     * KEY=%s ---> %s" % (key, g[key]))

            if key == 'data':
                print ("       + shape=%s, size=%s, dype=%s" %
                       (g[key].shape, g[key].size, g[key].dtype))

            if key == 'what' or key == 'how':
                for att in g[key].attrs.keys():
                    print ("       + ATT=%s ---> %s" %
                           (att, g[key].attrs[att]))

        for att in g.attrs.keys():
            print ("     * ATT=%s ---> %s" % (att, g.attrs[att]))

        print

    print (" --- FILE: %s" % fil)


# noinspection PyUnusedLocal,PyUnusedLocal,PyUnusedLocal,PyUnusedLocal
def read_var(fil, varstr, ver):
    flg = False

    # CC4CL limits: ./trunk/src/ECPConstants.F90
    ref_min = 0.
    ref_max = 1.5 * 100.
    bt_min = 140.0
    bt_max = 350.0
    lat_min = -90.0
    lat_max = 90.0
    lon_min = -180.0
    lon_max = 180.0
    sza_min = 0.
    sza_max = 180.0

    while not flg:
        try:
            for key in fil.keys():
                g = fil['/' + key + '/']
                if key == varstr:
                    flg = True

                    for att in g.attrs.keys():
                        if att == "channel":
                            chan_attr = g.attrs[att]
                        if att == "description":
                            desc_attr = g.attrs[att]

                    add = fil[g.name + '/what']
                    # how = fil[g.name+'/how'] #not in sunsatangles h5
                    var = fil[g.name + '/data'].value
                    gain = add.attrs["gain"]
                    offs = add.attrs["offset"]
                    noda = add.attrs["nodata"]
                    miss = add.attrs["missingdata"]
                    name = add.attrs["dataset_name"]

                    # missing and nodata masking
                    miss_mask = ma.masked_values(var, miss)
                    noda_mask = ma.masked_values(miss_mask, noda)
                    temp_var = (noda_mask * gain) + offs

                    # boundary masking
                    if desc_attr.lower() == "solar zenith angle":
                        lim_mask = ma.mask_or(temp_var < sza_min,
                                              temp_var > sza_max)

                    elif desc_attr.lower().startswith("avhrr"):
                        if chan_attr == "1" or chan_attr == "2" \
                                or chan_attr.lower() == "3a":
                            lim_mask = ma.mask_or(temp_var < ref_min,
                                                  temp_var > ref_max)

                        elif chan_attr == "4" or chan_attr == "5" \
                                or chan_attr.lower() == "3b":
                            lim_mask = ma.mask_or(temp_var < bt_min,
                                                  temp_var > bt_max)

                    # noinspection PyUnboundLocalVariable
                    fin_var = ma.masked_where(lim_mask, temp_var)

                    break

            for key2 in g.keys():
                if key2 == varstr:
                    flg = True
                    add = fil[g[key2].name + '/what']
                    var = fil[g[key2].name + '/data'].value
                    gain = add.attrs["gain"]
                    offs = add.attrs["offset"]
                    noda = add.attrs["nodata"]
                    miss = add.attrs["missingdata"]
                    name = add.attrs["dataset_name"]

                    # missing and nodata masking
                    miss_mask = ma.masked_values(var, miss)
                    noda_mask = ma.masked_values(miss_mask, noda)
                    fin_var = (noda_mask * gain) + offs

                    break

        finally:
            if not flg:
                print (" *** Cannot find %s variable in file ***" % varstr)
            else:
                if ver:
                    # noinspection PyUnboundLocalVariable
                    print ("   + %s" % name)
                    # noinspection PyUnboundLocalVariable
                    print ("     shape: %s, size: %s, type: %s , min: %s, max: %s"
                           % (fin_var.shape, fin_var.size, fin_var.dtype,
                              fin_var.min(), fin_var.max()))
                    # noinspection PyUnboundLocalVariable,PyUnboundLocalVariable,PyUnboundLocalVariable,PyUnboundLocalVariable
                    print ("     missingdata: %s, nodata: %s, gain: %s, offs: %s"
                           % (miss, noda, gain, offs))
                    print ("     non-masked elements: %s" % ma.count(fin_var))
                    print ("     masked elements    : %s" % ma.count_masked(fin_var))
                return fin_var, name


def read_latlon(f, ver):
    lat, latnam = read_var(f, 'lat', ver)
    lon, lonnam = read_var(f, 'lon', ver)

    all_masks = [lat < -90., lat > 90., lon < -180., lon > 180.]
    total_mask = reduce(np.logical_or, all_masks)

    lat = ma.masked_where(total_mask, lat)
    lon = ma.masked_where(total_mask, lon)

    return lat, lon


def read_avhrrgac(f, a, tim, cha, ver):

    if ver:
        print ("   -------------------------------------------")
        print ("   * Original data for %s and %s" % (cha, tim))
        print ("   -------------------------------------------")

    # get angle and geolocation
    sza, szanam = read_var(a, 'image1', ver)
    lat, latnam = read_var(f, 'lat', ver)
    lon, lonnam = read_var(f, 'lon', ver)

    # get measurement
    if cha == 'ch1':
        tardat, tarname = read_var(f, 'image1', ver)
        tar = tardat / 100.
    if cha == 'ch2':
        tardat, tarname = read_var(f, 'image2', ver)
        tar = tardat / 100.
    if cha == 'ch3b':
        tar, tarname = read_var(f, 'image3', ver)
    if cha == 'ch4':
        tar, tarname = read_var(f, 'image4', ver)
    if cha == 'ch5':
        tar, tarname = read_var(f, 'image5', ver)
    if cha == 'ch3a':
        tardat, tarname = read_var(f, 'image6', ver)
        tar = tardat / 100.

    # some lat/lon fields are not fill_value although they should be
    # lat/lon min/max outside realistic values
    # fixed here in read_var
    # but then tar and lat/lon do not have the same masked elements
    # thus:
    all_masks = [lat < -90., lat > 90., lon < -180., lon > 180.]
    total_mask = reduce(np.logical_or, all_masks)
    lat = ma.masked_where(total_mask, lat)
    lon = ma.masked_where(total_mask, lon)
    # noinspection PyUnboundLocalVariable
    tar = ma.masked_where(total_mask, tar)
    sza = ma.masked_where(total_mask, sza)

    # select time
    if tim == 'day':
        # consider only daytime, i.e. sza < 80
        lon = ma.masked_where(sza >= 80., lon)
        lat = ma.masked_where(sza >= 80., lat)
        tar = ma.masked_where(sza >= 80., tar)

    if tim == 'twilight':
        # consider only twilight, i.e. 80 <= sza < 90
        # mask everything outside the current sza range
        omask = ma.mask_or(sza <= 80, sza > 90)
        lon = ma.masked_where(omask, lon)
        lat = ma.masked_where(omask, lat)
        tar = ma.masked_where(omask, tar)

    if tim == 'night':
        # consider only night, i.e. sza >= 90
        lon = ma.masked_where(sza < 90., lon)
        lat = ma.masked_where(sza < 90., lat)
        tar = ma.masked_where(sza < 90., tar)

    # noinspection PyUnboundLocalVariable
    parlst = [latnam, lonnam, tarname]
    datlst = [lat, lon, tar]

    if ver:
        cnt = 0
        print
        print ("   -------------------------------------------")
        print ("   * Masked Arrays: %s" % tim)
        print ("   -------------------------------------------")
        for item2 in datlst:
            print ("   + \'%s\', shape: %s, size: %s, type: %s , min: %s, max: %s" %
                   (parlst[cnt], item2.shape, item2.size,
                    item2.dtype, item2.min(), item2.max()))
            print ("     non-masked elements: %s" % ma.count(item2))
            print ("     masked elements    : %s" % ma.count_masked(item2))
            cnt += 1
        print

    return lat, lon, tar