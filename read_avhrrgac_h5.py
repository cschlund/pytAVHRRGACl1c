#
# reading avhrrgac level 1c H5 files
# C. Schlundt, July 2014
# H. Hoeschen, Oct. 2014, added get_data_size
# C. Schlundt, Nov. 2014, added read_qualflags
# 

import numpy as np
import numpy.ma as ma
import scipy.weave as weave
from scipy.ndimage.filters import uniform_filter


def gridbox_mean(data, fill_value, box_size):
    """
    For each element in C{data}, calculate the mean value of all valid elements
    in the C{box_size} x C{box_size} box around it.

    Masked values are not taken into account.

    @param data: 2D masked array
    @param fill_value: Value to be used to fill masked elements
    @param box_size: Box size
    @return: Gridbox mean
    @rtype: numpy.ma.core.MaskedArray
    """
    if not box_size % 2 == 1:
        raise ValueError('Box size must be odd.')

    # Fill masked elements
    fdata = data.astype('f8').filled(fill_value)

    # Allocate filtered image
    filtered = np.zeros(data.shape, dtype='f8')
    nrows, ncols = data.shape

    c_code = """
    int row, col, rowbox, colbox, nbox;
    double fill_value_d = (double) fill_value;
    int radius = (box_size-1)/2;


    for(row=0; row<nrows; row++)
    {
        for(col=0; col<ncols; col++)
        {
            filtered(row,col) = 0;
            nbox = 0;
            for(rowbox=row-radius; rowbox<=row+radius; rowbox++)
            {
                for(colbox=col-radius; colbox<=col+radius; colbox++)
                {
                    if(rowbox >= 0 && rowbox < nrows && colbox >= 0 and colbox < ncols)
                    {
                        if(fdata(rowbox,colbox) != fill_value_d)
                        {
                            filtered(row,col) += fdata(rowbox,colbox);
                            nbox += 1;
                        }
                    }
                }
            }
            if(nbox > 0)
            {
                filtered(row,col) /= (double) nbox;
            }
        }
    }
    return_val=0;
    """

    # Execute inline C code
    err = weave.inline(
        c_code,
        ['fdata', 'filtered', 'fill_value', 'box_size', 'nrows', 'ncols'],
        type_converters=weave.converters.blitz,
        compiler="gcc"
    )
    if err != 0:
        raise RuntimeError('Blitz failed with returncode {0}'.format(err))

    # Re-mask fill values
    return np.ma.masked_equal(filtered, fill_value)


def gridbox_std(data, box_size, fill_value):
    """
    For each element in C{data}, calculate the standard deviation of all valid
    elements in the C{box_size} x C{box_size} box around it.

    Masked values are not taken into account. Since

        std = sqrt( mean(data^2) - mean(data)^2 )

    we can use L{gridbox_mean} to compute the standard deviation.

    @param data: 2D masked array
    @param fill_value: Value to be used to fill masked elements
    @param box_size: Box size
    @return: Gridbox standard deviation
    @rtype: numpy.ma.core.MaskedArray
    """
    mean_squared = np.square(gridbox_mean(data, box_size=box_size, fill_value=fill_value))
    squared_mean = gridbox_mean(np.square(data), box_size=box_size, fill_value=fill_value)
    return np.ma.sqrt(squared_mean - mean_squared)


def get_stddev(data, size): 
    """
    Old: do not use because it does not work out at boundaries and masked values!
    """
    mean_squared = np.square(uniform_filter(data, size=size, mode='reflect'))
    squared_mean = uniform_filter(np.square(data), size=size, mode='reflect')
    #std = np.sqrt(np.ma.masked_equal(squared_mean - mean_squared, 0))
    std = np.sqrt(squared_mean - mean_squared)
    return std


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


def read_var(fil, varstr, ver):
    flg = False

    apply_minmax = False #True
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
                    #miss_mask = ma.masked_values(var, miss)
                    #noda_mask = ma.masked_values(miss_mask, noda)
                    #temp_var = (noda_mask * gain) + offs
                    mask = np.ma.logical_or(var == miss, var == noda)
                    temp_var = gain*np.ma.masked_where(mask, var) + offs

                    if apply_minmax:
                        # boundary masking
                        if desc_attr.lower() == "solar zenith angle":
                            lim_mask = ma.mask_or(temp_var < sza_min, temp_var > sza_max)

                        elif desc_attr.lower().startswith("avhrr"):
                            if chan_attr == "1" or chan_attr == "2" or chan_attr.lower() == "3a": 
                                lim_mask = ma.mask_or(temp_var < ref_min, temp_var > ref_max)

                            elif chan_attr == "4" or chan_attr == "5" or chan_attr.lower() == "3b": 
                                lim_mask = ma.mask_or(temp_var < bt_min, temp_var > bt_max)

                        fin_var = ma.masked_where(lim_mask, temp_var)
                    else:
                        fin_var = temp_var

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
                    #miss_mask = ma.masked_values(var, miss)
                    #noda_mask = ma.masked_values(miss_mask, noda)
                    #fin_var = (noda_mask * gain) + offs
                    mask = np.ma.logical_or(var == miss, var == noda)
                    fin_var = gain*np.ma.masked_where(mask, var) + offs

                    break

        finally:
            if not flg:
                print (" *** Cannot find %s variable in file ***" % varstr)
            else:
                if ver:
                    print ("   + %s" % name)
                    print ("     shape: %s, size: %s, type: %s , min: %s, max: %s"
                           % (fin_var.shape, fin_var.size, fin_var.dtype, fin_var.min(), fin_var.max()))
                    print ("     missingdata: %s, nodata: %s, gain: %s, offs: %s" % (miss, noda, gain, offs))
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


def read_avhrrgac(f, a, tim, cha, tsm_corr=None, ver=None):

    if ver:
        print ("   -------------------------------------------")
        print ("   * Original data for %s and %s" % (cha, tim))
        print ("   -------------------------------------------")

    # get angle and geolocation
    sza, szanam = read_var(a, 'image1', ver)
    lat, latnam = read_var(f, 'lat', ver)
    lon, lonnam = read_var(f, 'lon', ver)

    # get measurement
    # channel 1
    tardat1, tarname1 = read_var(f, 'image1', ver)
    tar1 = tardat1 / 100.
    # channel 2
    tardat2, tarname2 = read_var(f, 'image2', ver)
    tar2 = tardat2 / 100.
    # channel 3b
    tar3, tarname3 = read_var(f, 'image3', ver)
    # channel 4
    tar4, tarname4 = read_var(f, 'image4', ver)
    # channel 5
    tar5, tarname5 = read_var(f, 'image5', ver)
    # channel 3a
    tardat6, tarname6 = read_var(f, 'image6', ver)
    tar6 = tardat6 / 100.


    # --- START temporary scan motor issue correction
    if tsm_corr:
        # absolute difference because ch1 is very similar to ch2
        abs_d12 = abs(tar1 - tar2)
        # relative difference because ch4 and ch5 differ
        rel_d45 = 100.0*(tar4 - tar5)/tar5

        # standard deviation of abs_d12 and rel_d45
        box_size = 3
        fill_value = -9999.0
        std_d12 = gridbox_std(abs_d12, box_size, fill_value)
        std_d45 = gridbox_std(rel_d45, box_size, fill_value)

        # using ch1, ch2, ch4, ch5 in combination
        # all channels seems to be affected throughout the whole orbit,
        # independent of VIS and NIR or day and night
        ind1 = np.where( (std_d12 > 0.02) & (std_d45 > 2.00) )
        tar1[ind1] = -999.0
        tar2[ind1] = -999.0
        tar3[ind1] = -999.0
        tar4[ind1] = -999.0
        tar5[ind1] = -999.0
        tar6[ind1] = -999.0
    # --- END temporary scan motor issue correction


    if cha == 'ch1':
        tar = tar1
        tarname = tarname1 
    elif cha == 'ch2':
        tar = tar2
        tarname = tarname2
    elif cha == 'ch3b':
        tar = tar3
        tarname = tarname3
    elif cha == 'ch4':
        tar = tar4
        tarname = tarname4
    elif cha == 'ch5':
        tar = tar5
        tarname = tarname5
    elif cha == 'ch3a':
        tar = tar6
        tarname = tarname6

    # some lat/lon fields are not fill_value although they should be
    # lat/lon min/max outside realistic values
    # fixed here in read_var
    # but then tar and lat/lon do not have the same masked elements
    # thus:
    all_masks = [lat < -90., lat > 90., lon < -180., lon > 180.]
    total_mask = reduce(np.logical_or, all_masks)
    lat = ma.masked_where(total_mask, lat)
    lon = ma.masked_where(total_mask, lon)
    tar = ma.masked_where(total_mask, tar)
    sza = ma.masked_where(total_mask, sza)

    # select time
    if tim == 'day_90sza':
        # consider only daytime, i.e. sza < 90
        lon = ma.masked_where(sza >= 90., lon)
        lat = ma.masked_where(sza >= 90., lat)
        tar = ma.masked_where(sza >= 90., tar)

    if tim == 'day':
        # consider only daytime, i.e. sza < 80
        lon = ma.masked_where(sza >= 80., lon)
        lat = ma.masked_where(sza >= 80., lat)
        tar = ma.masked_where(sza >= 80., tar)

    if tim == 'twilight':
        # consider only twilight, i.e. 80 <= sza < 90
        # mask everything outside the current sza range
        omask = ma.mask_or(sza < 80, sza >= 90)
        lon = ma.masked_where(omask, lon)
        lat = ma.masked_where(omask, lat)
        tar = ma.masked_where(omask, tar)

    if tim == 'night':
        # consider only night, i.e. sza >= 90
        lon = ma.masked_where(sza < 90., lon)
        lat = ma.masked_where(sza < 90., lat)
        tar = ma.masked_where(sza < 90., tar)

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
