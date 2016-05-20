#
# Post-proc blacklisting based on logfile & pystat & overlap analyses
#
import datetime
from subs_avhrrgac import get_satellite_list
from subs_avhrrgac import post_blacklist_reasons


def read_manually_selected_orbits( file_txt ):
    """
    The post_blacklist.txt read contains orbits, which show bad L1c quality,
    most probably due to scan motor issues.
    These orbits were identified manually by DWD based on 
    pystat results and plotting each orbit (vis & ir channels: 1 & 5).
    """
    pdict = post_blacklist_reasons()
    orbit_list = list()

    obj = open( file_txt, mode="r" )
    lines = obj.readlines()
    obj.close()

    for l in lines:
        line = l.strip('\n')
        if '#' in line:
            continue
        splt = line.split()
        orbit_list.append( splt[0]+'.gz' )

    # due to midnight orbits, make unique filename list
    return pdict['post7'], set(orbit_list)


def list_ch3a_zero_reflectance():
    """
    There are periods where channel 3a is active
    but contains zero reflectance measurements.
    """
    pdict = post_blacklist_reasons()
    bdict = dict()
    satlist = ["NOAA15","NOAA15","NOAA15","NOAA15","NOAA18","NOAA19"]
    datlist = ["sdate","edate"]
    x = 0
    for s in satlist:
        bdict[x] = dict()
        bdict[x][s] = dict()
        for d in datlist:
            bdict[x][s][d] = 0
        x += 1

    # NOAA-15
    # period
    bdict[0]["NOAA15"]["sdate"] = datetime.datetime(1999, 3,  9,  0,  0,  0)
    bdict[0]["NOAA15"]["edate"] = datetime.datetime(1999, 4, 20, 23, 59, 59)
    # single day
    bdict[1]["NOAA15"]["sdate"] = datetime.datetime(2000, 4, 8,  0,  0,  0)
    bdict[1]["NOAA15"]["edate"] = datetime.datetime(2000, 4, 8, 23, 59, 59)
    # single day
    bdict[2]["NOAA15"]["sdate"] = datetime.datetime(2000, 5, 18,  0,  0,  0)
    bdict[2]["NOAA15"]["edate"] = datetime.datetime(2000, 5, 18, 23, 59, 59)
    # single day
    bdict[3]["NOAA15"]["sdate"] = datetime.datetime(2000, 6, 16,  0,  0,  0)
    bdict[3]["NOAA15"]["edate"] = datetime.datetime(2000, 6, 16, 23, 59, 59)
    # NOAA-18
    # period
    bdict[4]["NOAA18"]["sdate"] = datetime.datetime(2005, 6, 23,  0,  0,  0)
    bdict[4]["NOAA18"]["edate"] = datetime.datetime(2005, 8,  5, 23, 59, 59)
    # NOAA-19
    # period
    bdict[5]["NOAA19"]["sdate"] = datetime.datetime(2009, 3, 19,  0,  0,  0)
    bdict[5]["NOAA19"]["edate"] = datetime.datetime(2009, 5, 14, 23, 59, 59)

    return pdict['post6'], bdict


def list_indexerror():
    """
    pyGAC resulted in IndexError: index out of bounds
    pyGAC failed on these orbits after pyGAC was updated
    w.r.t. NOAA-7 and NOAA-9 clock drift error correction
    has been enabled,
    i.e. pyGAC was successful on these orbits when
    clock drift error correction was disabled.
    """
    pdict = post_blacklist_reasons()

    olist = ["NSS.GHRR.NC.D82046.S0524.E0718.B0334243.WI.gz",
             "NSS.GHRR.NC.D82160.S1451.E1636.B0495758.GC.gz",
             "NSS.GHRR.NC.D82321.S0611.E0803.B0722526.WI.gz",
             "NSS.GHRR.NF.D86309.S0228.E0421.B0977172.WI.gz",
             "NSS.GHRR.NF.D87049.S0356.E0547.B1125354.WI.gz",
             "NSS.GHRR.NF.D87140.S0231.E0425.B1253637.WI.gz",
             "NSS.GHRR.NF.D87140.S0420.E0614.B1253738.WI.gz",
             "NSS.GHRR.NF.D87217.S0204.E0358.B1362223.WI.gz"]

    return pdict['post5'], olist


def list_along_track_too_long():
    """
    Diana Stein found these L1b orbits during CLARA-A2
    processing, where pyGAC provided L1c files where the
    along_track dimension is too large, i.e. ydim too long.
    """
    pdict = post_blacklist_reasons()

    olist = ["NSS.GHRR.NK.D09357.S2056.E2242.B6037880.WI.gz",
             "NSS.GHRR.NK.D10075.S0422.E0617.B6155152.WI.gz",
             "NSS.GHRR.NK.D10298.S2008.E2150.B6473637.WI.gz",
             "NSS.GHRR.NK.D11129.S1504.E1552.B6752424.WI.gz",
             "NSS.GHRR.NK.D12036.S1904.E2022.B7140001.WI.gz",
             "NSS.GHRR.NL.D07100.S0445.E0638.B3375152.WI.gz",
             "NSS.GHRR.NL.D08218.S0916.E1102.B4057273.WI.gz",
             "NSS.GHRR.NL.D11317.S0738.E0932.B5744243.WI.gz",
             "NSS.GHRR.NN.D06032.S0311.E0506.B0361920.GC.gz",
             "NSS.GHRR.NN.D09253.S0123.E0318.B2219293.WI.gz",
             "NSS.GHRR.NN.D12226.S0457.E0652.B3726061.WI.gz",
             "NSS.GHRR.NP.D14087.S0248.E0316.B2645555.SV.gz",
             "NSS.GHRR.NP.D14365.S2230.E2349.B3038989.GC.gz",
             "NSS.GHRR.NC.D84283.S0744.E0931.B1699899.WI.gz"]

    return pdict['post4'], olist


def list_bad_l1c_quality():
    """
    Blacklist orbits where the L1c quality is not OK, mainly
    based on pystat analysis.
    """
    pdict = post_blacklist_reasons()
    bdict = dict()
    satlist = ["NOAA6","NOAA8","NOAA17"]
    datlist = ["sdate","edate"]
    for s in satlist:
        bdict[s] = dict()
        for d in datlist:
            bdict[s][d] = 0

    # NOAA-6
    bdict["NOAA6"]["sdate"] = datetime.datetime(1981, 8, 14,  0,  0,  0)
    bdict["NOAA6"]["edate"] = datetime.datetime(1982, 8,  2, 23, 59, 59)
    # NOAA-8
    bdict["NOAA8"]["sdate"] = datetime.datetime(1983, 5,  4,  0,  0,  0)
    bdict["NOAA8"]["edate"] = datetime.datetime(1983, 9, 19, 23, 59, 59)
    # NOAA-17
    bdict["NOAA17"]["sdate"] = datetime.datetime(2010, 3, 1,  0,  0,  0)
    bdict["NOAA17"]["edate"] = datetime.datetime(2012, 1, 1, 23, 59, 59)

    return pdict['post3'], bdict


def list_no_valid_l1c_data():
    """
    List of days, where pyGAC did not provide any reasonable L1c orbits.
    """
    pdict = post_blacklist_reasons()
    bdict2 = dict()
    satlist = get_satellite_list()
    for s in satlist:
        bdict2[s] = dict()
    
    # fill dict with dates based on logfile analysis, where no L1c files have
    # been created during AVHRR GAC L1C procession VERSION 2
    bdict2["NOAA8"]["198405"] = [31]
    bdict2["NOAA8"]["198406"] = [1,2,3]
    bdict2["NOAA8"]["198508"] = [10,11]
    bdict2["NOAA8"]["198509"] = [16]
    bdict2["NOAA10"]["198808"] = [16,17,18,19,20,21,22]
    bdict2["NOAA10"]["198906"] = [1,2,3,4,5,6]
    bdict2["NOAA10"]["199004"] = [25,26,27,28,29,30]
    bdict2["NOAA10"]["199005"] = [1]
    bdict2["NOAA10"]["199101"] = [8,9,10,11,12,13,14]
    bdict2["NOAA7"]["198205"] = [28]
    bdict2["NOAA7"]["198307"] = [27,28,29,30,31]
    bdict2["NOAA7"]["198308"] = [1,2]
    bdict2["NOAA7"]["198309"] = [21,22,23,24,25,26]
    bdict2["NOAA7"]["198407"] = [23]
    bdict2["NOAA11"]["199409"] = [14,15,16,17,18,19,20,25,28]
    bdict2["NOAA11"]["199410"] = [6,8,9,10,11,12]
    bdict2["NOAA12"]["199310"] = [13,14,15,16,17,18,19]
    bdict2["NOAA14"]["200101"] = [1]
    bdict2["NOAA14"]["200112"] = [17,18,19,20,21,22,23,24,25,26,27,28,29,30,31]
    bdict2["NOAA14"]["200207"] = [28]
    bdict2["NOAA15"]["200007"] = [11,23,24,25,26,27,29,30]
    bdict2["NOAA15"]["200008"] = [2,3,4,5,7,9,11,12,13,14,16,17,18,19,20,21,22,23,24,26,27,28,31]
    bdict2["NOAA15"]["200009"] = [4,18,19,20,21,23,24,25,26,28,29]
    bdict2["NOAA15"]["200010"] = [1,3,4,5,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,31]
    bdict2["NOAA15"]["200011"] = [4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,23,24,25,28]
    bdict2["NOAA15"]["200012"] = [1,2,3,4,5,6,7,9,10,11,12,14,15,18,22,23,25,26,27]
    bdict2["NOAA15"]["200101"] = [6,7,8,11,12,13,14,15,16,17,18,19,20,21,22,31]
    bdict2["NOAA15"]["200102"] = [1,9,10]
    bdict2["NOAA15"]["200702"] = [16,17]
    bdict2["NOAA15"]["200703"] = [1,2,3,8,9,10]
    bdict2["NOAA17"]["200206"] = [25,26,27,28,29,30]
    bdict2["NOAA17"]["200207"] = [1,2,3,4,5,6,7,8,9]
    bdict2["NOAA17"]["201010"] = [7,8]
    bdict2["NOAA18"]["200505"] = [20,21,22,23,24,25,26,27,28,29,30,31]
    bdict2["NOAA18"]["200506"] = [1,2,3,4]
    bdict2["NOAA19"]["200902"] = [6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21]
    bdict2["METOPA"]["200709"] = [18]
    bdict2["METOPA"]["200803"] = [20]
    bdict2["METOPB"]["201412"] = [31]

    return pdict['post2'], bdict2, satlist


def list_wrong_l1c_timestamp():
    """
    List of AVHRR GAC orbits, which got a wrong L1c timestamp from pygac.
    """
    pdict = post_blacklist_reasons()

    # C. Schlundt: avhrrgac proc. version 2 [May/June 2015]
    #              (several pygac updates w.r.t. timestamp)
    bdict = dict()
    # detected during GAC_overlap.py
    # reason: two different l1bfiles produces the same l1cfile
    bdict["NSS.GHRR.NC.D83172.S1921.E2106.B1028384.WI.gz"] = "ECC_GAC_avhrr_noaa7_99999_19830621T2102005Z_19830621T2217055Z.h5"
    bdict["NSS.GHRR.NC.D83206.S0019.E0207.B1076667.GC.gz"] = "ECC_GAC_avhrr_noaa7_99999_19830726T0019507Z_19830726T0207452Z.h5"
    # based on logfile analysis
    bdict["NSS.GHRR.NH.D92104.S1732.E1842.B1830304.WI.gz"] = "ECC_GAC_avhrr_noaa11_99999_19910414T1732594Z_19910414T1842164Z.h5"
    bdict["NSS.GHRR.ND.D95015.S1456.E1641.B0000000.GC.gz"] = "ECC_GAC_avhrr_noaa12_99999_19970115T1456586Z_19970115T1641386Z.h5"
    bdict["NSS.GHRR.ND.D95320.S0657.E0851.B0000000.WI.gz"] = "ECC_GAC_avhrr_noaa12_99999_19961115T0657221Z_19961115T0851491Z.h5"
    bdict["NSS.GHRR.NJ.D95035.S0905.E1046.B0000000.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_19980204T0905006Z_19980204T1046101Z.h5"
    bdict["NSS.GHRR.NJ.D99286.S2145.E2333.B2467071.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20110511T1433055Z_20110511T1438440Z.h5"
    bdict["NSS.GHRR.NJ.D99287.S1640.E1834.B2468182.WI.gz"] = "ECC_GAC_avhrr_noaa14_99999_20140216T0741375Z_20140216T0837485Z.h5"
    bdict["NSS.GHRR.NJ.D99287.S1459.E1645.B2468081.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20140216T1110190Z_20140216T1123500Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S0002.E0100.B3095253.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T0002345Z_20020528T0100095Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S0050.E0231.B3095354.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T0050210Z_20020528T0231020Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S0225.E0420.B3095455.WI.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T0225425Z_20020528T0420190Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S0414.E0609.B3095556.WI.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T0414575Z_20020528T0609235Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S0604.E0758.B3095657.WI.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T0604180Z_20020528T0758475Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S0754.E0940.B3095758.WI.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T0754500Z_20020528T0940520Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S0935.E1121.B3095859.WI.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T0935540Z_20020528T1121535Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S1116.E1311.B3095960.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T1116520Z_20020528T1311180Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S1306.E1439.B3096061.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T1306130Z_20020528T1439125Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S1434.E1628.B3096162.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T1434125Z_20020528T1628120Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S1623.E1800.B3096263.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T1623060Z_20020528T1800340Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S1805.E1940.B3096364.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T1805230Z_20020528T1940235Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S1935.E2105.B3096465.WI.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T1935335Z_20020528T2105070Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S2100.E2254.B3096466.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T2100025Z_20020528T2254295Z.h5"
    bdict["NSS.GHRR.NJ.D01001.S2249.E0043.B3096667.GC.gz"] = "ECC_GAC_avhrr_noaa14_99999_20020528T2249160Z_20020529T0043530Z.h5"
    bdict["NSS.GHRR.NK.D99144.S2359.E0153.B0535153.GC.gz"] = "ECC_GAC_avhrr_noaa15_99999_19990525T0000041Z_19990525T0153486Z.h5"
    bdict["NSS.GHRR.NK.D00257.S2303.E0048.B1214951.WI.gz"] = "ECC_GAC_avhrr_noaa15_99999_20000914T0047076Z_20000914T0048346Z.h5"

    blist = list()
    for key in bdict:
        blist.append(key)

    return pdict['post1'], blist

