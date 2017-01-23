pytAVHRRGACl1c
=======================

This repository contains various tools for plotting, reading, and analysing AVHRR GAC L1c data.


Tools, which require a config file:

    add_sql_metadata.py 
        config_add_sql_metadata.py

    plot_quick_l1c_analysis.py
        config_plot_quick_l1c_analysis.py

    run_pygac.py
        config_run_pygac.py


These tools have **HELP** options:
    
    GAC_overlap.py [-h] --sqlfile SQLFILE

    add2sqlite_l1c_info.py [-h] -l1b L1B_FILE -l1c L1C_FILE 
        -dir L1C_PATH -dbf DB_FILE [-tmpdir TMP_DIR] [-ver]

    delete_data_from_ecfs.py [-h] -e ECFS_BASEPATH [-p PATTERN] [-s [SUBDIR [SUBDIR ...]]]

    get_equator_crossing_time.py [-h] --start_date START_DATE --end_date END_DATE --l1c_path L1C_PATH [--verbose]

    get_volume_of_ecfsdir.py [-h] -e ECFS_BASEPATH -p PATTERN

    plot_avhrr_ect_ltan.py [-h] -db DBFILE -out OUTDIR [-sd SDATE] [-ed EDATE] 
                           [-sats [SATELLITES [SATELLITES ...]]] 
                           [-ign [IGNORE [IGNORE ...]]] [-cci] [-pri] [-ver] [-show] [-leg]

    plot_blacklisting_hist.py [-h] -dbf DBFILE [-out OUTPUTDIR] [-sat SATELLITE] [-pre] [-proc] [-post]

    plot_missing_scanlines.py [-h] -db DBFILE [-sat [SATELLITES [SATELLITES ...]]] 
                              [-out OUTDIR] [-sd SDATE] [-ed EDATE] [-ver] [-show]

    plot_pystat_results.py [-h] -db DBFILE -out OUTDIR [-sd START_DATE] [-ed END_DATE] 
                           [-cha [CHANNELS [CHANNELS ...]]] [-tim [TIMES [TIMES ...]]] 
                           [-sat [SATELLITES [SATELLITES ...]]] [-tar TARGET] [-fit] 
                           [-ver] [-show] [-cdiff] [--linestyle LINESTYLE]

    read_avhrrgac_sql.py [-h] -d DBFILE [-v] [-s [SATELLITES [SATELLITES ...]]] 
                         [-a] [-b] [-wb] [-wc] [-mc] [-pf] [-pre] [-proc] [-post]
                         [-s4d SEARCH4DAYS] [-ts] [-no] [-bad] [-temp] [-ydim] [-ie] [-ch3a]

    run_pystat_add2sqlite.py [-h] -d DATE -s SATELLITE -i INPDIR -g GSQLITE [-b BINSIZE] [-t] [-v]

    vis_avhrrgac.py [-h] -dbf DBFILE [-reg REGION] [-out OUTPUTDIR]
                    [-bmb BACKGROUND] [-ver] [-cha CHANNEL]
                    [-fil [FILES [FILES ...]]] [-dat DATE] [-inp INPUTDIR]
                    [-tim TIME] [-off] [-mid] [-qfl] [-d12] [-d45] [-smc] [-std]

