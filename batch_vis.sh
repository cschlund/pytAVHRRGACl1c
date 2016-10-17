#!/usr/bin/env bash

# ----------------------------------------------------------------------------
# Default settings
# ----------------------------------------------------------------------------

SQL="/cmsaf/cmsaf-cld7/AVHRR_GAC_L1c/20160329_AVHRR_GAC_L1c_aux_inf_v2/SQLs/SQLs_v2_201602_proc_2015/AVHRR_GAC_archive_v2_201603_post_overlap.sqlite3"
OUTPUT="/data/cschlund/temporary_scan_motor_issue_plots"
INPUT="/data/cschlund/avhrrgac_l1c/test/temporary_scan_motor_issue"
TTT="ECC_GAC_avhrr_noaa14_99999_20011024T1743063Z_20011024T1919343Z.h5"
REGION=(glo)
BAND=(ch1 ch2 ch3b ch4 ch5)


# ----------------------------------------------------------------------------
usage()
# ----------------------------------------------------------------------------
{
cat << EOF
 usage: $0 options 
 
 This script is calling vis_avhrrgac.py using the SQLite database: $SQL
 
 OPTIONS:
  -h     Show this message
  -i     Input path (default: $INPUT)
  -o     Output path (default: $OUTPUT)
  -r     Regions to be plotted, e.g. -r eur,afr (default: glo)
  -b     Bands to be plotted, e.g. -b ch1,ch4 (default: all)
  -t     Testfile: $INPUT/$TTT
  -d     Plot channel differences: abs_d12=ABS(ch1-ch2), rel_d45=100*(ch4-ch5)/ch5
  -s     Plot standard deviations of abs_d12 and rel_d45
  -m     Plot measurements (default: all)
  -c     Plot measurements applying TSM correction (default: all)
EOF
}


# ----------------------------------------------------------------------------
while getopts "hi:o:r:b:tdsmc" OPTION
# ----------------------------------------------------------------------------
do
    case $OPTION in
        h)
            usage
            exit 1
            ;;
        i)
            INPUT=$OPTARG
            ;;
        o)
            OUTPUT=$OPTARG
            ;;
        r)
            REGION=(`echo $OPTARG | cut -d "," --output-delimiter=" " -f 1-`)
            ;;
        b)
            BAND=(`echo $OPTARG | cut -d "," --output-delimiter=" " -f 1-`)
            ;;
        t)
            TEST=1
            ;;
        d)
            DIFF=1
            ;;
        s)
            STD=1
            ;;
        m)
            MES=1
            ;;
        c)
            TSM=1
            ;;
        ?)
            usage
            exit
            ;;
    esac
done

# file_list
if [[ $TEST -eq 1 ]]
then
    file_list=$(ls $INPUT/$TTT)
else
    file_list=$(ls $INPUT/*avhrr*h5)
fi


# ----------------------------------------------------------------------------
# call AVHRR GAC L1c data
# ----------------------------------------------------------------------------


cmd="./vis_avhrrgac.py -dbf ${SQL} -out ${OUTPUT} "

for i in ${file_list}; do 
    for r in "${REGION[@]}"; do

        echo " +++ Working on file: $i"

        if [[ $STD -eq 1 ]]
        then
            echo " * $r -- std_d12, std_d45"
            ${cmd} -std -d12 -reg $r -fil $i 
            ${cmd} -std -d45 -reg $r -fil $i 
        fi

        if [[ $DIFF -eq 1 ]]
        then
            echo " * $r -- d12, d45"
            ${cmd} -d12 -reg $r -fil $i
            ${cmd} -d45 -reg $r -fil $i 
        fi

        if [[ $MES -eq 1 ]]
        then
            echo " * $r -- ${BAND[@]}"
            for c in "${BAND[@]}"; do 
                ${cmd} -bmb shaderelief -cha $c -reg $r -fil $i 
            done
        fi

        if [[ $TSM -eq 1 ]]
        then
            echo " * $r -- ${BAND[@]}"
            for c in "${BAND[@]}"; do 
                ${cmd} -smc -bmb shaderelief -cha $c -reg $r -fil $i 
            done
        fi

        echo

    done
done
