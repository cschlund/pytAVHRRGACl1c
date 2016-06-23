#!/usr/bin/env bash

#file_list=$(ls /data/cschlund/avhrrgac_l1c/noaa18/*avhrr*h5)
#for i in $file_list; do 
#    ./vis_avhrrgac.py -dbf dbfiles/main_dbfile -out ./figures/vis -bmb bluemarble -cha ch4 -fil $i 
#done

files=(
/data/cschlund/avhrrgac_l1c/noaa18/ECC_GAC_avhrr_noaa18_99999_20080615T1016530Z_20080615T1204050Z.h5
/data/cschlund/avhrrgac_l1c/noaa18/ECC_GAC_avhrr_noaa18_99999_20080615T1158380Z_20080615T1353225Z.h5
/data/cschlund/avhrrgac_l1c/noaa18/ECC_GAC_avhrr_noaa18_99999_20080615T1347530Z_20080615T1535275Z.h5
)

files=(
/data/cschlund/avhrrgac_l1c/noaa14/ECC_GAC_avhrr_noaa14_99999_20011020T0111123Z_20011020T0257548Z.h5
)

#for channel in ch1 ch4; do 
for channel in ch4; do 

    ./vis_avhrrgac.py  \
        -dbf dbfiles/main_dbfile \
        -out ./figures/vis \
        -bmb bluemarble \
        -cha $channel \
        -reg ort \
        -fil ${files[*]}

done

