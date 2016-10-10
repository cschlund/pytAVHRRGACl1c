#!/usr/bin/env bash

inp="/data/cschlund/avhrrgac_l1c/test/temporary_scan_motor_issue"
out="/data/cschlund/temporary_scan_motor_issue_plots"
reg="-reg nat_zoom "
reg="-reg glo "
cmd="./vis_avhrrgac.py -dbf ./dbfiles/main_dbfile -out ${out} ${reg} "

file_list=$(ls $inp/*avhrr*h5)
file_list=$(ls $inp/ECC_GAC_avhrr_noaa14_99999_20011024T1743063Z_20011024T1919343Z.h5)
file_list=$(ls $inp/*avhrr*h5)
for i in $file_list; do 

    # -- standard deviation plots
    ${cmd} -std -d12 -fil $i 
    ${cmd} -std -d45 -fil $i 

    # -- channel difference plots
    ${cmd} -d12 -fil $i
    ${cmd} -d45 -fil $i 

    # -- corrected channel plots
    for c in ch1 ch2 ch3b ch4 ch5; do 
    #for c in ch1 ch4; do 
        ${cmd} -smc -bmb shaderelief -cha $c -fil $i 
    done

    # -- uncorrected channel plots
    for c in ch1 ch2 ch3b ch4 ch5; do 
    #for c in ch1 ch4; do 
        ${cmd} -bmb shaderelief -cha $c -fil $i 
    done

done

