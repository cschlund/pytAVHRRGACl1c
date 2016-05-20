#!/usr/bin/env bash

pro="python plot_pystat_results.py"
dbf="-db dbfiles/pystat_dbfile"
pro2="python plot_missing_scanlines.py"
dbf2="-db dbfiles/main_dbfile"
out="-out ./figures"

${pro} ${dbf} ${out}/ch3a_issue -sd 19990309 -ed 20000616 -sat noaa15 -cha ch1
${pro} ${dbf} ${out}/ch3a_issue -sd 19990309 -ed 20000616 -sat noaa15 -cha ch3a
#${pro} ${dbf} ${out}/ch3a_issue -sd 20050623 -ed 20050805 -sat noaa18 -cha ch1
#${pro} ${dbf} ${out}/ch3a_issue -sd 20050623 -ed 20050805 -sat noaa18 -cha ch3a
#${pro} ${dbf} ${out}/ch3a_issue -sd 20090319 -ed 20090514 -sat noaa19 -cha ch1
#${pro} ${dbf} ${out}/ch3a_issue -sd 20090319 -ed 20090514 -sat noaa19 -cha ch3a

#${pro} ${dbf} ${out}/All
#${pro} ${dbf} ${out}/AVHRR-1 -sd 19790101 -ed 19920101 -sat noaa6 noaa8 noaa10
#${pro} ${dbf} ${out}/AVHRR-2 -sd 19810101 -ed 20030101 -sat noaa7 noaa9 noaa11 noaa12 noaa14
#${pro} ${dbf} ${out}/AVHRR-3 -sd 19980101 -ed 20170101 -sat noaa15 noaa16 noaa17 noaa18 noaa19 metopa metopb
#
#for i in NOAA6 NOAA7 NOAA8 NOAA9 NOAA10 NOAA11 NOAA12 NOAA14 NOAA15 NOAA16 NOAA17 NOAA18 NOAA19 METOPA METOPB; do 
#    ${pro} ${dbf} ${out}/${i} -sat ${i}
#    ${pro2} ${dbf2} ${out}/missing_scanlines -sat ${i}
#done

