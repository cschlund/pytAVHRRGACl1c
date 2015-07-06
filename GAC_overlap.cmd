#!/bin/ksh
#PBS -N GAC_overlap
#PBS -q ns
#PBS -S /usr/bin/ksh
#PBS -m e
#PBS -M dec4@ecmwf.int
#PBS -p 70
#PBS -l EC_threads_per_task=1
#PBS -l EC_memory_per_task=3000mb
#PBS -l EC_ecfs=0
#PBS -l EC_mars=0
#PBS -o /perm/ms/de/sf7/cschlund/pytAVHRRGACl1c/log/GAC_overlap_pbs.out
#PBS -e /perm/ms/de/sf7/cschlund/pytAVHRRGACl1c/log/GAC_overlap_pbs.err

set -x

job_name=${PBS_JOBNAME}
job_name=${job_name:-CL}
primary_id=`echo ${PBS_JOBID} | cut -f1 -d"."`
primary_id=${primary_id:-CL}
jid=${job_name}_ID${primary_id}
id=`date +%s`
jid=${jid}_US${id}

BASE=/perm/ms/de/sf7/cschlund
REPO=${BASE}/pytAVHRRGACl1c
TOOL=${REPO}/GAC_overlap.py
PROC=${BASE}/GAC_PROC/ECFlow_AvhrrGacL1c_proc
SQLT=${PROC}/sql/AVHRR_GAC_archive_v2_newstats_overlap.sqlite3
LOGO=${REPO}/log/${jid}.out
LOGE=${REPO}/log/${jid}.err

cd ${REPO}
mkdir -p ${REPO}/log

python ${TOOL} -g ${SQLT} > ${LOGO} 2> ${LOGE}

status=${?}
if [ $status -ne 0 ]; then
  echo " --- FAILED"
  return 1
fi

# --- end of /perm/ms/de/sf7/cschlund/pytAVHRRGACl1c/GAC_overlap.cmd ---
