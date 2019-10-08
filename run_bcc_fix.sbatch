#!/bin/bash
#SBATCH -N 5
#SBATCH -C haswell
#SBATCH -q regular
#SBATCH -J artmip_cmip6
#SBATCH -t 01:15:00

#OpenMP settings:
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export OMP_PLACES=threads
export OMP_PROC_BIND=spread

DATESTR=$(date +"%Y%m%d_%H:%M:%s")
export LOG_FILE="log_bcc_fix_${DATESTR}.log"

export NUMPY_EXPERIMENTAL_ARRAY_FUNCTION=0

#run the application:
srun -n 80 -c 4 --cpu_bind=cores /global/homes/t/taobrien/.conda/envs/artmip_cmip6/bin/python -Xfaulthandler -u fix_bcc_files.py &> $LOG_FILE

