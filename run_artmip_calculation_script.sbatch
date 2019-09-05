#!/bin/bash
#SBATCH -N 20
#SBATCH -C haswell
#SBATCH -q regular
#SBATCH -J artmip_calc
#SBATCH -t 00:59:00

#OpenMP settings:
export OMP_NUM_THREADS=1
export OMP_PLACES=threads
export OMP_PROC_BIND=spread

DATESTR=$(date +"%Y%m%d_%H:%M:%s")
export LOG_FILE="log_artmip_integral_calcs_${DATESTR}.log"

export NUMPY_EXPERIMENTAL_ARRAY_FUNCTION=0

#run the application:
srun -n 320 -c 4 --cpu_bind=cores /global/homes/t/taobrien/.conda/envs/artmip_cmip6/bin/python -Xfaulthandler -u run_parallel_integration_calculation.py > $LOG_FILE

