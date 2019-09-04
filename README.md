This repository contains scripts for calculating integrated vapor transport (IVT) and integrated water vapor (IWV) from CMIP6 data for the Atmospheric River Tracking Method Intercomparison Project (ARTMIP).

# Getting Started

There are three main steps in this process:

 1. Generate a list of all available CMIP6 files within a CMIP6 mirror directory:
    This will generate a file with a name like `cmip6_list_20190904.txt`
 2. Generate a list of all files from which ARTMIP quantities can be calculated:
    This will generate a file with a name like `cmip6_artmip_files_to_process_20190904.csv`.
 3. Run the parallel IVT/IWV calculation script.
 
 
The end result of this process will be a new directory `$SCRATCH/ARTMIP_CMIP6/` that contains a directory with a structure similar to the CMIP6 mirror, but with new variables:

 * prw - IWV
 * uhusavi - the northward component of the IVT vector
 * vhusavi - the eastward component of the IVT vector
 * windhusavi - the magnitude of the IVT vector

These scripts are designed to run at NERSC, and customization will likely be necessary for running elsewhere.
    
Example:
```bash
bash generate_cmip6_file_list.bash
python generate_cmip6_artmip_list.py cmip6_list_$(date +%Y%m%d).txt
bash submit_artmip_calculation_script.bash
```
    
 
