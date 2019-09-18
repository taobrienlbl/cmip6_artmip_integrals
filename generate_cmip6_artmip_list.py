#!/usr/bin/env python
# coding: utf-8
import progressbar
import pandas as pd
import database
import datetime as dt
import sys
import schwimmbad

# set the input file list
input_file_list = "/global/u1/t/taobrien/m1517_taobrien/cmip6_hackathon/cmip6_list_20190909.txt",
if len(sys.argv) >= 2:
    input_file_list = sys.argv[1]

# get the list of CMIP6 runs with hus at 6 hourly output on native model levels
cmip6_database = database.load(input_file_list = input_file_list, cache_file = input_file_list.replace('.txt','.pk'))

# get only the historical and ssp585 simulations
df_historical = database.select_by_dict(cmip6_database, 
                                        simulation = 'historical',
                                        variable = "hus",
                                        group = '6hrLev')


df_ssp585 = database.select_by_dict(cmip6_database,
                            simulation = 'ssp585',
                            variable = "hus",
                            group = '6hrLev')

cmip6_native_levs = pd.concat([df_historical, df_ssp585])

def find_matching_files(run):
    model, simulation, ensemble, group, file_id = run[0]
    
    # set the current hus file
    qa_df = run[1]
    
    # search for corresponding ua and va files
    ua_df = database.select_by_dict(cmip6_database,
                                 model = model,
                                 simulation = simulation,
                                 ensemble = ensemble,
                                 group = group,
                                 file_id = file_id,
                                 variable = "ua")
    va_df = database.select_by_dict(cmip6_database,
                              model = model,
                              simulation = simulation,
                              ensemble = ensemble,
                              group = group,
                              file_id = file_id,
                              variable = "va")
    
    # initialize the filename strings to be empty
    qa_file = ""
    ua_file = ""
    va_file = ""
    
    # extract the file name
    # sort by "version" and use the latest version of each file
    if len(qa_df) > 0:
        qa_file = database.reconstruct_path(qa_df.sort_values(by = "version")).iloc[-1]
    if len(ua_df) > 0:
        ua_file = database.reconstruct_path(ua_df.sort_values(by = "version")).iloc[-1]
    if len(va_df) > 0:
        va_file = database.reconstruct_path(va_df.sort_values(by = "version")).iloc[-1]
        
    triplet_line = ",".join([qa_file, ua_file, va_file])
    
    return triplet_line


pool = schwimmbad.MPIPool()
if pool.is_master():
    print("Pool starting: searching through {} files".format(len(cmip6_native_levs)))
else:
    pool.wait()
    sys.exit(0)

# use mpi parallelism to search for matching files in the CMIP6 database
triplet_file_lines = pool.map(find_matching_files, cmip6_native_levs.groupby(by = ["model", "simulation", "ensemble", "group", "file_id"]))
pool.close()
print("Pool finished")
        
# write a csv file, where each row is a set of files to process for IVT and IWV
triplet_file_string = "\n".join(triplet_file_lines)
yymmdd_string = dt.datetime.today().strftime("%Y%m%d")
with open("cmip6_artmip_files_to_process_{}.csv".format(yymmdd_string), 'w') as fout:
          fout.write(triplet_file_string)

