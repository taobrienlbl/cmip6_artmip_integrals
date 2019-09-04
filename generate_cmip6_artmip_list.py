#!/usr/bin/env python
# coding: utf-8
import progressbar
import database
import datetime as dt

# get the list of CMIP6 runs with hus at 6 hourly output on native model levels
cmip6_database = database.load()
cmip6_historical_plevs = database.select_by_dict(cmip6_database,
                                                 variable = "hus",
                                                 simulation = "historical",
                                                 group = "6hrLev")

bar = progressbar.ProgressBar(maxval=len(cmip6_historical_plevs), \
        widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
bar.start()

triplet_file_lines = []
i = 0
for run in cmip6_historical_plevs.groupby(by = ["model", "simulation", "ensemble", "group", "file_id"]):
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
    
    triplet_file_lines.append(triplet_line)
    
    i += 1
    bar.update(i)

bar.finish()
        
# write a csv file, where each row is a set of files to process for IVT and IWV
triplet_file_string = "\n".join(triplet_file_lines)
yymmdd_string = dt.datetime.today().strftime("%Y%m%d")
with open("cmip6_artmip_files_to_process_{}.csv".format(yymmdd_string), 'w') as fout:
          fout.write(triplet_file_string)

