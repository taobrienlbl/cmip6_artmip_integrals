#!/usr/bin/env python
# coding: utf-8
""" This script uses MPI to parallize the calculation of IWV and IVT on all available CMIP6 data. """

from calculate_artmip_vertical_integrals import calculate_artmip_vertical_integrals
import simplempi.simpleMPI as simpleMPI
import sys
import datetime as dt
import traceback

smpi = simpleMPI.simpleMPI()

if smpi.rank == 0:
    # get the file containing the list of files to run on
    cmip6_list_file = "cmip6_artmip_files_to_process_20190904.csv"
    if len(sys.argv) >= 2:
        cmip6_list_file = sys.argv[1]

    # read the list of files
    with open(cmip6_list_file) as fin:
        triplet_list = fin.readlines()
else:
    triplet_list = None

    
my_triplet_list = smpi.scatterList(triplet_list)

output_file_lists = []
for triplet in my_triplet_list:
    try:
        output_files = calculate_artmip_vertical_integrals(triplet)
    except: 
        traceback.print_exc()
        smpi.pprint("Skipping ahead b/c calculation failed on `{}`".format(triplet))
    output_file_lists.append(output_files)
    
prw_files = []
windhusavi_files = []
uhusavi_files = []
vhusavi_files = []
for file_list in output_file_lists:
    # if only prw was calculated...
    if len(file_list) == 1:
        # append prw to the list of files
        prw_file = file_list[0]
        prw_files.append(prw_file)
    # otherwise..
    else:
        prw_file, windhusavi_file, uhusavi_file, vhusavi_file = file_list
        
        # append all variables to the list of files
        prw_files.append(prw_file)
        windhusavi_files.append(windhusavi_file)
        uhusavi_files.append(uhusavi_file)
        vhusavi_files.append(vhusavi_file)

# write a set of lists of all files produced
with open("prw_files_rank{}_{}.txt".format(smpi.rank, dt.datetime.now().strftime("%Y%m%d")), "w") as fout:
    fout.write("\n".join(prw_files))
with open("windhusavi_files_rank{}_{}.txt".format(smpi.rank, dt.datetime.now().strftime("%Y%m%d")), "w") as fout:
    fout.write("\n".join(windhusavi_files))
with open("uhusavi_files_rank{}_{}.txt".format(smpi.rank, dt.datetime.now().strftime("%Y%m%d")), "w") as fout:
    fout.write("\n".join(uhusavi_files))
with open("vhusavi_files_rank{}_{}.txt".format(smpi.rank, dt.datetime.now().strftime("%Y%m%d")), "w") as fout:
    fout.write("\n".join(vhusavi_files))