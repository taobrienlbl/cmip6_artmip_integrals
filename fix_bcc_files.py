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
    cmip6_list_file = "fix_bcc_list_20191007.csv"
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
        output_files = calculate_artmip_vertical_integrals(triplet, do_clobber = True)
    except: 
        traceback.print_exc()
        smpi.pprint("Skipping ahead b/c calculation failed on `{}`".format(triplet))
    output_file_lists.append(output_files)
