#!/bin/bash
# this script generates a list of all available CMIP6 files within a CMIP6 mirror directory

CMIP6_BASE_DIR=/global/cscratch1/sd/cmip6/CMIP6/CMIP/
CMIP6_FILE_LIST=cmip6_list_$(date +%Y%m%d).txt
echo "Generating $CMIP6_FILE_LIST"

# search DECK files
find $CMIP6_BASE_DIR -name \*.nc > $CMIP6_FILE_LIST

# search SSP files
CMIP6_BASE_DIR=/global/cscratch1/sd/cmip6/CMIP6/ScenarioMIP/
find $CMIP6_BASE_DIR -name \*.nc >> $CMIP6_FILE_LIST
