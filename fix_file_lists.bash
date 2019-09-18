#!/bin/bash

INPUT_DIR="file_lists"
OUTPUT_DIR="file_lists_1950-2100"
cp -r $INPUT_DIR $OUTPUT_DIR

VARS="prw  uhusavi  vhusavi  windhusavi"

for var in $VARS
do
    ipsl_files=`ls ${OUTPUT_DIR}/${var}/historical/${var}_historical_IPSL-CM6*`
    for file in $ipsl_files
    do
        # use only the last six lines for the IPSL historical file lists
        tail -13 ${file} > tmp.txt
        mv tmp.txt $file
    done

done
