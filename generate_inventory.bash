#!/bin/bash
#BASE_DIR=/project/projectdirs/m1517/cascade/taobrien/artmip/tier2/ARTMIP_CMIP6/
BASE_DIR=/global/cscratch1/sd/taobrien/ARTMIP_CMIP6
INVENTORY_FILE=cmip6_artmip_inventory_$(date +%Y%m%d).txt
find ${BASE_DIR} -name \*.nc \
    | grep windhusavi \
    > ${INVENTORY_FILE}
find ${BASE_DIR} -name \*.nc \
    | grep uhusavi \
    >> ${INVENTORY_FILE}
find ${BASE_DIR} -name \*.nc \
    | grep vhusavi \
    >> ${INVENTORY_FILE}
find ${BASE_DIR} -name \*.nc \
    | grep prw \
    >> ${INVENTORY_FILE}
