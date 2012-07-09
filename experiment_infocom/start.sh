#!/bin/bash
cd ${CLUSTERDFSHOME}

nohup ./bin/datanode --optimize -d ${CLUSTERDFSDATA} -i True -f True > ${CLUSTERDFSLOG} 2>&1 &
#nohup ./bin/datanode -d ${CLUSTERDFSDATA} -i True -f True > ${CLUSTERDFSLOG} 2>&1 &

nohup python experiment_infocom/cauchy_coding > ${CLUSTERDFSLOG}.cauchy 2>&1 &

exit