#!/bin/bash

cd $HOME/Projects/ClusterDFS
nohup ./bin/datanode --optimize -d /localdata -i True -p 7777 &> /localdata/datanode.out < /dev/null &
#nohup ./bin/datanode -d /localdata -i True -p 7777 &> /localdata/datanode.out < /dev/null &
sleep 1
