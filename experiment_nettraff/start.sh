#!/bin/bash

cd $HOME/Projects/ClusterDFS
nohup python bin/datanode -d /localdata -i True -p 8888 &> /tmp/datanode.out < /dev/null &
nohup python experiment_nettraff/bulkmoves.py &> /tmp/bulkmoves.out < /dev/null &
sleep 1
