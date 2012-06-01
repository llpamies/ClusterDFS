#!/bin/bash

cd ..
for i in `seq 0 15`; do
    let port=9000+$i
    echo "Starting datanode in localhost:$port"
    ./bin/datanode --optimize -d ./tests/data/ -p $port > ./tests/out$i 2>&1&
    #./bin/datanode -d ./tests/data/ -p $port -i True > ./tests/out$i 2>&1&
done
