import os
import sys
sys.path.append('./lib/')
import time
import signal
import logging

from clusterdfs.datanode import DataNodeConfig, DataNode, DataNodeClient

config = DataNodeConfig.from_dict({'datadir':'/tmp/','isolated':True, 'port':4426})

print 'generating file...'
os.system("dd if=/dev/urandom of=/tmp/_test_random_file_64MB_a bs=5000000 count=1 &> /dev/null")

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
    
pid = os.fork() 
if pid==0:
    h = logging.FileHandler('/tmp/datanode.log') 
    h.setFormatter(logging.Formatter("%(levelname)s: %(name)s - %(message)s"))
    logger.addHandler(h)
        
    datanode = DataNode(config)
    try:
        datanode.init()
        datanode.finalize()
    except KeyboardInterrupt:
        print 'finalizing.....'
        datanode.finalize()
else:
    h = logging.FileHandler('/tmp/test.log') 
    h.setFormatter(logging.Formatter("%(levelname)s: %(name)s - %(message)s"))
    logger.addHandler(h)
    
    time.sleep(3)
    client = DataNodeClient('localhost', config.port)
    client.retrieve('_test_random_file_64MB_a','/tmp/_test_random_file_64MB_b')
    
    if not os.system('diff /tmp/_test_random_file_64MB_a /tmp/_test_random_file_64MB_b &> /dev/null'):
        print 'ok'
        
    os.kill(pid, signal.SIGINT)