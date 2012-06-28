import os
import sys
sys.path.append('./lib/')
import time
import signal
import logging
        
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
        
n = 16

children = []
for i in xrange(n):
    child = os.fork()
    if child:
        children.append(child)
    else:
        from clusterdfs.datanode import DataNodeConfig, DataNode, DataNodeClient
        config = DataNodeConfig.from_dict({'datadir':'tests/data/','isolated':True, 'port':3900})        

        h = logging.FileHandler('/tmp/datanode%d.log'%i) 
        h.setFormatter(logging.Formatter("%(levelname)s: %(name)s - %(message)s"))
        logger.addHandler(h)

        config.port += i
        datanode = DataNode(config)
        try:
            datanode.init()
            datanode.finalize()
        except KeyboardInterrupt:
            print 'finalizing.....'
            datanode.finalize()
        finally:
            sys.exit(0)
        assert False

from clusterdfs.datanode import DataNodeConfig, DataNode, DataNodeClient
config = DataNodeConfig.from_dict({'datadir':'tests/data/','isolated':True, 'port':3900})  
h = logging.FileHandler('/tmp/test.log') 
h.setFormatter(logging.Formatter("%(levelname)s: %(name)s - %(message)s"))
logger.addHandler(h)
try:
    time.sleep(2)
    block_id = 'none'

    #client = DataNodeClient('localhost', config.port+15)
    #client.coding(block_id, 'enc_node15')
    
    client = DataNodeClient('localhost', config.port+10)
    client.coding(block_id, 'dec_node10')
    
finally:    
    print 'killing'
    for child in children:
        os.kill(child, signal.SIGINT)
    