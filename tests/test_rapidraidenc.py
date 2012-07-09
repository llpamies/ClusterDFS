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
        config = DataNodeConfig.from_dict({'datadir':'/home/ubuntu/data/','isolated':True, 'port':3900})        

        
        h = logging.FileHandler('/tmp/datanode%i.log'%i) 
        h.setFormatter(logging.Formatter("datanode%02d %%(created)f %%(levelname)s: %%(name)s - %%(message)s"%i))
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
config = DataNodeConfig.from_dict({'datadir':'/home/ubuntu/data/','isolated':True, 'port':3900})  
h = logging.FileHandler('/tmp/test.log') 
h.setFormatter(logging.Formatter("%(levelname)s: %(name)s - %(message)s"))
logger.addHandler(h)
try:
    time.sleep(2)
    n = 16
    block_id = 'girl.64mb'
    nodes = nodes=';'.join(map(str, (('localhost',3900+i) for i in xrange(16))))

    t = time.time()
    client = DataNodeClient('localhost', config.port+15)
    client.coding(block_id, 'test', nodes)
    #client.coding(block_id, 'enc_node15', nodes)
    print 'Encoding time: %.2fs'%(time.time()-t)
    '''  
    client = DataNodeClient('localhost', config.port+10)
    client.coding(block_id, 'dec_node10', nodes)
    print 'Decoding time: %.2fs'%(time.time()-time)
    '''
    
finally:
    time.sleep(1)
    print 'killing'
    for child in children:
        os.kill(child, signal.SIGINT)
    