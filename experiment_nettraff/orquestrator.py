import random
import gevent
from clusterdfs.networking import Client, NetworkHeader

def repair_block(repairing_node, repairing_from, block_size):
    print repairing_node, repairing_from, block_size
    try:
        c = Client(*repairing_node)
    except:
        print repairing_node, 'connection error'
        return
    header = {'nodes':repairing_from, 'length':block_size}
    c.send(header)
    res = c.recv()
    print repairing_node, res

num_nodes = 51
node_names = ['thinclient-%02d'%i for i in xrange(num_nodes)]
data_nodes = [(n,8888) for n in node_names]
bulk_nodes = [(n,9999) for n in node_names]
#storage_per_node = 1*1000*1000*1000*1000 #(1TB)
storage_per_node = 10*1000*1000*1000 #(1GB)
block_size = 100*1000*1000 #(100MB)
n = 8 # using a (8,3) RS code
k = 3
redundancy = float(n)/k 
blocks_per_node = storage_per_node/block_size
num_blocks = blocks_per_node*num_nodes

processes = []
for block_id in xrange(blocks_per_node):
    repairing_node = random.choice(bulk_nodes)
    repairing_from = random.sample(data_nodes, k) 
    processes.append(gevent.spawn(repair_block, repairing_node, repairing_from, block_size))
gevent.joinall(processes)
