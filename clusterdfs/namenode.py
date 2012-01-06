#!/usr/bin/env python

import time
import logging
import argparse
import socket
import collections
from networking import *

class NameNodeConfig(object):
    port = 7770

    def __init__(self, args):
        for k, v in args.__dict__.iteritems():
            if v!=None: self.__dict__[k] = v

class NameNodeHeader:
    OP_PING = 0
    OP_GETNODES = 1

class NameNodeQuery(ServerHandle):
    def get_address(self):  
        if self.address[0].startswith('127.'):
            return self.local_address()
        else:
            return self.address[0]

    def process_query(self):
        if self.header['op']==NameNodeHeader.OP_PING:
            return self.ping()
        elif self.header['op']==NameNodeHeader.OP_GETNODES:
            return self.getnodes()
        else:
            assert False

    def ping(self):
        datanode_addr = self.get_address(), self.header['datanode_port']
        logging.debug('Receiving ping from %s', datanode_addr)

        if datanode_addr not in self.server.db_pings:
            self.server.db_nodes.append(datanode_addr)
        self.server.db_pings[datanode_addr] = time.time()

        stored_blocks = self.recv()
        for file_uuid, block_uuid in stored_blocks:
            self.server.db_direct_lookup[file_uuid][block_uuid] = datanode_addr
            self.server.db_reverse_lookup[datanode_addr].add((file_uuid, block_uuid))
        
        return ServerResponse.ok(msg='Blocks processed.')

    def getnodes(self):
        num_nodes = self.headers['numnodes']
        sample = random.sample(self.server.db_nodes, num_nodes)
        self.send(sample)
        return ServerResponse.ok(msg='List found.')

class NameNode(Server):
    def __init__(self, config):
        self.config = config
        logging.info("Configuring NameNode to listen on localhost:%d"%(self.config.port))
        Server.__init__(self, NameNodeQuery, port=self.config.port)

        self.db_nodes = []
        self.db_direct_lookup = collections.defaultdict(dict)
        self.db_reverse_lookup = collections.defaultdict(set)
        self.db_pings = {}

    def init(self):
        self.serve()

    def finalize(self):
        logging.info("Finalizing NameNode...")
        pass

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)

    from processname import setprocname
    if not setprocname():
        logging.error('Cannot change the process for %s.', __file__)

    parser = argparse.ArgumentParser(description='NameNode: tracker for datanodes.')
    parser.add_argument('-p', action="store", default=None, dest="port", type=int, help="Port where NameNode listens.")
    config = NameNodeConfig(parser.parse_args())

    nn = NameNode(config)
    try:
        nn.init()
    except KeyboardInterrupt:
        nn.finalize()
