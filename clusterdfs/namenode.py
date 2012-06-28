#!/usr/bin/env python

import time
import uuid
import shelve
import logging
import argparse
import socket
import collections

from common import Config
from headers import *
from networking import *

class NameNodeConfig(Config):
    port = 7770
    tree_file = 'namenode_tree.shelve'
    meta_file = 'namenode_meta.shelve'
    
    def check(self):
        pass

NameNodeTreeNode = collections.namedtuple('NameNodeTreeNode', ['id', 'name', 'children', 'parent', 'dir'])    

class NameNodeTreeException(Exception):
    pass

class NameNodeTree(object):
    rootname = '__root__'
    
    def __init__(self, config):
        self.dict = shelve.open(config.tree_file)
        if self.rootname not in self.dict:
            self.store(NameNodeTreeNode(id=self.rootname, name=None, children={None:self.rootname}, parent=None, dir=True))
        self.root = self.dict[self.rootname] 
    
    def store(self, node):
        self.dict[node.id] = node
    
    def parse(self, path, parse_last=False):
        node = self.root
        route = path.strip('/')
        if route=='': return node, node if parse_last else None
        route = route.split('/')
        for name in route[:-1]:
            if name not in node.children:
                raise NameNodeTreeException("Incorrect path %s."%(path))
            node = self.dict[node.children[name]]
        
        if parse_last:
            if route[-1] not in node.children:
                raise NameNodeTreeException("Incorrect path %s."%(path))
            return node, self.dict[node.children[route[-1]]]
        else:
            return node, route[-1]
        
    def create(self, path, directory=False):
        parent, name = self.parse(path)
        if not parent.dir: 
            raise NameNodeTreeException('Parent path should be a directory.')
        if name in parent.children: 
            raise NameNodeTreeException('The file exists.')
        new = NameNodeTreeNode(id=uuid.uuid4().hex, name=name, children={}, parent=parent.id, dir=directory)
        parent.children[name] = new.id
        self.store(parent)
        self.store(new)
        self.dict.sync()
      
    def _delete_recursive(self, node):
        for child in node.children.itervalues():
            self._delete_recursive(self.dict[child])
            del self.dict[child]
            
    def delete(self, path, recursive=False):
        parent, node = self.parse(path, True)
        if node.dir and not recursive:
            raise NameNodeTreeException('Attempting to delete a non-empty dir.')
        self.dict.pop(node.id)
        parent.children.pop(node.name)
        self.store(parent)
        if node.dir:
            self._delete_recursive(node)
        self.dict.sync()
        
    def move(self, src, dst):
        parent_src, node_src = self.parse(src, True)
        del parent_src.children[node_src.name]
        self.store(parent_src)
        self.dict.sync()
        
        parent_dst, name_dst = self.parse(dst)
        parent_dst.children[name_dst] = node_src.id
        new = NameNodeTreeNode(id=node_src.id, name=name_dst, children=node_src.children, parent=parent_dst.id, dir=node_src.dir)
        self.store(parent_dst)
        self.store(new)
        self.dict.sync()
        
    def list(self, path):
        parent, node = self.parse(path, True)
        assert node.dir
        path = '/'+path.strip('/')
        return [path + '/' + child.name for child in node.children]

    def _show(self, node, sepc=0):
        sep = '  '*sepc
        for child in node.children.itervalues():
            if child==self.rootname: continue
            childn = self.dict[child]
            if childn.dir:
                print sep, childn.name+'/'
                self._show(childn, sepc+1)
            else:
                print sep, childn.name

    def show(self, path):
        parent, node = self.parse(path, True)
        self._show(node)
    
    def showall(self):
        for node in self.dict.itervalues():
            print node

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

if __name__=='__main__':
    config = NameNodeConfig()

    t = NameNodeTree(config)
    t.showall()
    print ''
    #t.delete('1/2', True)
    '''
    t.create('/1', True)
    t.create('/1/2/', True)
    t.create('/1/2/3', True)
    t.create('/1/2/3/aaa')
    t.create('/1/2/3/bbb')
    t.create('/1/2/3/ccc')
    '''
    t.move('1/2','1/aa')
    
    print ''
    t.showall()
    t.show('/')
    