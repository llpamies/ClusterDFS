#!/usr/bin/env python

import io
import os
import os.path
import errno
import socket
import logging
import argparse

from namenode import NameNodeHeader
from networking import *
from bufferedio import FileBufferedIO, SocketBufferedIO

class DataNodeConfig(object):
    port = 7777
    bind_addr = '0.0.0.0'
    datadir = 'datadir/'
    namenode_addr = 'localhost'
    namenode_port = 7770
    ping_timeout = 10

    def __init__(self, args):
        for k, v in args.__dict__.iteritems():
            if v!=None: self.__dict__[k] = v

        if not self.datadir.endswith('/'):
            self.datadir = self.datadir+'/'

class DataNodeHeader(object):
    OP_STORE = 0
    OP_RETRIEVE = 1
    OP_REMOVE = 2

class DataNodeStore(ServerHandle):
    def process_query(self):
        if self.header['op']==DataNodeHeader.OP_STORE:
            return self.store_block()
        elif self.header['op']==DataNodeHeader.OP_RETRIEVE:
            return self.retrieve_block()
        else:
            assert False
    
    def forward_block(self, dst_fd, dst_node):
        def inner(data):
            if dst_node: dst_node.socket.sendall(data)
            dst_fd.write(data)
        return inner

    def store_block(self):
        # Read block properties
        block_id = self.header['id']
        block_size = self.header['length']
        logging.info("Receiving block '%s' (%d bytes) from %s.", block_id, block_size, self.address)

        # Get the forward list and the next forward node
        next_node = None
        next_forward_list = []
        if 'fwdlist' in self.header:
            forward_list = self.header['fwdlist']
            if forward_list:
                logging.info("Forwarding '%s' to %s.", block_id, repr(forward_list[0]))
                logging.info("Remaining forwards: %d.", len(forward_list)-1)
                next_node = Client(*forward_list[0])
                next_forward_list = forward_list[1:]

        # Send header to next node
        if next_node:
            header = self.header.copy()
            header['fwdlist'] = next_forward_list
            next_node.send(header)

        # Destination file.
        dst_fd = io.open(os.path.join(self.server.config.datadir, block_id), 'wb')

        try:
            # Process incoming data
            SocketBufferedIO(self.socket, block_size, self.forward_block(dst_fd, next_node)).run()
            
            # Receive response from next_node
            if next_node:
                response = next_node.recv()
                if response['code']==ServerResponse.RESPONSE_OK:
                    logging.info("Block '%s' (%d bytes) stored & forwarded successfully."%(block_id, block_size))
                    return ServerResponse.ok(msg='Block stored & forwarded successfully.')
                else:
                    return response
            else:
                logging.info("Block '%s' (%d bytes) stored successfully."%(block_id, block_size))
                return ServerResponse.ok(msg='Block stored successfully.')

        except IOError:
            logging.info("Transmission from %s failed.", self.address)
            return ServerResponse.error(msg='Transmission failed.')

        finally:
            # Clean-up resources
            if next_node: next_node.kill()
            dst_fd.close()

    def retrieve_block(self):
        # Read block properties
        block_id = self.header['id']
        block_path = os.path.join(self.server.config.datadir, block_id)
        block_size = os.path.getsize(path)
        block_offset = self.header['offset'] if ('offset' in self.header) else 0
        block_length = self.header['length'] if ('length' in self.header) else block_size

        # Do error control
        if block_length+block_offset < block_size:
            return ServerResponse.error(msg='The requested data is larger than block_size.')

        # Measuring size
        logging.info("Sending block '%s' (%d bytes, %d offset) to %s."%(block_id, block_length, block_offset, self.address))
    
        # Send block size
        self.send(block_length)

        # Process block
        for data in FileIterable(path):
            self.socket.sendall(data)

        return ServerResponse.ok(msg='Block retrieved successfully.')

    def retrieve_block_callback(self, data):
        """Called by FileBufferedIO in :py:meth:retrieve_block"""
        self.socket.sendall(data)

class DataNodeNotifier(object):
    def __init__(self, config, server):
        self.config = config
        self.server = server
        self.process = gevent.spawn(self.timeout)
        self.ping = {'op':NameNodeHeader.OP_PING, 'datanode_port':self.config.port}

    def stop(self):
        self.process.kill()

    def timeout(self):
        while True:
            # send ping
            try:
                logging.debug('Sending ping.')
                ne = NetworkEndpoint(gevent.socket.create_connection((self.config.namenode_addr, self.config.namenode_port)))
                ne.send(self.ping)
                ne.send([])
                response = ne.recv()
                if response['code']!=ServerResponse.RESPONSE_OK:
                    logging.error('Error delivering ping to nameserver: %s', response['msg']) 

            except socket.error, (value,message):
                logging.error("Error delivering ping to nameserver: %s."%(message))
            
            # sleep timeout
            gevent.sleep(self.config.ping_timeout)

class DataNode(Server):
    def __init__(self, config):
        self.config = config
        logging.info("Configuring DataNode to listen on localhost:%d"%(self.config.port))
        Server.__init__(self, DataNodeStore, port=self.config.port)
        self.notifier = DataNodeNotifier(self.config, self)
        self.lock_file = os.path.join(self.config.datadir, '.lock')

    def init(self):
        self.lock_datadir()
        self.serve()

    def finalize(self):
        self.notifier.stop()
        self.unlock_datadir()

    def lock_datadir(self):
        logging.info("Locking %s", self.lock_file)

        if not os.path.exists(self.config.datadir):
            raise Exception("DataNode cannot lock data dir (invalid datadir).")

        elif os.path.exists(self.lock_file):
            raise Exception("DataNode cannot lock data dir (locked dir).")

        else:
            open(self.lock_file, 'w').close()

    def unlock_datadir(self):
        assert os.path.exists(self.lock_file)
        logging.info("Unlocking %s", self.lock_file)
        os.remove(self.lock_file)
        
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)

    from processname import setprocname
    if not setprocname():
        logging.error('Cannot change the process for %s.', __file__)

    parser = argparse.ArgumentParser(description='DataNode')
    parser.add_argument('-d', action="store", default=None, dest="datadir", type=str, help="Directory to store raw data.")
    parser.add_argument('-l', action="store", default="0.0.0.0", dest="bind_addr", type=str, help="DataNode binding address.")
    parser.add_argument('-p', action="store", default=None, dest="port", type=int, help="Port where DataNode listens.")
    parser.add_argument('-na', action="store", default='localhost', dest="namenode_addr", type=str, help="Address of the NameNode.")
    parser.add_argument('-np', action="store", default=7770, dest="namenode_port", type=int, help="Port of the NameNode.")
    config = DataNodeConfig(parser.parse_args())

    try:
        dn = DataNode(config)
        dn.init()
        dn.finalize()
    except KeyboardInterrupt:
        logging.info("Finalizing DataNode...")
        dn.finalize()
    except Exception:
        logging.error("Fatal Error!!")
        raise
