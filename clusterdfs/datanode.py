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
from bufferedio import FileInputStream, FileOutputStream, SocketInputStream, SocketOutputStream

class DataNodeConfig(object):
    port = 7777
    bind_addr = '0.0.0.0'
    datadir = 'datadir/'
    namenode_addr = 'localhost'
    namenode_port = 7770
    ping_timeout = 10
    isolated = False

    def __init__(self, args):
        for k, v in args.__dict__.iteritems():
            if v!=None: self.__dict__[k] = v

        if not self.datadir.endswith('/'):
            self.datadir = self.datadir+'/'

class DataNodeHeader(object):
    OP_STORE = 0
    OP_RETRIEVE = 1
    OP_REMOVE = 2

class DataNodeQuery(ServerHandle):
    def process_query(self):
        if self.header['op']==DataNodeHeader.OP_STORE:
            return self.store_block()
        elif self.header['op']==DataNodeHeader.OP_RETRIEVE:
            return self.retrieve_block()
        else:
            assert False
    
    def store_block(self):
        # Should be xor the content?
        xor = self.header['xor'] if 'xor' in self.header else False

        # Read block properties
        block_id = self.header['id']
        block_path = os.path.join(self.server.config.datadir, block_id)
        block_size = self.header['length']

        # Check headers
        if block_size<=0:
            return ServerResponse.error(msg='Block size be larger than zero.')

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

        block_input_stream = None
        local_block_output_stream = None

        try:
            # prepare streams...
            block_input_stream = SocketInputStream(self.socket, block_size)
            if xor:
                local_block_output_stream = XOROutputStream(block_path)
            else:
                local_block_output_stream = FileOutputStream(block_path)

            if next_node:
                # Send header to next node
                header = self.header.copy()
                header['xor'] = xor
                header['fwdlist'] = next_forward_list
                next_node.send(header)
            
                # Prepare stream
                next_node_output_stream = SocketOutputStream(next_node.socket)
                
                # store and send to next node
                block_input_data.sendto(local_block_output_stream, next_node_output_stream)
            
                # Receive response from next_node
                response = next_node.recv()
                if response['code']==ServerResponse.RESPONSE_OK:
                    logging.info("Block '%s' (%d bytes) stored & forwarded successfully."%(block_id, block_size))
                    return ServerResponse.ok(msg='Block stored & forwarded successfully.')
                else:
                    return response

            else:
                # store only
                block_input_stream.sendto(local_block_output_stream)
            
                logging.info("Block '%s' (%d bytes) stored successfully."%(block_id, block_size))
                return ServerResponse.ok(msg='Block stored successfully.')

        except IOError:
            logging.info("Transmission from %s failed.", self.address)
            return ServerResponse.error(msg='Transmission failed.')

        finally:
            # Release sockets and close files
            if next_node:
                next_node.kill() # there is no need to close output_stream since endpoint does it.
            if local_block_output_stream:
                local_block_output_stream.close()

    def retrieve_block(self):
        # Should be xor the content?
        xor = self.header['xor'] if 'xor' in self.header else False

        # Read block properties
        block_id = self.header['id']
        block_path = os.path.join(self.server.config.datadir, block_id)
        block_size = os.path.getsize(block_path)
        block_offset = self.header['offset'] if ('offset' in self.header) else 0
        block_length = self.header['length'] if ('length' in self.header) else block_size

        # Do error control
        if block_length+block_offset < block_size:
            return ServerResponse.error(msg='The requested data is larger than block_size.')

        logging.info("Sending block '%s' (%d bytes, %d offset) to %s."%(block_id, block_length, block_offset, self.address))
    
        # Send block size
        # TODO: Send block header instead of block size!
        self.send(block_length)

        try:
            # Send block data
            block_finput_stream = FileInputStream(block_path, block_length)
            local_block_output_stream = SocketOutputStream(self.socket)
            block_finput_stream.sendto(local_block_output_stream)
            return ServerResponse.ok(msg='Block retrieved successfully.')

        except IOError:
            logging.info("Transmission from %s failed.", self.address)
            return ServerResponse.error(msg='Transmission failed.')
        
        finally:
            # there is no need to close output_stream since endpoint does it.
            block_finput_stream.close()

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
        Server.__init__(self, DataNodeQuery, port=self.config.port)
        if not self.config.isolated:
            self.notifier = DataNodeNotifier(self.config, self)

    def init(self):
        self.serve()

    def finalize(self):
        if not self.config.isolated:
            self.notifier.stop()
