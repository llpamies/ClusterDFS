import sys
import numpy
import itertools
import gevent
import gevent.queue
import logging
import cPickle as pickle

from headers import *
from bufferedio import *
from networking import Client
from galoisarray import GaloisArray

#coding = sys.modules[__name__]

class CodingException(Exception):
    pass

class RemoteNetCodingStream(Client):
    def __init__(self, operations):
        self.header = {'op':DataNodeHeader.OP_CODING, 'coding':operations.serialize()}
        super(RemoteNetCodingStream, self).__init__(*operations.node_address)

    def get_stream(self):
        self.send(self.header)
        stream = self.recv()
        if not isinstance(stream, InputStream):
            raise TypeError("An InputStream was expected.")
        return stream

class NetCodingInputStream(InputStream):
    def __init__(self, operations, block_store):
        self.operations = operations
        self.block_store = block_store

        # Create dictionaries
        self.clients = {}
        self.streams = {}
        self.readers = {}
        self.writers = {}

        temp_size = None

        for stream in self.operations.streams:
            ts = type(stream)
            if ts==tuple:
                if stream[1]=='r':
                    self.streams[stream[0]] = self.block_store.get_input_stream(stream[0])
                    self.readers[stream[0]] = InputStreamReader(self.streams[stream[0]])
                    if temp_size==None:
                        temp_size = self.streams[stream[0]].size
                        
                    elif temp_size!=self.streams[stream[0]].size:
                        raise CodingException('The streams in NetCodingInputStream are not aligned.')
                        
                elif stream[1]=='w':
                    self.streams[stream[0]] = self.block_store.get_output_stream(stream[0])
                    self.writers[stream[0]] = OutputStreamWriter(self.streams[stream[0]])

                else:
                    raise TypeError('Incompatible stream mode "%s". Should be "r" or "w".'%(str(stream[1])))

            elif ts==NetCodingOperations:
                remote_coding = RemoteNetCodingStream(stream)
                self.streams[stream] = remote_coding.get_stream()
                self.readers[stream] = InputStreamReader(self.streams[stream])
                self.clients[stream] = remote_coding

            else:
                raise TypeError('Invalid operation stream. Found "%s" and should be a (str,str)-tuple or NetCodingOperations instance)'%str(stream))

        super(NetCodingInputStream, self).__init__(temp_size)

    def has_output(self):
        return self.operations.output_buffer!=None

    def finalize(self):
        for stream in self.streams.itervalues():
            logging.debug(stream)
            stream.finalize()

        for client in self.clients.itervalues():
            client.kill()

    def read(self, iobuffer, nbytes=None):
        if nbytes==None:
            nbytes = iobuffer.size
        else:
            nbytes = min(iobuffer.size, nbytes)

        logging.debug('Reading %d bytes from NetCodingInputStream.', nbytes)

        non_disposable_buffers = set()
        buffers = {}

        if self.operations.output_buffer!=None:
            logging.debug('Output buffer is named "%s".', self.operations.output_buffer)
            buffers[self.operations.output_buffer] = iobuffer
            non_disposable_buffers.add(iobuffer)

        for instruction in self.operations.instructions:
            logging.debug('NetCodingInputStream is processing instruction %s', str(instruction))

            if instruction[0]=='COPY':
                dst_buffer = buffers[instruction[1]]
                src_buffer = buffers[instruction[2]]
                #HERE

            elif instruction[0]=='READ':
                dst_buffer = instruction[1]
                src_stream = self.readers[instruction[2]]
                buffers[dst_buffer] = src_stream.read()
                logging.debug('READ done with %d bytes.', buffers[dst_buffer].length)
            
            elif instruction[0]=='WRITE':
                src_buffer = buffers[instruction[1]]
                dst_stream = self.writers[instruction[2]]
                dst_stream.write(src_buffer)
                non_disposable_buffers.add(src_buffer)

            elif instruction[0]=='MULADD':
                src_buffer = buffers[instruction[1]]
                literal_value = instruction[2]
                if len(instruction)>3:
                    dst_buffer = buffers[instruction[3]]
                else:
                    dst_buffer = src_buffer
                
                if src_buffer.size!=dst_buffer.size:
                    raise CodingException('Buffers sizes are not aligned.')

                src = GaloisArray(src_buffer.size/2, bitfield=16, buffer=src_buffer.buff)
                dst = GaloisArray(dst_buffer.size/2, bitfield=16, buffer=dst_buffer.buff)
                src.multadd(literal_value, dst)
                dst_buffer.size = src_buffer.size

            elif instruction[0]=='MULT':
                src_buffer = buffers[instruction[1]]
                literal_value = instruction[2]
                if len(instruction)>3:
                    dst_buffer = buffers[instruction[3]]
                else:
                    dst_buffer = src_buffer
                
                if src_buffer.size!=dst_buffer.size:
                    raise CodingException('Buffers sizes are not aligned.')

                src = GaloisArray(src_buffer.size/2, bitfield=16, buffer=src_buffer.buff)
                dst = GaloisArray(dst_buffer.size/2, bitfield=16, buffer=dst_buffer.buff)
                src.mult(literal_value, dst)
                dst_buffer.size = src_buffer.size

            else:
                assert False, 'Invalid coding instruction.'

        # Requeue all buffers back to their reader queues.
        logging.debug(non_disposable_buffers)
        logging.debug(buffers)
        for buffname, buffvalue in buffers.iteritems():
            if buffvalue not in non_disposable_buffers:
                logging.debug('Resseting "%s" buffer.', buffname)
                buffvalue.reset()

        logging.debug("Read %d bytes."%(iobuffer.length if self.operations.output_buffer!=None else 0))
        return iobuffer.length if self.operations.output_buffer!=None else 0

class NetCodingOperations(object):
    def __init__(self, node_address, streams, output_buffer=None):
        self.node_address = node_address
        self.streams = streams
        self.output_buffer = output_buffer
        self.instructions = []

    def add(self, *inst):
        self.instructions.append(inst)

    def serialize(self):
        return pickle.dumps(self)

    def has_output(self):
        return self.output_buffer!=None

    @staticmethod
    def unserialize(s):
        return pickle.loads(s)

    @staticmethod
    def op_readblock(node_address, block_id):
        op = NetCodingOperations(node_address, [block_id], 'buff')
        op.add('READ', 'buff', block_id)
        return op
