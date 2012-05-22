import sys
import numpy
import itertools
import gevent
import gevent.queue
import logging
import cPickle as pickle

from common import *
from headers import *
from bufferedio import *
from networking import Client
from galoisarray import GaloisArray

#coding = sys.modules[__name__]

class CodingException(Exception):
    pass

@ClassLogger
class RemoteNetCoding(Client):
    def __init__(self, operations):
        self.header = {'op':DataNodeHeader.OP_CODING, 'coding':operations.serialize()}
        super(RemoteNetCoding, self).__init__(*operations.node_address)

    def get_stream(self):
        self.send(self.header)
        stream = self.recv()
        if not isinstance(stream, InputStream):
            raise TypeError("An InputStream was expected.")
        return stream

    def execute(self):
        self.send(self.header)
        ack = self.recv()
        if type(ack)!=bool or (not ack):
            raise CodingException("Failed to receive remote coding ACK.")

@ClassLogger
class NetCodingExecutor(object):
    def __init__(self, operations, block_store):
        self.operations = operations
        self.block_store = block_store

        # Create dictionaries
        self.buffers = {}
        self.clients = {}
        self.streams = {}
        self.readers = {}
        self.writers = {}

        self.non_disposable_buffers = set()
        self.size = None

        for stream in self.operations.streams:
            ts = type(stream)
            if ts==tuple:
                if stream[1]=='r':
                    self.streams[stream[0]] = self.block_store.get_input_stream(stream[0])
                    self.readers[stream[0]] = iter(InputStreamReader(self.streams[stream[0]], debug_name=stream[0]))
                    if self.size==None:
                        self.size = self.streams[stream[0]].size
                        
                    elif self.size!=self.streams[stream[0]].size:
                        raise CodingException('The streams in NetCodingInputStream are not aligned.')
                        
                elif stream[1]=='w':
                    self.streams[stream[0]] = self.block_store.get_output_stream(stream[0])
                    self.writers[stream[0]] = OutputStreamWriter(self.streams[stream[0]])

                else:
                    raise TypeError('Incompatible stream mode "%s". Should be "r" or "w".'%(str(stream[1])))

            elif ts==NetCodingOperations:
                remote_coding = RemoteNetCoding(stream)
                self.streams[stream] = remote_coding.get_stream()
                self.readers[stream] = iter(InputStreamReader(self.streams[stream], debug_name=stream))
                self.clients[stream] = remote_coding

                if self.size==None:
                    self.size = self.streams[stream].size
                    
                elif self.size!=self.streams[stream].size:
                    raise CodingException('The streams in NetCodingInputStream are not aligned.')

            else:
                raise TypeError('Invalid operation stream. Found "%s" and should be a (str,str)-tuple or NetCodingOperations instance)'%str(stream))

    def finalize(self):
        if __debug__: self.logger.debug('Finalizing coding...')
        
        if __debug__: self.logger.debug('Waiting for writers..')
        for writer in self.writers.itervalues():
            writer.join()

        if __debug__: self.logger.debug('Finalizing streams..')
        for stream in self.streams.itervalues():
            self.logger.debug(stream)
            stream.finalize()

        if __debug__: self.logger.debug('Killing clients..')
        for client in self.clients.itervalues():
            client.kill()
        
    def execute_instruction(self, instruction):
        if __debug__: self.logger.debug('NetCodingInputStream is processing instruction %s', str(instruction))
       
        bytes_processed = None

        if instruction[0]=='COPY':
            dst_buffer = self.buffers[instruction[1]]
            src_buffer = self.buffers[instruction[2]]
            src_buffer.copy_to(dst_buffer)
            bytes_processed = dst_buffer.length

        elif instruction[0]=='LOAD':
            self.buffers[instruction[1]] = self.readers[instruction[2]].next()
            bytes_processed = self.buffers[instruction[1]].length
        
        elif instruction[0]=='WRITE':
            src_buffer = self.buffers[instruction[1]]
            dst_stream = self.writers[instruction[2]]
            dst_stream.write(src_buffer)
            self.non_disposable_buffers.add(src_buffer)
            bytes_processed = src_buffer.length

        elif instruction[0]=='MULADD':
            src_buffer = self.buffers[instruction[3]]
            literal_value = instruction[2]
            dst_buffer = self.buffers[instruction[1]]
            
            if src_buffer.length==0:
                raise CodingException('Empty buffer.')
            if src_buffer.size!=dst_buffer.size:
                self.logger.error('Buffer sizes are not aligned.')
                raise CodingException('Buffers sizes are not aligned.')

            src = GaloisArray(src_buffer.size/2, bitfield=16, buffer=src_buffer.buff)
            dst = GaloisArray(dst_buffer.size/2, bitfield=16, buffer=dst_buffer.buff)
            src.multadd(literal_value, dst, add=True)
            dst_buffer.length = src_buffer.length
            bytes_processed = dst_buffer.length

        elif instruction[0]=='MULT':
            src_buffer = self.buffers[instruction[3]]
            literal_value = instruction[2]
            dst_buffer = self.buffers[instruction[1]]
           
            if src_buffer.length==0:
                raise CodingException('Empty buffer.')
            if src_buffer.size!=dst_buffer.size:
                self.logger.error('Buffer sizes are not aligned.')
                raise CodingException('Buffer sizes are not aligned.')

            src = GaloisArray(src_buffer.size/2, bitfield=16, buffer=src_buffer.buff)
            dst = GaloisArray(dst_buffer.size/2, bitfield=16, buffer=dst_buffer.buff)
            src.multadd(literal_value, dst, add=False)
            dst_buffer.length = src_buffer.length
            bytes_processed = dst_buffer.length

        else:
            assert False, 'Invalid coding instruction.'

        if bytes_processed==None:
            raise CodingException('The number of processed bytes was not set.')

        return bytes_processed

    def execute_step(self, output_buffer=None):
        bytes_processed = None

        self.non_disposable_buffers.clear()
        self.buffers.clear()
        
        if self.operations.output_buffer!=None:
            if output_buffer!=None:
                self.buffers[self.operations.output_buffer] = output_buffer
                self.non_disposable_buffers.add(output_buffer)
            else:
                assert False

        for instruction in self.operations.instructions:
            bp = self.execute_instruction(instruction)
            if bytes_processed!=None and bytes_processed!=bp:
                raise CodingException('Buffer sizes are not aligned.')
            bytes_processed = bp

        # Requeue all buffers back to their reader queues.
        for name, iobuffer in self.buffers.iteritems():
            if iobuffer not in self.non_disposable_buffers:
                if __debug__: self.logger.debug('Resseting "%s" buffer.', name)
                iobuffer.reset()

        if __debug__: self.logger.debug('Coding processed %d bytes.', bytes_processed)
        return bytes_processed

    def execute(self):
        read = 0
        while read<self.size:
            if __debug__: self.logger.debug('execute iter %d/%d', read, self.size)
            read += self.execute_step()
        self.finalize()

@ClassLogger
class NetCodingInputStream(InputStream):
    def __init__(self, executor):
        self.executor = executor
        super(NetCodingInputStream, self).__init__(self.executor.size)

    def finalize(self):
        self.executor.finalize()

    def read(self, iobuffer, nbytes=None):
        if nbytes==None:
            nbytes = iobuffer.size
        else:
            nbytes = min(iobuffer.size, nbytes)

        if __debug__: self.logger.debug('Reading %d bytes from NetCodingInputStream %s.', nbytes, hex(id(self)))
        num_read = self.executor.execute_step(iobuffer)
        if __debug__: self.logger.debug('Coding processed %d bytes.', num_read)
        assert num_read==iobuffer.length, (num_read,iobuffer.length)
        return num_read

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

    def is_stream(self):
        return self.output_buffer!=None

    @staticmethod
    def unserialize(s):
        return pickle.loads(s)

    @staticmethod
    def op_readblock(node_address, block_id):
        op = NetCodingOperations(node_address, [block_id], 'buff')
        op.add('READ', 'buff', block_id)
        return op
