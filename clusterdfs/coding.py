import os
import collections
import gevent.queue

from headers import DataNodeHeader
from common import ClassLogger
from bufferedio import InputStreamReader, InputStream
from networking import Client
from galoisbuffer import GaloisBuffer

class CodingException(Exception):
    pass

class TemporalBufferQueue(gevent.queue.Queue):
    queues = {}

    def __init__(self, name, *args, **kwargs):
        super(TemporalBufferQueue, self).__init__(*args, **kwargs)
        TemporalBufferQueue.queues[name] = self
        
    @staticmethod
    def get_temp(name):
        return TemporalBufferQueue.queues[name]

    @staticmethod
    def delete_temp(name):
        TemporalBufferQueue.pop(name)
        
@ClassLogger
class RemoteNetCodingReader(InputStreamReader):
    def __init__(self, node_addr, block_id, coding_id, stream_id,
                  nodes, **kwargs):
        nodes = ';'.join(map(str, nodes))
        self.client = Client(*node_addr)
        self.header = DataNodeHeader.generate(DataNodeHeader.OP_CODING, 
                                              block_id, coding_id, stream_id, 
                                              nodes)
        self.client.send(self.header)
        
        super(RemoteNetCodingReader, self).__init__(self.client.recv_stream(),
                                                    async=True, **kwargs)
    
    def finalize(self, kill=False):
        if not kill:
            self.client.assert_ack()
        super(RemoteNetCodingReader, self).finalize(kill)

@ClassLogger
class NetCodingResolver(object):
    def __init__(self, block_id, stream_id, block_store, nodes):
        self.stream_id = stream_id
        self.block_id = block_id
        self.block_store = block_store
        self.nodes = nodes
    
    def get_enc_node(self, coding_id):
        return self.nodes[coding_id]
    
    def get_reader(self, key):
        assert False, "unimplemented"
    
    def get_writer(self, key):
        assert False, "unimplemented"

@ClassLogger
class NetCodingExecutor(object):
    #FIXME: We need a process to monitor the queues and collect garbage!
    queues = {}
    sizes = {}
    numreg = collections.defaultdict(int)

    def __init__(self, operations, resolver, stream_id, bf=16):
        self.stream_id = stream_id
        self.operations = operations
        self.resolver = resolver
        self.finalized = False
        self.bitfield_op = bf

        # Create dictionaries
        self.buffers = {}
        self.readers = {}
        self.writers = {}
        
        if __debug__: self.logger.debug('New executor for %d %s.', os.getpid(),
                                        self.stream_id)
        if __debug__: self.logger.debug('Processing streams...')
        for stream_name, stream_type in self.operations.streams:
            if __debug__: self.logger.debug('Processing stream: %s.',
                                            unicode(stream_name))
            if stream_type=='r':
                self.readers[stream_name] = self.resolver.\
                get_reader(stream_name)
                    
            elif stream_type=='w':
                self.writers[stream_name] = self.resolver.\
                get_writer(stream_name)

            else:
                raise TypeError('Invalid operation stream.')
        if __debug__: self.logger.debug('Processing streams finished.')
        
        self.size = None
        for reader in self.readers.itervalues():
            s = reader.input_stream.size
            if self.size==None:
                self.size = s
            elif self.size!=s:
                raise CodingException('Reader sizes are not aligned: %d, %d.'\
                                      %(self.size, s))
                
        # If there is no input stream... (no fixed size) we
        # try to get the size from a previous set executor in the same stream.
        if self.size==None:
            self.size = NetCodingExecutor.sizes[self.stream_id]
            
        assert self.size!=None
        
        if self.stream_id not in NetCodingExecutor.queues:
            NetCodingExecutor.queues[self.stream_id] = collections\
            .defaultdict(gevent.queue.Queue)
        
        # Keep track of which iobuffers we have to free here.
        self.disposable_buffers = set()
        
        NetCodingExecutor.sizes[self.stream_id] = self.size
        NetCodingExecutor.numreg[self.stream_id] += 1

    def finalize(self):
        if self.finalized:
            return
        
        self.finalized = True
        
        '''
        NetCodingExecutor.numreg[self.stream_id] -= 1
        if NetCodingExecutor.numreg[self.stream_id] == 0:
            del NetCodingExecutor.sizes[self.stream_id]
            del NetCodingExecutor.queues[self.stream_id]
        '''               
        if __debug__: self.logger.debug('Finalizing coding...')
        
        for reader in self.readers.itervalues():
            reader.finalize(True)
            
        for writer in self.writers.itervalues():
            writer.finalize()
            
        if __debug__: self.logger.debug('Waiting for writers..')    
        
        for writer in self.writers.itervalues():
            writer.join()

        '''            
        if __debug__: self.logger.debug('Killing clients..')
        for reader in self.readers.itervalues():
            if isinstance(reader, RemoteNetCodingReader):
                reader.finalize(kill)
        '''
                 
    def execute_instruction(self, instruction):
        if __debug__: self.logger.debug('NetCodingInputStream %s is '
                                        'processing instruction %s',
                                        self.stream_id, str(instruction))
       
        bytes_processed = None

        if instruction[0]=='COPY':
            dst_buffer = self.buffers[instruction[1]]
            src_buffer = self.buffers[instruction[2]]
            src_buffer.copy_to(dst_buffer)
            bytes_processed = dst_buffer.length
        
        elif instruction[0]=='PUSH':
            queue = instruction[1]
            buff = self.buffers[instruction[2]]
            #if __debug__: self.logger.debug("Non disposable: %d", id(buff))
            self.disposable_buffers.remove(buff)
            NetCodingExecutor.queues[self.stream_id][queue].put(buff)
            bytes_processed = buff.length
        
        elif instruction[0]=='POP':
            assert self.stream_id in NetCodingExecutor.queues
            queue = instruction[1]
            buff = NetCodingExecutor.queues[self.stream_id][queue].get()
            #if __debug__: self.logger.debug("Make disposable: %d", id(buff))
            self.disposable_buffers.add(buff)
            self.buffers[instruction[2]] = buff
            bytes_processed = buff.length
            
        elif instruction[0]=='LOAD':
            buff = self.readers[instruction[2]].get()
            self.buffers[instruction[1]] = buff
            #if __debug__: self.logger.debug("Make disposable: %d", id(buff))
            self.disposable_buffers.add(buff)
            bytes_processed = self.buffers[instruction[1]].length
        
        elif instruction[0]=='WRITE':
            buff = self.buffers[instruction[1]]
            dst_stream = self.writers[instruction[2]]
            #if __debug__: self.logger.debug("Non disposable: %d", id(buff))
            self.disposable_buffers.remove(buff)
            dst_stream.write(buff)
            bytes_processed = buff.length
           
        elif instruction[0]=='IADD':
            src_buffer = self.buffers[instruction[2]]
            dst_buffer = self.buffers[instruction[1]]
            src = GaloisBuffer(src_buffer.size, bitfield=self.bitfield_op, 
                               buffer=src_buffer.buff)
            dst = GaloisBuffer(dst_buffer.size, bitfield=self.bitfield_op, 
                               buffer=dst_buffer.buff)
            dst += src
            dst_buffer.length = src_buffer.length
            bytes_processed = dst_buffer.length
            
        elif instruction[0]=='MULADD':
            src_buffer = self.buffers[instruction[3]]
            literal_value = instruction[2]
            dst_buffer = self.buffers[instruction[1]]
            
            if src_buffer.length==0:
                raise CodingException('Empty buffer.')
            if src_buffer.size!=dst_buffer.size:
                self.logger.error('Buffer sizes are not aligned.')
                raise CodingException('Buffers sizes are not aligned.')

            src = GaloisBuffer(src_buffer.size, bitfield=self.bitfield_op, 
                               buffer=src_buffer.buff)
            dst = GaloisBuffer(dst_buffer.size, bitfield=self.bitfield_op, 
                               buffer=dst_buffer.buff)
            src.multadd(literal_value, dest=dst, add=True)
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

            src = GaloisBuffer(src_buffer.size, bitfield=self.bitfield_op, 
                               buffer=src_buffer.buff)
            dst = GaloisBuffer(dst_buffer.size, bitfield=self.bitfield_op, 
                               buffer=dst_buffer.buff)
            src.multadd(literal_value, dest=dst, add=False)
            dst_buffer.length = src_buffer.length
            bytes_processed = dst_buffer.length

        else:
            raise CodingException('Invalid coding instruction: %s'\
                                  %(str(instruction)))

        if bytes_processed==None:
            raise CodingException('The number of processed bytes was not set '
                                  'for instruction: %s'%(str(instruction)))

        return bytes_processed

    def execute_step(self, output=None):
        bytes_processed = None
        
        if self.operations.output!=None:
            if output!=None:
                self.buffers[self.operations.output] = output
            else:
                assert False

        for instruction in self.operations.instructions:
            bp = self.execute_instruction(instruction)
            if bytes_processed!=None and bytes_processed!=bp:
                raise CodingException('Buffer sizes are not aligned.')
            bytes_processed = bp
            
        # Release used buffers
        for iobuffer in self.disposable_buffers:
            iobuffer.free()
        self.disposable_buffers.clear()
        
        if __debug__: self.logger.debug('Coding processed %d bytes.', 
                                        bytes_processed)
        return bytes_processed

    def execute(self):
        read = 0
        while read<self.size:
            if __debug__: self.logger.debug('execute iter %d/%d', 
                                            read, self.size)
            read += self.execute_step()
            yield read
            

@ClassLogger
class NetCodingInputStream(InputStream):
    def __init__(self, executor):
        self.executor = executor
        super(NetCodingInputStream, self).__init__(self.executor.size)

    def finalize(self):
        super(NetCodingInputStream, self).finalize()
        self.executor.finalize()

    def read(self, iobuffer, nbytes=None):
        if nbytes==None:
            nbytes = iobuffer.size
        else:
            nbytes = min(iobuffer.size, nbytes)

        if __debug__: self.logger.debug('Reading %d bytes from '
                                        'NetCodingInputStream %s.', nbytes,
                                        hex(id(self)))
        num_read = self.executor.execute_step(iobuffer)
        if __debug__: self.logger.debug('Coding processed %d bytes.', num_read)
        assert num_read==iobuffer.length, (num_read,iobuffer.length)
        return num_read

class NetCodingOperations(object):
    def __init__(self, node, streams=[], output=None):
        self.node = node
        self.streams = streams
        self.output = output
        self.instructions = []

    def add(self, inst):
        self.instructions.append(inst)

    def is_stream(self):
        return self.output!=None
