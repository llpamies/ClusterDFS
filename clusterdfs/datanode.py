import os
import math
import uuid
import gevent
import logging
import tempfile
import importlib
  
from common import Config, ClassLogger
from coding import  NetCodingExecutor, NetCodingInputStream
from headers import DataNodeHeader, NameNodeHeader
from networking import Client, Server, ServerHandle
from bufferedio import FileInputStream, FileOutputStream, InputStreamReader, OutputStreamWriter
              
class DataNodeConfig(Config):
    port = 13100
    bind_addr = '0.0.0.0'
    datadir = None
    namenode_addr = 'localhost'
    namenode_port = 13200
    ping_timeout = 10
    fakeout = False
    isolated = False
    coding_mod_name = 'clusterdfs.rapidraid'

    def check(self):
        if self.datadir==None:
            self.datadir = tempfile.mkdtemp()
        if not self.datadir.endswith('/'):
            self.datadir = self.datadir+'/'
        return self
    
    def _get_coding_mod(self, singleton=[]):
        if not singleton:
            singleton.append(importlib.import_module(self.coding_mod_name))
        return singleton[0]
        
    coding_mod = property(_get_coding_mod)

@ClassLogger      
class BlockStoreManager(object):
    def __init__(self, config):
        self.data_dir = config.datadir
        self.fake_out = config.fakeout

    def path(self, block_id):
        return os.path.join(self.data_dir, block_id)

    def fake_path(self, block_id):
        return "/dev/null"
    
    def get_size(self, block_id):
        return os.path.getsize(self.path(block_id))

    def get_input_stream(self, block_id):
        '''
            Returns a FileInputStream for the block with block_id.
        '''
        return FileInputStream(self.path(block_id))

    def get_output_stream(self, block_id):
        '''
            Returns a FileOutputStream for the block with block_id.
        '''
        pathfunc = self.fake_path if self.fake_out else self.path
        return FileOutputStream(pathfunc(block_id))

    def get_reader(self, block_id, debug_name=None):
        '''
            Returns a FileInputStream for the block with block_id.
        '''
        return InputStreamReader(self.get_input_stream(block_id), debug_name=debug_name)

    def get_writer(self, block_id, debug_name=None):
        '''
            Returns a FileOutputStream for the block with block_id.
        '''
        return OutputStreamWriter(self.get_output_stream(block_id), debug_name=debug_name)

@ClassLogger   
class DataNodeQuery(ServerHandle):
    def process_query(self):
        header = DataNodeHeader.parse(self.recv())
        
        op = header['operation']
        if op==DataNodeHeader.OP_STORE:
            return self.store_block(**header)
        
        elif op==DataNodeHeader.OP_RETRIEVE:
            return self.retrieve_block(**header)
        
        elif op==DataNodeHeader.OP_CODING:
            return self.node_coding(**header)
        
        elif op==DataNodeHeader.OP_INSERT:
            return self.insert_data(**header)
        
        else:
            assert False
    
    def store_block(self, block_id=None, **kwargs):
        self.logger.info("Storing block '%s'.", block_id)

        reader = self.recv_reader()

        '''
        if 'fwdlist' in self.header:
            # Get the forward list and the next forward node
            forward_list = self.header['fwdlist']
            logging.info("Forwarding '%s' to %s.", block_id, repr(forward_list[0]))
            logging.info("Remaining forwards: %d.", len(forward_list)-1)
            next_node = Client(*forward_list[0])
            next_forward_list = forward_list[1:]
                
            # Send header to next node
            header = self.header.copy()
            header['fwdlist'] = next_forward_list
            next_node.send(header)
            
            # processing
            writer = OutputStreamWriter(self.server.block_store.get_output_stream(block_id), next_node.output_stream)
            reader.flush(writer)
            writer.finalize()
            writer.join()
            next_node.assert_ack()

        else:
        '''
        # processing
        writer = OutputStreamWriter(self.server.block_store.get_output_stream(block_id))
        reader.flush(writer)
        writer.finalize()
        writer.join()
        
        logging.info("Block '%s' stored successfully.", block_id)

    def retrieve_block(self, block_id=None, **kwargs):
        self.logger.info("Sending block '%s'.", block_id)
        input_stream = self.server.block_store.get_input_stream(block_id)
        self.send(input_stream)
        reader = InputStreamReader(input_stream, debug_name='retrieve')
        writer = self.new_writer()
        reader.flush(writer)
        writer.finalize()
        writer.join()
        self.logger.info('Block %s sent successfully.', block_id)

    def insert_data(self, block_id=None, **kwargs):
        coding = self.server.config.coding_mod
        instream = self.recv_stream()
        real_size = instream.size 
        block_size = int(math.ceil(float(real_size)/coding.k))
        store_size = block_size*coding.k
        self.logger.info("Inserting a file of size %d, block size %d.", store_size, block_size)
        for i in xrange(coding.k):
            self.logger.info("Inserting part %d of %d.", i, coding.k)
            reader = InputStreamReader(instream, size=block_size)
            writer = OutputStreamWriter(self.server.block_store.get_output_stream(block_id+"_part%d"%i))
            reader.flush(writer)
            writer.finalize()
            writer.join()

        logging.info("Object '%s' inserted successfully.", block_id)


    def node_coding(self, block_id='', coding_id=None, stream_id='', nodes='', **kwargs):
        if not block_id:
            raise ValueError("'block_id' is not provided.")
        if not coding_id:
            raise ValueError("'coding_id' is not provided.")
        if not nodes:
            raise ValueError("'nodes' is not provided.")
        # Generate an ID that will be the ID for all the coding stream (pipeline).
        if stream_id=='':
            stream_id = uuid.uuid4().hex
            
        reader = None
        writer = None
        coding_executor = None
        try:
            self.logger.info("Starting coding operation %s - %s", coding_id, stream_id)
            coding = self.server.config.coding_mod
            
            nodes = map(eval, nodes.split(';'))
            coding_operations = coding.operations[coding_id]
            resolver = coding.RapidRaidResolver(block_id, stream_id, self.server.block_store, nodes, config=self.server.config)
            coding_executor = NetCodingExecutor(coding_operations, resolver, stream_id)
            
            
            if coding_operations.is_stream():
                if __debug__: self.logger.debug("Forwarding coding stream.")
                input_stream = NetCodingInputStream(coding_executor)
                reader = InputStreamReader(input_stream, debug_name='coding_result', async=False)
                self.send(input_stream)
                writer = self.new_writer(async=False)
                reader.flush(writer)
                writer.finalize()
                writer.join()
                
            else:
                if __debug__: self.logger.debug("Executing locally.")
                self.send(coding_executor.size)
                for read in coding_executor.execute():
                    self.send(read)
            
            self.logger.info('Coding ended successfully.')

        finally:
            if __debug__:  self.logger.debug("executing datanode finally statement.")
            if coding_executor: coding_executor.finalize()
            if reader: reader.finalize(True)
            if writer: writer.finalize()
            #FIXME: Find a way to close all open descriptors.

@ClassLogger
class DataNodeNotifier(object):
    def __init__(self, config, server):
        self.config = config
        self.server = server
        self.process = gevent.spawn(self.timeout)
        self.ping = {'op':NameNodeHeader.OP_PING, 'datanode_port':self.config.port}

    def stop(self):
        self.process.kill()

    def timeout(self):
        # FIXME: REWRITE!!!!
        while True:
            # send ping
            try:
                logging.debug('Sending ping.')
                '''
                ne = NetworkEndpoint(gevent.socket.create_connection((self.config.namenode_addr, self.config.namenode_port)))
                ne.send(self.ping)
                ne.send([])
                response = ne.recv()
                '''
                
            except Exception as e:
                logging.error("Cannot deliver ping to nameserver: %s."%unicode(e))
            
            # sleep timeout
            gevent.sleep(self.config.ping_timeout)

@ClassLogger
class DataNode(Server):
    def __init__(self, config):
        self.config = config
        self.logger.info("Configuring DataNode to listen on localhost:%d", self.config.port)
        self.logger.info("DataNode data dir: %s", config.datadir)
        self.logger.info("Using a fake out? %s", unicode(config.fakeout))
        Server.__init__(self, DataNodeQuery, port=self.config.port)

        self.block_store = BlockStoreManager(self.config)

        if not self.config.isolated:
            self.notifier = DataNodeNotifier(self.config, self)

    def init(self):
        self.serve()

    def finalize(self):
        if not self.config.isolated:
            self.notifier.stop()

@ClassLogger
class DataNodeClient(Client):
    def __init__(self, address, port):
        super(DataNodeClient, self).__init__(address, port)
    
    def insert(self, block_id, local_path):
        self.send(DataNodeHeader.generate(DataNodeHeader.OP_INSERT, block_id))
        writer = self.new_writer()
        istream = FileInputStream(local_path)
        self.send(istream)
        reader = InputStreamReader(istream)
        reader.flush(writer)
        writer.finalize()
        writer.join()
        istream.finalize()
        if __debug__: self.logger.debug("Wating for ACK.")
        self.assert_ack()
        if __debug__: self.logger.debug("END.")
    
    def retrieve(self, block_id, local_path):
        self.send(DataNodeHeader.generate(DataNodeHeader.OP_RETRIEVE, block_id))
        reader = self.recv_reader()
        ostream = FileOutputStream(local_path)
        writer = OutputStreamWriter(ostream)
        reader.flush(writer)
        writer.finalize()
        writer.join()
        ostream.finalize()
        if __debug__: self.logger.debug("Wating for ACK.")
        self.assert_ack()
        if __debug__: self.logger.debug("END.")
        
    def coding(self, block_id, coding_id, nodes):        
        self.send(DataNodeHeader.generate(DataNodeHeader.OP_CODING, block_id, coding_id, nodes=nodes))
        if __debug__: self.logger.debug("Wating for ACK.")
        size = self.recv()
        assert isinstance(size, int) or isinstance(size, long)
        read = self.recv()
        assert isinstance(read, int) or isinstance(read, long)
        while read<size:
            read = self.recv()
            assert isinstance(read, int) or isinstance(read, long)
            if __debug__: self.logger.debug("Got %d out of %d.",read, size)
        assert size==read
        self.assert_ack()
        if __debug__: self.logger.debug("END.")