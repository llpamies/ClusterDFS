import avro.io
import avro.schema
import tempfile

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
  
from coding import *
from headers import *
from networking import *
from bufferedio import *

class DataNodeHeader(object):
    # Available 'op' codes:
    OP_STORE = 0
    OP_RETRIEVE = 1
    OP_CODING = 2
            
    schema = avro.schema.parse("""\
                    {"type": "record",
                     "name": "DataNodeHeader",
                     "fields": [{"name": "op", "type": "int"},\
                                {"name": "id", "type": "string"}]\
                     }""")
  
    @staticmethod
    def parse(s):
        if not isinstance(s, str):
            raise TypeError("must be a string")
        reader = StringIO(s)
        decoder = avro.io.BinaryDecoder(reader)
        datum_reader = avro.io.DatumReader(writers_schema=DataNodeHeader.schema, readers_schema=DataNodeHeader.schema)
        return datum_reader.read(decoder)
    
    @staticmethod
    def generate(op, id_):
        writer = StringIO()
        encoder = avro.io.BinaryEncoder(writer)
        datum_writer = avro.io.DatumWriter(writers_schema=DataNodeHeader.schema)
        datum_writer.write({'op':op,'id':id_}, encoder)
        return writer.getvalue() 
              
class DataNodeConfig(object):
    port = 13100
    bind_addr = '0.0.0.0'
    datadir = None
    namenode_addr = 'localhost'
    namenode_port = 13200
    ping_timeout = 10
    isolated = False

    def __init__(self):
        pass
        
    @classmethod
    def from_args(cls, args):
        c = cls()   
        for k, v in args.__dict__.iteritems():
            if v!=None: c.__dict__[k] = v
        return c.check()
    
    @classmethod
    def from_dict(cls, d):
        c = cls()
        for k, v in d.iteritems():
            if v!=None: c.__dict__[k] = v
        return c.check()

    def check(self):
        if self.datadir==None:
            self.datadir = tempfile.mkdtemp()
        if not self.datadir.endswith('/'):
            self.datadir = self.datadir+'/'
        return self
        
class BlockStoreManager(object):
    def __init__(self, data_dir):
        self.data_dir = data_dir

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
        return FileOutputStream(self.fake_path(block_id))
        #return FileOutputStream(self.path(block_id))


@ClassLogger
class DataNodeQuery(ServerHandle):
    def process_query(self):
        header = DataNodeHeader.parse(self.recv()) 
        
        if header['op']==DataNodeHeader.OP_STORE:
            return self.store_block(header['id'])
        
        elif header['op']==DataNodeHeader.OP_RETRIEVE:
            return self.retrieve_block(header['id'])
        
        elif header['op']==DataNodeHeader.OP_CODING:
            return self.node_coding(header['id']) #FIXME
        
        else:
            assert False
    
    def store_block(self, block_id):
        reader = self.recv_reader()

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
            # processing
            writer = OutputStreamWriter(self.server.block_store.get_output_stream(block_id))
            reader.flush(writer)
            writer.finalize()
            writer.join()
        
        logging.info("Block '%s' stored successfully.", block_id)

    def retrieve_block(self, block_id):
        self.logger.info("Sending block '%s'.", block_id)
        input_stream = self.server.block_store.get_input_stream(block_id)
        self.send(input_stream)
        reader = InputStreamReader(input_stream, debug_name='retrieve')
        writer = self.new_writer()
        reader.flush(writer)
        writer.finalize()
        writer.join()
        self.logger.info('Block %s sent successfully.', block_id)

    def node_coding(self, block_id):
        coding_executor = None
        try:
            self.logger.info("Starting coding operation.")
            coding_operations = NetCodingOperations.unserialize(self.header['coding'])
            coding_executor = NetCodingExecutor(coding_operations, self.server.block_store)
        
            if coding_operations.is_stream():
                self.logger.debug("Forwarding coding stream.")
                input_stream = NetCodingInputStream(coding_executor)
                reader = InputStreamReader(input_stream, debug_name='coding_result')
                self.send(input_stream)
                writer = self.new_writer()
                reader.flush(writer)
                writer.finalize()
                writer.join()
            else:
                if __debug__: self.logger.debug("Executing locally.")
                coding_executor.execute()
            self.logger.info('Coding finalized successfully.')

        finally:
            if coding_executor: coding_executor.finalize()

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
        while True:
            # send ping
            try:
                logging.debug('Sending ping.')
                ne = NetworkEndpoint(gevent.socket.create_connection((self.config.namenode_addr, self.config.namenode_port)))
                ne.send(self.ping)
                ne.send([])
                response = ne.recv()
                if response['code']!=NetworkHeader.OK:
                    logging.error('Cannot deliver ping to nameserver: %s', response['msg']) 

            except socket.error, (value, message):
                logging.error("Cannot deliver ping to nameserver: %s."%(message))
            
            # sleep timeout
            gevent.sleep(self.config.ping_timeout)

class DataNode(Server):
    def __init__(self, config):
        self.config = config
        logging.info("Configuring DataNode to listen on localhost:%d"%(self.config.port))
        logging.info("DataNode data dir: %s"%(config.datadir))
        Server.__init__(self, DataNodeQuery, port=self.config.port)

        self.block_store = BlockStoreManager(self.config.datadir)

        if not self.config.isolated:
            self.notifier = DataNodeNotifier(self.config, self)

    def init(self):
        self.serve()

    def finalize(self):
        if not self.config.isolated:
            self.notifier.stop()
