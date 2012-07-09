import avro.io
import avro.schema

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
    
class DataNodeHeader(object):
    # Available 'op' codes:
    OP_STORE = 0
    OP_RETRIEVE = 1
    OP_CODING = 2
    OP_INSERT = 3
            
    schema = avro.schema.parse("""\
                    {"type": "record",
                     "name": "DataNodeHeader",
                     "fields": [{"name": "operation", "type": "int"},\
                                {"name": "nodes", "type": "string"},
                                {"name": "block_id", "type": "string"},
                                {"name": "stream_id", "type": "string"},
                                {"name": "coding_id", "type": "string"}]\
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
    def generate(operation, block_id=None, coding_id='', stream_id='', nodes=''):
        writer = StringIO()
        encoder = avro.io.BinaryEncoder(writer)
        datum_writer = avro.io.DatumWriter(writers_schema=DataNodeHeader.schema)
        datum_writer.write({'operation':operation,'block_id':block_id,
                            'coding_id':coding_id,'stream_id':stream_id,
                            'nodes':nodes}, encoder)
        return writer.getvalue() 
    
class NameNodeHeader:
    OP_PING = 0
    OP_GETNODES = 1
