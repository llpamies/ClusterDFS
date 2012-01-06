import os.path
import uuid
import struct
import hashlib
import cStringIO

class DataBlockHeader(object):
    '''Each stored block has a 256 byte header. This is its binary structure (all little-endian):
     + 32 bytes (  0 --  31) --> file uuid (hex formatted string)
     + 32 bytes ( 32 --  71) --> block uuid (hex formatted string)
     +  8 bytes ( 72 --  83) --> offset in the file (unsigned int)
     +  8 bytes ( 84 -- 111) --> block size (unsigned int)
     + 32 bytes (112 -- 143) --> block's SHA256 checksum (hex formatted string)
     +112 bytes (144 -- 255) --> reserved for future uses
    '''

    SIZE = 256
    FIELDS = ['file_uuid','block_uuid','offset','size','sha256']
    DEFAULTS = [None, None, 0, 0, None]

    def __init__(self, **kwargs):
        for field, default in zip(self.FIELDS, self.DEFAULTS):
            setattr(self, field, kwargs[field] if field in kwargs else default)

    @classmethod
    def parse(cls, f):
        inst = DataBlockHeader()

        inst.file_uuid = f.read(32)
        inst.block_uuid = f.read(32)
        inst.offset, = struct.unpack('<Q', f.read(8))
        inst.size, = struct.unpack('<Q', f.read(8))
        inst.sha256 = f.read(32)

        f.seek(cls.SIZE) 

        return inst

    def dump(self):
        raw = cStringIO.StringIO()
        raw.write(self.file_uuid)
        raw.write(self.block_uuid)
        raw.write(struct.pack('<Q', self.offset))
        raw.write(struct.pack('<Q', self.size))
        raw.write(self.sha256)
        raw.write(' '*112)
        data = raw.getvalue()
        assert len(data)==self.SIZE
        return data
    
class DataBlock(file):
    def init(self, header):
        self.header = header
        if self.mode == 'w':
            self.sha256 = hashlib.sha256()

    def write(self, data):
        if self.mode == 'r':
            raise IOError("Block is in read-only mode.")
        if self.closed:
            raise IOError("Block is closed.")
        length = len(data)
        self.sha256.update(data)
        self.header.size += length
        self.header.offset += length
        file.write(self, data)

    def close(self):
        if self.mode=='w':
            # Compute sha256 digest
            digest = self.sha256.hexdigest()
            if len(digest)<32:
                digest = '0'*(32-len(digest)) + digest
            self.header.sha256 = digest
            # Sync header
            self.seek(0)
            file.write(self, self.header.dump())

        file.close(self)

    def __del__(self):
        if not self.closed:
            self.close()

    def __inexistent__(self, *args, **kwargs):
        assert False, "Inexistent."

    readline = __inexistent__
    writelines = __inexistent__
    
    @staticmethod
    def filename(file_uuid, block_uuid):
        return '%s_%s'%(file_uuid, block_uuid)

    @staticmethod
    def create(base_dir='', file_uuid=None, block_uuid=None):
        if file_uuid==None: file_uuid = '%032x'%uuid.uuid4().int
        if block_uuid==None: block_uuid = '%032x'%uuid.uuid4().int
        
        header = DataBlockHeader(file_uuid=file_uuid, block_uuid=block_uuid)
        f = DataBlock(os.path.join(base_dir, DataBlock.filename(file_uuid, block_uuid)), 'w')
        f.init(header)
        f.seek(DataBlockHeader.SIZE)
        return f

    @staticmethod
    def open(file_uuid, block_uuid, base_dir=''):
        f = DataBlock(os.path.join(base_dir, DataBlock.filename(file_uuid, block_uuid)), 'r')
        f.init(DataBlockHeader.parse(f))
        return f

if __name__=='__main__':
    d = DataBlock.create(base_dir='dd1')
    fid = d.header.file_uuid
    bid = d.header.block_uuid
    d.write('aaaaaaa\n asdasdasd')
    d.close()

    d = DataBlock.open(fid, bid, base_dir='dd1')
    print d.read()
    d.close()
