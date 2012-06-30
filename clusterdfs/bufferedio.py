import io
import sys
import gc
import os.path
import numpy
import gevent.queue
import gevent.event
import types

from common import *
     
@ClassLogger
class IOBuffer(object):
    pool = []
    
    @staticmethod
    def create(factory, *args, **kwargs):      
        if len(IOBuffer.pool)>0:
            obj = IOBuffer.pool.pop()
            obj.reset(factory, *args, **kwargs)
            return obj
        
        else:
            return IOBuffer(factory, *args, **kwargs)
            
    def free(self):
        pass

    def __del__(self):
        IOBuffer.pool.append(self)
        self.factory.event.set()
        
    def __init__(self, factory, size=io.DEFAULT_BUFFER_SIZE):
        self.buff = bytearray(size)
        self.mem = memoryview(self.buff)
        self.reset(factory, size=size)

    def reset(self, factory, size=io.DEFAULT_BUFFER_SIZE):
        self.factory = factory
        self.size = size
        self.length = 0

    def data(self):
        if self.length==self.size:
            return self.buff
        elif self.length<self.size:
            return self.mem[:self.length]
        else:
            assert False

    def copy_to(self, iobuffer):
        if self.size!=iobuffer.size:
            raise ValueError("Buffers do not have the same size.")
        iobuffer.mem[:] = self.mem[:]
        iobuffer.length = self.length
        return iobuffer

    def as_numpy_byte_array(self):
        return numpy.ndarray((self.size,), dtype=numpy.uint8, buffer=self.buff)

@ClassLogger
class IOBufferFactory(object):    
    def __init__(self, max_active=5):
        self.max_active = max_active
        self.active = 0
        self.event = gevent.event.Event()

    def create(self, *args, **kwargs):
        #return IOBuffer(self, *args, **kwargs)
        '''
        if self.active>10*self.max_active:
            gc.collect()
        assert self.active<=10*self.max_active
        '''
        if self.active==self.max_active:
            self.event.wait()
            self.active -= 1
            self.event.clear()
            '''
            if self.event.wait(1):
                self.active -= 1
                self.event.clear()
            '''
        self.active += 1
        return IOBuffer.create(self, *args, **kwargs)
    
@ClassLogger
class InputStreamReader(object):
    def __init__(self, input_stream, debug_name=None, num_buffers=5, size=None):
        '''
        If the 'size' is larger than the 'available()' bytes in the input stream,
        then garbage is read to achieve 'size'.
        '''
        if not isinstance(input_stream, InputStream):
            raise TypeError('input_stream must be an InputStream instance.')

        self.debug_name = debug_name
        self.exc_info = None
        self.input_stream = input_stream
        self.queue = gevent.queue.Queue()
        self.process = gevent.spawn(self._run)
        self.buffer_fact = IOBufferFactory(num_buffers)
        self.finalized = False        
        self.size = size if size!=None else self.input_stream.size
        
        self.logger.debug("Starting new %d bytes reader.", self.size)

    def _run(self):
        try:
            bytes_left = self.size
            while bytes_left>0:
                if __debug__: self.logger.debug("%s iteration %d/%d.", 
                                                self.debug_name or hex(id(self)),
                                                self.size-bytes_left, self.size)
                
                iobuffer = self.buffer_fact.create()
                avail = self.input_stream.available()
                self.logger.debug("uffer: %d",iobuffer.length)
                self.logger.debug("a:%d; b:%d; l:%d", avail, iobuffer.size, bytes_left)
                if avail>0:
                    nbytes = min(iobuffer.size, avail, bytes_left)
                    self.logger.debug("------%d", nbytes)
                    bytes_left -= self.input_stream.read(iobuffer, nbytes=nbytes)
                    
                else:
                    nbytes = min(bytes_left, iobuffer.size)
                    iobuffer.length = nbytes
                    bytes_left -= nbytes
                    
                self.queue.put(iobuffer)
                del iobuffer
            if __debug__: self.logger.debug("Reader has successfully finished.")
            return False
        
        except Exception, e:
            self.logger.error('Reader subprocesses got a %s exception.', e.__class__.__name__)
            self.logger.error(unicode(e))
            self.exc_info = sys.exc_info()
            return True
        
        finally:
            self.queue.put(StopIteration)
            self.finalized = True

    def get(self):
        try:
            iobuffer = self.queue.get()
            if self.exc_info:
                raise self.exc_info[1], None, self.exc_info[2]
            return iobuffer
        except StopIteration:
            raise IOError("Reader ended before it was expected!")
        
    def __iter__(self):
        for iobuffer in self.queue:
            if self.exc_info:
                raise self.exc_info[1], None, self.exc_info[2]
            yield iobuffer
        if self.process.get():
            raise self.exc_info[1], None, self.exc_info[2]

    def finalize(self):
        self.input_stream.finalize()
        
    def flush(self, writer):
        if not isinstance(writer, OutputStreamWriter):
            raise TypeError("Should be an OutputStreamWriter.")
        for iobuffer in self:
            writer.write(iobuffer)
        self.finalize()

@ClassLogger
class OutputStreamWriter(object):
    def __init__(self, output_stream):
        self.output_stream = output_stream
        self.exc_info = None
        self.queue = gevent.queue.Queue()
        self.process = gevent.spawn(self._run)
        self.finalizing = False
        self.finalized = False

    def finalize(self):
        self.finalizing = True
        self.queue.put(StopIteration)

    def write(self, iobuffer):
        if __debug__: self.logger.debug('Received a buffer to write.')
        if self.finalized or self.finalizing:
            raise IOError('Internal writer process finished.')
        self.queue.put(iobuffer)

    def _run(self):
        try:
            for iobuffer in self.queue:
                if __debug__: self.logger.debug('Processing a buffer to write.')
                self.output_stream.write(iobuffer)
                if __debug__: self.logger.debug('Freeing written buffer.')
                iobuffer.free()
                del iobuffer
            return False

        except Exception, e:
            if self.exc_info!=None:
                self.exc_info = sys.exc_info()
                return True
            else:
                return False

        finally:
            self.finalized = True
            self.output_stream.finalize()

    def join(self):
        if self.process.get():
            raise self.exc_info[1], None, self.exc_info[2]

@ClassLogger
class InputStream(object):
    def __init__(self, size):
        if (type(size)!=int and type(size)!=long) or size<=0:
            raise TypeError("Parameter size must be a positive integer, got %s."%str(size))
        self.size = size
        self.bytes_left = size

    def available(self):
        return self.bytes_left
        
    def finalize(self):
        pass

class FileInputStream(InputStream):
    def __init__(self, filename, offset=0):
        InputStream.__init__(self, os.path.getsize(filename))
        self.fileio = io.open(filename, 'rb')
        self.fileio.seek(offset)

    def read(self, iobuffer, nbytes=None):
        if nbytes==None:
            nbytes = iobuffer.size
        else:
            nbytes = min(iobuffer.size, nbytes)

        read = 0
        while read<nbytes:
            read += self.fileio.readinto(iobuffer.mem[read:nbytes])
        
        if read==0:
            raise IOError("File error or probably reached end of file.")
        
        iobuffer.length = read
        self.bytes_left -= read
        return read

    def finalize(self):
        self.fileio.close()

@ClassLogger
class FileOutputStream(object):
    def __init__(self, filename):
        if __debug__: self.logger.debug("Opening (w): %s", filename)
        self.fileio = io.open(filename, 'wb')

    def write(self, iobuffer):
        num = self.fileio.write(iobuffer.data())
        if num!=iobuffer.length:
            raise IOError("Error writing to file.")

    def finalize(self):
        self.fileio.close()

class NetworkInputStream(InputStream):
    def __init__(self, endpoint, size):
        InputStream.__init__(self, size)
        self.endpoint = endpoint

    def read(self, iobuffer, nbytes=None):
        if nbytes==None:
            nbytes = iobuffer.size
        else:
            nbytes = min(iobuffer.size, nbytes)

        read = 0
        while read<nbytes:
            read += self.endpoint.recv_into(iobuffer.mem[read:nbytes])
        
        iobuffer.length = read
        self.bytes_left -= read
        return read

    def finalize(self):
        pass
        #self.endpoint.kill()

@ClassLogger
class NetworkOutputStream(object):
    def __init__(self, endpoint):
        self.endpoint = endpoint

    def write(self, iobuffer):
        self.endpoint.send(iobuffer)
    
    def finalize(self):
        #self.endpoint.kill()
        pass