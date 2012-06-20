import io
import sys
import os.path
import numpy
import socket
import gevent
import gevent.queue
import itertools
import collections

from common import *

class IOBuffer(object):
    def __init__(self, size=io.DEFAULT_BUFFER_SIZE):
        self.size = size
        self.buff = bytearray(self.size)
        self.mem = memoryview(self.buff)
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
class ReusableIOBuffer(IOBuffer):
    def __init__(self, queue, *args, **kwargs):
        self._queue = queue
        super(ReusableIOBuffer, self).__init__(*args, **kwargs)

    def reset(self):
        self._queue.put(self)

@ClassLogger
class DelayedReusableIOBuffer(IOBuffer):
    '''
        Delays the requeue until num calls to requeue.
    '''
    def __init__(self, iobuffer, num):
        assert isinstance(iobuffer, ReusableIOBuffer)
        self.iobuffer = iobuffer
        self.num = num

    def reset(self):
        if self.num>1:
            self.num -= 1
        else:
            self.iobuffer.reset()

    def __getattr__(self, attribute):
        return getattr(self.iobuffer, attribute)

@ClassLogger
class InputStreamReader(object):
    def __init__(self, input_stream, num_buffers=2, debug_name=None):
        if input_stream.size<10000:
            raise Exception("lalala"+str(input_stream.size))
        if not isinstance(input_stream, InputStream):
            raise TypeError('input_stream must be an InputStream instance.')

        self.debug_name = debug_name
        self.exc_info = None
        self.input_stream = input_stream
        self.free_queue = gevent.queue.Queue()
        self.busy_queue = gevent.queue.Queue()
        self.process = gevent.spawn(self._run)
        #self.temp_writer = None

        # Generate pool of buffers
        for i in xrange(num_buffers):
            self.free_queue.put(ReusableIOBuffer(self.free_queue))

        self.finalized = False

    def _run(self):
        try:
            read = 0
            while read<self.input_stream.size:
                if __debug__: self.logger.debug("%s iteration %d/%d.", self.debug_name or hex(id(self)), read, self.input_stream.size)
                iobuffer = self.free_queue.get()
                read += self.input_stream.read(iobuffer, nbytes=self.input_stream.size-read)
                self.busy_queue.put(iobuffer)
            if __debug__: self.logger.debug("Reader has successfully finished.")
            return False
        
        except Exception, e:
            self.logger.error('Reader subprocesses got a %s exception.', e.__class__.__name__)
            self.logger.error(unicode(e))
            self.exc_info = sys.exc_info()
            return True
        
        finally:
            self.busy_queue.put(StopIteration)
            self.finalized = True

    def __iter__(self):
        for iobuffer in self.busy_queue:
            if self.exc_info:
                raise self.exc_info[1], None, self.exc_info[2]
            yield iobuffer
        if self.process.get():
            raise self.exc_info[1], None, self.exc_info[2]

    def flush(self, writer):
        if not isinstance(writer, OutputStreamWriter):
            raise TypeError("Should be an OutputStreamWriter.")
        for iobuffer in self:
            writer.write(iobuffer)

@ClassLogger
class OutputStreamWriter(object):
    def __init__(self, *output_streams):
        self.num_outputs = len(output_streams)
        self.output_streams = output_streams
        self.exc_info = None
        self.queues = [gevent.queue.Queue() for i in xrange(self.num_outputs)]
        self.processes = [gevent.spawn(self._run, *x) for x in itertools.izip(self.output_streams, self.queues)]
        self.finalizing = False
        self.finalized = False

    def finalize(self):
        self.finalizing = True
        for queue in self.queues:
            queue.put(StopIteration)

    def write(self, iobuffer):
        if __debug__: self.logger.debug('Received a buffer to write.')
        if self.finalized or self.finalizing:
            raise IOError('Internal writer process finished.')
        for queue in self.queues:
            queue.put(DelayedReusableIOBuffer(iobuffer, self.num_outputs))

    def _run(self, stream, queue):
        try:
            for iobuffer in queue:
                if __debug__: self.logger.debug('Processing a buffer to write.')
                stream.write(iobuffer)
                if __debug__: self.logger.debug('Reseting written buffer.')
                iobuffer.reset()
            return False

        except Exception, e:
            if self.exc_info!=None:
                self.exc_info = sys.exc_info()
                return True
            else:
                return False

        finally:
            self.finalized = True

    def join(self):
        for process, queue in itertools.izip(self.processes, self.queues):
            if process.get():
                raise self.exc_info[1], None, self.exc_info[2]

class ReadCountMeta(type):
    def __new__(cls, classname, bases, classdict):
        def new_read(self, *args, **kwargs):
            v = self._original_read(*args, **kwargs)
            self.read_count += v
            return v
        
        classdict['read_count'] = 0
        
        if 'read' in classdict:
            classdict['_original_read'] = classdict['read']
            classdict['read'] = new_read

        return type.__new__(cls, classname, bases, classdict)

@ClassLogger
class InputStream(object):
    __metaclass__ = ReadCountMeta

    def __init__(self, size):
        if (type(size)!=int and type(size)!=long) or size<=0:
            raise TypeError("Parameter size must be a positive integer, got %s."%str(size))
        self.size = size

    def is_processed(self):
        return self.read_count>=self.size 

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
        return read

    def finalize(self):
        self.fileio.close()

@ClassLogger
class FileOutputStream(object):
    def __init__(self, filename):
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
        return read

    def finalize(self):
        pass

@ClassLogger
class NetworkOutputStream(object):
    def __init__(self, endpoint):
        self.endpoint = endpoint

    def write(self, iobuffer):
        self.endpoint.send(iobuffer)
    
    def finalize(self):
        pass
