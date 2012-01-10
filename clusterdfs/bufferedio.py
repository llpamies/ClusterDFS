import io
import numpy
import socket
import gevent
import gevent.queue
import collections

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

class BufferedConsumer(object):
    def __init__(self, output_stream, reusable_buffers):
        self.output_stream = output_stream
        self.reusable_buffers = reusable_buffers
        self.read_queue = gevent.queue.Queue()
        self.process = gevent.spawn(self.run)

    def run(self):
        for iobuffer in self.read_queue:
            self.output_stream.write(iobuffer)
            self.reusable_buffers.put(iobuffer)

class SyncronizedBufferQueue(gevent.queue.Queue):
    def __init__(self, num_buffers, *args, **kwargs):
        gevent.queue.Queue.__init__(self, *args, **kwargs)
        self.num_buffers = num_buffers
        self.counters = collections.defaultdict(int)

    def put(self, item, *args, **kwargs):
        if 'force' in kwargs:
            force = kwargs.pop('force')
        else:
            force = False

        if force:
            gevent.queue.Queue.put(self, item, *args, **kwargs)
            self.counters[item] = 0
        else:
            self.counters[item] += 1
            if self.counters[item]==self.num_buffers:
                gevent.queue.Queue.put(self, item, *args, **kwargs)
                self.counters[item] = 0

class InputStream(object):
    def __init__(self, size):
        if size<=0:
            raise TypeError("size must be positive.")
        self.size = size

    def iterated_read(self, queue):
        read = 0
        while read<self.size:
            iobuffer = queue.get()
            read += self.read(iobuffer, nbytes=self.size-read)
            yield iobuffer

    def sendto(self, *output_streams, **kwargs):
        if 'num_buffers' in kwargs:
            num_buffers = kwargs['num_buffers']
        else:
            num_buffers = 2

        if len(output_streams)==0:
            raise TypeError("output_streams should be a non-empty list.")

        if num_buffers<=0:
            raise TypeError("num_buffers must be positive.")

        reusable_buffers = SyncronizedBufferQueue(len(output_streams))
        for i in xrange(num_buffers):
            reusable_buffers.put(IOBuffer(), force=True)

        consumers = []
        for output_stream in output_streams:
            consumers.append(BufferedConsumer(output_stream, reusable_buffers))
            
        # read input data and send it to consumers
        for iobuffer in self.iterated_read(reusable_buffers):
            for consumer in consumers:
                consumer.read_queue.put(iobuffer)

        # terminate consumer processes
        for consumer in consumers:
            consumer.read_queue.put(StopIteration)
        
        # wait until all finish
        for consumer in consumers:
            consumer.process.join()

class FileInputStream(InputStream):
    def __init__(self, filename, size, offset=0):
        InputStream.__init__(self, size)
        self.fileio = io.open(filename, 'rb')
        self.fileio.seek(offset)

    def read(self, iobuffer, nbytes=None):
        if nbytes==None or nbytes >= iobuffer.size:
            num = self.fileio.readinto(iobuffer.buff)
        else:
            num = self.fileio.readinto(iobuffer.mem[:nbytes])
        
        if num==0:
            raise IOError("File error or probably reached end of file.")
        
        iobuffer.length = num
        return num

    def close(self):
        self.fileio.close()

class FileOutputStream(object):
    def __init__(self, filename):
        self.fileio = io.open(filename, 'wb')

    def write(self, iobuffer):
        num = self.fileio.write(iobuffer.data())
        if num!=iobuffer.length:
            raise IOError("Error writing to file.")

    def close(self):
        self.fileio.close()

class SocketInputStream(InputStream):
    def __init__(self, socket, size):
        InputStream.__init__(self, size)
        self.socket = socket

    def read(self, iobuffer, nbytes=None):
        if nbytes==None or nbytes >= iobuffer.size:
            num = self.socket.recv_into(iobuffer.buff)
        else:
            num = self.socket.recv_into(iobuffer.mem[:nbytes])
        
        if num<=0:
            raise IOError("Socket disconnected.")
        
        iobuffer.length = num
        return num

    def close(self):
        self.socket.shutdown(socket.SHUT_WR)
        self.socket.close()

class SocketOutputStream(object):
    def __init__(self, socket):
        self.socket = socket

    def write(self, iobuffer):
        self.socket.sendall(iobuffer.data())
    
    def close(self):
        self.socket.shutdown(socket.SHUT_WR)
        self.socket.close()
