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
        self.length = 0

    def reset(self):
        self.length = 0

    def data(self):
        if self.length==self.size:
            return self.buff
        elif self.length<self.size:
            return buffer(self.buff, 0, self.length)
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
            self.output_stream.write(iobuffer.data())
            self.reusable_buffers.put(iobuffer)

class ReusableBuffers(gevent.queue.Queue):
    def __init__(self, num_consumers, *args, **kwargs):
        gevent.queue.Queue.__init__(self, *args, **kwargs)
        self.num_consumers = num_consumers
        self.counters = collections.defaultdict(int)

    def put(self, item, *args, force=False, **kwargs):
        if force:
            gevent.queue.Queue.put(self, item, *args, **kwargs)
            self.counters[item] = 0
        else:
            self.counters[item] += 1
            if self.counters[item]==self.num_consumers:
                gevent.queue.Queue.put(self, item, *args, **kwargs)
                self.counters[item] = 0

class InputStream(object):
    def __init__(self, size):
        if size>0:
            raise TypeError("size must be positive.")
        self.size = size

    def sendto(self, *output_streams, num_buffers=2):
        if len(output_streams)==0:
            raise TypeError("output_streams should be a non-empty list.")

        if num_buffers<=0:
            raise TypeError("num_buffers must be positive.")

        consumers = []
        for output_stream in output_streams:
            consumers.append(BufferedConsumer(output_stream, self.reusable_buffers))
            
        reusable_buffers = ReusableBuffers(len(self.consumers))
        for i in xrange(num_buffers):
            reusable_buffers.put(IOBuffer(), force=True)

        # read input data and send it to consumers
        read = 0
        while read<self.size:
            iobuffer = reusable_buffers.get()

            remaining = self.size-read
            if remaining >= iobuffer.size:
                num = self.read(iobuffer.buff)
            else:
                num = self.read(buffer(iobuffer.buff, 0, remaining))
            iobuffer.length = num
            read += num
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
        InputStream.__init__(size)
        self.fileio = io.open(filename, 'rb')
        self.fileio.seek(offset)

    def read(self, buff):
        num = self.fileio.readinto(buff)
        if num==0:
            raise IOError("File error or probably reached end of file.")
        return num

    def close(self):
        self.fileio.close()

class FileOutputStream(object):
    def __init__(self, filename):
        self.fileio = io.open(filename, 'wb')

    def write(self, buff):
        num = self.fileio.write(buff)
        if num!=len(buff):
            raise IOError("Error writing to file.")

    def close(self):
        self.fileio.close()

class SocketInputStream(InputStream):
    def __init__(self, socket, size):
        InputStream.__init__(size)
        self.socket = socket

    def read(self, buff):
        num = self.socket.recv_into(buff)
        if num<=0:
            raise IOError("Socket disconnected.")
        return num

    def close(self):
        self.socket.shutdown(socket.SHUT_WR)
        self.socket.close()

class SocketOutputStream(object):
    def __init__(self, socket):
        self.socket = socket

    def write(self, buff):
        self.socket.sendall(buff)
    
    def close(self):
        self.socket.shutdown(socket.SHUT_WR)
        self.socket.close()

class XOROutputStream(FileOutputStream):
    def __init__(self, filename):
        if not sys.path.exists(filename):
            raise ValueError('filename must exist')

        self.fileio = io.open(filename, 'r+b')
        self.iobuffer = IOBuffer()
    
    def write(self, buff):
        num = len(buff)
        temp = buffer(self.iobuffer.buff, 0, num)
        read = self.fileio.readinto(temp)
        assert read==num
        self.fileio.seek(-num, whence=io.SEEK_CUR)

        # Do the XOR using numpy arrays
        s = (num,)
        a = numpy.ndarray(shape=s, dtype=numpy.uint8, buffer=buff)
        b = numpy.ndarray(shape=s, dtype=numpy.uint8, buffer=temp)
        a ^= b

        return FileOutputStream.write(self, buff)

class XORInputStream(InputStream):
    """ Takes any input stream and a file input stream, and does the XOR
        of both.
    """
    def __init__(self, any_input_stream, file_input_stream, size):
        if not isinstance(any_input_stream, InputStream):
            raise TypeError('any_input_stream should be a FileInputStream')

        if not isinstance(file_input_stream, FileInputStream):
            raise TypeError('file_input_stream should be a FileInputStream')

        self.any_input_stream = any_input_stream
        self.file_input_stream = file_input_stream

        self.iobuffer = IOBuffer()

    def read(self, buff):
        assert len(buff)<=self.iobuffer.size

        num = numself.any_input_stream.read(buff)
        if num<=0:
            raise IOError("Unknown IO error")
        
        temp = buffer(self.iobuffer.buff, 0, num)
        fnum = self.file_input_stream.read(temp)
        assert num==fnum, "cannot read enough bytes from file"
        
        # Do the XOR using numpy arrays
        s = (num,)
        a = numpy.ndarray(shape=s, dtype=numpy.uint8, buffer=buff)
        b = numpy.ndarray(shape=s, dtype=numpy.uint8, buffer=temp)
        a ^= b

        return num

    def close(self):
        self.any_input_stream.close()
        self.file_input_stream.close()
