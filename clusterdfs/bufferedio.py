import io
import numpy
import gevent
import gevent.queue
import gevent.socket

class IOBuffer(object):
    def __init__(self):
        #self.buff = bytearray(io.DEFAULT_BUFFER_SIZE)
        self.buff = numpy.ndarray(shape=(io.DEFAULT_BUFFER_SIZE), dtype=numpy.uint8)
        self.mem = memoryview(self.buff)
        self.length = 0

    def data(self):
        return self.mem[0:self.length]

class BufferedIO(object):
    def __init__(self, size=0, callback=None, num_buffers=2):
        assert callback!=None
        assert size>0
        assert num_buffers>0

        self.write_queue = gevent.queue.Queue()
        self.read_queue = gevent.queue.Queue()

        for i in xrange(num_buffers):
            self.write_queue.put(IOBuffer())

        self.reader = gevent.spawn(self.process)
        self.callback = callback
        self.end_reading = False
        self.processed = 0
        self.size = size

    def process(self):
        while (not self.end_reading) or self.read_queue.qsize()>0:
            iobuffer = self.read_queue.get()
            self.callback(iobuffer.data())
            self.write_queue.put(iobuffer)
    
    def fill_buffer(self):
        assert False, 'Unimplemented method'

    def finished(self):
        return self.processed >= self.size

    def run(self):
        while not self.finished():
            iobuffer = self.write_queue.get()
            self.processed += self.fill_buffer(iobuffer)
            self.read_queue.put(iobuffer)
        
        # Wait until reader finishes
        self.end_reading = True
        self.reader.join()
        
class FileBufferedIO(BufferedIO):
    def __init__(self, f, *args, offset=0, **kwargs):
        BufferedIO.__init__(self, *args, **kwargs)
        self.fio = io.open(f, 'rb')
        self.fio.seek(offset)

    def fill_buffer(self, iobuffer):
        num = self.fio.readinto(iobuffer.buff)
        iobuffer.length = num
        return num

class SocketBufferedIO(BufferedIO):
    def __init__(self, socket, *args, **kwargs):
        BufferedIO.__init__(self, *args, **kwargs)
        self.socket = socket

    def fill_buffer(self, iobuffer):
        num = self.socket.recv_into(iobuffer.buff, min(len(iobuffer.buff), self.size-self.processed))
        if num>0:
            iobuffer.length = num
            self.received += num
            return num
        else:
            raise IOError("Socket disconnected.")

__all__ = ['SocketBufferedIO','FileBufferedIO']
