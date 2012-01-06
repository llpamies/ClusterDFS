import io
import gevent
import gevent.queue
import gevent.socket
import numpy

class IOBuffer(object):
    def __init__(self):
        self.buff = bytearray(io.DEFAULT_BUFFER_SIZE)
        #self.buff = numpy.ndarray(shape=(io.DEFAULT_BUFFER_SIZE), dtype=numpy.byte)
        self.mem = memoryview(self.buff)
        self.length = 0

    def data(self):
        return self.mem[0:self.length]

class BufferedIO(object):
    def __init__(self, callback, num_buffers=2):
        self.write_queue = gevent.queue.Queue()
        self.read_queue = gevent.queue.Queue()

        for i in xrange(num_buffers):
            self.write_queue.put(IOBuffer())

        self.reader = gevent.spawn(self.process)
        self.callback = callback
        self.end_reading = False
        self.processed = 0

    def process(self):
        while (not self.end_reading) or self.read_queue.qsize()>0:
            iobuffer = self.read_queue.get()
            self.callback(iobuffer.data())
            self.write_queue.put(iobuffer)
    
    def fill_buffer(self):
        assert False, 'Unimplemented method'

    def finished(self):
        assert False, 'Unimplemented method'

    def run(self):
        while not self.finished():
            iobuffer = self.write_queue.get()
            self.processed += self.fill_buffer(iobuffer)
            self.read_queue.put(iobuffer)
        
        # Wait until reader finishes
        self.end_reading = True
        self.reader.join()
        
class FileBufferedIO(BufferedIO):
    def __init__(self, f, *args, **kwargs):
        BufferedIO.__init__(self, *args, **kwargs)
        self.fio = io.open(f, 'rb')
        self.tofinish = False

    def finished(self):
        return self.tofinish

    def fill_buffer(self, iobuffer):
        assert not self.tofinish
        num = self.fio.readinto(iobuffer.buff)
        iobuffer.length = num
        if num==0: self.tofinish = True
        return num

class SocketBufferedIO(BufferedIO):
    def __init__(self, socket, size, *args, **kwargs):
        BufferedIO.__init__(self, *args, **kwargs)
        self.socket = socket
        self.received = 0
        self.size = size

    def finished(self):
        return self.received==self.size

    def fill_buffer(self, iobuffer):
        assert self.received<self.size

        num = self.socket.recv_into(iobuffer.buff, min(len(iobuffer.buff), self.size-self.received))
        if num>0:
            iobuffer.length = num
            self.received += num
            return num
        else:
            raise IOError("Socket disconnected.")
