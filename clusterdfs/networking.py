import sys
import struct
import cPickle
import cStringIO
import logging
import commands
import socket
import gevent.server
import gevent.socket
import traceback

from common import *
from bufferedio import *

class NetworkHeader(object):
    ERROR = 1
    VALUE = 2
    STREAM = 3
    BUFFER = 4

class NetworkException(Exception):
    def __init__(self, message=''):
        self.message = message
        type, value, tb = sys.exc_info()
        self.localtrace = traceback.extract_tb(tb)
        self.nettrace = []
    
    def log_forward(self, node):
        self.nettrace.insert(0, node)

    def __str__(self):
        return 'NetworkError:\n  Network flow:\n    '+\
               '\n    '.join(map(lambda x: 'dfs://%s:%d'%x, self.nettrace))+\
               '\n' + ''.join(traceback.format_list(self.localtrace))+\
               self.message

@ClassLogger
class NetworkEndpoint(object):
    def __init__(self, socket):
        self.socket = socket
        self.reading_stream = None
        self.output_stream = NetworkOutputStream(self)

        '''
            The sendall socket mehtod can only be used in blocking (timeout==None) sockets.
        '''
        if self.socket.gettimeout()==None:
            if __debug__: self.logger.debug("Using '_send_bytes_sendall' function.")
            self._send_bytes = self._send_bytes_sendall
        else:
            if __debug__: self.logger.debug("Using '_send_bytes_iter' function.")
            self._send_bytes = self._send_bytes_iter

        self._streamed = 0

    def get_stream(self):
        self.send(self.header)
        stream = self.recv()
        if not isinstance(stream, InputStream):
            raise TypeError("An InputStream was expected.")
        return stream

    def get_reader(self):
        return InputStreamReader(self.get_stream())

    def new_writer(self):
        return OutputStreamWriter(self.output_stream)

    def _get_header(self):
        raw_data = self._recv_bytes(5)
        return struct.unpack('<BI', raw_data)

    def _send_header(self, header_type, len_packet):
        data = struct.pack('<BI', header_type, len_packet)
        self._send_bytes(data)
    
    def _send_bytes_sendall(self, data):
        self.socket.sendall(data)

    def _send_bytes_iter(self, data):
        total = len(data)
        sent = 0
        while sent<total:
            sent += self.socket.send(data[sent:])
        if __debug__: self.logger.debug("Sent %d bytes."%(sent)) 

    def _recv_error(self, error_len):
        raw_data = self._recv_bytes(error_len)
        error = cPickle.loads(raw_data)
        assert isinstance(error, Exception)
        if __debug__: self.logger.debug("Received NetworkException.") 
        raise error

    def _recv_bytes(self, num_bytes):
        data = ''
        old_len = 0
        while len(data)<num_bytes:
            data += self.socket.recv(num_bytes-old_len)
            new_len = len(data)
            if new_len==old_len:
                raise IOError("Connection lost while receiving bytes.")
            old_len = new_len
        assert len(data)==num_bytes
        return data

    def recv(self):
        try:
            if self.reading_stream!=None:
                if not self.reading_stream.is_processed():
                    raise Exception("Cannot receive data from the socket until the stream is processed.")
                self.reading_stream = None

            packet_type, data_len = self._get_header()

            if packet_type==NetworkHeader.ERROR:
                self._recv_error(data_len)

            elif packet_type==NetworkHeader.VALUE:
                raw_data = self._recv_bytes(data_len)
                assert len(raw_data)==data_len, (len(raw_data),data_len)
                if __debug__: self.logger.debug("Received python value.") 
                return cPickle.loads(raw_data)
            
            elif packet_type==NetworkHeader.STREAM:
                if __debug__: self.logger.debug("Received stream (%d bytes).", data_len)
                self._streamed = 0
                return NetworkInputStream(self, data_len)

            else:
                raise TypeError("Incompatible NetworkHeader value %d."%(packet_type))

        except:
            self.kill()
            raise

    def recv_into(self, memview):
        try:
            packet_type, data_len = self._get_header()

            if packet_type==NetworkHeader.ERROR:
                #It raises an exception
                self._recv_error(data_len)
                assert False, 'Exception should be raised.'
            
            elif packet_type==NetworkHeader.BUFFER:
                if len(memview)<data_len:
                    raise IOError("The memory buffer is too small.")
                received = 0
                while received<data_len:
                    new_recv = self.socket.recv_into(memview[received:])
                    if new_recv==0:
                        raise IOError("Connection reset.")
                    received += new_recv 
                assert received==data_len, (received, data_len)
                self._streamed += received
                if __debug__: self.logger.debug("Received buffer (%d bytes).", data_len)
                return received

            elif packet_type==NetworkHeader.VALUE:
                if __debug__: self.logger.debug("Invalid VALUE header after receiving %d streamed bytes."%(self._streamed))
                raise ValueError("Cannot parse VALUE from recv_into.")

            else:
                if __debug__: self.logger.debug("Unknown header after receiving %d streamed bytes."%(self._streamed))
                raise TypeError("Incompatible NetworkHeader value %d."%(packet_type))

        except:
            self.kill()
            raise

    def send(self, obj):
        if isinstance(obj, NetworkException):
            if __debug__: self.logger.debug('Sending exception')
            data = cPickle.dumps(obj)
            self._send_header(NetworkHeader.ERROR, len(data))
            self._send_bytes(data)

        elif isinstance(obj, IOBuffer):
            if __debug__: self.logger.debug('Sending buffer')
            self._send_header(NetworkHeader.BUFFER, obj.length)
            self._send_bytes(obj.data())

        elif isinstance(obj, InputStream):
            if __debug__: self.logger.debug('Sending stream')
            self._send_header(NetworkHeader.STREAM, obj.size)

        else: #py obj
            if __debug__: self.logger.debug('Sending object')
            data = cPickle.dumps(obj)
            self._send_header(NetworkHeader.VALUE, len(data))
            self._send_bytes(data)

    def local_address(self):
        return commands.getoutput("/sbin/ifconfig").split("\n")[1].split()[1][5:]

    def kill(self):
        self.socket.shutdown(socket.SHUT_WR)
        self.socket.close()

@ClassLogger
class ServerHandle(NetworkEndpoint):
    def __init__(self, server, socket, address):
        NetworkEndpoint.__init__(self, socket)
        self.address = address
        self.server = server

    def handle(self):
        response = False
        try:
            self.header = self.recv()
            self.process_query()
            response = True

        except socket.error as e:
            self.logger.error("Failed connection from %s: %s."%(repr(self.address), unicode(e)))
            response = None

        except NetworkException as e:
            self.logger.error("RaisedNetworkException:\n"+unicode(e))
            e.log_forward((socket.gethostname(),self.address[1]))
            self.send(e)

        except Exception as e:
            self.logger.error("RaisedException:\n"+traceback.format_exc())
            e = NetworkException(type(e).__name__+': '+unicode(e))
            e.log_forward((socket.gethostname(),self.address[1]))
            self.send(e)

        finally:
            try:
                if response!=None: self.send(response)
                self.socket.shutdown(socket.SHUT_WR)
                self.socket.close()
            except:
                pass

class Server(object):
    def __init__(self, handle_class=ServerHandle, addr='', port=7777, timeout=None):
        self.server = gevent.server.StreamServer((addr, port), self.handle)
        self.handle_class = handle_class
        self.timeout = timeout

    def serve(self):
        self.server.serve_forever()

    def handle(self, s, address):
        s.settimeout(self.timeout)
        server_handle = self.handle_class(self, s, address)
        server_handle.handle()
        
class Client(NetworkEndpoint):
    def __init__(self, addr, port):
        self.address = (addr, port)
        try:
            socket = gevent.socket.create_connection(self.address, timeout=None)
        except:
            raise IOError("Cannot connect to "+unicode(self.address))
        NetworkEndpoint.__init__(self, socket)
 
    def assert_ack(self):
        ack = self.recv()
        if type(ack)!=bool or (not ack):
            raise CodingException("Failed to receive remote ACK.")
