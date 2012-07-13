import sys
import struct
import commands
import gevent.server
import gevent.socket
import traceback
import socket

from common import ClassLogger
from bufferedio import NetworkOutputStream, OutputStreamWriter, InputStream,\
                       InputStreamReader, NetworkInputStream, IOBuffer

class NetworkHeader(object):
    ERROR = 1
    INTEGER = 2
    STRING = 3
    # Each stream is received as a header indicating the total length
    # and consequent buffers, each of them indicating its length too.
    STREAM_HEADER = 4  
    STREAM_BUFFER = 5

class NetworkException(Exception):
    def __init__(self, message='', trace=''):
        super(NetworkException, self).__init__(self)
        self.trace = trace
        if not trace:
            exc_info = sys.exc_info()
            err_list = traceback.extract_tb(exc_info[2])
            self.trace = ''.join(traceback.format_list(err_list))
            self.trace += '  '+message
    
    def log_forward(self, node):
        self.trace = '  dfs://%s:%d\n'%(node) + self.trace

    def serialize(self):
        return self.trace

    @classmethod
    def unserialize(klass, s):
        return klass(trace=s)

    def __str__(self):
        return '\n'+self.trace

@ClassLogger
class NetworkEndpoint(object):
    def __init__(self, socket):
        self.socket = socket
        self.reading_stream = None
        self.output_stream = NetworkOutputStream(self)

        '''
            The sendall socket mehtod can only be used in blocking 
            (timeout==None) sockets.
        '''
        if self.socket.gettimeout()==None:
            if __debug__: self.logger.debug("Using '_send_bytes_sendall' "
                                            "function.")
            self._send_bytes = self._send_bytes_sendall
        else:
            if __debug__: self.logger.debug("Using '_send_bytes_iter' "
                                            "function.")
            self._send_bytes = self._send_bytes_iter

        self._streamed = 0
        self._to_stream = 0
        self._partial_buffer = 0
    
    def new_writer(self, async=False):
        return OutputStreamWriter(self.output_stream, async=async)
    
    def recv_stream(self):
        stream = self.recv()
        if not isinstance(stream, InputStream):
            raise TypeError("An InputStream was expected.")
        return stream

    def recv_reader(self, async=False):
        return InputStreamReader(self.recv_stream(), async=async)

    def _recv_integer(self):
        # signed 8 bytes integer
        return struct.unpack('!q', self._recv_bytes(8))[0]
    
    def _send_integer(self, integer):
        # signed 8 bytes integer
        return self._send_bytes(struct.pack('!q', integer))
    
    def _recv_header(self):
        raw_data = self._recv_bytes(5)
        # signed 1 byte integer + signed 4 byte integer
        return struct.unpack('!bi', raw_data) 

    def _send_header(self, header_type, len_packet=0):
        # signed 1 byte integer + signed 4 byte integer
        data = struct.pack('!bi', header_type, len_packet)
        self._send_bytes(data)
    
    def _send_bytes_sendall(self, data):
        ret = self.socket.sendall(data)
        assert ret==None

    def _send_bytes_iter(self, data):
        total = len(data)
        sent = 0
        while sent<total:
            sent += self.socket.send(data[sent:])
        if __debug__: self.logger.debug("Sent %d bytes."%(sent)) 

    def _recv_error(self, error_len):
        raw_data = self._recv_bytes(error_len)
        error = NetworkException.unserialize(raw_data)
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
            if self._partial_buffer>0:
                raise IOError("Cannot read new data if the previous buffer "
                              "wasn't completely read.")
                
            if self.reading_stream!=None:
                if not self.reading_stream.is_processed():
                    raise Exception("Cannot receive data from the socket until"
                                    " the stream is processed.")
                self.reading_stream = None

            packet_type, data_len = self._recv_header()
            if __debug__: self.logger.debug("Received header: %d %d.",
                                            packet_type, data_len)

            if packet_type==NetworkHeader.ERROR:
                if __debug__: self.logger.debug("Received error (%d bytes).",
                                                data_len)
                self._recv_error(data_len)
            
            elif packet_type==NetworkHeader.INTEGER:
                if __debug__: self.logger.debug("Received integer.")
                return self._recv_integer()
            
            elif packet_type==NetworkHeader.STRING:
                if __debug__: self.logger.debug("Received string (%d bytes).",
                                                data_len)
                return self._recv_bytes(data_len)
            
            elif packet_type==NetworkHeader.STREAM_HEADER:
                if __debug__: self.logger.debug("Received stream (%d bytes).",
                                                data_len)
                self._streamed = 0
                self._to_stream = data_len
                return NetworkInputStream(self, data_len)

            else:
                raise TypeError("Incompatible NetworkHeader value %d."\
                                %(packet_type))

        except:
            #self.kill()
            raise

    def _fill(self, memview):
        nbytes = min(self._partial_buffer, len(memview))
        received = self.socket.recv_into(memview[:nbytes])
        if received == 0:
            raise IOError("Connection reset.")
        self._partial_buffer -= received 
        if __debug__: self._streamed += received
        if __debug__: self.logger.debug("Received buffer (%d bytes).", received)
        return received
        '''
        nbytes = min(self._partial_buffer, len(memview))
        received = 0
        while received < nbytes:
            new_recv = self.socket.recv_into(memview[received:nbytes])
            if new_recv == 0:
                raise IOError("Connection reset.")
            received += new_recv
        self._partial_buffer -= received 
        self._streamed += received
        if __debug__: self.logger.debug("Received buffer (%d bytes).", received)
        return received
        '''
    
    def recv_into(self, memview):
        try:
            if self._partial_buffer==0:
                packet_type, data_len = self._recv_header()

                if packet_type==NetworkHeader.ERROR:
                    #It raises an exception
                    self._recv_error(data_len)
                    assert False, 'Exception should be raised.'
                
                elif packet_type==NetworkHeader.STREAM_BUFFER:
                    self._partial_buffer = data_len
                    return self._fill(memview)
    
                else:
                    if __debug__: self.logger.debug("Invalid header after "
                                                    "receiving %d streamed "
                                                    "bytes out of %d."\
                                                    %(self._streamed,
                                                      self._to_stream))
                                                    
                    raise TypeError("Incompatible NetworkHeader value %d."\
                                    %(packet_type))
                
            else:
                return self._fill(memview)

        except:
            self.kill()
            raise

    def send(self, obj):
        if isinstance(obj, NetworkException):
            if __debug__: self.logger.debug('Sending exception')
            data = obj.serialize()
            self._send_header(NetworkHeader.ERROR, len(data))
            self._send_bytes(data)

        elif isinstance(obj, IOBuffer):
            if __debug__: self.logger.debug('Sending stream buffer')
            self._send_header(NetworkHeader.STREAM_BUFFER, obj.length)
            self._send_bytes(obj.data())

        elif isinstance(obj, InputStream):
            if __debug__: self.logger.debug('Sending stream header')
            self._send_header(NetworkHeader.STREAM_HEADER, obj.size)

        elif isinstance(obj, str):
            if __debug__: self.logger.debug('Sending string')
            self._send_header(NetworkHeader.STRING, len(obj))
            self._send_bytes(obj)
            
        elif isinstance(obj, int) or isinstance(obj, long):
            if __debug__: self.logger.debug('Sending integer')
            self._send_header(NetworkHeader.INTEGER)
            self._send_integer(obj)
        
        else:
            raise TypeError('Invalid type.')

    def local_address(self):
        ifconfig = commands.getoutput("/sbin/ifconfig")
        return ifconfig.split("\n")[1].split()[1][5:]

    def kill(self):
        if __debug__: self.logger.debug("Killing socket.")
        self.socket.shutdown(socket.SHUT_WR)
        self.socket.close()

@ClassLogger
class ServerHandle(NetworkEndpoint):
    def __init__(self, server, socket, address):
        NetworkEndpoint.__init__(self, socket)
        self.address = address
        self.server = server

    def process_query(self):
        raise NotImplementedError()

    def handle(self):
        send_ack = True
        response = False
        try:
            self.process_query()
            response = True

        except socket.error as e:
            self.logger.error("Failed connection from %s (child of %s): %s."\
                              %(repr(self.address), repr(self.server.address),
                                unicode(e)))
                        
        except NetworkException as e:
            self.logger.error("RaisedNetworkException:\n"+unicode(e))
            e.log_forward((socket.gethostname(),self.server.address[1]))
            self.send(e)
            send_ack = False

        except Exception as e:
            self.logger.error("RaisedException:\n"+traceback.format_exc())
            ne = NetworkException(type(e).__name__+': '+unicode(e))
            ne.log_forward((socket.gethostname(),self.server.address[1]))
            self.send(ne)
            send_ack = False

        finally:
            if __debug__:  self.logger.debug("executing finally statement.")
            try:
                if send_ack:
                    self.send(0 if response else -1)
                if __debug__: self.logger.debug("Closing connection.")
                self.socket.shutdown(socket.SHUT_WR)
                self.socket.close()
            except Exception as e:
                if __debug__: self.logger.debug("ERROR: %s",unicode(e))
                pass

class Server():
    def __init__(self, handle_class=ServerHandle, addr='', port=7777):
        self.address = (addr,port)
        self.server = gevent.server.StreamServer(self.address,
                                                 self.netser_handle)
        self.handle_class = handle_class

    '''
    def init_socket(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        super(Server, self).init_socket()
    '''
        
    def serve(self):
        self.server.serve_forever()

    def netser_handle(self, s, address):
        server_handle = self.handle_class(self, s, address)
        server_handle.handle()

@ClassLogger
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
        if __debug__: self.logger.debug("Received %s", unicode(ack))
        if type(ack)!=int:
            raise IOError("Failed to receive remote ACK.")
        if ack!=0:
            raise IOError("Remote call failed.")
