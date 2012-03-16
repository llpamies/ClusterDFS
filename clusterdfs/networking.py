import struct
import cPickle
import logging
import commands
import socket
import gevent.server
import gevent.socket

class NetworkHeader(object):
    OK = 0
    ERROR = 1
    QUERY = 2
    RESPONSE = 3

    @classmethod
    def ok(cls, msg=''):
        return {'code':cls.OK, 'msg':msg}

    @classmethod
    def error(cls, msg=''):
        return {'code':cls.ERROR, 'msg':msg}

    @classmethod
    def query(cls, **kwargs):
        kwargs['code'] = cls.QUERY
        return kwargs

    @classmethod
    def response(cls, **kwargs):
        kwargs['code'] = cls.RESPONSE
        return kwargs

class NetworkException(Exception):
    pass

class NetworkEndpoint(object):
    def __init__(self, socket):
        self.socket = socket

    def recv(self):
        # get data_len
        raw_data = self.socket.recv(4)
        if len(raw_data)!=4:
            raise IOError("Connection lost receiving header: got %d bytes."%len(raw_data))
        data_len, = struct.unpack('<I', raw_data)

        # get data
        raw_data = self.socket.recv(data_len)
        assert len(raw_data)==data_len
        return cPickle.loads(raw_data)

    def send(self, data):
        raw_data = cPickle.dumps(data)
        self.socket.sendall(struct.pack('<I', len(raw_data)))
        self.socket.sendall(raw_data)

    def local_address(self):
        return commands.getoutput("/sbin/ifconfig").split("\n")[1].split()[1][5:]

    def kill(self):
        self.socket.shutdown(socket.SHUT_WR)
        self.socket.close()

class ServerHandle(NetworkEndpoint):
    def __init__(self, server, socket, address):
        NetworkEndpoint.__init__(self, socket)
        self.address = address
        self.server = server
        self.response_sent = False

    def process_query(self):
        return NetworkHeader.ok()
   
    def send_response(self, resp):
        if self.response_sent:
            raise NetworkException('Response was already sent.')
        self.response_sent = True
        self.send(resp)

    def handle(self):
        try:
            self.header = self.recv()
            response = self.process_query()
            if response!=None:
                if self.response_sent:
                    raise NetworkException('Response was already sent.')
                self.send(response)
            elif not self.response_sent:
                self.send(NetworkHeader.ok())

        except socket.error as (value,message):
            logging.error("Failed connection from %s: %s."%(repr(self.address), message))

        except Exception as e:
            logging.error("Internal error: "+unicode(e))
            self.send(NetworkHeader.error(msg=unicode(e)))
            raise

class Server(object):
    def __init__(self, handle_class=ServerHandle, addr='', port=7777):
        self.server = gevent.server.StreamServer((addr, port), self.handle)
        self.handle_class = handle_class

    def serve(self):
        self.server.serve_forever()

    def handle(self, s, address):
        try:
            server_handle = self.handle_class(self, s, address)
            server_handle.handle()
        finally:
            s.shutdown(socket.SHUT_WR)
            s.close()
        
class Client(NetworkEndpoint):
    def __init__(self, addr, port):
        self.address = (addr, port)
        try:
            socket = gevent.socket.create_connection(self.address)
        except:
            raise NetworkException("Cannot connect to "+unicode(self.address))
        NetworkEndpoint.__init__(self, socket)
