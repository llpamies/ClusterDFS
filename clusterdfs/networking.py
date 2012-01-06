import struct
import cPickle
import logging
import commands
import socket
import gevent.server
import gevent.socket

class ServerResponse(object):
    RESPONSE_OK = 0
    RESPONSE_ERROR = 1

    @classmethod
    def ok(cls, msg='', data={}):
        return {'code':cls.RESPONSE_OK, 'msg':msg, 'data':data}

    @classmethod
    def error(cls, msg='', data={}):
        return {'code':cls.RESPONSE_ERROR, 'msg':msg, 'data':data}

class NetworkException(Exception):
    pass

class NetworkEndpoint(object):
    def __init__(self, socket):
        self.socket = socket

    def recv(self):
        # get data_len
        raw_data = self.socket.recv(4)
        if len(raw_data)!=4:
            raise NetworkException("Connection lost receiving header")
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
        self.header = None
        self.server = server

    def process_query(self):
        print self.header
        return ServerResponse.ok()
    
    def handle(self):
        try:
            self.header = self.recv()
            response = self.process_query()
            if response!=None: self.send(response)

        except NetworkException, e:
            logging.error("Failed connection from %s: %s."%(repr(self.address), e))
            self.socket.close()
        
        except socket.error, (value,message):
            logging.error("Failed connection from %s: %s."%(repr(self.address), message))
            self.socket.close()

class Server(object):
    def __init__(self, handle_class=ServerHandle, addr='', port=7777):
        self.server = gevent.server.StreamServer((addr, port), self.handle)
        self.handle_class = handle_class

    def serve(self):
        self.server.serve_forever()

    def handle(self, socket, address):
        server_handle = self.handle_class(self, socket, address)
        server_handle.handle()
        
class Client(NetworkEndpoint):
    def __init__(self, addr, port):
        NetworkEndpoint.__init__(self, gevent.socket.create_connection((addr, port)))
