#!/usr/bin/env python
import sys
import os.path
from datanode import DataNodeHeader
from bufferedio import FileBufferedIO
from networking import *

class DFS(Client):
    def store(self, path):
        length = os.path.getsize(path)
        header = {'op':DataNodeHeader.OP_STORE, 'length':length, 'id':path.split('/')[-1]}
        header['fwdlist'] = [('172.21.48.151',7777)]
        print header
        self.send(header)
        FileBufferedIO(path, lambda d: self.socket.sendall(d)).run()
        result = self.recv()
        print result

    def retrieve(self, name, path):
        header = {'op':DataNodeHeader.OP_RETRIEVE, 'id':name}
        print header
        self.send(header)
        size = self.recv()
        f = open(path, 'wb')
        for data in SocketIterable(self.socket, size):
            f.write(data)
        f.close()
        result = self.recv()
        print result

if __name__=='__main__':
    server = sys.argv[1]
    port = int(sys.argv[2])
    dfs = DFS(server, port)

    operation = sys.argv[3]
    if operation=='STORE':
        path = sys.argv[4]
        dfs.store(path)
    elif operation=='RETRIEVE':
        name = sys.argv[4]
        path = sys.argv[5]
        dfs.retrieve(name, path)
    else:
        assert False
