#!/usr/bin/env python
import os
import time

from clusterdfs.coding import XORInputStream
from clusterdfs.datanode import DataNodeHeader
from clusterdfs.bufferedio import FileInputStream, FileOutputStream, SocketOutputStream, SocketInputStream
from clusterdfs.networking import Server, ServerHandle, Client, NetworkHeader

class BulkHandle(ServerHandle):
    def process_query(self):
        return self.receive_bulk('girl.mp4', self.header['nodes'], read_offset=0, read_length=self.header['length'])

    def send_bulk(self, file_path, datanodes):
        dn_clients = [Client(*dn) for dn in datanodes]
        file_length = os.path.getsize(file_path)

        # Send headers
        header = {'op':DataNodeHeader.OP_STORE, 'length':file_length, 'id':file_path.split('/')[-1]}
        for client in dn_clients:
            client.send(header)

        # Send file
        fis = FileInputStream(file_path, file_length)
        fis.sendto(*(SocketOutputStream(c.socket) for c in dn_clients))

        # Check responses
        for client in dn_clients:
            serv_resp = client.recv()
            if serv_resp['code']!=ServerResponse.RESPONSE_OK:
                raise Exception(serv_resp.msg)

    def receive_bulk(self, file_id, datanodes, read_offset=None, read_length=None):
        t = time.time()
        dn_clients = [Client(*dn) for dn in datanodes]
        
        # Send headers
        header = {'op':DataNodeHeader.OP_RETRIEVE, 'id':file_id}
        if read_offset!=None:
            header['offset'] = read_offset
        if read_length!=None:
            header['length'] = read_length
        
        for client in dn_clients:
            client.send(header)
        
        # Check responses
        sizes = []
        for client in dn_clients:
            serv_resp = client.recv()
            if serv_resp['code']==NetworkHeader.ERROR:
                return NetworkHeader.error("One of the server replied: "+serv_resp['msg'])
            else:
                sizes.append(serv_resp['length'])

        # Process sizes
        size = sizes[0]
        if sizes.count(size)!=len(sizes):
            raise Exception('The files do not have the same size.')

        # Recv file
        xis = XORInputStream([SocketInputStream(c.socket, size) for c in dn_clients], size)
        xis.sendto(FileOutputStream('/dev/null'))
 
        return NetworkHeader.response(time=time.time()-t)
       
if __name__=='__main__':
    b = Server(BulkHandle, port=9999)
    b.serve()
