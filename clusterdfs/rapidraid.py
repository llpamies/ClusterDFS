from clusterdfs.coding import NetCodingOperations, NetCodingResolver, RemoteNetCodingReader
from clusterdfs.bufferedio import InputStreamReader, FileInputStream, OutputStreamWriter, FileOutputStream

import re
import galoisbuffer

bf = 16

xis = [48386, 55077, 40589, 63304, 49062, 47871, 17507, 49390,
        54054, 45897, 55796, 27611, 50294, 30336, 882, 60087,
        56842, 14836, 7618, 32265, 22869, 38138]

psis = [11950, 26509, 16377, 40200, 35199, 3729, 10098, 16621,
         3878, 45829, 1937, 60293, 38528, 31094, 7594, 45107,
         15330, 63820, 41913, 20615, 58324, 16983]

xisi = map(lambda x: galoisbuffer.inverse_val(x, bitfield=bf), xis)
           
psisi = map(lambda x: galoisbuffer.inverse_val(x, bitfield=bf), psis)

# Decoding operations:

dec_node0_aux = NetCodingOperations('dec_node0_aux', output='stream')
dec_node0_aux.add(('POP','queue','temp'))
dec_node0_aux.add(('COPY','stream','temp'))

dec_node1_aux = NetCodingOperations('dec_node1_aux', output='stream')
dec_node1_aux.add(('POP','queue','temp'))
dec_node1_aux.add(('COPY','stream','temp'))

dec_node2_aux = NetCodingOperations('dec_node2_aux', output='stream')
dec_node2_aux.add(('POP','queue','temp'))
dec_node2_aux.add(('COPY','stream','temp'))

dec_node3_aux = NetCodingOperations('dec_node3_aux', output='stream')
dec_node3_aux.add(('POP','queue','temp'))
dec_node3_aux.add(('COPY','stream','temp'))

dec_node4_aux = NetCodingOperations('dec_node4_aux', output='stream')
dec_node4_aux.add(('POP','queue','temp'))
dec_node4_aux.add(('COPY','stream','temp'))

dec_node5_aux = NetCodingOperations('dec_node5_aux', output='stream')
dec_node5_aux.add(('POP','queue','temp'))
dec_node5_aux.add(('COPY','stream','temp'))

# Decoding Operations:

dec_node0 = NetCodingOperations('dec_node0', [('coded0', 'r'), ('orig0', 'w')], output='stream')
dec_node0.add(('LOAD','temp','coded0'))
dec_node0.add(('MULT', 'temp', xisi[0], 'temp'))
dec_node0.add(('MULT', 'stream', psis[0], 'temp'))
dec_node0.add(('WRITE', 'temp', 'orig0'))
dec_node0.add(('PUSH', 'queue', 'temp'))

dec_node1 = NetCodingOperations('dec_node1', [('coded1', 'r'), ('orig1', 'w'), ('dec_node0', 'r')], output='stream')
dec_node1.add(('LOAD','temp','coded1'))
dec_node1.add(('LOAD','prev','dec_node0'))
dec_node1.add(('IADD', 'temp', 'prev'))
dec_node1.add(('MULT', 'temp', xisi[1], 'temp'))
dec_node1.add(('WRITE', 'temp', 'orig1'))
dec_node1.add(('PUSH', 'queue', 'temp'))
dec_node1.add(('COPY', 'stream', 'prev'))
dec_node1.add(('MULADD', 'stream', psis[1],'temp'))

dec_node2 = NetCodingOperations('dec_node2', [('coded2', 'r'), ('orig2', 'w'), ('dec_node1', 'r')], output='stream')
dec_node2.add(('LOAD','temp','coded2'))
dec_node2.add(('LOAD','prev','dec_node1'))
dec_node2.add(('IADD', 'temp', 'prev'))
dec_node2.add(('MULT', 'temp', xisi[2], 'temp'))
dec_node2.add(('WRITE', 'temp', 'orig2'))
dec_node2.add(('PUSH', 'queue', 'temp'))
dec_node2.add(('COPY', 'stream', 'prev'))
dec_node2.add(('MULADD', 'stream', psis[2],'temp'))

dec_node3 = NetCodingOperations('dec_node3', [('coded3', 'r'), ('orig3', 'w'), ('dec_node2', 'r')], output='stream')
dec_node3.add(('LOAD','temp','coded3'))
dec_node3.add(('LOAD','prev','dec_node2'))
dec_node3.add(('IADD', 'temp', 'prev'))
dec_node3.add(('MULT', 'temp', xisi[3], 'temp'))
dec_node3.add(('WRITE', 'temp', 'orig3'))
dec_node3.add(('PUSH', 'queue', 'temp'))
dec_node3.add(('COPY', 'stream', 'prev'))
dec_node3.add(('MULADD', 'stream', psis[3],'temp'))

dec_node4 = NetCodingOperations('dec_node4', [('coded4', 'r'), ('orig4', 'w'), ('dec_node3', 'r')], output='stream')
dec_node4.add(('LOAD','temp','coded4'))
dec_node4.add(('LOAD','prev','dec_node3'))
dec_node4.add(('IADD', 'temp', 'prev'))
dec_node4.add(('MULT', 'temp', xisi[4], 'temp'))
dec_node4.add(('WRITE', 'temp', 'orig4'))
dec_node4.add(('PUSH', 'queue', 'temp'))
dec_node4.add(('COPY', 'stream', 'prev'))
dec_node4.add(('MULADD', 'stream', psis[4],'temp'))

dec_node5 = NetCodingOperations('dec_node5', [('coded5', 'r'), ('orig5', 'w'), ('dec_node4', 'r'), ('dec_node0_aux', 'r')], output='stream')
dec_node5.add(('LOAD','temp','coded5'))
dec_node5.add(('LOAD','prev','dec_node4'))
dec_node5.add(('LOAD','prevaux','dec_node0_aux'))
dec_node5.add(('IADD', 'temp', 'prev'))
dec_node5.add(('MULADD', 'temp', xis[5], 'prevaux'))
dec_node5.add(('MULT', 'temp', xisi[6], 'temp'))
dec_node5.add(('WRITE', 'temp', 'orig5'))
dec_node5.add(('PUSH', 'queue', 'temp'))
dec_node5.add(('COPY', 'stream', 'prev'))
dec_node5.add(('MULADD', 'stream', psis[5],'prevaux'))
dec_node5.add(('MULADD', 'stream', psis[6],'temp'))

dec_node6 = NetCodingOperations('dec_node6', [('coded6', 'r'), ('orig6', 'w'), ('dec_node5', 'r'), ('dec_node1_aux', 'r')], output='stream')
dec_node6.add(('LOAD','temp','coded6'))
dec_node6.add(('LOAD','prev','dec_node5'))
dec_node6.add(('LOAD','prevaux','dec_node1_aux'))
dec_node6.add(('IADD', 'temp', 'prev'))
dec_node6.add(('MULADD', 'temp', xis[7], 'prevaux'))
dec_node6.add(('MULT', 'temp', xisi[8], 'temp'))
dec_node6.add(('WRITE', 'temp', 'orig6'))
dec_node6.add(('COPY', 'stream', 'prev'))
dec_node6.add(('MULADD', 'stream', psis[7],'prevaux'))
dec_node6.add(('MULADD', 'stream', psis[8],'temp'))

dec_node7 = NetCodingOperations('dec_node7', [('coded7', 'r'), ('orig7', 'w'), ('dec_node6', 'r'), ('dec_node2_aux', 'r')], output='stream')
dec_node7.add(('LOAD','temp','coded7'))
dec_node7.add(('LOAD','prev','dec_node6'))
dec_node7.add(('LOAD','prevaux','dec_node2_aux'))
dec_node7.add(('IADD', 'temp', 'prev'))
dec_node7.add(('MULADD', 'temp', xis[9], 'prevaux'))
dec_node7.add(('MULT', 'temp', xisi[10], 'temp'))
dec_node7.add(('WRITE', 'temp', 'orig7'))
dec_node7.add(('COPY', 'stream', 'prev'))
dec_node7.add(('MULADD', 'stream', psis[9],'prevaux'))
dec_node7.add(('MULADD', 'stream', psis[10],'temp'))

dec_node8 = NetCodingOperations('dec_node8', [('coded8', 'r'), ('orig8', 'w'), ('dec_node7', 'r'), ('dec_node3_aux', 'r')], output='stream')
dec_node8.add(('LOAD','temp','coded8'))
dec_node8.add(('LOAD','prev','dec_node7'))
dec_node8.add(('LOAD','prevaux','dec_node3_aux'))
dec_node8.add(('IADD', 'temp', 'prev'))
dec_node8.add(('MULADD', 'temp', xis[11], 'prevaux'))
dec_node8.add(('MULT', 'temp', xisi[12], 'temp'))
dec_node8.add(('WRITE', 'temp', 'orig8'))
dec_node8.add(('COPY', 'stream', 'prev'))
dec_node8.add(('MULADD', 'stream', psis[11],'prevaux'))
dec_node8.add(('MULADD', 'stream', psis[12],'temp'))

dec_node9 = NetCodingOperations('dec_node9', [('coded9', 'r'), ('orig9', 'w'), ('dec_node8', 'r'), ('dec_node4_aux', 'r')], output='stream')
dec_node9.add(('LOAD','temp','coded9'))
dec_node9.add(('LOAD','prev','dec_node8'))
dec_node9.add(('LOAD','prevaux','dec_node4_aux'))
dec_node9.add(('IADD', 'temp', 'prev'))
dec_node9.add(('MULADD', 'temp', xis[13], 'prevaux'))
dec_node9.add(('MULT', 'temp', xisi[14], 'temp'))
dec_node9.add(('WRITE', 'temp', 'orig9'))
dec_node9.add(('COPY', 'stream', 'prev'))
dec_node9.add(('MULADD', 'stream', psis[13],'prevaux'))
dec_node9.add(('MULADD', 'stream', psis[14],'temp'))

dec_node10 = NetCodingOperations('dec_node10', [('coded10', 'r'), ('orig10', 'w'), ('dec_node9', 'r'), ('dec_node5_aux', 'r')])
dec_node10.add(('LOAD','temp','coded10'))
dec_node10.add(('LOAD','prev','dec_node9'))
dec_node10.add(('LOAD','prevaux','dec_node5_aux'))
dec_node10.add(('IADD', 'temp', 'prev'))
dec_node10.add(('MULADD', 'temp', xis[15], 'prevaux'))
dec_node10.add(('MULT', 'temp', xisi[16], 'temp'))
dec_node10.add(('WRITE', 'temp', 'orig10'))

# Encoding operations:

enc_node0 = NetCodingOperations('enc_node0', [('part0', 'r'), ('coded0', 'w')], output='stream')
enc_node0.add(('LOAD', 'temp', 'part0'))
enc_node0.add(('MULT', 'stream', psis[0], 'temp'))
enc_node0.add(('MULT', 'temp', xis[0], 'temp'))
enc_node0.add(('WRITE', 'temp', 'coded0'))

enc_node1 = NetCodingOperations('enc_node1', [('part1', 'r'), ('coded1', 'w'), ('enc_node0','r')], output='stream')
enc_node1.add(('LOAD', 'local', 'part1'))
enc_node1.add(('LOAD', 'prev', 'enc_node0'))
enc_node1.add(('COPY', 'stream', 'prev'))
enc_node1.add(('MULADD', 'stream', psis[1], 'local'))
enc_node1.add(('MULADD', 'prev', xis[1], 'local'))
enc_node1.add(('WRITE', 'prev', 'coded1'))

enc_node2 = NetCodingOperations('enc_node2', [('part2', 'r'), ('coded2', 'w'), ('enc_node1','r')], output='stream')
enc_node2.add(('LOAD', 'local', 'part2'))
enc_node2.add(('LOAD', 'prev', 'enc_node1'))
enc_node2.add(('COPY', 'stream', 'prev'))
enc_node2.add(('MULADD', 'stream', psis[2], 'local'))
enc_node2.add(('MULADD', 'prev', xis[2], 'local'))
enc_node2.add(('WRITE', 'prev', 'coded2'))

enc_node3 = NetCodingOperations('enc_node3', [('part3', 'r'), ('coded3', 'w'), ('enc_node2','r')], output='stream')
enc_node3.add(('LOAD', 'local', 'part3'))
enc_node3.add(('LOAD', 'prev', 'enc_node2'))
enc_node3.add(('COPY', 'stream', 'prev'))
enc_node3.add(('MULADD', 'stream', psis[3], 'local'))
enc_node3.add(('MULADD', 'prev', xis[3], 'local'))
enc_node3.add(('WRITE', 'prev', 'coded3'))

enc_node4 = NetCodingOperations('enc_node4', [('part4', 'r'), ('coded4', 'w'), ('enc_node3','r')], output='stream')
enc_node4.add(('LOAD', 'local', 'part4'))
enc_node4.add(('LOAD', 'prev', 'enc_node3'))
enc_node4.add(('COPY', 'stream', 'prev'))
enc_node4.add(('MULADD', 'stream', psis[4], 'local'))
enc_node4.add(('MULADD', 'prev', xis[4], 'local'))
enc_node4.add(('WRITE', 'prev', 'coded4'))

enc_node5 = NetCodingOperations('enc_node5', [('part5', 'r'), ('part0', 'r'), ('coded5', 'w'), ('enc_node4','r')], output='stream')
enc_node5.add(('LOAD', 'local0', 'part0'))
enc_node5.add(('LOAD', 'local5', 'part5'))
enc_node5.add(('LOAD', 'prev', 'enc_node4'))
enc_node5.add(('COPY', 'stream', 'prev'))
enc_node5.add(('MULADD', 'stream', psis[5], 'local0'))
enc_node5.add(('MULADD', 'stream', psis[6], 'local5'))
enc_node5.add(('MULADD', 'prev', xis[5], 'local0'))
enc_node5.add(('MULADD', 'prev', xis[6], 'local5'))
enc_node5.add(('WRITE', 'prev', 'coded5'))

enc_node6 = NetCodingOperations('enc_node6', [('part6', 'r'), ('part1', 'r'), ('coded6', 'w'), ('enc_node5','r')], output='stream')
enc_node6.add(('LOAD', 'local1', 'part1'))
enc_node6.add(('LOAD', 'local6', 'part6'))
enc_node6.add(('LOAD', 'prev', 'enc_node5'))
enc_node6.add(('COPY', 'stream', 'prev'))
enc_node6.add(('MULADD', 'stream', psis[7], 'local1'))
enc_node6.add(('MULADD', 'stream', psis[8], 'local6'))
enc_node6.add(('MULADD', 'prev', xis[7], 'local1'))
enc_node6.add(('MULADD', 'prev', xis[8], 'local6'))
enc_node6.add(('WRITE', 'prev', 'coded6'))

enc_node7 = NetCodingOperations('enc_node7', [('part7', 'r'), ('part2', 'r'), ('coded7', 'w'), ('enc_node6','r')], output='stream')
enc_node7.add(('LOAD', 'local2', 'part2'))
enc_node7.add(('LOAD', 'local7', 'part7'))
enc_node7.add(('LOAD', 'prev', 'enc_node6'))
enc_node7.add(('COPY', 'stream', 'prev'))
enc_node7.add(('MULADD', 'stream', psis[9], 'local2'))
enc_node7.add(('MULADD', 'stream', psis[10], 'local7'))
enc_node7.add(('MULADD', 'prev', xis[9], 'local2'))
enc_node7.add(('MULADD', 'prev', xis[10], 'local7'))
enc_node7.add(('WRITE', 'prev', 'coded7'))

enc_node8 = NetCodingOperations('enc_node8', [('part8', 'r'), ('part3', 'r'), ('coded8', 'w'), ('enc_node7','r')], output='stream')
enc_node8.add(('LOAD', 'local3', 'part3'))
enc_node8.add(('LOAD', 'local8', 'part8'))
enc_node8.add(('LOAD', 'prev', 'enc_node7'))
enc_node8.add(('COPY', 'stream', 'prev'))
enc_node8.add(('MULADD', 'stream', psis[11], 'local3'))
enc_node8.add(('MULADD', 'stream', psis[12], 'local8'))
enc_node8.add(('MULADD', 'prev', xis[11], 'local3'))
enc_node8.add(('MULADD', 'prev', xis[12], 'local8'))
enc_node8.add(('WRITE', 'prev', 'coded8'))

enc_node9 = NetCodingOperations('enc_node9', [('part9', 'r'), ('part4', 'r'), ('coded9', 'w'), ('enc_node8','r')], output='stream')
enc_node9.add(('LOAD', 'local4', 'part4'))
enc_node9.add(('LOAD', 'local9', 'part9'))
enc_node9.add(('LOAD', 'prev', 'enc_node8'))
enc_node9.add(('COPY', 'stream', 'prev'))
enc_node9.add(('MULADD', 'stream', psis[13], 'local4'))
enc_node9.add(('MULADD', 'stream', psis[14], 'local9'))
enc_node9.add(('MULADD', 'prev', xis[13], 'local4'))
enc_node9.add(('MULADD', 'prev', xis[14], 'local9'))
enc_node9.add(('WRITE', 'prev', 'coded9'))

enc_node10 = NetCodingOperations('enc_node10', [('part10', 'r'), ('part5', 'r'), ('coded10', 'w'), ('enc_node9','r')], output='stream')
enc_node10.add(('LOAD', 'local5', 'part5'))
enc_node10.add(('LOAD', 'local10', 'part10'))
enc_node10.add(('LOAD', 'prev', 'enc_node9'))
enc_node10.add(('COPY', 'stream', 'prev'))
enc_node10.add(('MULADD', 'stream', psis[15], 'local5'))
enc_node10.add(('MULADD', 'stream', psis[16], 'local10'))
enc_node10.add(('MULADD', 'prev', xis[15], 'local5'))
enc_node10.add(('MULADD', 'prev', xis[16], 'local10'))
enc_node10.add(('WRITE', 'prev', 'coded10'))

enc_node11 = NetCodingOperations('enc_node11', [('part6', 'r'), ('coded11', 'w'), ('enc_node10','r')], output='stream')
enc_node11.add(('LOAD', 'local', 'part6'))
enc_node11.add(('LOAD', 'prev', 'enc_node10'))
enc_node11.add(('COPY', 'stream', 'prev'))
enc_node11.add(('MULADD', 'stream', psis[17], 'local'))
enc_node11.add(('MULADD', 'prev', xis[17], 'local'))
enc_node11.add(('WRITE', 'prev', 'coded11'))

enc_node12 = NetCodingOperations('enc_node12', [('part7', 'r'), ('coded12', 'w'), ('enc_node11','r')], output='stream')
enc_node12.add(('LOAD', 'local', 'part7'))
enc_node12.add(('LOAD', 'prev', 'enc_node11'))
enc_node12.add(('COPY', 'stream', 'prev'))
enc_node12.add(('MULADD', 'stream', psis[18], 'local'))
enc_node12.add(('MULADD', 'prev', xis[18], 'local'))
enc_node12.add(('WRITE', 'prev', 'coded12'))

enc_node13 = NetCodingOperations('enc_node13', [('part8', 'r'), ('coded13', 'w'), ('enc_node12','r')], output='stream')
enc_node13.add(('LOAD', 'local', 'part8'))
enc_node13.add(('LOAD', 'prev', 'enc_node12'))
enc_node13.add(('COPY', 'stream', 'prev'))
enc_node13.add(('MULADD', 'stream', psis[19], 'local'))
enc_node13.add(('MULADD', 'prev', xis[19], 'local'))
enc_node13.add(('WRITE', 'prev', 'coded13'))

enc_node14 = NetCodingOperations('enc_node14', [('part9', 'r'), ('coded14', 'w'), ('enc_node13','r')], output='stream')
enc_node14.add(('LOAD', 'local', 'part9'))
enc_node14.add(('LOAD', 'prev', 'enc_node13'))
enc_node14.add(('COPY', 'stream', 'prev'))
enc_node14.add(('MULADD', 'stream', psis[20], 'local'))
enc_node14.add(('MULADD', 'prev', xis[20], 'local'))
enc_node14.add(('WRITE', 'prev', 'coded14'))

enc_node15 = NetCodingOperations('enc_node15', [('part10', 'r'), ('coded15', 'w'), ('enc_node14','r')])
enc_node15.add(('LOAD', 'local', 'part10'))
enc_node15.add(('LOAD', 'prev', 'enc_node14'))
enc_node15.add(('MULADD', 'prev', xis[21], 'local'))
enc_node15.add(('WRITE', 'prev', 'coded15'))

operations = {}
operations['enc_node0'] = enc_node0
operations['enc_node1'] = enc_node1
operations['enc_node2'] = enc_node2
operations['enc_node3'] = enc_node3
operations['enc_node4'] = enc_node4
operations['enc_node5'] = enc_node5
operations['enc_node6'] = enc_node6
operations['enc_node7'] = enc_node7
operations['enc_node8'] = enc_node8
operations['enc_node9'] = enc_node9
operations['enc_node10'] = enc_node10
operations['enc_node11'] = enc_node11
operations['enc_node12'] = enc_node12
operations['enc_node13'] = enc_node13
operations['enc_node14'] = enc_node14
operations['enc_node15'] = enc_node15

operations['dec_node0'] = dec_node0
operations['dec_node1'] = dec_node1
operations['dec_node2'] = dec_node2
operations['dec_node3'] = dec_node3
operations['dec_node4'] = dec_node4
operations['dec_node5'] = dec_node5
operations['dec_node6'] = dec_node6
operations['dec_node7'] = dec_node7
operations['dec_node8'] = dec_node8
operations['dec_node9'] = dec_node9
operations['dec_node10'] = dec_node10

operations['dec_node0_aux'] = dec_node0_aux
operations['dec_node1_aux'] = dec_node1_aux
operations['dec_node2_aux'] = dec_node2_aux
operations['dec_node3_aux'] = dec_node3_aux
operations['dec_node4_aux'] = dec_node4_aux
operations['dec_node5_aux'] = dec_node5_aux

class RapidRaidResolver(NetCodingResolver):
    def __init__(self, *args, **kwargs):
        self.dataenc_node_config = kwargs.pop('config')
        super(RapidRaidResolver, self).__init__(*args, **kwargs)
    
    def get_reader(self, key):
        if key.startswith('enc_node'):
            coding_id = key[8:]
            coding_id_int = int(re.search("(\d*)",coding_id).group(0))         
            return RemoteNetCodingReader(self.get_enc_node(coding_id_int),
                                          self.block_id, key, self.stream_id)
        
        elif key.startswith('dec_node'):
            coding_id = key[8:]
            coding_id_int = int(re.search("(\d*)",coding_id).group(0))
            return RemoteNetCodingReader(self.get_enc_node(coding_id_int),
                                          self.block_id, key, self.stream_id)
        
        elif key.startswith('part'):
            coding_id = int(key[4:])
            return self.block_store.get_reader(self.get_part(coding_id))
        
        elif key.startswith('coded'):
            coding_id = int(key[5:])
            return self.block_store.get_reader(self.get_coded(coding_id))
        
        else:
            assert False
    
    def get_writer(self, key):
        if key.startswith('coded'):
            coding_id = int(key[5:])
            return self.block_store.get_writer(self.get_coded(coding_id))
        
        elif key.startswith('orig'):
            coding_id = int(key[4:])
            return self.block_store.get_writer(self.get_orig(coding_id))
          
        else:
            assert False
            
    def get_enc_node(self, coding_id):
        #return ('thinclient-%02d'%coding_id, 7000+coding_id)
        return ('localhost', 3900 + coding_id)
    
    def get_part(self, coding_id):
        #return 'girl.64mb.part%d'%coding_id
        return 'part%d'%coding_id
    
    def get_coded(self, coding_id):
        #return 'girl.64mb.coded'
        return 'coded%d'%coding_id
    
    def get_orig(self, coding_id):
        #return 'girl.64mb.coded'
        return 'orig%d'%coding_id
    
__all__ = [operations, RapidRaidResolver]
