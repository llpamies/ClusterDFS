import random
import numpy
import time

from clusterdfs.bufferedio import IOBuffer
from cauchyec import CauchyEC
from galoisbuffer import GaloisBuffer

def test_ec(k=11, m=5, w=4, iters=1000):
    code = CauchyEC(k, m, bitfield=w)
    buffers = [IOBuffer().as_numpy_byte_array() for i in xrange(k+m)]
    
    t = time.time()
    for it in xrange(iters):
        random.shuffle(buffers)
        decoded_buffers = buffers[:k] 
        encoded_buffers = buffers[k:]
        code.encode(decoded_buffers, encoded_buffers)
        
    return time.time()-t

def test_pipe(iters=1000, bits=8):
    mx = (2**bits)-1
    rr = numpy.random.randint(0, mx, size=IOBuffer.defsize)
    iobuffers = [IOBuffer() for i in xrange(16)]
    buffers = [GaloisBuffer(iob.size, bitfield=bits, buffer=iob.buff) for iob in iobuffers]
    
    for b in iobuffers:
        array = b.as_numpy_byte_array()
        array += rr*random.randrange(0,mx)
        
    t = time.time()
    for it in xrange(iters):
        random.shuffle(buffers)
        iobuffers[-1].copy_to(iobuffers[-2])
        for i in xrange(2+(it&1)):
            buffers[i].multadd(random.randrange(0,mx), dest=buffers[i+1], add=True)
    return (time.time()-t)*16

iters = 500
print 'caucy', test_ec(iters=iters)
print 'pipe8', test_pipe(iters=iters, bits=8)
print 'pipe16', test_pipe(iters=iters, bits=16)