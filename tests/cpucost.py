import random
import numpy
import time

from clusterdfs.bufferedio import IOBuffer
from cauchyec import CauchyEC
from galoisbuffer import GaloisBuffer

def test_ec(k=11, m=5, w=4, iters=1000):
    code = CauchyEC(k, m, bitfield=w)
    buffers = [IOBuffer().as_numpy_byte_array()for i in xrange(k+m)]
    
    t = time.time()
    for it in xrange(iters):
        random.shuffle(buffers)
        decoded_buffers = buffers[:k] 
        encoded_buffers = buffers[k:]
        code.encode(decoded_buffers, encoded_buffers)
        
    return time.time()-t

def test_pipe(iters=1000):
    rr = numpy.random.randint(0, 65000, size=IOBuffer.defsize)
    buffers = [IOBuffer() for i in xrange(16)]
    for b in buffers:
        array = b.as_numpy_byte_array()
        array += rr*random.randrange(0,65000)
        
    t = time.time()
    for it in xrange(iters):
        random.shuffle(buffers)
        buffers[-1].copy_to(buffers[-2])
        for i in xrange(2+(it&1)):
            buffers[i].galois.multadd(random.randrange(0,65000),\
                                      dest=buffers[i+1].galois, add=True)
    return (time.time()-t)*16

iters = 500
print test_ec(iters=iters)
print test_pipe(iters=iters)
