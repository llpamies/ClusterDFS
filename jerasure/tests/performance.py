import sys
sys.path[0] = 'build/lib.linux-i686-2.7'

import time
import numpy
import random
import galoisbuffer

N = 8192
for bf in [8,16,32]:
    t = time.time()

    ba = bytearray(N)
    bb = bytearray(N)

    ra = numpy.ndarray((N,), dtype=numpy.uint8, buffer=ba)
    rb = numpy.ndarray((N,), dtype=numpy.uint8, buffer=bb)

    for i in xrange(N):
        ra[i] = random.randrange(256) 
        rb[i] = random.randrange(256)

    a = galoisbuffer.GaloisBuffer(N, bitfield=bf, buffer=ba)
    b = galoisbuffer.GaloisBuffer(N, bitfield=bf, buffer=bb)

    iters = 100000
    x = int((2**bf)/2-3)

    for i in xrange(iters):
        a.multadd(x, b, add=True)

    print bf, time.time()-t
