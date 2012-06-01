import sys
sys.path[0] = 'build/lib.linux-i686-2.7'

import numpy
import numpy.random
import galoisarray

bf = 8
N = 5000*(32/bf)

ra = numpy.random.randint(2**31-1, size=N)
rb = numpy.random.randint(2**31-1, size=N)

a = galoisarray.GaloisArray(N, bitfield=bf, buffer=ra.data)
b = galoisarray.GaloisArray(N, bitfield=bf, buffer=rb.data)

iters = 100000
x = int((2**bf)/2-3)

for i in xrange(iters):
    a += b
    a.multadd(x)
