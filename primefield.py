import sys
import numpy
import numpy.random

p = 4294967291
m = 32

A = numpy.random.randint(sys.maxint, size=20)
B = numpy.random.randint(sys.maxint, size=20)
C = numpy.random.randint(sys.maxint, size=20)
A.dtype = numpy.uint32
B.dtype = numpy.uint32
C.dtype = numpy.uint32

X = (A*((B+C)%p))%p
Y = ((A*B)%p + (A*C)%p)%p

print X
print Y
print X==Y
