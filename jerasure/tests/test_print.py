import sys
sys.path[0] = 'build/lib.linux-i686-2.7'

import numpy
import random
from cauchyec import CauchyEC

numpy.set_printoptions(linewidth=160)

k = 3
n = 7
m = n-k 
w = 4
size = 32

print 'Creating raw objects and encoded buffers...'
raw_data = [numpy.ndarray(shape=(size,), dtype=numpy.uint8) for i in xrange(k)]
for i,buff in enumerate(raw_data):
    buff.fill(random.randrange(256))
encoded_data = [numpy.zeros(size, dtype=numpy.uint8) for i in xrange(m)]

code = CauchyEC(k, m, bitfield=w)
code.encode(raw_data, encoded_data)

print '\nRAW DATA:'
for i,d in enumerate(raw_data):
    print i,d

print '\nPARITY DATA:'
for i,d in enumerate(encoded_data):
    print i,d

erasures = random.sample(range(n), m)
print '\nERASURES:',erasures

for i in erasures:
    if i<k:
        raw_data[i].fill(0)
    else:
        encoded_data[i-k].fill(0)

code.decode(raw_data, encoded_data, erasures)

print '\nAFTER REPAIRING:'

print '\nRAW DATA:'
for i,d in enumerate(raw_data):
    print i,d

print '\nPARITY DATA:'
for i,d in enumerate(encoded_data):
    print i,d
