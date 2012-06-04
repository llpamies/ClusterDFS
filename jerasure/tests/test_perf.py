from __future__ import division

import sys
sys.path[0] = 'build/lib.linux-i686-2.7'

import io
import sys
import numpy
import timeit
import random
from cauchyec import CauchyEC

#numpy.set_printoptions(linewidth=160)

k = 3
n = 7
m = n-k 
w = 8
defaultsize = io.DEFAULT_BUFFER_SIZE
sizeoflong = 4

def coding_tests(raw_data, encoded_data, bitfield, iters=10):
    code = CauchyEC(k, m, bitfield=bitfield)

    for i in xrange(iters):
        code.encode(raw_data, encoded_data)

    erasures = random.sample(range(n), m)
    for i in erasures:
        if i<k:
            raw_data[i].fill(-1)
        else:
            encoded_data[i-k].fill(-1)
    code.decode(raw_data, encoded_data, erasures)

number = 1000
for w in xrange(4,33):
    try:
        size = w*sizeoflong*int(defaultsize/w/sizeoflong)
        iters = int(number*defaultsize/size)
        raw_data = [numpy.ndarray(shape=(size,), dtype=numpy.uint8) for i in xrange(k)]
        for i,buff in enumerate(raw_data):
            buff.fill(random.randrange(256))
        encoded_data = [numpy.zeros(size, dtype=numpy.uint8) for i in xrange(m)]
        print w, timeit.timeit(lambda: coding_tests(raw_data, encoded_data, w) ,number=iters)
    except Exception,e:
        print w, unicode(e)
