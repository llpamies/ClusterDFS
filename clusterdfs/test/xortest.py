import time
import random
import struct
import numpy

total_time = 0
size = 5000
num_iters = 300

a = bytearray(size)
b = bytearray(size)
c = bytearray(size)
_a = numpy.ndarray(shape=(size), dtype='b')
_b = numpy.ndarray(shape=(size), dtype='b')
_c = numpy.ndarray(shape=(size), dtype='b')

times = {'m1':0, 'm2':0, 'm3':0, 'm4':0, 'm5':0}

for it in xrange(num_iters):
    for i in xrange(size):
        a[i] = chr(random.randrange(0, 256))
        b[i] = chr(random.randrange(0, 256))
        _a[i] = random.randrange(0, 256)
        _b[i] = random.randrange(0, 256)

    # method1: 
    init = time.time()
    for i in xrange(size):
        c[i] = a[i]^b[i]
    times['m1'] += time.time() - init
    
    # method2: 
    init = time.time()
    offset = 0
    for i in xrange(size/8):
        ai, = struct.unpack_from('Q', buffer(a, offset, offset+8))
        bi, = struct.unpack_from('Q', buffer(b, offset, offset+8))
        struct.pack_into('Q', c, offset, ai^bi)
        offset += 8
    times['m2'] += time.time() - init

    # method3: 
    init = time.time()
    c = bytearray(numpy.array(a)^numpy.array(b))
    times['m3'] += time.time() - init

    # method4: 
    init = time.time()
    r = numpy.array(a)^numpy.array(b)
    for i in xrange(size):
        c[i] = r[i]
    times['m4'] += time.time() - init

    # method5:
    init = time.time()
    _c = _a^_b
    times['m5'] += time.time() - init

ks = times.keys()
ks.sort()
for k in ks:
    print k, times[k]/num_iters
