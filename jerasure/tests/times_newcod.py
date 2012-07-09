from __future__ import division

import sys
sys.path[0] = 'build/lib.linux-i686-2.7'

import io
import sys
import numpy
import timeit
import random

from pipe import PipeCod
from galoisbuffer import GaloisBuffer
#numpy.set_printoptions(linewidth=160)

class PipeTest:
    def __init__(self, v, len, num, bf):
        self.code_pipe = PipeCod(v, num, bf)
        self.len = len
        self.bf = bf

    def test(self, inputs, output):
        self.code_pipe.encode(inputs, output)

class NaiveTest:
    def __init__(self, v, len, num, bf):
        self.v = v
        self.len = len
        self.bf = bf

    def test(self, inputs, output):
        gb = []
        for i in inputs:
            gb.append(GaloisBuffer(self.len, bitfield=self.bf, buffer=i))

        for i in xrange(len(gb)-1):
            gb[i].multadd(self.v, gb[-1], add=True)

        output[:] = inputs[-1][:]

n = 3
w = 8
size = io.DEFAULT_BUFFER_SIZE
v = random.randrange(2**w)
iters = 10000

allbuffs = [numpy.ndarray(shape=(size,), dtype=numpy.uint8) for i in xrange(n+1)]
for i,buff in enumerate(allbuffs):
    buff.fill(random.randrange(2**w))
ins = allbuffs[:-1]
out = allbuffs[-1]
for C in [PipeTest, NaiveTest]:
    p = C(v, size, n, w)
    '''
    p.test(ins, out)
    print out
    '''
    print C, timeit.timeit(lambda: p.test(ins, out), number=iters)
