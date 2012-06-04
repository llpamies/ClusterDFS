cimport numpy
import numpy
from jerasure cimport *
import logging

class GaloisBuffer:
    def __init__(self, size, buffer=None, bitfield=8):
        self.size = size
        self.bitfield = bitfield
        self.maxv = (2<<bitfield)-1

        if self.bitfield not in [8,16,32]:
            raise ValueError("Incompatible bitfield. Use 8, 16 or 32.")

        if (self.size*8)%self.bitfield!=0:
            raise ValueError("Buffer size should be multiple of bitfield (size is %d and bitfield %d)."%(self.size, self.bitfield))
        
        if buffer==None:
            self.buff = numpy.ndarray(shape=(self.size,), dtype=numpy.uint8)
        else:
            if len(buffer)<self.size:
                raise ValueError("Buffer is too small.")
            self.buff = numpy.ndarray(shape=(self.size,), buffer=buffer, dtype=numpy.uint8)

    def __repr__(self):
        return self.buff.__repr__()

    def __str__(self):
        return self.buff.__str__()

    def __add__(self, other):
        if not isinstance(other, GaloisBuffer):
            raise TypeError("Operand must be a GaloisBuffer.")
        return GaloisBuffer(self.size, buffer=self.buff.__xor__(other.buff).data, bitfield=self.bitfield)

    def __iadd__(self, other):
        if not isinstance(other, GaloisBuffer):
            raise TypeError("Operand must be a GaloisBuffer.")
        self.buff.__ixor__(other.buff)
        return self

    def __mul__(self, other):
        if type(other)!=int:
            raise TypeError("Multiplication operand must be an integer.")
        
        if other<0 or other>self.maxv:
            raise ValueError("Value out of range.")

        return self.multadd(other, dest=GaloisBuffer(self.size, bitfield=self.bitfield), add=False)

    def __imul__(self, other):
        if type(other)!=int:
            raise TypeError("Multiplication operand must be an integer.")
       
        if other<0 or other>self.maxv:
            raise ValueError("Value out of range.")

        return self.multadd(other, dest=self, add=False)

    def multadd(self, other, dest=None, add=False):
        if type(other)!=int:
            raise TypeError("Multiplication operand must be an integer.")
        
        if other<0 or other>self.maxv:
            raise ValueError("Value out of range.")

        if dest!=None and not isinstance(dest, GaloisBuffer):
            raise TypeError("dest operand must be a GaloisBuffer.")

        if dest.size!=self.size:
            raise ValueError("Buffers must have the same size.")

        dest = dest if dest else self
        add = 1 if add else 0

        if self.bitfield==8:
            galois_w08_region_multiply(numpy.PyArray_BYTES(self.buff), other, self.size, numpy.PyArray_BYTES(dest.buff), add)
        elif self.bitfield==16:
            galois_w16_region_multiply(numpy.PyArray_BYTES(self.buff), other, self.size, numpy.PyArray_BYTES(dest.buff), add)
        elif self.bitfield==32:
            galois_w32_region_multiply(numpy.PyArray_BYTES(self.buff), other, self.size, numpy.PyArray_BYTES(dest.buff), add)
        else:
            assert False
      
        return dest

    def inverse_val(self, val):
        if type(val)!=int:
            raise TypeError("Inversion operand must be an integer.")

        if val<0 or val>self.maxv:
            raise ValueError("Value out of range.")

        return galois_inverse(val, self.bitfield)

def inverse_val(val, bitfield=8):
    if type(val)!=int:
        raise TypeError("Inversion operand must be an integer.")
    
    if val<0 or val>(2<<bitfield)-1:
        raise ValueError("Value out of range.")

    return galois_inverse(val, bitfield)
