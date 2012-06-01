cimport numpy
import numpy
from jerasure cimport *
import logging

class GaloisArray:
    def __init__(self, size, buffer=None, bitfield=16):
        self.size = size
        self.bitfield = bitfield
        self.bytes = self.size*self.bitfield/8

        if self.bitfield not in [8,16,32]:
            raise ValueError("Incompatible bitfield. Use 8, 16 or 32.")

        if buffer!=None and len(buffer)!=self.bytes:
            raise ValueError("Incompatible buffer size.")

        if self.bitfield==8:
            self.buff = numpy.ndarray(shape=(self.size,), buffer=buffer, dtype=numpy.uint8)
        elif self.bitfield==16:
            self.buff = numpy.ndarray(shape=(self.size,), buffer=buffer, dtype=numpy.uint16)
        else:
            self.buff = numpy.ndarray(shape=(self.size,), buffer=buffer, dtype=numpy.uint32)

    def __repr__(self):
        return self.buff.__repr__()

    def __str__(self):
        return self.buff.__str__()

    def __add__(self, other):
        if not isinstance(other, GaloisArray):
            raise TypeError("Operand must be a GaloisArray.")
        return GaloisArray(self.size, buffer=self.buff.__xor__(other.buff).data, bitfield=self.bitfield)

    def __iadd__(self, other):
        if not isinstance(other, GaloisArray):
            raise TypeError("Operand must be a GaloisArray.")
        self.buff.__ixor__(other.buff)
        return self

    def __mul__(self, other):
        if type(other)!=int:
            raise TypeError("Multiplication operand must be an integer.")
        
        return self.multadd(other, dest=GaloisArray(self.size, bitfield=self.bitfield), add=False)

    def __imul__(self, other):
        if type(other)!=int:
            raise TypeError("Multiplication operand must be an integer.")
       
        return self.multadd(other, dest=self, add=False)

    def multadd(self, other, dest=None, add=False):
        if type(other)!=int:
            raise TypeError("Multiplication operand must be an integer.")
        
        if dest!=None and not isinstance(dest, GaloisArray):
            raise TypeError("dest operand must be a GaloisArray.")

        dest = dest if dest else self
        add = 1 if add else 0

        if self.bitfield==8:
            galois_w08_region_multiply(numpy.PyArray_BYTES(self.buff), other, self.bytes, numpy.PyArray_BYTES(dest.buff), add)
        elif self.bitfield==16:
            galois_w16_region_multiply(numpy.PyArray_BYTES(self.buff), other, self.bytes, numpy.PyArray_BYTES(dest.buff), add)
        else:
            galois_w32_region_multiply(numpy.PyArray_BYTES(self.buff), other, self.bytes, numpy.PyArray_BYTES(dest.buff), add)
      
        return dest

    def inverse_val(self, val):
        if type(val)!=int:
            raise TypeError("Inversion operand must be an integer.")
        return galois_inverse(val, self.bitfield)

def inverse_val(val, bitfield=16):
    if type(val)!=int:
        raise TypeError("Inversion operand must be an integer.")
    return galois_inverse(val, bitfield)
