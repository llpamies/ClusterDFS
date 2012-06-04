'''
CauchyEC - A python wrapper for Cauchy Reed-Solomon codes from Jerasure Library.
Copright (C) 2012 Lluis Pamies-Juarez <lluis@pamies.cat>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''

cimport numpy
import numpy
import cython
from jerasure cimport *
import logging

cdef extern from "stdlib.h":
    void* malloc(size_t size)
    void free(void* ptr)

class CauchyECException(Exception):
    pass

cdef class CauchyEC:
    cdef int k
    cdef int m
    cdef int bitfield
    cdef int packetsize
    cdef int* bitmatrix
    cdef int* matrix
    cdef int* erasures
    cdef int** schedule
    cdef char **raw_ptrs
    cdef char **encoded_ptrs

    def __cinit__(self, k, m, **kwargs):
        self.raw_ptrs = <char**>malloc(k*sizeof(char*))
        if self.raw_ptrs==NULL:
            raise CauchyECException("malloc error: raw_ptrs")

        self.encoded_ptrs = <char**>malloc(m*sizeof(char*))
        if self.encoded_ptrs==NULL:
            free(self.raw_ptrs)
            raise CauchyECException("malloc error: encoded_ptrs")

        self.erasures = <int*>malloc((m+1)*sizeof(int))
        if self.erasures==NULL:
            free(self.raw_ptrs)
            free(self.encoded_ptrs)
            raise CauchyECException("malloc error: erasures")

    def __dealloc__(self):
        free(self.raw_ptrs)
        free(self.encoded_ptrs)
        free(self.erasures)

    def __init__(self, k, m, bitfield=16):
        if type(k)!=int:
            raise TypeError("n should be integer")
        if type(m)!=int:
            raise TypeError("k should be integer")
        if type(bitfield)!=int:
            raise TypeError("bitfield should be integer")
        #if bitfield not in [8,16,32]:
        #    raise ValueError("Incompatible bitfield. Use 8, 16 or 32.")
        if k<=0:
            raise ValueError("k>0")
        if m<=1:
            raise ValueError("m>1")
        if m>(1<<bitfield):
            raise ValueError("code is too large")

        self.k = k
        self.m = m
        self.bitfield = bitfield
        self.packetsize = sizeof(long) # adapt it to the word size of the architecture used

        self.matrix = cauchy_good_general_coding_matrix(self.k, self.m, self.bitfield)
        if self.matrix==NULL:
            raise CauchyECException("Failed initializing the Cauchy matrix.")

        self.bitmatrix = jerasure_matrix_to_bitmatrix(self.k, self.m, self.bitfield, self.matrix);
        self.schedule = jerasure_smart_bitmatrix_to_schedule(self.k, self.m, self.bitfield, self.bitmatrix);

    def encode(self, raw_data not None, encoded_data not None):
        cdef int i, size

        if len(raw_data)!=self.k:
            raise ValueError("len(raw_data)!=k")
        
        if len(encoded_data)!=self.m:
            raise ValueError("len(encoded_data)!=m")
            
        size = len(raw_data[0])

        if any(len(raw_data[i])!=size for i in range(len(raw_data))):
            raise ValueError("all vectors should have the same size")
        
        if any(len(encoded_data[i])!=size for i in range(len(encoded_data))):
            raise ValueError("all vectors should have the same size")

        if size%(self.bitfield*self.packetsize)!=0:
            raise ValueError("the size of data vectors should be multiple of bitfield*sizeof(long) (%d)"%(self.bitfield*self.packetsize))

        for i in range(self.k):
            self.raw_ptrs[i] = numpy.PyArray_BYTES(raw_data[i])

        for i in range(self.m):
            self.encoded_ptrs[i] = numpy.PyArray_BYTES(encoded_data[i])
      
        jerasure_schedule_encode(self.k, self.m, self.bitfield, self.schedule, self.raw_ptrs, self.encoded_ptrs, size, self.packetsize) 

    def decode(self, raw_data not None, encoded_data not None, erasures):
        cdef int i, size, num_erasures

        if len(raw_data)!=self.k:
            raise ValueError("len(raw_data)!=k")
        
        if len(encoded_data)!=self.m:
            raise ValueError("len(encoded_data)!=m")
            
        size = len(raw_data[0])

        if any(len(raw_data[i])!=size for i in range(len(raw_data))):
            raise ValueError("all vectors should have the same size")
        
        if any(len(encoded_data[i])!=size for i in range(len(encoded_data))):
            raise ValueError("all vectors should have the same size")

        if size%(self.bitfield*self.packetsize)!=0:
            raise ValueError("the size of data vectors should be multiple of bitfield*sizeof(long) (%d)"%(self.bitfield*self.packetsize))

        if len(erasures)>self.m:
            raise ValueError("too many erasures")

        for i in range(self.k):
            self.raw_ptrs[i] = numpy.PyArray_BYTES(raw_data[i])

        for i in range(self.m):
            self.encoded_ptrs[i] = numpy.PyArray_BYTES(encoded_data[i])

        for i,erasure in enumerate(erasures):
            self.erasures[i] = erasure
        self.erasures[len(erasures)] = -1

        jerasure_schedule_decode_lazy(self.k, self.m, self.bitfield, self.bitmatrix, self.erasures,\
                                      self.raw_ptrs, self.encoded_ptrs, size, self.packetsize, 1)
