# ClusterDFS -  A Experimental Distributed Storage System
Copyright (C) 2012 [Lluis Pamies-Juarez](http://lluis.pamies.cat)

*This program is free software: you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option) any
later version.*

*This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
details.*

*You should have received a copy of the GNU General Public License along with
this program. If not, see <http://www.gnu.org/licenses/>.*

## Description

ClusterDFS is an experimental distributed storage system implemented in
python for fast prototyping of network codes for storage. The main features
of ClusterDFS are:

* Fast I/O and concurrency thanks to the asynchronous network library and
  lightweight threads offered by [gevent](http://www.gevent.org).
* Network buffers are wrapped with *GaloisBuffer* arrays that allow to
  perform encoding operations directly over them, without requiring extra
  memory allocations.

The current implementation allows to execute different *DataNodes*
(bin/datanode) across different computers, allowing to store, retrieve and
encode data in each of them.

## Basic Operations

The basic operations implemented by now in *DataNode* are:

 * **STORE**: Reads a network stream and stores it in the local file-system
   with a specific ID.
 * **RETRIEVE**: Serves a stored block from the local file-system.
 * **CODING**: Executes a coding operation. The ID of the coding operation
   and its implementation is stored in *DataNodeConfig.coding_mod_name*.
   Returns the coding result as a network stream.

## Coding Operations

When the *DataNode* executes a CODING operation, it retrieves a list of
operations from *DataNodeConfig.coding_mod_name* and executes them
sequentially for all buffers in a data stream. The list of operations is a
combination of the following:

 * **LOAD**: Loads a file or network buffer under a certain ID. When loading
   from a network, it can load the result of a remote coding operation.
 * **WRITE**: Writes a buffer to a file.
 * **IADD**: Performs an in-place addition of two buffer (xor operation).
 * **MULT**: Performs an in-place multiplication of two buffer (Galois
   arithmetic).
 * **MULADD**: Multiplies two buffers and adds the result to one of the
   buffers (Galois arithmetic).

## RapidRAID Codes

In *clusterdfs/rapidraid.py* we provide an implementation of a pipelined
erasure code scheme that encodes 11 replicated data blocks to generate 16
parity blocks. This RapidRAID implementation is a (16,11) erasure code
defined as a set of 16 instruction lists, each to be executed in each of the
storage nodes.

## Future Directions

* Addition of a *NameNode* (possibly distributed in a DHT) to monitor the
  blocks stored in each of the *DataNode*s.
* Implement a CODING operation to operate in a PUSH manner, sending/encoding
  data from a source to different nodes, instead of the PULL strategy
  implemented now where a node requests encoded data from other nodes.
