# MMKV: Fast persistence key-value engine on memory mapped file
MMKV is a BSD licensed, fast persistence key-value engine built on memory mapped file.


## Building MMKV
It only can be compiled on Linux now.   
To build the whole project, just type `make` to compile lib & tests.   
It should compile to a static library and test executable in `src` directory, such as libmmkv.a, mmkv-test etc. 

MMKV depends on `boost`, if the `boost` headers could not found by the compiler defaulty, set env `BOOST_INC` first.
	
	BOOST_INC=/boost_headers/include make


## Features
- Designed for application servers wanting to store many complex data sturctures on locally in shared memory.
- Persistence Key-value store, the value could be any complex data structure
- Multiple processes concurrency suported
- Most redis data stuctures&api suported
- Custom POD type supported.
- Very fast, which have similar performance compared to same data structure in memory.


## Warnings
- Application/System crashes may corrupt the whole data store if it doing write operations. 

## Status
- Still in development, but already used in a real project which need a very fast cache for multiple processes.