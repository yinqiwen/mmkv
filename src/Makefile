#-------------------------------------------------------------------------------
# Copyright (c) 2015-2015, yinqiwen <yinqiwen@gmail.com>
# All rights reserved.
#  
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#  
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#   * Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions and the following disclaimer in the
#     documentation and/or other materials provided with the distribution.
#   * Neither the name of Redis nor the names of its contributors may be used
#     to endorse or promote products derived from this software without
#     specific prior written permission.
#  
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS 
# BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
# THE POSSIBILITY OF SUCH DAMAGE.
#-------------------------------------------------------------------------------
#Makefile
#
# Created on: 2015-5-19
#     Author: yinqiwen

CXX=g++
CC=gcc

MMKV_VERSION=0.1.0

uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

LIB_PATH=$(PWD)/../deps

OPTIMIZATION?=-O2
OPT=$(OPTIMIZATION)

CXXFLAGS=-Wall -g ${OPT} -fPIC -D__STDC_FORMAT_MACROS -DMMKV_VERSION='"${MMKV_VERSION}"' -DXXH_NAMESPACE=LZ4_
CCFLAGS=-Wall -std=gnu99 ${OPT} -fPIC -pedantic -g -D__STDC_FORMAT_MACROS -DMMKV_VERSION='"${MMKV_VERSION}"' -DXXH_NAMESPACE=LZ4_
LDFLAGS=-g 


LZ4_PATH=${LIB_PATH}/lz4
LZ4_LIBA=${LZ4_PATH}/liblz4.a


SPARSEHASH_VER=sparsehash-2.0.2
SPARSEHASH_PATH=${LIB_PATH}/${SPARSEHASH_VER}
SPARSEHASH_FILE=${SPARSEHASH_PATH}.tar.gz
SPARSEHASH_CONFIG=${SPARSEHASH_PATH}/src/config.h

BOOST_INC?=/usr/include
INCS=-I./ -I${LIB_PATH} -I${LZ4_PATH} -I${SPARSEHASH_PATH}/src/ -I${BOOST_INC}  

LIBS= ../deps/lz4/liblz4.a -lpthread

%.o : %.cpp
	${CXX} -c ${CXXFLAGS} ${INCS} $< -o $@

%.o : %.c
	${CC} -c ${CCFLAGS} ${INCS} $< -o $@


COMMON_OBJECTS := mmkv.o mmkv_logger.o mmkv_impl.o malloc.o memory.o mmap.o locks.o t_string.o t_list.o t_hash.o t_zset.o t_set.o bitops.o utils.o \
                  hyperloglog.o

TESTOBJ := ../test/ut.o ../test/test_main.o

#DIST_LIB = libardb.so
DIST_LIBA = libmmkv.a


all: test_boost $(DIST_LIB) test  

$(DIST_LIB): $(COMMON_OBJECTS)
	${CXX} -shared -o $@ $^

$(DIST_LIBA):$(COMMON_OBJECTS)
	ar rcs $@ $^

test_boost:
	@rm test_boost.cpp 2> /dev/null;\
	echo "#include <boost/interprocess/offset_ptr.hpp>" >> test_boost.cpp ;\
	echo "#include <stdio.h>" >> test_boost.cpp;\
	echo "int main(){" >> test_boost.cpp;\
	echo "return 0;}" >> test_boost.cpp;\
	${CXX} -I${BOOST_INC}   test_boost.cpp 2> /dev/null;\
	cret=$$?; \
	rm test_boost.cpp 2> /dev/null;\
	rm a.out 2> /dev/null;\
	if [ $$cret -ne 0 ] ;then  \
		echo "Boost interprocess headers is not found in ${BOOST_INC}. Use 'BOOST_INC=/your_boost_dir make' to try again."; \
		exit 1; \
	fi  
	
.PHONY: lz4
lz4: $(LZ4_LIBA)
$(LZ4_LIBA):
	echo ">>>>> Building LZ4" && \
	cd ${LZ4_PATH} && \
	$(MAKE)  && \
	echo ">>>>> Done building LZ4"

.PHONY: sparsehash
sparsehash: $(SPARSEHASH_CONFIG)
$(SPARSEHASH_CONFIG):
	echo ">>>>> Building sparsehash" && \
	cd ${LIB_PATH}; tar zxvf ${SPARSEHASH_FILE};\
	patch -p0 < ./sparsehash.patch;\
	cd ${SPARSEHASH_PATH}; ./configure --enable-namespace=mmkv_google;  \
	${MAKE} src/sparsehash/internal/sparseconfig.h && \
	echo ">>>>> Done building sparsehash"

lib: lz4 sparsehash $(DIST_LIBA)

test:  lib ${TESTOBJ} 
	${CXX} -o mmkv-test  ${TESTOBJ} $(DIST_LIBA) ${LIBS}
	
clean_test:
	rm -f  ${TESTOBJ} mmkv-test
	
clean:
	rm -f  ${COMMON_OBJECTS} ${TESTOBJ} $(DIST_LIBA) mmkv-test

