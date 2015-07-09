/*
 *Copyright (c) 2015-2015, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 *
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 *THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef CONTAINERS_HPP_
#define CONTAINERS_HPP_

#include "types.hpp"
#include <sparsehash/sparse_hash_map>
#include <sparsehash/dense_hash_map>
//#include <sparsehash/sparse_hash_set>
#include <string.h>
#include <new>
#include <string>
#include <vector>
#include <set>
#include <map>
#include <alloca.h>
#include "mmkv_allocator.hpp"

namespace mmkv
{

    typedef boost::interprocess::offset_ptr<void> VoidPtr;

    //typedef boost::interprocess::offset_ptr<CStr> CStrPtr;
    typedef std::pair<Object, VoidPtr> StringObjectPair;
    typedef std::pair<const Object, Object> StringPair;
    typedef Allocator<StringObjectPair> StringObjectTableAllocator;
    typedef Allocator<StringPair> StringMapAllocator;
    typedef StringMapAllocator ObjectMapAllocator;
    typedef Allocator<Object> ObjectAllocator;

    typedef Allocator<ScoreValue> ScoreValueAllocator;
    typedef mmkv::btree::btree_map<Object, VoidPtr, std::less<Object>, StringObjectTableAllocator> StringObjectTable;
    typedef mmkv::btree::btree_set<ScoreValue, std::less<ScoreValue>, ScoreValueAllocator> SortedSet;
    typedef boost::interprocess::offset_ptr<StringObjectTable> StringObjectTablePtr;
    typedef boost::interprocess::deque<Object, ObjectAllocator> StringList;
    typedef mmkv::btree::btree_set<Object, std::less<Object>, ObjectAllocator> StringSet;
    typedef std::set<Object> StdObjectSet;
    typedef std::map<Object, double> StdObjectScoreTable;
    typedef mmkv::btree::btree_map<Object, Object, std::less<Object>, StringMapAllocator> StringMap;
    typedef std::vector<StringSet*> StringSetArray;

    //typedef mmkv::btree::btree_map<MMKeyPtr, MMValuePtr, MMKeyPtrLess, MMKVAllocator> MMKVTable;
    typedef khmap_t<Object, Object, ObjectHash, ObjectEqual, StringMapAllocator> MMKVTable;
    //typedef mmkv_google::sparse_hash_map<Object, Object, ObjectHash, ObjectEqual, StringMapAllocator> MMKVTable;
    typedef Allocator<TTLValue> TTLValueAllocator;
    typedef mmkv::btree::btree_set<TTLValue, std::less<TTLValue>, TTLValueAllocator> TTLValueSet;

    typedef khmap_t<Object, Object, ObjectHash, ObjectEqual, StringMapAllocator> StringHashTable;
    typedef std::pair<const Object, double> StringScoreValue;
    typedef Allocator<StringScoreValue> StringScoreValueAllocator;
    typedef khmap_t<Object, double, ObjectHash, ObjectEqual, StringScoreValueAllocator> StringDoubleTable;

    typedef std::pair<const TTLKey, int64_t> TTLValuePair;
    typedef Allocator<TTLValuePair> TTLValuePairAllocator;
    typedef mmkv_google::sparse_hash_map<TTLKey, uint64_t, TTLKeyHash, TTLKeyEqual, TTLValuePairAllocator> TTLValueTable;

    struct ZSet
    {
            SortedSet set;
            StringDoubleTable scores;
            ZSet(const Allocator<char>& allocator) :
                    set(std::less<ScoreValue>(), ScoreValueAllocator(allocator)), scores(
                            StringScoreValueAllocator(allocator))
            {

            }
            ZSet(const ZSet& other) :
                    set(other.set), scores(other.scores.get_allocator())
            {
                const_cast<StringDoubleTable&>(other.scores).clone(&scores);
            }
            void Clear()
            {
                set.clear();
                scores.clear();
            }
    };

}

#endif /* CONTAINERS_HPP_ */
