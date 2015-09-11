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
#include <sparsehash/sparse_hash_set>
#include <boost/interprocess/containers/vector.hpp>
#include <string.h>
#include <new>
#include <string>
#include <vector>
#include <set>
#include <map>
#include <alloca.h>
#include "mmkv_allocator.hpp"
#include "collections/incremental_rehashmap.hpp"

namespace mmkv
{
    typedef boost::interprocess::offset_ptr<void> VoidPtr;

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
    typedef std::vector<StringSet*> StringSetArray;

    typedef mmkv::btree::btree_map<Object, Object, std::less<Object>, StringMapAllocator> ObjectBTreeTable;
    typedef mmkv_google::dense_hash_map<Object, Object, ObjectHash, ObjectEqual, StringMapAllocator> ObjectHashTable;
    typedef incremental_rehashmap<Object, Object, ObjectHash, ObjectEqual, StringMapAllocator> ObjectReHashTable;
    typedef ObjectReHashTable MMKVTable;

    typedef Allocator<TTLValue> TTLValueAllocator;
    typedef mmkv::btree::btree_set<TTLValue, std::less<TTLValue>, TTLValueAllocator> TTLValueSet;
    typedef mmkv::btree::btree_set<DBID, std::less<DBID>, Allocator<DBID> > DBIDSet;

    typedef mmkv_google::sparse_hash_map<Object, Object, ObjectHash, ObjectEqual, StringMapAllocator> SparseStringHashTable;
//    struct StringHashTable: public SparseStringHashTable
//    {
//            StringHashTable(const StringMapAllocator& allocator) :
//                    SparseStringHashTable(0, ObjectHash(), ObjectEqual(), allocator)
//            {
//                Object empty;
//                set_deleted_key(empty);
//            }
//    };
//    typedef ObjectReHashTable StringHashTable;
    struct StringHashTable: public ObjectBTreeTable
    {
            StringHashTable(const StringMapAllocator& allocator) :
                    ObjectBTreeTable(std::less<Object>(), allocator)
            {
            }
    };

    typedef std::pair<const Object, long double> StringScoreValue;
    typedef Allocator<StringScoreValue> StringScoreValueAllocator;
    typedef mmkv_google::sparse_hash_map<Object, long double, ObjectHash, ObjectEqual, StringScoreValueAllocator> SparseStringDoubleTable;
    typedef mmkv::btree::btree_map<Object, long double, std::less<Object>, StringScoreValueAllocator> ObjectDoubleBTreeTable;
//    struct StringDoubleTable: public SparseStringDoubleTable
//    {
//            StringDoubleTable(const StringScoreValueAllocator& allocator) :
//                    SparseStringDoubleTable(0, ObjectHash(), ObjectEqual(), allocator)
//            {
//                Object empty;
//                set_deleted_key(empty);
//            }
//    };
    struct StringDoubleTable: public ObjectDoubleBTreeTable
    {
            StringDoubleTable(const StringScoreValueAllocator& allocator) :
                ObjectDoubleBTreeTable(std::less<Object>(), allocator)
            {
            }
    };

    typedef std::pair<const TTLKey, int64_t> TTLValuePair;
    typedef Allocator<TTLValuePair> TTLValuePairAllocator;
    //typedef mmkv_google::sparse_hash_map<TTLKey, uint64_t, TTLKeyHash, TTLKeyEqual, TTLValuePairAllocator> TTLValueTable;
    typedef mmkv::btree::btree_map<TTLKey, uint64_t, std::less<TTLKey>, TTLValuePairAllocator> TTLValueTable;

    struct ExpireInfoSet
    {
            TTLValueSet set;
            TTLValueTable map;
            ExpireInfoSet(const Allocator<char>& allocator) :
                    set(std::less<TTLValue>(), allocator), map(std::less<TTLKey>(), allocator)
            {
                //TTLKey empty;
                //map.set_deleted_key(empty);
            }
    };

    typedef boost::interprocess::offset_ptr<ExpireInfoSet> ExpireInfoSetOffsetPtr;
    typedef boost::interprocess::vector<ExpireInfoSetOffsetPtr, Allocator<ExpireInfoSetOffsetPtr> > ExpireInfoSetArray;

    struct ZSet
    {
            SortedSet set;
            StringDoubleTable scores;
            ZSet(const Allocator<char>& allocator) :
                    set(std::less<ScoreValue>(), ScoreValueAllocator(allocator)), scores(
                            StringScoreValueAllocator(allocator))
            {
            }
            void Clear()
            {
                set.clear();
                scores.clear();
            }
    };
}

#endif /* CONTAINERS_HPP_ */
