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
    typedef ObjectBTreeTable MMKVTable;

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

//    template<class Key, class T, class HashFcn, class EqualKey, class LessKey, class Alloc>
//    class HashBtreeMap
//    {
//        public:
//            typedef Key key_type;
//            typedef T data_type;
//            typedef T mapped_type;
//            typedef std::pair<const Key, T> value_type;
//            typedef HashFcn hasher;
//            typedef EqualKey key_equal;
//            typedef LessKey key_compare;
//            typedef Alloc allocator_type;
//            typedef size_t size_type;
//        private:
//            typedef mmkv::btree::btree_map<key_type, data_type, key_compare, allocator_type> BtreeTable;
//            typedef boost::interprocess::offset_ptr<BtreeTable> BtreeTablePtr;
//            typedef typename allocator_type::template rebind<BtreeTable>::other btree_allocator_type;
//            typedef typename allocator_type::template rebind<BtreeTablePtr>::other btree_table_ptr_allocator_type;
//            typedef boost::interprocess::vector<BtreeTablePtr, btree_table_ptr_allocator_type> BtreeTablePtrArray;
//            BtreeTablePtrArray tables;
//            allocator_type allocator;
//            btree_allocator_type get_btree_allocator()
//            {
//                return btree_allocator_type(allocator);
//            }
//            BtreeTable* get_btree_table(size_t pos, bool create_if_notexist)
//            {
//                BtreeTable* table = tables[pos].get();
//                if (NULL == table && create_if_notexist)
//                {
//                    table = get_btree_allocator().allocate(1);
//                    ::new (table) BtreeTable(key_compare(), allocator);
//                    tables[pos] = table;
//                }
//                return table;
//            }
//
//        public:
//            struct iterator
//            {
//                    BtreeTablePtrArray* tables;
//                    size_t table_pos;
//                    typename BtreeTable::iterator bit;
//                    iterator() :
//                            tables(NULL), table_pos(0)
//                    {
//                    }
//                    iterator(BtreeTablePtrArray& ts) :
//                            tables(&ts), table_pos(ts.size())
//                    {
//                    }
//                    iterator(BtreeTablePtrArray& ts, size_t pos, typename BtreeTable::iterator it) :
//                            tables(&ts), table_pos(pos), bit(it)
//                    {
//                    }
//                    BtreeTablePtrArray& GetBtreeTables()
//                    {
//                        return *tables;
//                    }
//                    bool operator==(const iterator &x) const
//                    {
//                        if (table_pos != x.table_pos)
//                        {
//                            return false;
//                        }
//                        if (table_pos == tables->size())
//                        {
//                            return true;
//                        }
//                        return bit == x.bit;
//                    }
//                    bool operator!=(const iterator &x) const
//                    {
//                        return !operator ==(x);
//                    }
//                    value_type& operator*() const
//                    {
//                        return bit.operator*();
//                    }
//                    value_type* operator->() const
//                    {
//                        return bit.operator->();
//                    }
//                    iterator& operator++()
//                    {
//                        bit++;
//                        if (bit != tables->at(table_pos)->end())
//                        {
//                            return *this;
//                        }
//                        table_pos++;
//                        while (table_pos < tables->size()
//                                && (tables->at(table_pos).get() == NULL || tables->at(table_pos)->empty()))
//                        {
//                            table_pos++;
//                        }
//                        bit = tables->at(table_pos)->begin();
//                        return *this;
//                    }
//                    iterator& operator--()
//                    {
//                        bit--;
//                        return *this;
//                    }
//                    iterator operator++(int)
//                    {
//                        iterator tmp = *this;
//                        ++*this;
//                        return tmp;
//                    }
//                    iterator operator--(int)
//                    {
//                        iterator tmp = *this;
//                        --*this;
//                        return tmp;
//                    }
//            };
//            HashBtreeMap(size_t bucket_count, const allocator_type& alloc):tables(btree_table_ptr_allocator_type(alloc)), allocator(alloc)
//            {
//                tables.resize(bucket_count);
//            }
//            void clear()
//            {
//                for (size_t i = 0; i < tables.size(); i++)
//                {
//                    BtreeTable* table = tables[i].get();
//                    if (NULL != table)
//                    {
//                        allocator.destroy(table);
//                        tables[i] = NULL;
//                    }
//                }
//            }
//            size_t size()
//            {
//                size_t count = 0;
//                for (size_t i = 0; i < tables.size(); i++)
//                {
//                    if (NULL != tables[i])
//                    {
//                        count += tables[i]->size();
//                    }
//                }
//                return count;
//            }
//            iterator end()
//            {
//                return iterator(tables);
//            }
//            iterator begin()
//            {
//                for (size_t i = 0; i < tables.size(); i++)
//                {
//                    if (NULL != tables[i])
//                    {
//                        typename BtreeTable::iterator it = tables[i]->begin();
//                        if (it != tables[i]->end())
//                        {
//                            return iterator(tables, i, it);
//                        }
//                    }
//                }
//                return end();
//            }
//            iterator find(const key_type& key)
//            {
//                hasher hf;
//                size_t hash = hf(key);
//                size_t pos = hash % tables.size();
//                BtreeTable* table = get_btree_table(pos, false);
//                if (NULL == table)
//                {
//                    return end();
//                }
//                typename BtreeTable::iterator it = table->find(key);
//                if (it == table->end())
//                {
//                    return end();
//                }
//                else
//                {
//                    return iterator(tables, pos, it);
//                }
//            }
//            std::pair<iterator, bool> insert(const value_type& val)
//            {
//                hasher hf;
//                size_t hash = hf(val.first);
//                size_t pos = hash % tables.size();
//                BtreeTable* table = get_btree_table(pos, true);
//                std::pair<typename BtreeTable::iterator, bool> ret = table->insert(val);
//                return std::make_pair(iterator(tables, pos, ret.first), ret.second);
//            }
//            void erase(iterator iter)
//            {
//                BtreeTable* table = tables[iter.table_pos].get();
//                table->erase(iter.bit);
//            }
//            size_type erase(const key_type& key)
//            {
//                iterator it = find(key);
//                if (it != end())
//                {
//                    erase(it);
//                    return 1;
//                }
//                return 0;
//            }
//    };
//    typedef HashBtreeMap<Object, Object, ObjectHash, ObjectEqual, std::less<Object>, StringMapAllocator> MMKVTable;
//typedef MMKVHashTable MMKVTable;
}

#endif /* CONTAINERS_HPP_ */
