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

#include "lock_guard.hpp"
#include "mmkv_impl.hpp"
#include <vector>

namespace mmkv
{
    struct IteratorCursor
    {
            MMKVImpl* kv;
            DBIDSet::iterator dbid_iter;
            MMKVTable* current_table;
            MMKVTable::iterator table_iter;
            StringHashTable* current_hash;
            StringHashTable::iterator hash_iter;
            StringList* current_list;
            StringList::iterator list_iter;
            SortedSet* current_zset;
            SortedSet::iterator zset_iter;
            StringSet* current_set;
            StringSet::iterator set_iter;
            IteratorCursor(MMKVImpl* _kv) :
                    kv(_kv), current_table(NULL), current_hash(NULL), current_list(NULL), current_zset(NULL), current_set(
                    NULL)
            {
                dbid_iter = kv->m_dbid_set->begin();
            }
            bool Valid()
            {
                return dbid_iter != kv->m_dbid_set->end();
            }
            bool NextList()
            {
                if (NULL != current_list && list_iter != current_list->end())
                {
                    list_iter++;
                    if (list_iter != current_list->end())
                    {
                        return true;
                    }

                }
                current_list = NULL;
                return false;
            }
            bool NextHash()
            {
                if (NULL != current_hash && hash_iter != current_hash->end())
                {
                    hash_iter++;
                    if (hash_iter != current_hash->end())
                    {
                        return true;
                    }
                }
                current_hash = NULL;
                return false;
            }
            bool NextSet()
            {
                if (NULL != current_set && set_iter != current_set->end())
                {
                    set_iter++;
                    if (set_iter != current_set->end())
                    {
                        return true;
                    }
                }
                current_set = NULL;
                return false;
            }
            bool NextZSet()
            {
                if (NULL != current_zset && zset_iter != current_zset->end())
                {
                    zset_iter++;
                    if (zset_iter != current_zset->end())
                    {
                        return true;
                    }
                }
                current_zset = NULL;
                return false;
            }
            void NextKey()
            {
                while (dbid_iter != kv->m_dbid_set->end())
                {
                    if (NULL == current_table)
                    {
                        current_table = kv->GetMMKVTable(*dbid_iter, false);
                        table_iter = current_table->begin();
                    }
                    else
                    {
                        table_iter++;
                    }
                    if (table_iter != current_table->end())
                    {
                        current_hash = NULL;
                        current_zset = NULL;
                        current_list = NULL;
                        current_set = NULL;
                        switch (table_iter->second.type)
                        {
                            case V_TYPE_LIST:
                            {
                                current_list = (StringList*) table_iter->second.RawValue();
                                list_iter = current_list->begin();
                                break;
                            }
                            case V_TYPE_SET:
                            {
                                current_set = (StringSet*) table_iter->second.RawValue();
                                set_iter = current_set->begin();
                                break;
                            }
                            case V_TYPE_ZSET:
                            {
                                current_zset = (SortedSet*) table_iter->second.RawValue();
                                zset_iter = current_zset->begin();
                                break;
                            }
                            case V_TYPE_HASH:
                            {
                                current_hash = (StringHashTable*) table_iter->second.RawValue();
                                hash_iter = current_hash->begin();
                                break;
                            }
                            default:
                            {
                                break;
                            }
                        }
                        return;
                    }
                    current_table = NULL;
                    dbid_iter++;
                }
            }
            void NextElement()
            {
                switch (table_iter->second.type)
                {
                    case V_TYPE_LIST:
                    {
                        NextList();
                        break;
                    }
                    case V_TYPE_SET:
                    {
                        NextSet();
                        break;
                    }
                    case V_TYPE_ZSET:
                    {
                        NextZSet();
                        break;
                    }
                    case V_TYPE_HASH:
                    {
                        NextHash();
                        break;
                    }
                    default:
                    {
                        break;
                    }
                }
            }
    };

    Iterator::Iterator(void* kv) :
            m_cursor(NULL)
    {
        MMKVImpl* _kv = (MMKVImpl*) kv;
        IteratorCursor* cursor = new IteratorCursor(_kv);
        cursor->kv->m_segment.Lock(READ_LOCK);
        m_cursor = cursor;
        cursor->NextKey();
    }
    bool Iterator::Valid()
    {
        IteratorCursor* cursor = (IteratorCursor*) m_cursor;
        return cursor == NULL ? false : cursor->Valid();

    }
    DBID Iterator::GetDBID()
    {
        IteratorCursor* cursor = (IteratorCursor*) m_cursor;
        return *(cursor->dbid_iter);
    }
    int Iterator::GetKey(std::string& key)
    {
        IteratorCursor* cursor = (IteratorCursor*) m_cursor;
        const Object& keyobj = cursor->table_iter->first;
        keyobj.ToString(key);
        return 0;
    }
    uint64_t Iterator::GetKeyTTL()
    {
        IteratorCursor* cursor = (IteratorCursor*) m_cursor;
        const Object& valobj = cursor->table_iter->second;
        if (!valobj.hasttl)
        {
            return 0;
        }
        return cursor->kv->GetTTL(*(cursor->dbid_iter), cursor->table_iter->first, cursor->table_iter->second);
    }
    ObjectType Iterator::GetValueType()
    {
        IteratorCursor* cursor = (IteratorCursor*) m_cursor;
        Object& value = cursor->table_iter->second;
        return (ObjectType) value.type;
    }
    size_t Iterator::ValueLength()
    {
        IteratorCursor* cursor = (IteratorCursor*) m_cursor;
        Object& value = cursor->table_iter->second;
        switch (value.type)
        {
            case V_TYPE_LIST:
            {
                return cursor->current_list->size();
            }
            case V_TYPE_SET:
            {
                return cursor->current_set->size();
            }
            case V_TYPE_ZSET:
            {
                return cursor->current_zset->size();
            }
            case V_TYPE_HASH:
            {
                return cursor->current_hash->size();
            }
            case V_TYPE_STRING:
            {
                return value.len;
            }
            default:
            {
                return 0;
            }
        }
    }
    int Iterator::GetStringValue(std::string& value)
    {
        IteratorCursor* cursor = (IteratorCursor*) m_cursor;
        ObjectType type = GetValueType();
        switch (type)
        {
            case V_TYPE_STRING:
            {
                if (cursor->table_iter != cursor->current_table->end())
                {
                    cursor->table_iter->second.ToString(value);
                    return 1;
                }
                return 0;
            }
            case V_TYPE_LIST:
            {
                if (cursor->current_list != NULL && cursor->list_iter != cursor->current_list->end())
                {
                    cursor->list_iter->ToString(value);
                    return 1;
                }
                return 0;
            }
            case V_TYPE_SET:
            {
                if (cursor->current_set != NULL && cursor->set_iter != cursor->current_set->end())
                {
                    cursor->set_iter->ToString(value);
                    return 1;
                }
                return 0;
            }
            default:
            {
                return -1;
            }
        }
    }
    int Iterator::GetHashEntry(std::string& field, std::string& value)
    {
        IteratorCursor* cursor = (IteratorCursor*) m_cursor;
        ObjectType type = GetValueType();
        if (type == V_TYPE_HASH)
        {
            if (cursor->current_hash != NULL && cursor->hash_iter != cursor->current_hash->end())
            {
                cursor->hash_iter->first.ToString(field);
                cursor->hash_iter->second.ToString(value);
                return 1;
            }
            return 0;
        }
        return -1;
    }
    int Iterator::GetZSetEntry(long double& score, std::string& value)
    {
        IteratorCursor* cursor = (IteratorCursor*) m_cursor;
        ObjectType type = GetValueType();
        if (type == V_TYPE_ZSET)
        {
            if (cursor->current_zset != NULL && cursor->zset_iter != cursor->current_zset->end())
            {
                score = cursor->zset_iter->score;
                cursor->zset_iter->value.ToString(value);
                return 1;
            }
            return 0;
        }
        return -1;
    }
    void Iterator::NextKey()
    {
        IteratorCursor* cursor = (IteratorCursor*) m_cursor;
        cursor->NextKey();
    }
    void Iterator::NextValueElement()
    {
        IteratorCursor* cursor = (IteratorCursor*) m_cursor;
        cursor->NextElement();
    }
    Iterator::~Iterator()
    {
        IteratorCursor* cursor = (IteratorCursor*) m_cursor;
        cursor->kv->m_segment.Unlock(READ_LOCK);
        delete cursor;
    }

    Iterator* MMKVImpl::NewIterator()
    {
        Iterator* iter = new Iterator(this);
        return iter;
    }
}

