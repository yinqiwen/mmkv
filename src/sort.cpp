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
#include "utils.hpp"
#include <algorithm>
#include <vector>
#include <deque>

namespace mmkv
{
    int MMKVImpl::GetValueByPattern(MMKVTable* table, const std::string& pattern, const Object& subst, Object& value)
    {
        const char *p, *f;
        const char* spat;
        /* If the pattern is "#" return the substitution object itself in order
         * to implement the "SORT ... GET #" feature. */
        spat = pattern.data();
        if (spat[0] == '#' && spat[1] == '\0')
        {
            value = subst;
            return 0;
        }

        /* If we can't find '*' in the pattern we return NULL as to GET a
         * fixed key does not make sense. */
        p = strchr(spat, '*');
        if (!p)
        {
            return -1;
        }

        f = strstr(spat, "->");
        if (NULL != f && (uint32_t) (f - spat) == (pattern.size() - 2))
        {
            f = NULL;
        }
        std::string keystr(pattern.data(), pattern.size());
        if (keystr.find(('*') != std::string::npos))
        {
            std::string ss;
            subst.ToString(ss);
            string_replace(keystr, "*", ss);
        }

        if (f == NULL)
        {
            const Object* value_data = FindMMValue(table, keystr);
            if (NULL != value_data)
            {
                if (value_data->type == V_TYPE_STRING)
                {
                    value = *value_data;
                }
                else
                {
                    return ERR_INVALID_TYPE;
                }
            }
            return 0;
        }
        else
        {
            size_t pos = keystr.find("->");
            std::string field = keystr.substr(pos + 2);
            keystr = keystr.substr(0, pos);
            const Object* value_data = FindMMValue(table, keystr);
            if (NULL != value_data)
            {
                if (value_data->type == V_TYPE_HASH)
                {
                    StringHashTable* hash = (StringHashTable*) value_data->RawValue();
                    StringHashTable::iterator it = hash->find(Object(field, true));
                    if (it != hash->end())
                    {
                        value = it->second;
                    }
                }
                else
                {
                    return ERR_INVALID_TYPE;
                }
            }
            return 0;
        }
    }

    bool MMKVImpl::MatchValueByPattern(MMKVTable* table, const std::string& pattern, const std::string& value_pattern,
            Object& subst)
    {
        Object value;
        if (0 != GetValueByPattern(table, pattern, subst, value))
        {
            return false;
        }
        std::string str;
        value.ToString(str);
        return stringmatchlen(value_pattern.data(), value_pattern.size(), str.c_str(), str.size(), 0) == 1 ? 0 : -1;
    }

    struct SortValue
    {
            Object value;
            Object cmp;
            bool alpha_cmp;
            SortValue(const Object& v) :
                    value(v), cmp(v), alpha_cmp(false)
            {
            }
            int Compare(const SortValue& other) const
            {
                return cmp.Compare(other.cmp, alpha_cmp);
            }
    };
    template<typename T>
    static bool greater_value(const T& v1, const T& v2)
    {
        return v1.Compare(v2) > 0;
    }
    template<typename T>
    static bool less_value(const T& v1, const T& v2)
    {
        return v1.Compare(v2) < 0;
    }
    int MMKVImpl::Sort(DBID db, const Data& key, const std::string& by, int limit_offset, int limit_count,
            const StringArray& get_patterns, bool desc_sort, bool alpha_sort, const Data& destination_key,
            const StringArrayResult& results)
    {
        if (destination_key.Len() > 0)
        {
            if (m_readonly)
            {
                return ERR_PERMISSION_DENIED;
            }
        }

        std::deque<std::string> store_list;
        std::string store_tmp_str;
        {
            RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
            MMKVTable* kv = GetMMKVTable(db, false);
            if (NULL == kv)
            {
                return ERR_ENTRY_NOT_EXIST;
            }
            const Object* value_data = FindMMValue(kv, key);
            if (NULL == value_data)
            {
                return ERR_ENTRY_NOT_EXIST;
            }
            std::vector<SortValue> sortvec;
            bool nosort = false;
            if (!strcasecmp(by.c_str(), "nosort"))
            {
                nosort = true;
            }
            size_t vlen = 0;

            switch (value_data->type)
            {
                case V_TYPE_LIST:
                {
                    StringList* data = (StringList*) value_data->RawValue();
                    vlen = data->size();
                    StringList::iterator it = data->begin();
                    while (it != data->end())
                    {
                        SortValue sv(*it);
                        sortvec.push_back(sv);
                        it++;
                    }
                    break;
                }
                case V_TYPE_SET:
                {
                    StringSet* data = (StringSet*) value_data->RawValue();
                    vlen = data->size();
                    StringSet::iterator it = data->begin();
                    while (it != data->end())
                    {
                        SortValue sv(*it);
                        sortvec.push_back(sv);
                        it++;
                    }
                    if (by.empty())
                    {
                        //set is already sorted
                        nosort = true;
                    }
                    break;
                }
                case V_TYPE_ZSET:
                {
                    ZSet* data = (ZSet*) value_data->RawValue();
                    vlen = data->set.size();
                    SortedSet::iterator it = data->set.begin();
                    while (it != data->set.end())
                    {
                        SortValue sv(it->value);
                        sortvec.push_back(sv);
                        it++;
                    }
                    if (by.empty())
                    {
                        //sorted set is already sorted
                        nosort = true;
                    }
                    break;
                }
                default:
                {
                    return ERR_INVALID_TYPE;
                }
            }
            if (limit_offset < 0)
            {
                limit_offset = 0;
            }
            if (limit_count < 0)
            {
                limit_count = vlen - limit_offset;
            }

            if (!nosort)
            {
                if (!by.empty())
                {
                    for (size_t i = 0; i < sortvec.size(); i++)
                    {
                        SortValue& sv = sortvec[i];
                        int err = GetValueByPattern(kv, by, sv.value, sv.cmp);
                        if (err < 0)
                        {
                            ERROR_LOG("Failed to get value by pattern:%s", by.c_str());
                            return err;
                        }
                        sv.alpha_cmp = alpha_sort;
                    }
                }
                if (!desc_sort)
                {
                    std::sort(sortvec.begin(), sortvec.end(), less_value<SortValue>);
                }
                else
                {
                    std::sort(sortvec.begin(), sortvec.end(), greater_value<SortValue>);
                }
            }
            uint32_t count = 0;
            for (size_t i = (size_t) limit_offset; i < sortvec.size() && count < (uint32_t) limit_count; i++, count++)
            {
                if (!get_patterns.empty())
                {
                    for (size_t j = 0; j < get_patterns.size(); j++)
                    {
                        int err = GetValueByPattern(kv, get_patterns[j], sortvec[i].value, sortvec[i].value);
                        if (err < 0)
                        {
                            ERROR_LOG("Failed to get value by pattern for:%s", get_patterns[j].c_str());
                            return err;
                        }
                        std::string& ss = destination_key.Len() > 0 ? store_tmp_str : results.Get();
                        sortvec[i].value.ToString(ss);
                        if (destination_key.Len() > 0)
                        {
                            store_list.push_back(ss);
                        }
                    }
                }
                else
                {
                    std::string& ss = destination_key.Len() > 0 ? store_tmp_str : results.Get();
                    sortvec[i].value.ToString(ss);
                    if (destination_key.Len() > 0)
                    {
                        store_list.push_back(ss);
                    }
                }
            }

        }
        if (destination_key.Len() > 0)
        {
            this->Del(db, DataArray(1, destination_key));
            RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
            EnsureWritableValueSpace();
            ObjectAllocator alloc = m_segment.ValueAllocator<Object>();
            int err;
            StringList* list = GetObject<StringList>(db, destination_key, V_TYPE_LIST, false, err)(alloc);
            if (0 != err)
            {
                return err;
            }
            std::deque<std::string>::iterator it = store_list.begin();
            while (it != store_list.end())
            {
                Object obj;
                m_segment.AssignObjectValue(obj, *it, false);
                list->push_back(obj);
                it++;
            }
        }
        return 0;
    }
}

