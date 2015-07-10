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
#include "utils.hpp"
#include "locks.hpp"
#include "lock_guard.hpp"
#include "mmkv_impl.hpp"
#include <math.h>
namespace mmkv
{
    Object& MMKVImpl::FindOrCreateStringValue(MMKVTable* table, const Data& key, const Data& create_base_value,
            bool reserve_ttl, bool& created)
    {
        Object tmpkey(key);
        std::pair<MMKVTable::iterator, bool> ret = table->insert(MMKVTable::value_type(tmpkey, Object()));
        Object& value_data = const_cast<Object&>(ret.first->second);
        if (ret.second)
        {
            m_segment.AssignObjectValue(value_data, create_base_value, false);
            created = true;
        }
        else
        {
            created = false;
        }
        return value_data;
    }

    int MMKVImpl::GenericSet(MMKVTable* table, DBID db, const Data& key, const Data& value, int32_t ex, int64_t px,
            int8_t nx_xx)
    {
        Object tmpkey(key);
        uint64_t ttl = 0;
        if (ex > 0)
        {
            ttl = ex;
            ttl *= 1000 * 1000;
            ttl += get_current_micros();
        }
        if (px > 0)
        {
            ttl = px;
            ttl *= 1000;
            ttl += get_current_micros();
        }
        std::pair<MMKVTable::iterator, bool> ret = table->insert(MMKVTable::value_type(tmpkey, Object()));
        const Object& kk = ret.first->first;
        Object& value_data = ret.first->second;
        if (!ret.second)
        {
            if (nx_xx == 0)  // only set key when key not exist
            {
                return ERR_ENTRY_EXISTED;
            }
            if (value_data.type != V_TYPE_STRING)
            {
                return ERR_INVALID_TYPE;
            }
            ClearTTL(db, kk, value_data);
            DestroyObjectContent(value_data);
        }
        else
        {
            if (nx_xx == 1) // only set key when key  exist
            {
                table->erase(ret.first);
                return ERR_ENTRY_NOT_EXIST;
            }
            AssignObjectContent(const_cast<Object&>(kk), key, true);
        }
        AssignObjectContent(value_data, value, false);
        if (ttl > 0)
        {
            value_data.hasttl = true;
            SetTTL(db, kk, value_data, ttl);
        }
        return 0;
    }

    int MMKVImpl::Set(DBID db, const Data& key, const Data& value, int32_t ex, int64_t px, int8_t nx_xx)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        MMKVTable* kv = GetMMKVTable(db, true);
        if (NULL == kv)
        {
            return ERR_DB_NOT_EXIST;
        }
        return GenericSet(kv, db, key, value, ex, px, nx_xx);
    }

    int MMKVImpl::GenericGet(MMKVTable* table, DBID db, const Data& key, std::string& value)
    {
        Object tmpkey(key);
        MMKVTable::iterator found = table->find(tmpkey);
        if (found == table->end())
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        Object& value_data = found->second;
        if (value_data.type != V_TYPE_STRING)
        {
            return ERR_INVALID_TYPE;
        }
        if (IsExpired(db, key, value_data))
        {
            return 0;
        }
        value_data.ToString(value);
        return 0;
    }

    int MMKVImpl::Get(DBID db, const Data& key, std::string& value)
    {
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        return GenericGet(kv, db, key, value);
    }

    int MMKVImpl::Append(DBID db, const Data& key, const Data& value)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        MMKVTable* kv = GetMMKVTable(db, true);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        bool created = false;
        Object& value_data = FindOrCreateStringValue(kv, key, value, false, created);
        if (!created)
        {
            if (value_data.type != V_TYPE_STRING)
            {
                return ERR_INVALID_TYPE;
            }
            std::string tmpstr;
            if (!IsExpired(db, key, value_data))
            {
                value_data.ToString(tmpstr);
            }
            tmpstr.append(value.Value(), value.Len());
            Object tmpkey(key);
            ClearTTL(db, tmpkey, value_data);
            DestroyObjectContent(value_data);
            m_segment.AssignObjectValue(value_data, tmpstr, false);
            // vptr = AllocateStringValue(tmpstr, false);
        }
        return 0;
    }

    int MMKVImpl::GetSet(DBID db, const Data& key, const Data& value, std::string& old_value)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        MMKVTable* kv = GetMMKVTable(db, true);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        bool created = false;
        Object& value_data = FindOrCreateStringValue(kv, key, value, false, created);
        if (!created)
        {
            if (value_data.type != V_TYPE_STRING)
            {
                return ERR_INVALID_TYPE;
            }
            if (!IsExpired(db, key, value_data))
            {
                value_data.ToString(old_value);
            }
            Object tmpkey(key);
            ClearTTL(db, tmpkey, value_data);
            DestroyObjectContent(value_data);
            m_segment.AssignObjectValue(value_data, value, false);
        }
        return 0;
    }

    int MMKVImpl::Strlen(DBID db, const Data& key)
    {
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return 0;
        }

        const Object* value_data = FindMMValue(kv, key);
        if (NULL == value_data)
        {
            return 0;
        }
        if (value_data->type != V_TYPE_STRING)
        {
            return ERR_INVALID_TYPE;
        }
        return value_data->len;
    }

    int MMKVImpl::Decr(DBID db, const Data& key, int64_t& new_val)
    {
        return IncrBy(db, key, -1, new_val);
    }
    int MMKVImpl::DecrBy(DBID db, const Data& key, int64_t decrement, int64_t& new_val)
    {
        return IncrBy(db, key, -decrement, new_val);
    }

    int MMKVImpl::GetRange(DBID db, const Data& key, int start, int end, std::string& value)
    {
        std::string vv;
        {
            RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
            MMKVTable* kv = GetMMKVTable(db, false);
            if (NULL == kv)
            {
                return ERR_ENTRY_NOT_EXIST;
            }
            GenericGet(kv, db, key, vv);
        }
        size_t strlen = vv.size();
        /* Convert negative indexes */
        if (start < 0)
            start = strlen + start;
        if (end < 0)
            end = strlen + end;
        if (start < 0)
            start = 0;
        if (end < 0)
            end = 0;
        if ((unsigned long long) end >= strlen)
            end = strlen - 1;

        /* Precondition: end >= 0 && end < strlen, so the only condition where
         * nothing can be returned is: start > end. */
        if (start > end || strlen == 0)
        {
            //do nothing
        }
        else
        {
            value = vv.substr(start, end - start + 1);
        }
        return 0;
    }
    int MMKVImpl::Incr(DBID db, const Data& key, int64_t& new_val)
    {
        return IncrBy(db, key, 1, new_val);
    }
    int MMKVImpl::IncrBy(DBID db, const Data& key, int64_t increment, int64_t& new_val)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        MMKVTable* kv = GetMMKVTable(db, true);
        if (NULL == kv)
        {
            return ERR_DB_NOT_EXIST;
        }
        Object tmpkey(key);
        std::pair<MMKVTable::iterator, bool> ret = kv->insert(MMKVTable::value_type(tmpkey, Object()));
        const Object& kk = ret.first->first;
        Object& value_data = ret.first->second;
        if (!ret.second)
        {
            if (value_data.type != V_TYPE_STRING)
            {
                return ERR_INVALID_TYPE;
            }

            ClearTTL(db, kk, value_data);
            if (!value_data.IsInteger())
            {
                return ERR_NOT_INTEGER;
            }
            else
            {
                new_val = value_data.IntegerValue() + increment;
                value_data.SetInteger(new_val);
            }
        }
        else
        {
            m_segment.AssignObjectValue(const_cast<Object&>(kk), key, true);
            new_val = increment;
            value_data.SetInteger(increment);
        }
        return 0;
    }
    int MMKVImpl::IncrByFloat(DBID db, const Data& key, double increment, double& new_val)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        MMKVTable* kv = GetMMKVTable(db, true);
        if (NULL == kv)
        {
            return ERR_DB_NOT_EXIST;
        }
        Object tmpkey(key);
        std::pair<MMKVTable::iterator, bool> ret = kv->insert(MMKVTable::value_type(tmpkey, Object()));
        const Object& kk = ret.first->first;
        Object& value_data = ret.first->second;
        if (!ret.second)
        {
            if (value_data.type != V_TYPE_STRING)
            {
                return ERR_INVALID_TYPE;
            }
            ClearTTL(db, kk, value_data);
            if (value_data.IsInteger())
            {
                new_val = value_data.IntegerValue() + increment;
                if (isnan(new_val) || isinf(new_val))
                {
                    return ERR_NOT_NUMBER;
                }
                if (is_integer(new_val))
                {
                    value_data.SetInteger((int64_t) new_val);
                    return 0;
                }
                else
                {
                    DestroyObjectContent(value_data);
                }
            }
            else
            {
                double vv = 0;
                if (!string2double(value_data.RawValue(), value_data.len, &vv))
                {
                    return ERR_NOT_NUMBER;
                }
                new_val = vv + increment;
                DestroyObjectContent(value_data);
            }
        }
        else
        {
            m_segment.AssignObjectValue(const_cast<Object&>(kk), key, true);
            new_val = increment;
            if (is_integer(increment))
            {
                value_data.SetInteger((int64_t) increment);
                return 0;
            }
        }
        char buf[256];
        int len = double2string(buf, sizeof(buf), new_val, true);
        m_segment.AssignObjectValue(value_data, Data(buf, len), false);
        return 0;
    }
    int MMKVImpl::MGet(DBID db, const DataArray& keys, StringArray& vals)
    {
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        vals.resize(keys.size());
        for (size_t i = 0; i < keys.size(); i++)
        {
            GenericGet(kv, db, keys[i], vals[i]);
        }
        return 0;
    }
    int MMKVImpl::MSet(DBID db, const DataPairArray& key_vals)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        MMKVTable* kv = GetMMKVTable(db, true);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        for (size_t i = 0; i < key_vals.size(); i++)
        {
            GenericSet(kv, db, key_vals[i].first, key_vals[i].second, -1, -1, -1);
        }
        return 0;
    }
    int MMKVImpl::MSetNX(DBID db, const DataPairArray& key_vals)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        MMKVTable* kv = GetMMKVTable(db, true);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        int count = 0;
        for (size_t i = 0; i < key_vals.size(); i++)
        {
            int ret = GenericSet(kv, db, key_vals[i].first, key_vals[i].second, -1, -1, 0);
            if (ret == 0)
            {
                count++;
            }
        }
        return count;
    }
    int MMKVImpl::PSetNX(DBID db, const Data& key, int64_t milliseconds, const Data& value)
    {
        return Set(db, key, value, -1, milliseconds, 0);
    }
    int MMKVImpl::SetEX(DBID db, const Data& key, int32_t secs, const Data& value)
    {
        return Set(db, key, value, secs, -1, -1);
    }
    int MMKVImpl::SetNX(DBID db, const Data& key, const Data& value)
    {
        return Set(db, key, value, -1, -1, 0);
    }
    int MMKVImpl::SetRange(DBID db, const Data& key, int offset, const Data& value)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        if (offset < 0)
        {
            return ERR_OFFSET_OUTRANGE;
        }
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        Object tmpkey(key);

        std::pair<MMKVTable::iterator, bool> ret = kv->insert(MMKVTable::value_type(tmpkey, Object()));
        const Object& kk = ret.first->first;
        Object& value_data = ret.first->second;
        std::string strvalue;
        if (!ret.second)
        {
            ClearTTL(db, kk, value_data);
            if (value_data.IsInteger() || value_data.len < (offset + value.Len()))
            {
                m_segment.ObjectMakeRoom(value_data, offset + value.Len(), false);
            }
        }
        else
        {
            m_segment.AssignObjectValue(const_cast<Object&>(kk), key, true);
            m_segment.ObjectMakeRoom(value_data, offset + value.Len(), false);
        }
        memcpy(value_data.WritableData() + offset, value.Value(), value.Len());
        return 0;
    }
}

