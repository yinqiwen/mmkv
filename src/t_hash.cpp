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
namespace mmkv
{
    int MMKVImpl::HDel(DBID db, const Data& key, const DataArray& fields)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        int err = 0;
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        StringHashTable* hash = GetObject<StringHashTable>(db, key, V_TYPE_HASH, false, err)();
        if (NULL == hash || 0 != err)
        {
            return err;
        }
        int removed = 0;
        for (size_t i = 0; i < fields.size(); i++)
        {
            StringHashTable::iterator found = hash->find(fields[i]);
            if (found != hash->end())
            {
                DestroyObjectContent(found->first);
                DestroyObjectContent(found->second);
                hash->erase(found);
                removed++;
            }
        }
        return removed;
    }
    int MMKVImpl::HExists(DBID db, const Data& key, const Data& field)
    {
        int err = 0;
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        StringHashTable* hash = GetObject<StringHashTable>(db, key, V_TYPE_HASH, false, err)();
        if (IS_NOT_EXISTS(err))
        {
            return 0;
        }
        if (0 != err)
        {
            return err;
        }
        StringHashTable::iterator found = hash->find(field);
        return found != hash->end();
    }
    int MMKVImpl::HGet(DBID db, const Data& key, const Data& field, std::string& val)
    {
        val.clear();
        int err = 0;
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        StringHashTable* hash = GetObject<StringHashTable>(db, key, V_TYPE_HASH, false, err)();
        if (NULL == hash || 0 != err)
        {
            return err;
        }
        StringHashTable::iterator found = hash->find(field);
        if (found != hash->end())
        {
            found->second.ToString(val);
            return 0;
        }
        else
        {
            return ERR_ENTRY_NOT_EXIST;
        }

    }
    int MMKVImpl::HGetAll(DBID db, const Data& key, StringArray& vals)
    {
        int err = 0;
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        StringHashTable* hash = GetObject<StringHashTable>(db, key, V_TYPE_HASH, false, err)();
        if (NULL == hash || 0 != err)
        {
            return err;
        }
        vals.clear();
        StringHashTable::iterator it = hash->begin();
        while (it != hash->end())
        {
            if (it.isfilled())
            {
                std::string field, value;
                it->first.ToString(field);
                it->second.ToString(value);
                vals.push_back(field);
                vals.push_back(value);
            }
            it++;
        }
        return 0;
    }
    int MMKVImpl::HIncrBy(DBID db, const Data& key, const Data& field, int64_t increment, int64_t& new_val)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        int err = 0;

        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        StringMapAllocator allocator = m_segment.ValueAllocator<StringPair>();
        StringHashTable* hash = GetObject<StringHashTable>(db, key, V_TYPE_HASH, true, err)(allocator);
        if (NULL == hash || 0 != err)
        {
            return err;
        }
        std::pair<StringHashTable::iterator, bool> ret = hash->insert(StringHashTable::value_type(field, Object()));
        if (ret.second)
        {
            m_segment.AssignObjectValue(const_cast<Object&>(ret.first->first), field, false);
            ret.first->second.SetInteger(increment);
            new_val = increment;
        }
        else
        {
            Object& data = ret.first->second;
            if (data.IsInteger())
            {
                new_val = data.IntegerValue() + increment;
                data.SetInteger(new_val);
            }
            else
            {
                return ERR_NOT_INTEGER;
            }
        }
        return 0;
    }
    int MMKVImpl::HIncrByFloat(DBID db, const Data& key, const Data& field, double increment, double& new_val)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        int err = 0;
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        StringMapAllocator allocator = m_segment.ValueAllocator<StringPair>();
        StringHashTable* hash = GetObject<StringHashTable>(db, key, V_TYPE_HASH, true, err)(allocator);
        if (NULL == hash || 0 != err)
        {
            return err;
        }
        std::pair<StringHashTable::iterator, bool> ret = hash->insert(
                StringHashTable::value_type(Object(field), Object()));
        if (ret.second)
        {
            m_segment.AssignObjectValue(const_cast<Object&>(ret.first->first), field, false);
            if (is_integer(increment))
            {
                ret.first->second.SetInteger((int64_t) increment);
            }
            else
            {
                char buf[256];
                int dlen = double2string(buf, sizeof(buf), increment, true);
                Data tmp(buf, dlen);
                m_segment.AssignObjectValue(ret.first->second, tmp, false);
            }
            new_val = increment;
        }
        else
        {
            Object& data = ret.first->second;
            double dv = 0;
            if (data.IsInteger())
            {
                new_val = data.IntegerValue() + increment;
            }
            else if (string2double(data.RawValue(), data.len, &dv))
            {
                new_val = dv + increment;
            }
            else
            {
                return ERR_NOT_NUMBER;;
            }
            if (is_integer(new_val) && data.SetInteger((int64_t) new_val))
            {
                //do nothing
            }
            else
            {
                DestroyObjectContent(data);
                char buf[256];
                int dlen = double2string(buf, sizeof(buf), new_val, true);
                m_segment.AssignObjectValue(data, Data(buf, dlen), false);
            }
        }
        return 0;
    }
    int MMKVImpl::HKeys(DBID db, const Data& key, StringArray& fields)
    {
        int err = 0;
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        StringHashTable* hash = GetObject<StringHashTable>(db, key, V_TYPE_HASH, false, err)();
        if (NULL == hash || 0 != err)
        {
            return err;
        }
        fields.clear();
        StringHashTable::iterator it = hash->begin();
        while (it != hash->end())
        {
            if (it.isfilled())
            {
                std::string field;
                it->first.ToString(field);
                fields.push_back(field);
            }
            it++;
        }
        return 0;
    }
    int MMKVImpl::HLen(DBID db, const Data& key)
    {
        int err = 0;
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        StringHashTable* hash = GetObject<StringHashTable>(db, key, V_TYPE_HASH, false, err)();
        if (IS_NOT_EXISTS(err))
        {
            return 0;
        }
        if (NULL == hash || 0 != err)
        {
            return err;
        }
        return hash->size();
    }
    int MMKVImpl::HMGet(DBID db, const Data& key, const DataArray& fields, StringArray& vals)
    {
        int err = 0;
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        StringHashTable* hash = GetObject<StringHashTable>(db, key, V_TYPE_HASH, false, err)();
        if (NULL == hash || 0 != err)
        {
            return err;
        }
        for (size_t i = 0; i < fields.size(); i++)
        {
            StringHashTable::iterator found = hash->find(fields[i]);
            std::string val;
            if (found != hash->end())
            {
                found->second.ToString(val);
            }
            vals.push_back(val);
        }
        return 0;
    }
    int MMKVImpl::HMSet(DBID db, const Data& key, const DataPairArray& field_vals)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        int err = 0;

        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        StringMapAllocator allocator = m_segment.ValueAllocator<StringPair>();
        StringHashTable* hash = GetObject<StringHashTable>(db, key, V_TYPE_HASH, true, err)(allocator);
        if (NULL == hash || 0 != err)
        {
            return err;
        }
        for (size_t i = 0; i < field_vals.size(); i++)
        {
            Object tmpk(field_vals[i].first);
            Object tmpv(field_vals[i].second);
            std::pair<StringHashTable::iterator, bool> ret = hash->insert(StringHashTable::value_type(tmpk, tmpv));
            if (ret.second)
            {
                m_segment.AssignObjectValue(const_cast<Object&>(ret.first->first), field_vals[i].first, false);
            }
            else
            {
                DestroyObjectContent(ret.first->second);
            }
            m_segment.AssignObjectValue(ret.first->second, field_vals[i].second, false);
        }
        return 0;
    }
    int MMKVImpl::HScan(DBID db, const Data& key, int cursor, const std::string& pattern, int32_t limit_count)
    {
        return -1;
    }
    int MMKVImpl::HSet(DBID db, const Data& key, const Data& field, const Data& val, bool nx)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        int err = 0;
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        StringMapAllocator allocator = m_segment.ValueAllocator<StringPair>();
        StringHashTable* hash = GetObject<StringHashTable>(db, key, V_TYPE_HASH, true, err)(allocator);
        if (NULL == hash || 0 != err)
        {
            return err;
        }
        Object tmpk(field);
        Object tmpv(val);
        std::pair<StringHashTable::iterator, bool> ret = hash->insert(StringHashTable::value_type(tmpk, tmpv));
        if (ret.second)
        {
            m_segment.AssignObjectValue(const_cast<Object&>(ret.first->first), field, false);
        }
        else
        {
            if (nx)
            {
                return ERR_ENTRY_EXISTED;
            }
            DestroyObjectContent(ret.first->second);
        }
        m_segment.AssignObjectValue(ret.first->second, val, false);
        return ret.second ? 1 : 0;
    }
    int MMKVImpl::HStrlen(DBID db, const Data& key, const Data& field)
    {
        int err = 0;
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        StringHashTable* hash = GetObject<StringHashTable>(db, key, V_TYPE_HASH, false, err)();
        if (IS_NOT_EXISTS(err))
        {
            return 0;
        }
        if (NULL == hash || 0 != err)
        {
            return err;
        }
        StringHashTable::iterator found = hash->find(field);
        if (found != hash->end())
        {
            return found->second.len;
        }
        return 0;
    }
    int MMKVImpl::HVals(DBID db, const Data& key, StringArray& vals)
    {
        int err = 0;
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        StringHashTable* hash = GetObject<StringHashTable>(db, key, V_TYPE_HASH, false, err)();
        if (NULL == hash || 0 != err)
        {
            return err;
        }
        vals.clear();
        StringHashTable::iterator it = hash->begin();
        while (it != hash->end())
        {
            if (it.isfilled())
            {
                std::string value;
                it->second.ToString(value);
                vals.push_back(value);
            }
            it++;
        }
        return 0;
    }
}

