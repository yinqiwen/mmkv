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
#include "mmkv_impl.hpp"
#include "locks.hpp"
#include "lock_guard.hpp"
#include "utils.hpp"
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include <limits.h>
#include <new>

namespace mmkv
{
    static const char* kTableConstName = "MMKVTable";
    static const char* kTTLConstName = "MMKVTTLSet";
    static const char* kTTLTableConstName = "MMKVTTLMap";
    static const char* kDBIDSetName = "MMKVDBIDSet";

    MMKVImpl::MMKVImpl() :
            m_readonly(false), m_ttlset(NULL), m_ttlmap(NULL), m_dbid_set(NULL)
    {

    }
    void* MMKVImpl::Malloc(size_t size)
    {
        return m_segment.Allocate(size, false);
    }
    void MMKVImpl::Free(void* p)
    {
        m_segment.Deallocate(p);
    }
    void MMKVImpl::Lock(bool readonly)
    {
        m_segment.Lock(readonly ? READ_LOCK : WRITE_LOCK);
    }
    void MMKVImpl::Unlock(bool readonly)
    {
        m_segment.Unlock(readonly ? READ_LOCK : WRITE_LOCK);
    }
    bool MMKVImpl::IsLocked(bool readonly)
    {
        return m_segment.IsLocked(readonly);
    }

    size_t MMKVImpl::KeySpaceUsed()
    {
        return m_segment.KeySpaceUsed();
    }
    size_t MMKVImpl::ValueSpaceUsed()
    {
        return m_segment.ValueSpaceUsed();
    }
    Allocator<char> MMKVImpl::GetCharAllocator()
    {
        return m_segment.ValueAllocator<char>();
    }

    int MMKVImpl::DeleteMMKVTable(DBID db)
    {
        LockGuard<SpinMutexLock> keylock_guard(m_kv_table_lock);
        if (m_kvs.size() > db)
        {
            m_kvs[db] = NULL;
        }
        char name[100];
        sprintf(name, "%s_%u", kTableConstName, db);
        return m_segment.EraseObject<MMKVTable>(name);
    }

    MMKVTable* MMKVImpl::GetMMKVTable(DBID db, bool create_if_notexist)
    {
        MMKVTable* kv = NULL;
        {
            LockGuard<SpinMutexLock> keylock_guard(m_kv_table_lock);
            if (m_kvs.size() > db)
            {
                kv = m_kvs[db];
                if (NULL != kv)
                {
                    return kv;
                }
            }
            else
            {
                m_kvs.resize(db + 1);
            }
        }
        char name[100];
        sprintf(name, "%s_%u", kTableConstName, db);
        if (create_if_notexist && !m_readonly)
        {
            ObjectMapAllocator allocator(m_segment.GetKeySpaceAllocator());
            bool created = false;
            //kv = m_segment.FindOrConstructObject<MMKVTable>(name, &created)(allocator);
            kv = m_segment.FindOrConstructObject<MMKVTable>(name, &created)(0, ObjectHash(), ObjectEqual(), allocator);
            if (created)
            {
                m_dbid_set->insert(db);
                Object empty;
                kv->set_empty_key(empty);
                Object deleted;
                deleted.SetData(DENSE_TABLE_DELETED_KEY, false);
                kv->set_deleted_key(deleted);
            }
        }
        else
        {
            kv = m_segment.FindObject<MMKVTable>(name);
        }
        if (NULL != kv)
        {
            LockGuard<SpinMutexLock> keylock_guard(m_kv_table_lock);
            m_kvs[db] = kv;
        }
        return kv;
    }

    const Object* MMKVImpl::FindMMValue(MMKVTable* table, const Data& key) const
    {
        Object tmpkey(key, false);
        MMKVTable::iterator found = table->find(tmpkey);
        if (found == table->end())
        {
            return NULL;
        }
        return &(found->second);
    }

    int MMKVImpl::ReOpen(bool lock)
    {
        if (!m_readonly)
        {
            TTLValueAllocator allocator(m_segment.GetKeySpaceAllocator());
            WriteLockGuard<MemorySegmentManager> keylock_guard(m_segment, lock);
            m_ttlset = m_segment.FindOrConstructObject<TTLValueSet>(kTTLConstName)(std::less<TTLValue>(), allocator);
            m_dbid_set = m_segment.FindOrConstructObject<DBIDSet>(kDBIDSetName)(std::less<DBID>(), allocator);
            bool created = false;
            TTLValuePairAllocator alloc2(m_segment.GetKeySpaceAllocator());
            m_ttlmap = m_segment.FindOrConstructObject<TTLValueTable>(kTTLTableConstName, &created)(0, TTLKeyHash(),
                    TTLKeyEqual(), alloc2);
            if (created)
            {
                TTLKey empty;
                m_ttlmap->set_deleted_key(empty);
            }
        }
        else
        {
            m_dbid_set = m_segment.FindObject<DBIDSet>(kDBIDSetName);
        }
        return 0;
    }

    int MMKVImpl::Open(const OpenOptions& open_options)
    {
        m_options = open_options;
        m_readonly = open_options.readonly;
        m_logger.loglevel = open_options.log_level;
        if (NULL != open_options.log_func)
        {
            m_logger.logfunc = open_options.log_func;
        }
        m_segment.SetLogger(m_logger);
        if (0 != m_segment.Open(open_options))
        {
            return -1;
        }
        ReOpen(true);
        if (open_options.verify)
        {
            //m_kv->verify();
        }
        return 0;
    }

    void MMKVImpl::ClearTTL(DBID db, const Object& key, Object& value)
    {
        if (value.hasttl)
        {
            TTLValue entry;
            entry.key.db = db;
            entry.key.key = key;
            TTLValueTable::iterator fit = m_ttlmap->find(entry.key);
            if (fit == m_ttlmap->end())
            {
                ABORT("No TTL value found for object");
                return;
            }
            entry.expireat = fit->second;
            value.hasttl = 0;
            m_ttlset->erase(entry);
            m_ttlmap->erase(fit);
        }
    }

    template<typename T>
    static inline void destroy_value(MemorySegmentManager& segment, T* value)
    {
        value->~T();
    }

    void MMKVImpl::DestroyObjectContent(const Object& obj)
    {
        if (obj.IsOffsetPtr())
        {
            m_segment.Deallocate(const_cast<char*>(obj.RawValue()));
        }
        else if (obj.IsInteger())
        {
            Object& o = const_cast<Object&>(obj);
            o.Clear();
        }
    }
    void MMKVImpl::AssignObjectContent(const Object& obj, const Data& data, bool in_keyspace)
    {
        m_segment.AssignObjectValue(const_cast<Object&>(obj), data, in_keyspace);
    }

    int MMKVImpl::GetPOD(DBID db, const Data& key, bool created_if_notexist, uint32_t expected_type, Data& v)
    {
        if (m_readonly && created_if_notexist)
        {
            return ERR_PERMISSION_DENIED;
        }
        MMKVTable* kv = GetMMKVTable(db, created_if_notexist ? true : false);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        Object tmpkey(key, false);
        Object* value_data = NULL;
        if (created_if_notexist)
        {
            std::pair<MMKVTable::iterator, bool> ret = kv->insert(MMKVTable::value_type(tmpkey, Object()));
            value_data = &(ret.first->second);
            if (ret.second)
            {
                value_data->type = V_TYPE_POD;
                m_segment.ObjectMakeRoom(*value_data, sizeof(PODHeader) + v.len);
                PODHeader* pod_header = (PODHeader*) (value_data->WritableData());
                pod_header->type = expected_type;
                v.data = value_data->RawValue() + sizeof(PODHeader);
                return 1;
            }
        }
        else
        {
            MMKVTable::iterator found = kv->find(tmpkey);
            if (found == kv->end())
            {
                return ERR_ENTRY_NOT_EXIST;
            }
            value_data = &(found->second);
        }
        if (IsExpired(db, key, *value_data))
        {
            if (!created_if_notexist)
            {
                return ERR_ENTRY_NOT_EXIST;
            }
            ClearTTL(0, tmpkey, *value_data);
            m_segment.ObjectMakeRoom(*value_data, sizeof(PODHeader) + v.len);
        }
        PODHeader* pod_header = (PODHeader*) (value_data->WritableData());
        if (pod_header->type != expected_type)
        {
            return ERR_INVALID_POD_TYPE;
        }
        if (value_data->len != sizeof(PODHeader) + v.len)
        {
            return ERR_POD_SIZE_MISMATCH;
        }
        v.data = value_data->RawValue() + sizeof(PODHeader);
        return 0;
    }

    int MMKVImpl::RegisterPODDestructor(uint32_t expected_type, PODestructor* des)
    {
        LockGuard<SpinMutexLock> keylock_guard(m_kv_table_lock);
        if (!m_destructors.insert(PODestructorTable::value_type(expected_type, des)).second)
        {
            return ERR_DUPLICATE_POD_TYPE;
        }
        return 0;
    }
    PODestructor* MMKVImpl::GetPODDestructor(uint32_t expected_type)
    {
        LockGuard<SpinMutexLock> keylock_guard(m_kv_table_lock);
        PODestructorTable::iterator found = m_destructors.find(expected_type);
        if (found == m_destructors.end())
        {
            return NULL;
        }
        return found->second;
    }

    bool MMKVImpl::IsExpired(DBID db, const Data& key, const Object& obj)
    {
        uint64_t ttl = GetTTL(db, Object(key, false), obj);
        if (ttl == 0)
        {
            return false;
        }
        return ttl <= get_current_micros();
    }

    Object MMKVImpl::CloneStrObject(const Object& obj, bool in_keyspace)
    {
        Object clone(obj);
        if (clone.IsOffsetPtr())
        {
            Data data(obj.RawValue(), obj.len);
            AssignObjectContent(clone, data, in_keyspace);
        }
        return clone;
    }

    void MMKVImpl::SetTTL(DBID db, const Object& key, Object& value, uint64_t ttl)
    {
        if (ttl == 0)
        {
            ClearTTL(db, key, value);
            return;
        }
        TTLValue entry;
        entry.key.db = db;
        entry.key.key = key;
        value.hasttl = 1;
        std::pair<TTLValueTable::iterator,bool> sit = m_ttlmap->insert(TTLValueTable::value_type(entry.key, ttl));
        if(!sit.second)
        {
            entry.expireat = sit.first->second;
            m_ttlset->erase(entry);
            sit.first->second = ttl;
        }
        entry.expireat = ttl;
        m_ttlset->insert(entry);
    }
    uint64_t MMKVImpl::GetTTL(DBID db, const Object& key, const Object& value)
    {
        if (!value.hasttl)
        {
            return 0;
        }
        TTLKey entry;
        entry.db = db;
        entry.key = key;
        TTLValueTable::iterator fit = m_ttlmap->find(entry);
        if (fit == m_ttlmap->end())
        {
            ABORT("No TTL value found for object");
            return 0;
        }
        uint64_t now = get_current_micros();
        return fit->second > now ? fit->second : 0;
    }

    int MMKVImpl::GenericDelValue(uint32_t type, void* ptr)
    {
        if (NULL == ptr)
        {
            return 0;
        }
        switch (type)
        {
            case V_TYPE_HASH:
            {
                StringHashTable* m = (StringHashTable*) ptr;
                StringHashTable::iterator it = m->begin();
                while (it != m->end())
                {
                    //if (it.isfilled())
                    {
                        DestroyObjectContent(it->first);
                        DestroyObjectContent(it->second);
                    }
                    it++;
                }
                destroy_value(m_segment, m);
                break;
            }
            case V_TYPE_LIST:
            {
                StringList* m = (StringList*) ptr;
                StringList::iterator it = m->begin();
                while (it != m->end())
                {
                    DestroyObjectContent(*it);
                    it = m->erase(it);
                }
                destroy_value(m_segment, m);
                break;
            }
            case V_TYPE_SET:
            {
                StringSet* m = (StringSet*) ptr;
                StringSet::iterator it = m->begin();
                while (it != m->end())
                {
                    Object obj = *it;
                    it = m->erase(it);
                    DestroyObjectContent(obj);
                }
                destroy_value(m_segment, m);
                break;
            }
            case V_TYPE_ZSET:
            {
                ZSet* m = (ZSet*) ptr;
                SortedSet::iterator it = m->set.begin();
                while (it != m->set.end())
                {
                    Object obj = it->value;
                    it = m->set.erase(it);
                    DestroyObjectContent(obj);
                }
                destroy_value(m_segment, m);
                break;
            }
            case V_TYPE_STRING:
            {
                break;
            }
            case V_TYPE_POD:
            {
                PODHeader* pod_header = (PODHeader*) ptr;
                void* obj = (char*) ptr + sizeof(PODHeader);
                PODestructor* des = GetPODDestructor(pod_header->type);
                if (NULL == des)
                {
                    WARN_LOG("Bo desturctor found for POD type:%u", pod_header->type);
                    return ERR_NO_DESTRUCTOR;
                }
                (*des)(obj);
                break;
            }
            default:
            {
                return ERR_INVALID_TYPE;
            }
        }
        return 0;
    }
    int MMKVImpl::GenericDelValue(const Object& v)
    {
        void* ptr = NULL;
        if (v.encoding == OBJ_ENCODING_OFFSET_PTR)
        {
            ptr = (void*) v.RawValue();
        }
        int err = GenericDelValue(v.type, ptr);
        if (0 == err)
        {
            DestroyObjectContent(v);
        }
        else
        {
            ERROR_LOG("Destroy value failed with err:%d for type:%u", err, v.type);
        }
        return err;
    }
    int MMKVImpl::GenericDel(MMKVTable* table, DBID db, const Object& key)
    {
        MMKVTable::iterator found = table->find(key);
        if (found != table->end())
        {
            Object& value_data = found->second;
            ClearTTL(db, key, value_data);
            int err = GenericDelValue(value_data);
            if (0 != err)
            {
                return err;
            }
            DestroyObjectContent(found->first);
            table->erase(found);
            return 1;
        }
        return 0;
    }

    int MMKVImpl::GenericInsertValue(MMKVTable* table, const Data& key, Object& v, bool replace)
    {
        Object tmpkey(key, false);
        std::pair<MMKVTable::iterator, bool> ret = table->insert(MMKVTable::value_type(tmpkey, v));
        if (!ret.second)
        {
            Object& old_data = ret.first->second;
            if (!replace)
            {
                return 0;
            }
            GenericDelValue(old_data);
            old_data = v;
        }
        else
        {
            m_segment.AssignObjectValue(const_cast<Object&>(ret.first->first), key, false);
        }
        return 1;
    }

    int MMKVImpl::Del(DBID db, const DataArray& keys)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        MMKVTable* kv = GetMMKVTable(db, true);
        if (NULL == kv)
        {
            return ERR_DB_NOT_EXIST;
        }
        int count = 0;
        for (size_t i = 0; i < keys.size(); i++)
        {
            int err = GenericDel(kv, db, Object(keys[i], false));
            if (err < 0)
            {
                if (keys.size() == 1)
                {
                    return err;
                }
                err = 0;
            }
            count += err;
        }
        return count;
    }

    int MMKVImpl::Type(DBID db, const Data& key)
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
        if (IsExpired(db, key, *value_data))
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        return value_data->type;
    }

    int MMKVImpl::Exists(DBID db, const Data& key)
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
        return 1;
    }

    int MMKVImpl::Persist(DBID db, const Data& key)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return 0;
        }
        const Object* value_data = FindMMValue(kv, key);
        if (NULL == value_data || value_data->hasttl == 0)
        {
            return 0;
        }
        ClearTTL(db, Object(key, false), *(const_cast<Object*>(value_data)));
        return 1;
    }

    int64_t MMKVImpl::TTL(DBID db, const Data& key)
    {
        int64_t pttl = PTTL(db, key);
        return pttl > 0 ? pttl / 1000:pttl;
    }
    int64_t MMKVImpl::PTTL(DBID db, const Data& key)
    {
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return -2;
        }
        const Object* value_data = FindMMValue(kv, key);
        if (NULL == value_data)
        {
            return -2;
        }
        return value_data->hasttl ? (GetTTL(db, Object(key, false), *value_data) - get_current_micros()) / 1000 : -1;
    }
    int MMKVImpl::Expire(DBID db, const Data& key, uint32_t secs)
    {
        return PExpireat(db, key, (uint64_t) (secs * 1000) + get_current_micros() / 1000);
    }
    int MMKVImpl::PExpire(DBID db, const Data& key, uint64_t milliseconds)
    {
        return PExpireat(db, key, milliseconds + get_current_micros() / 1000);
    }
    int MMKVImpl::PExpireat(DBID db, const Data& key, uint64_t milliseconds_timestamp)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return 0;
        }
        Object key_obj(key, false);
        MMKVTable::iterator found = kv->find(key_obj);
        if (found == kv->end())
        {
            return 0;
        }
        SetTTL(db, found->first, found->second, milliseconds_timestamp * 1000);
        return 1;
    }

    int MMKVImpl::GenericMoveKey(DBID src_db, const Data& src_key, DBID dest_db, const Data& dest_key, bool nx)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        if (src_db == dest_db && src_key == dest_key)
        {
            return nx ? 0 : 1;
        }
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        MMKVTable* src_kv = GetMMKVTable(src_db, false);
        MMKVTable* dst_kv = GetMMKVTable(dest_db, nx ? false : true);
        if (NULL == src_kv || NULL == dst_kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        Object src_key_obj(src_key, false);
        MMKVTable::iterator found = src_kv->find(src_key_obj);
        if (found == src_kv->end())
        {
            return ERR_ENTRY_NOT_EXIST;
        }

        Object tmpkey2(dest_key, false);
        std::pair<MMKVTable::iterator, bool> ret = dst_kv->insert(MMKVTable::value_type(tmpkey2, found->second));
        const Object& kk = ret.first->first;
        if (ret.second)
        {
            src_kv->erase(found);
            if (tmpkey2 == src_key_obj)
            {
                const_cast<Object&>(kk) = found->first;
            }
            else
            {
                m_segment.AssignObjectValue(const_cast<Object&>(kk), dest_key, true);
            }
            return 1;
        }
        else
        {
            if (!nx)
            {
                ret.first->second = found->second;
                src_kv->erase(found);
                return 1;
            }
            return 0;
        }
    }

    int MMKVImpl::Move(DBID db, const Data& key, DBID destdb)
    {
        return GenericMoveKey(db, key, destdb, key, true);
    }
    int MMKVImpl::Rename(DBID db, const Data& key, const Data& new_key)
    {
        return GenericMoveKey(db, key, db, new_key, false);
    }
    int MMKVImpl::RenameNX(DBID db, const Data& key, const Data& new_key)
    {
        return GenericMoveKey(db, key, db, new_key, true);
    }
    int MMKVImpl::RandomKey(DBID db, std::string& key)
    {
        key.clear();
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv || kv->size() == 0)
        {
            return 0;
        }
        int loop_count = 0;
        while (true)
        {
            MMKVTable::iterator it = kv->begin();
            it.advance(random_between_int32(0, INT_MAX) % kv->bucket_count());
            if (it != kv->end())
            {
                it->first.ToString(key);
                return 0;
            }
            loop_count++;
            if (loop_count > 20)
            {
                break;
            }
        }
        MMKVTable::iterator it = kv->begin();
        while (it != kv->end())
        {
            //if (it.isfilled())
            {
                it->first.ToString(key);
                return 0;
            }
            it++;
        }
        return 0;
    }
    int MMKVImpl::Keys(DBID db, const std::string& pattern, const StringArrayResult& keys)
    {
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return 0;
        }
        MMKVTable::iterator it = kv->begin();
        while (it != kv->end())
        {
            //if (it.isfilled())
            {
                std::string key_str;
                it->first.ToString(key_str);
                if (pattern == "*"
                        || stringmatchlen(pattern.c_str(), pattern.size(), key_str.data(), key_str.size(), 0) == 1)
                {
                    std::string& ss = keys.Get();
                    ss = key_str;
                }
            }
            it++;
        }
        return 0;
    }

    int64_t MMKVImpl::Scan(DBID db, int64_t cursor, const std::string& pattern, int32_t limit_count,
            const StringArrayResult& result)
    {
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return 0;
        }
        int match_count = 0;
        int pos = cursor >= kv->bucket_count() ? kv->bucket_count() : cursor;
        MMKVTable::iterator it = kv->begin();
        it.advance(pos);
        while (it != kv->end())
        {
            //if (it.isfilled())
            {
                std::string key_str;
                it->first.ToString(key_str);
                if (pattern == ""
                        || stringmatchlen(pattern.c_str(), pattern.size(), key_str.c_str(), key_str.size(), 0) == 1)
                {
                    std::string& ss = result.Get();
                    ss = key_str;
                    match_count++;
                    if (limit_count > 0 && match_count >= limit_count)
                    {
                        break;
                    }
                }
            }
            pos++;
            it++;
        }
        return it == kv->end() ? 0 : pos;
    }

    int64_t MMKVImpl::DBSize(DBID db)
    {
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return 0;
        }
        return kv->size();
    }
    int MMKVImpl::FlushDB(DBID db)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> guard(m_segment);
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return 0;
        }

        MMKVTable::iterator it = kv->begin();
        while (it != kv->end())
        {
            ClearTTL(db, it->first, it->second);
            DestroyObjectContent(it->first);
            GenericDelValue(it->second);
            it++;
        }
        kv->clear();
        m_dbid_set->erase(db);
        DeleteMMKVTable(db);
        return 0;
    }
    int MMKVImpl::FlushAll()
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        m_kvs.clear();
        m_segment.ReCreate(true);
        m_ttlset = NULL;
        m_ttlmap = NULL;
        m_dbid_set = NULL;
        ReOpen(false);
        return 0;
    }

    int MMKVImpl::RemoveExpiredKeys(uint32_t max_removed, uint32_t max_time)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        uint32_t removed = 0;
        uint64_t start = get_current_micros();
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        while (removed < max_removed && !m_ttlset->empty())
        {
            TTLValueSet::iterator it = m_ttlset->begin();
            uint64_t now = get_current_micros();
            if (now - start >= max_time * 1000)
            {
                return removed;
            }
            if (it->expireat > now)
            {
                return removed;
            }
            MMKVTable* kv = GetMMKVTable(it->key.db, false);
            if (NULL == kv)
            {
                ERROR_LOG("Invalid entry for empty kv:%u", it->key.db);
            }
            else
            {
                m_ttlmap->erase(it->key);
                int err = GenericDel(kv, it->key.db, it->key.key);
                if (err <= 0)
                {
                    ERROR_LOG("Invalid entry for delete error:%d", err);
                }
                m_ttlset->erase(it);
            }
            removed++;
        }
        return removed;
    }

    int MMKVImpl::Backup(const std::string& file)
    {
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        return m_segment.Backup(file);
    }
    int MMKVImpl::Restore(const std::string& from_file)
    {
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        int err = m_segment.Restore(from_file);
        m_kvs.clear();
        m_ttlset = NULL;
        m_ttlmap = NULL;
        ReOpen(false);
        return err;
    }
//    int MMKVImpl::Restore(const std::string& backup_dir, const std::string& to_dir)
//    {
//        return m_segment.Restore(backup_dir, to_dir);
//    }
//    bool MMKVImpl::CompareDataStore(const std::string& file)
//    {
//        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
//        return m_segment.CheckEqual(file);
//    }

    int MMKVImpl::EnsureWritableValueSpace(size_t space_size)
    {
        if (!m_readonly && (m_options.create_options.autoexpand || space_size > 0))
        {
            if (0 == space_size)
            {
                space_size = m_options.create_options.ensure_space_size;
            }
            if (m_segment.EnsureWritableValueSpace(space_size) > 0)
            {
                m_kvs.clear();
                m_ttlset = NULL;
                m_ttlmap = NULL;
                ReOpen(false);
                return 1;
            }
        }
        return 0;
    }
    int MMKVImpl::EnsureWritableSpace(size_t space_size)
    {
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        return EnsureWritableValueSpace(space_size);
    }

    int MMKVImpl::GetAllDBID(DBIDArray& ids)
    {
        ids.clear();
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        if (NULL == m_dbid_set)
        {
            return 0;
        }
        DBIDSet::iterator it = m_dbid_set->begin();
        while (it != m_dbid_set->end())
        {
            ids.push_back(*it);
            it++;
        }
        return 0;
    }

    MMKVImpl::~MMKVImpl()
    {
    }
}

