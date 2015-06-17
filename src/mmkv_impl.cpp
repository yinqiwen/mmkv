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
#include <new>

namespace mmkv
{
    static const char* kTableConstName = "MMKVTable";
    static const char* kTTLConstName = "MMKVTTLSet";
    static const char* kTTLTableConstName = "MMKVTTLMap";

    MMKVImpl::MMKVImpl() :
            m_readonly(false), m_ttlset(NULL), m_ttlmap(NULL)
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
            RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
            kv = m_segment.FindOrConstructObject<MMKVTable>(name)(allocator);
        }
        else
        {
            RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
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
        Object tmpkey(key);
        MMKVTable::iterator found = table->find(tmpkey);
        if (found == table->end())
        {
            return NULL;
        }
        return &(found.value());
    }

    int MMKVImpl::Open(const OpenOptions& open_options)
    {
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
        if (!open_options.readonly)
        {
            TTLValueAllocator allocator(m_segment.GetKeySpaceAllocator());
            RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
            m_ttlset = m_segment.FindOrConstructObject<TTLValueSet>(kTTLConstName)(std::less<TTLValue>(), allocator);
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
    }
    void MMKVImpl::AssignObjectContent(const Object& obj, const Data& data, bool in_keyspace, bool try_int_encoding)
    {
        m_segment.AssignObjectValue(const_cast<Object&>(obj), data, in_keyspace, try_int_encoding);
    }

    int MMKVImpl::GetPOD(DBID db, const Data& key, bool created_if_notexist, uint32_t expected_type, Data& v)
    {
        if (m_readonly && created_if_notexist)
        {
            return ERR_PERMISSION_DENIED;
        }
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        Object tmpkey(key);
        Object* value_data = NULL;
        if (created_if_notexist)
        {
            std::pair<MMKVTable::iterator, bool> ret = kv->insert(MMKVTable::value_type(tmpkey, Object()));
            value_data = const_cast<Object*>(&(ret.first.value()));
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
            value_data = const_cast<Object*>(&(found.value()));
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
        uint64_t ttl = GetTTL(db, key, obj);
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
        entry.expireat = ttl;
        if (!m_ttlset->insert(entry).second)
        {
            return;
        }
        value.hasttl = 1;
        m_ttlmap->insert(TTLValueTable::value_type(entry.key, ttl));
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
        return fit->second;
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
                    if (it.isfilled())
                    {
                        DestroyObjectContent(it.key());
                        DestroyObjectContent(it.value());
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
                    DestroyObjectContent(*it);
                    it = m->erase(it);
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
                    DestroyObjectContent(it->value);
                    it = m->set.erase(it);
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
            ERROR_LOG("Destroy value failed with err:%d", err);
        }
        return err;
    }
    int MMKVImpl::GenericDel(MMKVTable* table, const Object& key)
    {
        //Object tmpkey(key);
        MMKVTable::iterator found = table->find(key);
        if (found != table->end())
        {
            const Object& value_data = found.value();
            int err = GenericDelValue(value_data);
            if (0 != err)
            {
                return err;
            }
            DestroyObjectContent(found.key());
            table->erase(found);
            return 1;
        }
        return 0;
    }

    int MMKVImpl::GenericInsertValue(MMKVTable* table, const Data& key, Object& v, bool replace)
    {
        Object tmpkey(key);
        std::pair<MMKVTable::iterator, bool> ret = table->insert(MMKVTable::value_type(tmpkey, v));
        if (!ret.second)
        {
            const Object& old_data = ret.first.value();
            if (!replace)
            {
                return 0;
            }
            GenericDelValue(old_data);
            ret.first.value(v);
        }
        else
        {
            m_segment.AssignObjectValue(const_cast<Object&>(ret.first.key()), key, false);
        }
        return 1;
    }

    int MMKVImpl::Del(DBID db, const DataArray& keys)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        MMKVTable* kv = GetMMKVTable(db, true);
        if (NULL == kv)
        {
            return ERR_DB_NOT_EXIST;
        }
        int count = 0;
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        for (size_t i = 0; i < keys.size(); i++)
        {
            count += GenericDel(kv, keys[i]);
        }
        return count;
    }

    int MMKVImpl::Type(DBID db, const Data& key)
    {
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
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
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        const Object* value_data = FindMMValue(kv, key);
        if (NULL == value_data)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        return 1;
    }

    int64_t MMKVImpl::TTL(DBID db, const Data& key)
    {
        return PTTL(db, key) / 1000;
    }
    int64_t MMKVImpl::PTTL(DBID db, const Data& key)
    {
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return 0;
        }
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        const Object* value_data = FindMMValue(kv, key);
        if (NULL == value_data)
        {
            return 0;
        }
        return value_data->hasttl ? GetTTL(db, key, *value_data) / 1000 : 0;
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
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        MMKVTable::iterator found = kv->find(key);
        if (found == kv->end())
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        SetTTL(db, key, const_cast<Object&>(found.value()), milliseconds_timestamp * 1000);
        return 0;
    }

    int MMKVImpl::GenericMoveKey(DBID src_db, const Data& src_key, DBID dest_db, const Data& dest_key, bool nx)
    {
        MMKVTable* src_kv = GetMMKVTable(src_db, false);
        MMKVTable* dst_kv = GetMMKVTable(dest_db, nx ? false : true);
        if (NULL == src_kv || NULL == dst_kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        MMKVTable::iterator found = src_kv->find(src_key);
        if (found == src_kv->end())
        {
            return 0;
        }

        Object tmpkey2(dest_key);
        std::pair<MMKVTable::iterator, bool> ret = dst_kv->insert(MMKVTable::value_type(tmpkey2, found.value()));
        const Object& kk = ret.first.key();
        if (ret.second)
        {
            src_kv->erase(found);
            if (tmpkey2 == src_key)
            {
                const_cast<Object&>(kk) = found.key();
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
                ret.first.value(found.value());
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
        //TODO
        return -1;
    }
    int MMKVImpl::Keys(DBID db, const std::string& pattern, StringArray& keys)
    {
        //TODO
        return -1;
    }
    int MMKVImpl::Sort(DBID db, const Data& key, const std::string& by, int limit_offset, int limit_count,
            const StringArray& get_patterns, bool desc, bool alpha_sort, const Data& destination_key)
    {
        //TODO
        return -1;
    }
    int MMKVImpl::Scan(DBID db, const Data& key, int cursor, const std::string& pattern, int32_t limit_count)
    {
        //TODO
        return -1;
    }

    int64_t MMKVImpl::DBSize(DBID db)
    {
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return 0;
        }
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        return kv->size();
    }
    int MMKVImpl::FlushDB(DBID db)
    {
        //TODO
        return -1;
    }
    int MMKVImpl::FlushAll()
    {
        //TODO
        return -1;
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
                ERROR_LOG("Invalid entry for empty kv");
            }
            else
            {
                int err = GenericDel(kv, it->key.key);
                if (0 != err)
                {
                    ERROR_LOG("Invalid entry for delete error:%d", err);
                }
            }
            m_ttlmap->erase(it->key);
            m_ttlset->erase(it);
            removed++;
        }
        return removed;
    }

    int MMKVImpl::SyncData()
    {
        m_segment.SyncKeySpace();
        m_segment.SyncValueSpace();
        return 0;
    }

    MMKVImpl::~MMKVImpl()
    {
    }
}

