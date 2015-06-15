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
#include "memory.hpp"
#include "lock_guard.hpp"
#include "locks.hpp"
#include "malloc-2.8.3.h"
#include "logger.hpp"
#include "thread_local.hpp"
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <sys/resource.h>

#define UNLOCKED 0
#define READ_LOCKED 1
#define WRITE_LOCKED 2

namespace mmkv
{
    static const int kMetaLength = 4096;
    static const int kHeaderLength = 1024 * 1024;
    static const int kLocksFileLength = 4096;
    static pid_t g_current_pid = 0;
    static ThreadLocal<uint32_t> g_lock_state;

    static inline int64_t allign_size(int64_t size, uint32_t allignment)
    {
        uint32_t left = size % allignment;
        if (left > 0)
        {
            return size + (allignment - left);
        }
        return size;
    }

    /*
     * this code not work well with fork(), but i'm use it since this lib would not runinng in a fork process.
     */
    static pid_t get_current_pid()
    {
        if (0 != g_current_pid)
        {
            return g_current_pid;
        }
        g_current_pid = getpid();
        return g_current_pid;
    }

    struct MMLock
    {
            SleepingRWLock lock;
            volatile pid_t writing_pid;
            volatile uint32_t reading_count;
            volatile bool inited;
            MMLock() :
                    writing_pid(0), reading_count(0), inited(false)
            {
            }
    };

    MemorySegmentManager::MemorySegmentManager() :
            m_readonly(false), m_lock_enable(false), m_named_objs(NULL), m_global_lock(NULL)
    {

    }
    void MemorySegmentManager::SetLogger(const Logger& logger)
    {
        m_logger = logger;
    }

    int MemorySegmentManager::Open(const OpenOptions& open_options)
    {
        if(!is_dir_exist(open_options.dir))
        {
            if(!open_options.create_if_notexist  || open_options.readonly)
            {
                ERROR_LOG("Dir:%s is not exist.", open_options.dir.c_str());
                return -1;
            }
            make_dir(open_options.dir);
        }

        m_lock_enable = open_options.use_lock;
        char data_path[open_options.dir.size() + 100];
        char locks_path[open_options.dir.size() + 100];
        sprintf(data_path, "%s/data", open_options.dir.c_str());
        sprintf(locks_path, "%s/locks", open_options.dir.c_str());

        FileLock open_lock;
        if (!open_lock.Init(open_options.dir))
        {
            ERROR_LOG("%s", open_lock.LastError().c_str());
            return -1;
        }
        LockGuard<FileLock> guard(open_lock);

        int open_ret = -1;
        MMapBuf data_buf(m_logger), locks_buf(m_logger);
        open_ret = locks_buf.OpenWrite(locks_path, kLocksFileLength, true);
        if (open_ret < 0)
        {
            ERROR_LOG("Open lock file failed!");
            return -1;
        }
        m_global_lock = (MMLock*) locks_buf.buf;
        if (!m_global_lock->inited)
        {
            m_global_lock->inited = 1;
        }

        if (open_options.readonly)
        {
            open_ret = data_buf.OpenRead(data_path);
        }
        else
        {
            open_ret = data_buf.OpenWrite(data_path, open_options.create_options.size, open_options.create_if_notexist);
        }

        if (open_ret < 0)
        {
            return -1;
        }

        madvise(data_buf.buf, data_buf.size, MADV_RANDOM);

        Meta* meta = (Meta*) data_buf.buf;
        Header* header = (Header*) data_buf.buf + kMetaLength;
        bool create_memory = false;
        if (open_ret == 1) //create file
        {
            create_memory = true;
            meta->size = open_options.create_options.size;
            meta->init_key_space_size = (int64_t) (meta->size * open_options.create_options.keyspace_factor);
            //allign key space size to 4096(page size)
            uint32_t page_size = 4096;
            meta->init_key_space_size = allign_size(meta->init_key_space_size, page_size);
            meta->init_value_space_size = meta->size - meta->init_key_space_size - kMetaLength - kHeaderLength;
        }

        void* key_space = data_buf.buf + kMetaLength + kHeaderLength;
        void* value_space = data_buf.buf + kMetaLength + kHeaderLength + meta->init_key_space_size;
        m_key_space.buf = create_mspace_with_base(key_space, meta->init_key_space_size, 0);
        m_key_space.size = meta->init_key_space_size;
        m_value_space.buf = create_mspace_with_base(value_space, meta->init_value_space_size, 0);
        m_value_space.size = meta->init_value_space_size;

        if (!open_options.readonly)
        {
            if (open_options.reserve_keyspace)
            {
                mlock(key_space, meta->init_key_space_size);
            }
            if (open_options.reserve_valuespace)
            {
                mlock(value_space, meta->init_value_space_size);
            }
        }

        if (create_memory)
        {
            StringObjectTableAllocator allocator(m_key_space, m_value_space);
            memset(header->named_objects, 0, sizeof(StringObjectTable));
            ::new ((void*) (header->named_objects)) StringObjectTable(std::less<Object>(), allocator);
        }
        m_named_objs = (StringObjectTable*) (header->named_objects);
        if (open_options.verify)
        {
            m_named_objs->verify();
        }
        return 0;
    }

    bool MemorySegmentManager::ObjectMakeRoom(Object& obj, size_t size, bool in_keyspace)
    {
        if (size <= obj.len && obj.encoding != OBJ_ENCODING_INT)
        {
            return true;
        }
        void* buf = NULL;
        if (obj.encoding == OBJ_ENCODING_INT)
        {
            size = obj.len > size ? obj.len : size;
        }
        if (size > 8)
        {
            buf = Allocate(size, in_keyspace);
            if (obj.len > 0)
            {
                if (obj.encoding == OBJ_ENCODING_INT)
                {
                    ll2string((char*) buf, obj.len, obj.IntegerValue());
                }
                else
                {
                    memcpy(buf, obj.RawValue(), obj.len);
                }
            }
            memset((char*) buf + obj.len, 0, size - obj.len);
            if (obj.IsOffsetPtr())
            {
                Deallocate(const_cast<char*>(obj.RawValue()));
            }
            obj.SetValue(buf);
        }
        obj.len = size;
        return true;
    }

    bool MemorySegmentManager::AssignObjectValue(Object& obj, const Data& value, bool in_keyspace,
            bool try_int_encoding)
    {
        long long int_val;
        if (try_int_encoding && !in_keyspace && value.Len() <= 21 && string2ll(value.Value(), value.Len(), &int_val))
        {
            return obj.SetInteger(int_val);
        }
        if (value.Len() <= 8 || value.Value() == NULL)
        {
            if(value.Value() == NULL)
            {
                obj.SetInteger((int64_t)(value.Len()));
            }else
            {
                obj.encoding = OBJ_ENCODING_RAW;
                obj.len = value.Len();
                memcpy(obj.data, value.Value(), value.Len());
            }
            return true;
        }
        void* buf = Allocate(value.Len(), in_keyspace);
        memcpy(buf, value.Value(), value.Len());
        obj.len = value.Len();
        obj.SetValue(buf);
        return true;
    }

    void* MemorySegmentManager::Allocate(size_t size, bool in_keyspace)
    {
        void* p = mspace_malloc(in_keyspace ? m_key_space.buf : m_value_space.buf, size);
        if (NULL == p)
        {
            throw std::bad_alloc();
        }
        return p;
    }
    void MemorySegmentManager::Deallocate(void* ptr)
    {
        if (ptr > m_value_space.buf)
        {
            mspace_free(m_value_space.buf, ptr);
        }
        else
        {
            mspace_free(m_key_space.buf, ptr);
        }
    }
    Allocator<char> MemorySegmentManager::GetKeySpaceAllocator()
    {
        Allocator<char> allocator(m_key_space, m_value_space);
        return allocator;
    }
    Allocator<char> MemorySegmentManager::GetValueSpaceAllocator()
    {
        Allocator<char> allocator(m_value_space, m_key_space);
        return allocator;
    }
    bool MemorySegmentManager::Lock(LockMode mode)
    {
        if(!LockEnable())
        {
            return true;
        }
        MMLock* lock = m_global_lock;
        if (NULL == lock)
        {
            return false;
        }
        bool ret = lock->lock.Lock(mode);
        if (mode == WRITE_LOCK && ret)
        {
            lock->writing_pid = get_current_pid();
            g_lock_state.SetValue(WRITE_LOCKED);
        }
        else
        {
            atomic_add(&lock->reading_count, 1);
            g_lock_state.SetValue(READ_LOCKED);
        }
        return ret;
    }
    bool MemorySegmentManager::Unlock(LockMode mode)
    {
        if(!LockEnable())
        {
            return true;
        }
        MMLock* lock = m_global_lock;
        if (NULL == lock)
        {
            return false;
        }
        if (mode == WRITE_LOCK)
        {
            lock->writing_pid = 0;
        }
        else
        {
            atomic_add(&lock->reading_count, -1);
        }
        bool ret = lock->lock.Unlock(mode);
        g_lock_state.SetValue(UNLOCKED);
        return ret;
    }

    bool MemorySegmentManager::IsLocked(bool readonly)
    {
        if (!LockEnable())
        {
            return true;
        }
        uint32_t lock_state = g_lock_state.GetValue();
        return readonly ? lock_state == READ_LOCK : lock_state == WRITE_LOCK;
    }

    bool MemorySegmentManager::Verify()
    {
        if (m_global_lock->writing_pid != 0)
        {
            if (kill(m_global_lock->writing_pid, 0) != 0)
            {
                return false;
            }
        }
        return true;
    }

    size_t MemorySegmentManager::KeySpaceUsed()
    {
        return mspace_used(m_key_space.buf);
    }

    size_t MemorySegmentManager::keySpaceCapacity()
    {
        return mspace_footprint(m_key_space.buf);
    }

    size_t MemorySegmentManager::ValueSpaceUsed()
    {
        return mspace_used(m_value_space.buf);
    }
    size_t MemorySegmentManager::ValueSpaceCapacity()
    {
        return mspace_footprint(m_value_space.buf);
    }
    bool MemorySegmentManager::LockEnable()
    {
        return m_lock_enable;
    }
}

