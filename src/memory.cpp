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
#include "mmkv_logger.hpp"
#include "thread_local.hpp"
#include "xxhash.h"
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <errno.h>

#define UNLOCKED 0
#define READ_LOCKED 1
#define WRITE_LOCKED 2

namespace mmkv
{
    static const int kHeaderLength = 1024 * 1024;
    static const int kMetaLength = 4096;
    static const int kLocksFileLength = 4096;
    static pid_t g_current_pid = 0;
    static ThreadLocal<uint32_t> g_lock_state;
    static const char* kBackupFileName = "mmkv.snapshot";
    static const char* kDataFileName = "data";

    static const uint32_t kMagicCode = 0xCD007B;
    static const uint32_t kVersionCode = 1;

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
            m_readonly(false), m_lock_enable(false), m_named_objs(NULL), m_global_lock(NULL), m_data_buf(NULL)
    {

    }
    void MemorySegmentManager::SetLogger(const Logger& logger)
    {
        m_logger = logger;
    }

    int MemorySegmentManager::PostInit()
    {
        Meta* meta = (Meta*) m_data_buf;
        madvise(m_data_buf, meta->file_size, MADV_RANDOM);
        void* key_space = (char*) meta + kHeaderLength + kMetaLength;
        void* value_space = NULL;
        if (meta->IsKeyValueSplit())
        {
            value_space = (char*) key_space + meta->init_key_space_size;
        }

        if (!m_open_options.readonly)
        {
            if (m_open_options.reserve_keyspace)
            {
                mlock(key_space, meta->init_key_space_size);
            }
            if (m_open_options.reserve_valuespace && NULL != value_space)
            {
                mlock(value_space, meta->size - meta->init_key_space_size);
            }
        }
        if (m_open_options.verify)
        {
            m_named_objs->verify();
        }
        return 0;
    }

    int MemorySegmentManager::Open(const OpenOptions& open_options)
    {
        if (!is_dir_exist(open_options.dir))
        {
            if (!open_options.create_if_notexist || open_options.readonly)
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

        m_data_buf = data_buf.buf;
        m_open_options = open_options;
        ReCreate(open_ret == 1);
        PostInit();
        return 0;
    }

    int MemorySegmentManager::ReCreate(bool overwrite)
    {
        MemorySpaceInfo key_space_info, value_space_info;
        key_space_info.is_keyspace = true;
        key_space_info.space = m_data_buf;
        value_space_info.space = m_data_buf;
        value_space_info.is_keyspace = false;

        Meta* meta = (Meta*) m_data_buf;
        Header* header = (Header*) ((char*) m_data_buf + kMetaLength);
        if (overwrite) //create file
        {
            meta->file_size = m_open_options.create_options.size;
            meta->size = m_open_options.create_options.size - kHeaderLength - kMetaLength;
            meta->init_key_space_size = (int64_t) (meta->size * m_open_options.create_options.keyspace_factor);
            //allign key space size to 4096(page size)
            uint32_t page_size = 4096;
            meta->init_key_space_size = allign_size(meta->init_key_space_size, page_size);
        }

        void* key_space = (char*) meta + kHeaderLength + kMetaLength;
        void* key_mspace = create_mspace_with_base(key_space, meta->init_key_space_size, 0, overwrite);

        void* value_space = NULL;
        void* value_mspace = NULL;
        if (meta->IsKeyValueSplit())
        {
            value_space = (char*) key_space + meta->init_key_space_size;
            value_mspace = create_mspace_with_base(value_space, meta->size - meta->init_key_space_size, 0, overwrite);
        }
        if (overwrite)
        {
            meta->keyspace_offset = (char*) key_mspace - (char*) m_data_buf;
            if (NULL != value_mspace)
            {
                meta->valuespace_offset = (char*) value_mspace - (char*) m_data_buf;
            }
            else
            {
                meta->valuespace_offset = meta->keyspace_offset;
            }
        }

        m_key_allocator = Allocator<char>(key_space_info);
        m_value_allocator = Allocator<char>(value_space_info);
        if (overwrite)
        {
            StringObjectTableAllocator allocator(m_key_allocator);
            memset(header->named_objects, 0, sizeof(StringObjectTable));
            ::new ((void*) (header->named_objects)) StringObjectTable(std::less<Object>(), allocator);
        }
        m_named_objs = (StringObjectTable*) (header->named_objects);
        return 0;
    }

    int MemorySegmentManager::EnsureWritableSpace(size_t space_size)
    {
        if (m_open_options.readonly)
        {
            ERROR_LOG("Permission denied to expand.");
            return -1;
        }
        Meta* meta = (Meta*) m_data_buf;
        size_t total_size = mspace_top_size((char*) (meta) + meta->keyspace_offset);
        if(meta->IsKeyValueSplit())
        {
            total_size += mspace_top_size((char*) (meta) + meta->valuespace_offset);
        }
        if(total_size < space_size)
        {
            size_t new_size = meta->file_size * 2;
            return Expand(new_size);
        }
        return 0;
    }

    int MemorySegmentManager::Expand(size_t new_size)
    {
        if (m_open_options.readonly)
        {
            ERROR_LOG("Permission denied to expand.");
            return -1;
        }
        Meta* meta = (Meta*) m_data_buf;
        if (meta->file_size >= new_size)
        {
            return 0;
        }
        size_t inc = new_size - meta->file_size;
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(*this);
        munmap(m_data_buf, meta->file_size);
        char data_path[m_open_options.dir.size() + 100];
        sprintf(data_path, "%s/data", m_open_options.dir.c_str());
        truncate(data_path, new_size);
        m_open_options.create_options.size = new_size;
        MMapBuf data_buf(m_logger);
        int open_ret = data_buf.OpenWrite(data_path, new_size, false);
        if (open_ret < 0)
        {
            return -1;
        }
        madvise(data_buf.buf, data_buf.size, MADV_RANDOM);
        m_data_buf = data_buf.buf;
        ReCreate(false);
        PostInit();
        meta = (Meta*) m_data_buf;
        meta->file_size = m_open_options.create_options.size;
        meta->size = m_open_options.create_options.size - kHeaderLength - kMetaLength;
        void* value_mspace = (char*) m_data_buf + meta->valuespace_offset;
        mspace_inc_size(value_mspace, inc);
        return 1;
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
            if (value.Value() == NULL)
            {
                obj.SetInteger((int64_t) (value.Len()));
            }
            else
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
        if (in_keyspace)
        {
            return m_key_allocator.allocate(size);
        }
        else
        {
            return m_value_allocator.allocate(size);
        }
    }
    void MemorySegmentManager::Deallocate(void* ptr)
    {
        if (NULL == ptr)
        {
            return;
        }
        m_key_allocator.deallocate_ptr((char*) ptr);
    }
    Allocator<char> MemorySegmentManager::GetKeySpaceAllocator()
    {
        return m_key_allocator;
    }
    Allocator<char> MemorySegmentManager::GetValueSpaceAllocator()
    {
        return m_value_allocator;
    }
    bool MemorySegmentManager::Lock(LockMode mode)
    {
        if (!LockEnable())
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
        if (!LockEnable())
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
        return readonly ? lock_state == READ_LOCKED : lock_state == WRITE_LOCKED;
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

    static int dump_cksum_str(XXH64_state_t* cksm64, XXH32_state_t* cksm32, const std::string& dir, std::string& cksm)
    {
        char xxhashsum[256];
        uint32_t offset = 0;
        uint64_t tmp1 = XXH64_digest(cksm64);
        uint32_t tmp2 = XXH32_digest(cksm32);
        for (uint32_t i = 0; i < sizeof(uint64_t); i++)
        {
            int ret = sprintf(xxhashsum + offset, "%02x", (char*) (&tmp1) + i);
            offset += ret;
        }
        for (uint32_t i = 0; i < sizeof(uint32_t); i++)
        {
            int ret = sprintf(xxhashsum + offset, "%02x", (char*) (&tmp2) + i);
            offset += ret;
        }
        cksm = xxhashsum;
        FILE* dest_file = fopen((dir + "/xxhash.cksm").c_str(), "w");
        if (NULL == dest_file)
        {
            return -1;
        }
        fwrite(xxhashsum, 1, offset, dest_file);
        fclose(dest_file);
    }

    static void xxhash_cksum_callback(const void* data, uint32_t len, void* cbdata)
    {
        void** tmp = (void**) cbdata;
        XXH64_state_t* cksm64 = (XXH64_state_t*) tmp[0];
        XXH32_state_t* cksm32 = (XXH32_state_t*) tmp[1];
        XXH32_update(cksm32, data, len);
        XXH64_update(cksm64, data, len);
    }

    int MemorySegmentManager::Backup(const std::string& dir)
    {
        int err = 0;
        if (NULL == m_data_buf)
        {
            ERROR_LOG("Empty data to backup.");
            return -1;
        }
        make_dir(dir);
        std::string cksm;
        XXH64_state_t* cksm64 = XXH64_createState();
        XXH32_state_t* cksm32 = XXH32_createState();
        void* chsumset[2];
        XXH64_reset(cksm64, kMagicCode);
        XXH32_reset(cksm32, kMagicCode);
        chsumset[0] = cksm64;
        chsumset[1] = cksm32;

        Meta* meta = (Meta*) m_data_buf;
        uint16_t meta_len = sizeof(Meta);
        Header* header = (Header*) ((char*) m_data_buf + kMetaLength);
        uint32_t header_len = sizeof(Header);
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(*this);
        void* key_mspace = (char*) meta + meta->keyspace_offset;
        void* key_space_start = (char*) meta + kMetaLength + kHeaderLength;
        void* key_mspace_top = mspace_top_address(key_mspace);
        void* value_mspace = (char*) meta + meta->valuespace_offset;
        void* value_space_start = (char*) meta + kMetaLength + kHeaderLength + meta->init_key_space_size;
        void* value_mspace_top = mspace_top_address(value_mspace);
        uint32_t now = time(NULL);
        char dest_file_path[dir.size() + 256];
        sprintf(dest_file_path, "%s/%s", dir.c_str(), kBackupFileName, now);

        FILE* dest_file = fopen(dest_file_path, "w");
        if (NULL == dest_file)
        {
            ERROR_LOG("Failed to open backup file:%s to write.", dest_file_path);
            return -1;
        }
        if (fwrite(&kMagicCode, sizeof(kMagicCode), 1, dest_file) != 1)
        {
            ERROR_LOG("Failed to save magic code");
            err = -1;
            goto _end;
        }
        if (fwrite(&kVersionCode, sizeof(kVersionCode), 1, dest_file) != 1)
        {
            ERROR_LOG("Failed to save version code");
            err = -1;
            goto _end;
        }

        //save meta content
        if (fwrite(&meta_len, sizeof(meta_len), 1, dest_file) != 1)
        {
            ERROR_LOG("Failed to save meta length");
            err = -1;
            goto _end;
        }
        if (fwrite(meta, 1, meta_len, dest_file) != meta_len)
        {
            ERROR_LOG("Failed to save meta content");
            err = -1;
            goto _end;
        }
        xxhash_cksum_callback(meta, meta_len, chsumset);
        //save header content
        if (fwrite(&header_len, sizeof(header_len), 1, dest_file) != 1)
        {
            ERROR_LOG("Failed to save header length");
            err = -1;
            goto _end;
        }
        if (fwrite(header, 1, header_len, dest_file) != header_len)
        {
            ERROR_LOG("Failed to save header space content");
            err = -1;
            goto _end;
        }
        xxhash_cksum_callback(header, header_len, chsumset);
        //compress & save key space
        if (0
                != lz4_compress_tofile((char*) key_space_start, (char*) key_mspace_top - (char*) key_space_start,
                        dest_file, xxhash_cksum_callback, chsumset))
        {
            ERROR_LOG("Failed to compress key space content");
            err = -1;
            goto _end;
        }
        //compress & save value space
        if (meta->IsKeyValueSplit())
        {
            if (0
                    != lz4_compress_tofile((char*) value_space_start,
                            (char*) value_mspace_top - (char*) value_space_start, dest_file, xxhash_cksum_callback,
                            chsumset))
            {
                ERROR_LOG("Failed to compress value space content");
                err = -1;
                goto _end;
            }
        }

        dump_cksum_str(cksm64, cksm32, dir, cksm);
        INFO_LOG("Snapshot cksm:%s", cksm.c_str());
        _end: fclose(dest_file);
        XXH64_freeState(cksm64);
        XXH32_freeState(cksm32);
        return err;
    }

    int MemorySegmentManager::Restore(const std::string& from, const std::string& to)
    {
        FILE* dest_file = NULL;
        int err = 0;
        make_dir(to);
        MMapBuf backup(m_logger);
        uint32_t header_space_len = 0, key_space_len = 0, value_space_len = 0;
        size_t chunk_decomp_size = 0;
        size_t buf_cursor = 0;
        char* compressed_key_space = NULL;
        char* compressed_value_space = NULL;
        std::string from_file = from + "/" + kBackupFileName;
        std::string to_file = to + "/" + kDataFileName;
        std::string cksm;
        XXH64_state_t* cksm64 = XXH64_createState();
        XXH32_state_t* cksm32 = XXH32_createState();
        void* chsumset[2];
        XXH64_reset(cksm64, kMagicCode);
        XXH32_reset(cksm32, kMagicCode);
        chsumset[0] = cksm64;
        chsumset[1] = cksm32;
        if (0 != backup.OpenRead(from_file))
        {
            ERROR_LOG("Failed to load backup file to resume.");
            return -1;
        }
        uint32_t magic_code, version_code;
        uint16_t meta_len = 0;
        uint32_t header_len = 0;
        Meta meta;
        Header header;

        if (backup.size < sizeof(meta_len) + sizeof(uint32_t) * 2)
        {
            ERROR_LOG("No sufficient space for  length header .");
            err = -1;
            goto _end;
        }
        memcpy(&magic_code, backup.buf, sizeof(uint32_t));
        memcpy(&version_code, backup.buf + sizeof(uint32_t), sizeof(uint32_t));
        memcpy(&meta_len, backup.buf + sizeof(uint32_t) * 2, sizeof(meta_len));
        buf_cursor = sizeof(uint32_t) * 2 + sizeof(meta_len);
        if (magic_code != kMagicCode)
        {
            ERROR_LOG("Wrong magic code in header.");
            err = -1;
            goto _end;
        }
        if (version_code == 0 || version_code > kVersionCode)
        {
            ERROR_LOG("Wrong version code:%d in header.", version_code);
            err = -1;
            goto _end;
        }
        if (backup.size < buf_cursor + meta_len)
        {
            ERROR_LOG("No sufficient space for meta content.");
            err = -1;
            goto _end;
        }
        if (meta_len > sizeof(Meta))
        {
            ERROR_LOG("Invalid meta len:%u", meta_len);
            err = -1;
            goto _end;
        }
        memcpy(&meta, backup.buf + buf_cursor, meta_len);
        xxhash_cksum_callback(&meta, meta_len, chsumset);
        buf_cursor += meta_len;

        //load header
        if (backup.size < buf_cursor + sizeof(uint32_t))
        {
            ERROR_LOG("No sufficient space for header len.");
            err = -1;
            goto _end;
        }
        memcpy(&header_len, backup.buf + buf_cursor, sizeof(uint32_t));
        if (header_len > sizeof(Header) || backup.size < buf_cursor + header_len + sizeof(uint32_t))
        {
            ERROR_LOG("Invalid header len:%u compare to %u", header_len, sizeof(Header));
            err = -1;
            goto _end;
        }
        buf_cursor += sizeof(uint32_t);
        memcpy(&header, backup.buf + buf_cursor, header_len);
        xxhash_cksum_callback(&header, header_len, chsumset);

        buf_cursor += header_len;
        dest_file = fopen(to_file.c_str(), "w");
        if (NULL == dest_file)
        {
            ERROR_LOG("Failed to open backup file:%s to write.", to.c_str());
            err = -1;
            goto _end;
        }
        ftruncate(fileno(dest_file), meta.file_size);

        fseek(dest_file, 0, SEEK_SET);
        if (fwrite(&meta, 1, sizeof(meta), dest_file) != sizeof(meta))
        {
            ERROR_LOG("Failed to write meta content in backup file.");
            err = -1;
            goto _end;
        }
        fseek(dest_file, kMetaLength, SEEK_SET);
        if (fwrite(&header, 1, sizeof(header), dest_file) != sizeof(header))
        {
            ERROR_LOG("Failed to write header content in backup file.");
            err = -1;
            goto _end;
        }
        fseek(dest_file, kMetaLength + kHeaderLength, SEEK_SET);

        compressed_key_space = backup.buf + buf_cursor;
        if (lz4_decompress_tofile(compressed_key_space, backup.size - buf_cursor, dest_file, &chunk_decomp_size,
                xxhash_cksum_callback, chsumset) != 0)
        {
            ERROR_LOG("decompress header content failed");
            err = -1;
            goto _end;
        }
        //
        buf_cursor += chunk_decomp_size;
        fseek(dest_file, kMetaLength + kHeaderLength + meta.init_key_space_size, SEEK_SET);
        compressed_value_space = backup.buf + buf_cursor;
        if (meta.IsKeyValueSplit())
        {
            if (lz4_decompress_tofile(compressed_value_space, backup.size - buf_cursor, dest_file, &chunk_decomp_size,
                    xxhash_cksum_callback, chsumset) != 0)
            {
                ERROR_LOG("decompress value space content failed");
                err = -1;
                goto _end;
            }
        }

        dump_cksum_str(cksm64, cksm32, to, cksm);
        INFO_LOG("Restore cksm:%s", cksm.c_str());
        _end: backup.Close();
        if (NULL != dest_file)
        {
            fclose(dest_file);
        }
        XXH64_freeState(cksm64);
        XXH32_freeState(cksm32);
        return err;
    }

    bool MemorySegmentManager::CheckEqual(const std::string& dir)
    {
        MMapBuf cmpbuf(m_logger, true);
        if (0 != cmpbuf.OpenRead(dir + "/" + kDataFileName))
        {
            ERROR_LOG("Failed to load compare file.");
            return false;
        }
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(*this);
        if (0 != memcmp(m_data_buf, cmpbuf.buf, sizeof(Meta)))
        {
            ERROR_LOG("Meta part is not equal.");
            return false;
        }
        if (0 != memcmp((char*) m_data_buf + kMetaLength, cmpbuf.buf + kMetaLength, sizeof(Header)))
        {
            ERROR_LOG("Header part is not equal.");
            return false;
        }
        Meta* meta = (Meta*) m_data_buf;
        char* key_mspace = (char*) meta + meta->keyspace_offset;
        char* key_space_start = (char*) meta + kMetaLength + kHeaderLength;
        char* key_mspace_stop = (char*) mspace_top_address(key_mspace);
        char* value_mspace = (char*) meta + meta->valuespace_offset;
        char* value_space_start = (char*) meta + kMetaLength + kHeaderLength + meta->init_key_space_size;
        char* value_mspace_stop = (char*) mspace_top_address(value_mspace);

        char* other_key_mspace = cmpbuf.buf + meta->keyspace_offset;
        char* other_key_space_start = cmpbuf.buf + kMetaLength + kHeaderLength;
        char* other_key_mspace_stop = (char*) mspace_top_address(other_key_mspace);
        char* other_value_mspace = cmpbuf.buf + meta->valuespace_offset;
        char* other_value_space_start = cmpbuf.buf + kMetaLength + kHeaderLength + meta->init_key_space_size;
        char* other_value_mspace_stop = (char*) mspace_top_address(other_value_mspace);

        if (key_mspace_stop - key_space_start != other_key_mspace_stop - other_key_space_start)
        {
            WARN_LOG("Key space length is not equal.");
            return false;
        }
        size_t cmpoff = 0;
        size_t rest = key_mspace_stop - key_space_start;
        while (rest > 0)
        {
            size_t len = rest > 4096 ? 4096 : rest;
            if (0 != memcmp(key_space_start + cmpoff, other_key_space_start + cmpoff, len))
            {
                WARN_LOG("Key space  is not equal at offset:%llu with len:%u.", cmpoff, len);
                return false;
            }
            cmpoff += len;
            rest -= len;
        }
        if (meta->IsKeyValueSplit())
        {
            if (value_mspace_stop - value_space_start != other_value_mspace_stop - other_value_space_start)
            {
                WARN_LOG("Value space length is not equal.");
                return false;
            }
            cmpoff = 0;
            rest = value_mspace_stop - value_space_start;
            while (rest > 0)
            {
                size_t len = rest > 4096 ? 4096 : rest;
                if (0 != memcmp(value_space_start + cmpoff, other_value_space_start + cmpoff, len))
                {
                    WARN_LOG("Value space  is not equal at offset:%llu with len:%u.", cmpoff, len);
                    return false;
                }
                cmpoff += len;
                rest -= len;
            }
        }
        return true;
    }

    size_t MemorySegmentManager::KeySpaceUsed()
    {
        return mspace_used(m_key_allocator.get_mspace());
    }

    size_t MemorySegmentManager::keySpaceCapacity()
    {
        return mspace_footprint(m_key_allocator.get_mspace());
    }

    size_t MemorySegmentManager::ValueSpaceUsed()
    {
        return mspace_used(m_value_allocator.get_mspace());
    }
    size_t MemorySegmentManager::ValueSpaceCapacity()
    {
        return mspace_footprint(m_value_allocator.get_mspace());
    }
    bool MemorySegmentManager::LockEnable()
    {
        return m_lock_enable;
    }
}

