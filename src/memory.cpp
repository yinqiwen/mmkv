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
    static const int kLocksFileLength = 1024 * 1024;
    static pid_t g_current_pid = 0;
    static ThreadLocal<uint32_t> g_lock_state;
    static const char* kBackupFileName = "mmkv.snapshot";
    static const char* kDataFileName = "data";

    static const uint32_t kMagicCode = 0xCD007B;
    static const uint32_t kVersionCode = 1;
    static const int kMaxReaderProcCount = 65536;
    static int g_reader_count_index = -1;

    static inline int64_t allign_page(int64_t size)
    {
        int page = sysconf(_SC_PAGESIZE);
        uint32_t left = size % page;
        if (left > 0)
        {
            return size - left;
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
    struct ReaderCount
    {
            pid_t pid;
            volatile uint32_t count;
    };
    struct MMLock
    {
            SleepingRWLock lock;
            volatile pid_t writer_pid;
            ReaderCount readers[kMaxReaderProcCount];
            volatile bool inited;
            MMLock() :
                    writer_pid(0), inited(false)
            {
                memset(readers, 0, sizeof(ReaderCount) * kMaxReaderProcCount);
            }
    };

    MemorySegmentManager::MemorySegmentManager() :
            m_readonly(false), m_lock_enable(false), m_named_objs(NULL), m_global_lock(
            NULL), m_data_buf(NULL)
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
        void* mspace = (char*) meta + kHeaderLength + kMetaLength;
        if (!m_open_options.readonly)
        {
            if (m_open_options.reserve_space)
            {
                mlock(mspace, meta->size);
            }
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
            memset(m_global_lock, 0, sizeof(MMLock));
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
        Verify();
        //get g_reader_count_index
        GetReaderCountIndex();
        PostInit();
        return 0;
    }

    int MemorySegmentManager::ReCreate(bool overwrite)
    {
        MemorySpaceInfo mspace_info;
        mspace_info.space = m_data_buf;

        Meta* meta = (Meta*) m_data_buf;
        Header* header = (Header*) ((char*) m_data_buf + kMetaLength);
        if (overwrite) //create file
        {
            meta->file_size = m_open_options.create_options.size;
            meta->size = m_open_options.create_options.size - kHeaderLength - kMetaLength;
            meta->size = allign_page(meta->size);
        }

        void* mspace_buf = (char*) meta + kHeaderLength + kMetaLength;
        void* mspace = create_mspace_with_base(mspace_buf, meta->size, 0, overwrite);

        if (overwrite)
        {
            meta->mspace_offset = (char*) mspace - (char*) m_data_buf;
        }

        m_space_allocator = Allocator<char>(mspace_info);
        if (overwrite)
        {
            StringObjectTableAllocator allocator(m_space_allocator);
            memset(header->named_objects, 0, sizeof(StringObjectTable));
            ::new ((void*) (header->named_objects)) StringObjectTable(std::less<Object>(), allocator);
        }
        m_named_objs = (StringObjectTable*) (header->named_objects);
        return 0;
    }

    int MemorySegmentManager::EnsureWritableValueSpace(size_t space_size)
    {
        if (m_open_options.readonly)
        {
            ERROR_LOG("Permission denied to expand.");
            return -1;
        }
        Meta* meta = (Meta*) m_data_buf;
        size_t top_size = mspace_top_size((char*) (meta) + meta->mspace_offset);
        if (space_size == 0)
        {
            if (top_size < (meta->size >> 1))
            {
                space_size = meta->file_size * 2;
            }
        }
        if (meta->size < space_size)
        {
            size_t new_size = space_size;
            if (new_size < static_cast<size_t>(meta->size * 1.5))
            {
                new_size = static_cast<size_t>(meta->size * 1.5);
            }
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
        uint64_t micros = get_current_micros();
        size_t old_file_size = meta->file_size;
        new_size = allign_page(new_size);
        size_t inc = new_size - meta->file_size;

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
        m_data_buf = data_buf.buf;
        ReCreate(false);
        PostInit();
        meta = (Meta*) m_data_buf;
        meta->file_size = m_open_options.create_options.size;
        meta->size = m_open_options.create_options.size - kHeaderLength - kMetaLength;
        void* value_mspace = (char*) m_data_buf + meta->mspace_offset;
        mspace_inc_size(value_mspace, inc);
        INFO_LOG("Cost %lluus to expand store from %llu to %llu.", get_current_micros() - micros, old_file_size, meta->file_size);
        return 1;
    }

    bool MemorySegmentManager::ObjectMakeRoom(Object& obj, size_t size)
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
            buf = Allocate(size);
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

    bool MemorySegmentManager::AssignObjectValue(Object& obj, const Data& value, bool try_int_encoding)
    {
        if (obj.IsInteger())
        {
            return true;
        }
        if (try_int_encoding) //try int encoding in value space
        {
            if (obj.SetInteger(value))
            {
                return true;
            }
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
        void* buf = Allocate(value.Len());
        memcpy(buf, value.Value(), value.Len());
        obj.len = value.Len();
        obj.SetValue(buf);
        return true;
    }

    void* MemorySegmentManager::Allocate(size_t size)
    {
        return m_space_allocator.allocate(size);
    }
    void MemorySegmentManager::Deallocate(void* ptr)
    {
        if (NULL == ptr)
        {
            return;
        }
        m_space_allocator.deallocate_ptr((char*) ptr);
    }
    Allocator<char> MemorySegmentManager::GetMSpaceAllocator()
    {
        return m_space_allocator;
    }

    int MemorySegmentManager::GetReaderCountIndex()
    {
        if (g_reader_count_index != -1)
        {
            return g_reader_count_index;
        }
        for (int i = 0; i < kMaxReaderProcCount; i++)
        {
            if (m_global_lock->readers[i].pid == get_current_pid() || m_global_lock->readers[i].pid == 0)
            {
                m_global_lock->readers[i].pid = get_current_pid();
                g_reader_count_index = i;
                return i;
            }
        }
        return 0;
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
            lock->writer_pid = get_current_pid();
            g_lock_state.SetValue(WRITE_LOCKED);
        }
        else
        {
            atomic_add(&(lock->readers[GetReaderCountIndex()].count), 1);
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
            lock->writer_pid = 0;
        }
        else
        {
            atomic_add(&(lock->readers[GetReaderCountIndex()].count), -1);
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
        if (m_global_lock->writer_pid != 0)
        {
            if (kill(m_global_lock->writer_pid, 0) != 0)
            {
                ERROR_LOG("Old write process crashed while writing.");
                if (m_open_options.open_ignore_error)
                {
                    WARN_LOG("Clear write lock state since 'open_ignore_error' is setted.");
                    m_global_lock->inited = true;
                    memset(m_global_lock, 0, sizeof(MMLock));
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }
        //clear dead readers lock state
        for (int i = 0; i < kMaxReaderProcCount; i++)
        {
            if (m_global_lock->readers[i].pid > 0)
            {
                if (kill(m_global_lock->readers[i].pid, 0) != 0)
                {
                    while (m_global_lock->readers[i].count > 0)
                    {
                        m_global_lock->lock.Unlock(READ_LOCK);
                        m_global_lock->readers[i].count--;
                    }
                    m_global_lock->readers[i].pid = 0;
                }
            }
        }
        m_named_objs->verify();
        return true;
    }

    static int dump_cksum_str(XXH64_state_t* cksm64, XXH32_state_t* cksm32, const std::string& cksm_file, std::string& cksm)
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
        FILE* dest_file = fopen(cksm_file.c_str(), "w");
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

    int MemorySegmentManager::Backup(const std::string& path)
    {
        int err = 0;
        if (NULL == m_data_buf)
        {
            ERROR_LOG("Empty data to backup.");
            return -1;
        }
        //make_dir(dir);
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
        void* key_mspace = (char*) meta + meta->mspace_offset;
        void* key_space_start = (char*) meta + kMetaLength + kHeaderLength;
        void* key_mspace_top = mspace_top_address(key_mspace);
        uint32_t now = time(NULL);

        FILE* dest_file = fopen(path.c_str(), "w");
        if (NULL == dest_file)
        {
            ERROR_LOG("Failed to open backup file:%s to write.", path.c_str());
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
        if (0 != lz4_compress_tofile((char*) key_space_start, (char*) key_mspace_top - (char*) key_space_start, dest_file, xxhash_cksum_callback, chsumset))
        {
            ERROR_LOG("Failed to compress key space content");
            err = -1;
            goto _end;
        }

        dump_cksum_str(cksm64, cksm32, path + ".cksm", cksm);
        INFO_LOG("Snapshot cksm:%s", cksm.c_str());
        _end: fclose(dest_file);
        XXH64_freeState(cksm64);
        XXH32_freeState(cksm32);
        return err;
    }

    int MemorySegmentManager::Restore(const std::string& from_file)
    {
        Meta* meta = (Meta*) m_data_buf;
        munmap(m_data_buf, meta->file_size);
        char data_path[m_open_options.dir.size() + 100];
        sprintf(data_path, "%s/data", m_open_options.dir.c_str());
        int err = Restore(from_file, data_path);
        MMapBuf data_buf(m_logger);
        if (m_open_options.readonly)
        {
            err = data_buf.OpenRead(data_path);
        }
        else
        {
            err = data_buf.OpenWrite(data_path, 0, false);
        }
        if (err < 0)
        {
            return -1;
        }
        m_data_buf = data_buf.buf;
        ReCreate(false);
        PostInit();
        return err;
    }

    int MemorySegmentManager::Restore(const std::string& from_file, const std::string& to_file)
    {
        FILE* dest_file = NULL;
        int err = 0;
        MMapBuf backup(m_logger);
        uint32_t header_space_len = 0, key_space_len = 0, value_space_len = 0;
        size_t chunk_decomp_size = 0;
        size_t buf_cursor = 0;
        char* compressed_key_space = NULL;
        char* compressed_value_space = NULL;
        //std::string from_file = from + "/" + kBackupFileName;
        //std::string to_file = to + "/" + kDataFileName;
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
            ERROR_LOG("Failed to open backup file:%s to write.", to_file.c_str());
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
        if (lz4_decompress_tofile(compressed_key_space, backup.size - buf_cursor, dest_file, &chunk_decomp_size, xxhash_cksum_callback, chsumset) != 0)
        {
            ERROR_LOG("decompress header content failed");
            err = -1;
            goto _end;
        }
        //
        buf_cursor += chunk_decomp_size;
        //fseek(dest_file, kMetaLength + kHeaderLength + meta.size, SEEK_SET);
        compressed_value_space = backup.buf + buf_cursor;

        dump_cksum_str(cksm64, cksm32, to_file + ".cksm", cksm);
        INFO_LOG("Restore cksm:%s", cksm.c_str());
        _end: backup.Close();
        if (NULL != dest_file)
        {
            fflush(dest_file);
            fsync(fileno(dest_file));
            fclose(dest_file);
        }
        XXH64_freeState(cksm64);
        XXH32_freeState(cksm32);
        return err;
    }

    bool MemorySegmentManager::CheckEqual(const std::string& file)
    {
        MMapBuf cmpbuf(m_logger, true);
        if (0 != cmpbuf.OpenRead(file))
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
        char* key_mspace = (char*) meta + meta->mspace_offset;
        char* key_space_start = (char*) meta + kMetaLength + kHeaderLength;
        char* key_mspace_stop = (char*) mspace_top_address(key_mspace);

        char* other_key_mspace = cmpbuf.buf + meta->mspace_offset;
        char* other_key_space_start = cmpbuf.buf + kMetaLength + kHeaderLength;
        char* other_key_mspace_stop = (char*) mspace_top_address(other_key_mspace);

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
        return true;
    }

    size_t MemorySegmentManager::MSpaceUsed()
    {
        return mspace_used(m_space_allocator.get_mspace());
    }

    size_t MemorySegmentManager::MSpaceCapacity()
    {
        return mspace_footprint(m_space_allocator.get_mspace());
    }

    bool MemorySegmentManager::LockEnable()
    {
        return m_lock_enable;
    }
}

