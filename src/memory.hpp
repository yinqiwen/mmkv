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
#ifndef MM_MEMORY_HPP_
#define MM_MEMORY_HPP_

#include "logger_macros.hpp"
#include "mmap.hpp"
#include "containers.hpp"
#include "locks.hpp"
#include <new>

namespace mmkv
{

    struct Header
    {
            VoidPtr key_space;
            VoidPtr value_space;
            char named_objects[sizeof(StringObjectTable)];
    };

    struct MMLock;
    class MMKV;
    class MemorySegmentManager
    {
        private:
            bool m_readonly;
            bool m_lock_enable;
            Logger m_logger;
            Allocator<char> m_key_allocator;
            Allocator<char> m_value_allocator;
            StringObjectTable* m_named_objs;
            MMLock* m_global_lock;
            void* m_data_buf;
            OpenOptions m_open_options;
            friend class MMKV;
            StringObjectTable& GetNamedObjects()
            {
                return *m_named_objs;
            }
            int PostInit();
            int Expand(size_t new_size);
            int GetReaderCountIndex();
            int Restore(const std::string& from_dir, const std::string& to_dir);
        public:
            MemorySegmentManager();
            void SetLogger(const Logger& logger);
            int ReCreate(bool overwrite);
            int Open(const OpenOptions& open_options);
            int EnsureWritableValueSpace(size_t space_size);

            bool AssignObjectValue(Object& obj, const Data& value, bool in_keyspace = false);
            bool ObjectMakeRoom(Object& obj, size_t size, bool in_keyspace = false);

            void* Allocate(size_t size, bool in_keyspace = false);
            void Deallocate(void* ptr);

            template<typename T>
            ConstructorProxy<T> NewObject(bool in_keyspace)
            {
                void* buf = Allocate(sizeof(T), in_keyspace);
                T* value = (T*) buf;
                ConstructorProxy<T> proxy;
                proxy.ptr = value;
                proxy.invoke_constructor = true;
                return proxy;
            }
            template<typename T>
            void DestroyObject(T* obj)
            {
                if(NULL != obj)
                {
                    obj->~T();
                    Deallocate(obj);
                }
            }

            template<typename T>
            ConstructorProxy<T> FindOrConstructObject(const char* name, bool* created = NULL)
            {
                Object str(name, false);
                StringObjectTable* table = m_named_objs;
                std::pair<StringObjectTable::iterator, bool> ret = table->insert(
                        StringObjectTable::value_type(str, NULL));
                ConstructorProxy<T> proxy;
                if (!ret.second)
                {
                    proxy.ptr = (T*) (ret.first->second.get());
                    proxy.invoke_constructor = false;
                    if (NULL != created)
                    {
                        *created = false;
                    }
                    return proxy;
                }
                AssignObjectValue(const_cast<Object&>(ret.first->first), name, true);
                proxy = NewObject<T>(true);
                ret.first->second = proxy.ptr;
                if (NULL != created)
                {
                    *created = true;
                }
                return proxy;
            }
            template<typename T>
            T* FindObject(const char* name)
            {
                Object str(name, false);
                StringObjectTable* table = m_named_objs;
                StringObjectTable::iterator it = table->find(str);
                if (it != table->end())
                {
                    return (T*) (it->second.get());
                }
                return NULL;
            }
            template<typename T>
            int EraseObject(const char* name)
            {
                Object str(name, false);
                StringObjectTable* table = m_named_objs;
                StringObjectTable::iterator it = table->find(str);
                if (it != table->end())
                {
                    T* p = (T*) (it->second.get());
                    if (NULL != p)
                    {
                        p->~T();
                        Allocator<T> alloc = m_key_allocator;
                        alloc.deallocate_ptr(p);
                    }
                    table->erase(it);
                    return 1;
                }
                return 0;
            }

            Allocator<char> GetKeySpaceAllocator();
            Allocator<char> GetValueSpaceAllocator();

            template<typename T>
            Allocator<T> KeyAllocator()
            {
                return Allocator<T>(GetKeySpaceAllocator());
            }
            template<typename T>
            Allocator<T> ValueAllocator()
            {
                return Allocator<T>(GetValueSpaceAllocator());
            }

            size_t KeySpaceUsed();
            size_t keySpaceCapacity();

            size_t ValueSpaceUsed();
            size_t ValueSpaceCapacity();

            bool Lock(LockMode mode);
            bool Unlock(LockMode mode);
            bool IsLocked(bool readonly);
            bool LockEnable();

            bool Verify();
            int Backup(const std::string& path);
            int Restore(const std::string& from_file);

            bool CheckEqual(const std::string& file);

    };
}

#endif /* MM_MEMORY_HPP_ */
