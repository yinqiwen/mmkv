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

    struct Meta
    {
            int64_t size;
            int64_t init_key_space_size;
            int64_t init_value_space_size;
    };

    struct MMLock;
    class MMKV;
    class MemorySegmentManager
    {
        private:
            bool m_readonly;
            bool m_lock_enable;
            Logger m_logger;
            MemorySpace m_key_space;
            MemorySpace m_value_space;
            StringObjectTable* m_named_objs;
            MMLock* m_global_lock;
            friend class MMKV;
            StringObjectTable& GetNamedObjects()
            {
                return *m_named_objs;
            }
        public:
            MemorySegmentManager();
            void SetLogger(const Logger& logger);
            int Open(const OpenOptions& open_options);

            bool AssignObjectValue(Object& obj, const Data& value, bool in_keyspace = false, bool try_int_encoding =
                    true);
            bool ObjectMakeRoom(Object& obj, size_t size, bool in_keyspace = false);

            void* Allocate(size_t size, bool in_keyspace = false);
            void Deallocate(void* ptr);
            template<typename T>
            ConstructorProxy<T> FindOrConstructObject(const char* name, bool* created = NULL)
            {
                Object str(name);
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
                void* buf = Allocate(sizeof(T), true);
                ret.first->second = buf;
                T* value = (T*) buf;
                proxy.ptr = value;
                proxy.invoke_constructor = true;
                if (NULL != created)
                {
                    *created = true;
                }
                return proxy;
            }
            template<typename T>
            T* FindObject(const char* name)
            {
                Object str(name);
                StringObjectTable* table = m_named_objs;
                StringObjectTable::iterator it = table->find(str);
                if (it != table->end())
                {
                    return (T*) (it->second.get());
                }
                return NULL;
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

            int SyncKeySpace();
            int SyncValueSpace();

            bool Verify();

    };
}

#endif /* MM_MEMORY_HPP_ */
