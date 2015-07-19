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

#ifndef PTMALLOC_ALLOCATOR_HPP_
#define PTMALLOC_ALLOCATOR_HPP_

#include <memory>
#include <new>
#include <algorithm>
#include <cstddef>
#include <stdexcept>
#include <boost/interprocess/offset_ptr.hpp>

extern "C" {
    extern void* mspace_malloc(void* msp, size_t bytes);
    extern void* mspace_realloc(void* msp, void* oldmem, size_t bytes);
    extern size_t mspace_usable_size(void* mem);
    extern void mspace_free(void* msp, void* mem);
}

namespace mmkv
{
    struct Meta
    {
            size_t file_size;
            size_t size;
            size_t init_key_space_size;
            size_t keyspace_offset;
            size_t valuespace_offset;
            Meta() :
                    file_size(0), size(0), init_key_space_size(0), keyspace_offset(0), valuespace_offset(0)
            {
            }
            inline bool IsKeyValueSplit()
            {
                return init_key_space_size != size;
            }
    };
    struct MemorySpaceInfo
    {
            boost::interprocess::offset_ptr<void> space;
            bool is_keyspace;
            MemorySpaceInfo() :
                    is_keyspace(false)
            {
            }
    };

    template<typename T>
    class Allocator
    {
        private:

            //Self type
            typedef Allocator<T> self_t;

            //typedef boost::interprocess::offset_ptr<void> VoidPtr;
            //Not assignable from related allocator
            template<class T2>
            Allocator& operator=(const Allocator<T2>&);

            //Not assignable from other allocator

            //Pointer to the allocator
            MemorySpaceInfo m_space;

        public:
            typedef T value_type;
            //typedef VoidPtr pointer;
            //typedef const VoidPtr const_pointer;
            //typedef T* pointer;
            //typedef const T* const_pointer;
            typedef boost::interprocess::offset_ptr<T> pointer;
            typedef const pointer const_pointer;
            typedef T& reference;
            typedef const T& const_reference;
            typedef size_t size_type;
            typedef ptrdiff_t difference_type;

            //!Obtains an allocator that allocates
            //!objects of type T2
            template<class T2>
            struct rebind
            {
                    typedef Allocator<T2> other;
            };
            Allocator()
            {
            }
            //!Constructor from the segment manager.
            //!Never throws
            Allocator(const MemorySpaceInfo& space) :
                    m_space(space)
            {
            }

            //!Constructor from other allocator.
            //!Never throws
            Allocator(const Allocator &other) :
                    m_space(other.m_space)
            {
            }

            //!Constructor from related allocator.
            //!Never throws
            template<class T2>
            Allocator(const Allocator<T2> &other) :
                    m_space(other.get_space())
            {
            }
            Allocator& operator=(const Allocator& other)
            {
                m_space = other.get_space();
                return *this;
            }

            //!Allocates memory for an array of count elements.
            //!Throws boost::interprocess::bad_alloc if there is no enough memory
            T* allocate(size_type count, const_pointer hint = 0)
            {
                (void) hint;
                void* p = NULL;
                Meta* meta = (Meta*) (m_space.space.get());
                if (m_space.is_keyspace || !meta->IsKeyValueSplit())
                {
                    p = mspace_malloc((char*) (meta) + meta->keyspace_offset, count * sizeof(T));
                }
                else
                {
                    p = mspace_malloc((char*) (meta) + meta->valuespace_offset, count * sizeof(T));
                }
                //allocate from other space
                if (NULL == p && meta->IsKeyValueSplit())
                {
                    if (m_space.is_keyspace)
                    {
                        p = mspace_malloc((char*) (meta) + meta->valuespace_offset, count * sizeof(T));
                    }
                    else
                    {
                        p = mspace_malloc((char*) (meta) + meta->keyspace_offset, count * sizeof(T));
                    }
                }
                if (NULL == p)
                {
                    throw std::bad_alloc();
                }
                return (T*) p;
            }

            void* realloc(void* oldmem, size_t bytes)
            {
                void* p = NULL;
                Meta* meta = (Meta*) (m_space.space.get());
                if (!meta->IsKeyValueSplit())
                {
                    p = mspace_realloc((char*) (meta) + meta->keyspace_offset, oldmem, bytes);
                }
                else
                {
                    char* valuespace = (char*) (meta) + meta->valuespace_offset;
                    if ((char*) p >= valuespace)
                    {
                        p = mspace_realloc(valuespace, oldmem, bytes);
                    }
                    else
                    {
                        p = mspace_realloc((char*) (meta) + meta->keyspace_offset, oldmem, bytes);
                    }

                }
                if (NULL == p)
                {
                    throw std::bad_alloc();
                }
                return p;
            }

            //!Deallocates memory previously allocated.
            //!Never throws

            inline void deallocate_ptr(const T* ptr, size_type n = 1)
            {
                if (NULL == ptr)
                    return;
                Meta* meta = (Meta*) (m_space.space.get());
                T* p = (T*) ptr;
                if (!meta->IsKeyValueSplit())
                {
                    mspace_free((char*) (meta) + meta->keyspace_offset, p);
                }
                else
                {
                    char* valuespace = (char*) (meta) + meta->valuespace_offset;
                    if ((char*) p >= valuespace)
                    {
                        mspace_free(valuespace, p);
                    }
                    else
                    {
                        mspace_free((char*) (meta) + meta->keyspace_offset, p);
                    }
                }
            }
            inline void deallocate(const pointer &ptr, size_type n = 1)
            {
                deallocate_ptr(ptr.get(), n);
            }

            template<typename R>
            void destroy(R* p)
            {
                if (NULL == p)
                {
                    return;
                }
                p->~R();
                deallocate_ptr((T*) p);
            }

            //!Returns the number of elements that could be allocated.
            //!Never throws
            size_type max_size() const
            {
                Meta* meta = (Meta*) (m_space.space.get());
                if (!meta->IsKeyValueSplit())
                {
                    return meta->size / sizeof(T);
                }
                else
                {
                    if (m_space.is_keyspace)
                    {
                        return meta->init_key_space_size / sizeof(T);
                    }
                    else
                    {
                        return (meta->size - meta->init_key_space_size) / sizeof(T);
                    }
                }
            }

            //!Swap segment manager. Does not throw. If each allocator is placed in
            //!different memory segments, the result is undefined.
            friend void swap(self_t &alloc1, self_t &alloc2)
            {
                //ipcdetail::do_swap(alloc1.mp_mngr, alloc2.mp_mngr);
            }

            //!Returns maximum the number of objects the previously allocated memory
            //!pointed by p can hold. This size only works for memory allocated with
            //!allocate, allocation_command and allocate_many.
            size_type size(const pointer &p) const
            {
                return (size_type) mspace_usable_size(p.get()) / sizeof(T);
            }

            //!Returns address of mutable object.
            //!Never throws
            pointer address(reference value) const
            {
                return &value;
            }

            //!Returns address of non mutable object.
            //!Never throws
            const_pointer address(const_reference value) const
            {
                return &value;
            }

            //!Constructs an object
            //!Throws if T's constructor throws
            //!For backwards compatibility with libraries using C++03 allocators
            template<class P>
            void construct(const pointer &ptr, const P& p)
            {
                ::new ((void*) (ptr.get())) P(p);
            }

            //!Destroys object. Throws if object's
            //!destructor throws
            void destroy(const pointer &ptr)
            {
                //(*ptr).~value_type();
                T* p = ptr.get();
                (*p).~value_type();
            }

            MemorySpaceInfo get_space() const
            {
                return m_space;
            }

            void* get_mspace()
            {
                Meta* meta = (Meta*) (m_space.space.get());
                if (m_space.is_keyspace || !meta->IsKeyValueSplit())
                {
                    return (char*) (meta) + meta->keyspace_offset;
                }
                else
                {
                    return (char*) (meta) + meta->valuespace_offset;
                }
            }
    };

    //!Equality test for same type
    //!of allocator
    template<class T> inline
    bool operator==(const Allocator<T> &alloc1, const Allocator<T> &alloc2)
    {
        return alloc1.get_space().space == alloc2.get_space().space;
    }

    //!Inequality test for same type
    //!of allocator
    template<class T> inline
    bool operator!=(const Allocator<T> &alloc1, const Allocator<T> &alloc2)
    {
        return alloc1.get_space().space != alloc2.get_space().space;
    }
    template<class T1, class T2> inline
    bool operator!=(const Allocator<T1> &alloc1, const Allocator<T2> &alloc2)
    {
        return alloc1.get_space().space != alloc2.get_space().space;
    }
}

#endif /* PTMALLOC_ALLOCATOR_HPP_ */
