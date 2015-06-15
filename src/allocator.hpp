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
#include "malloc-2.8.3.h"

namespace mmkv
{
    struct MemorySpace
    {
            void* buf;
            size_t size;
            MemorySpace() :
                    buf(NULL), size(0)
            {
            }
    };
    struct MemorySpaceOffset
    {
            boost::interprocess::offset_ptr<void> buf;
            size_t size;
            MemorySpaceOffset() :
                    size(0)
            {
            }
            MemorySpaceOffset(const MemorySpace& space) :
                    buf(space.buf), size(space.size)
            {
            }
            MemorySpaceOffset(const MemorySpaceOffset& space) :
                    buf(space.buf), size(space.size)
            {
            }
            bool InSpace(const void* p)
            {
                const void* max_addr = (const char*) buf.get() + size;
                return p > buf.get() && p < max_addr;
            }
            bool InSpace(const boost::interprocess::offset_ptr<void>& ptr)
            {
                return InSpace(ptr.get());
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
            MemorySpaceOffset m_primary_space;
            MemorySpaceOffset m_secondary_space;

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

            //!Constructor from the segment manager.
            //!Never throws
            Allocator(MemorySpace space1, MemorySpace space2) :
                    m_primary_space(space1), m_secondary_space(space2)
            {
            }

            //!Constructor from other allocator.
            //!Never throws
            Allocator(const Allocator &other) :
                    m_primary_space(other.m_primary_space), m_secondary_space(other.m_secondary_space)
            {
            }

            //!Constructor from related allocator.
            //!Never throws
            template<class T2>
            Allocator(const Allocator<T2> &other) :
                    m_primary_space(other.get_primary_space()), m_secondary_space(other.get_secondary_space())
            {
            }
            Allocator& operator=(const Allocator& other)
            {
                m_primary_space = other.get_primary_space();
                m_secondary_space = other.get_secondary_space();
                return *this;
            }

            //!Allocates memory for an array of count elements.
            //!Throws boost::interprocess::bad_alloc if there is no enough memory
            T* allocate(size_type count, const_pointer hint = 0)
            {
                (void) hint;
                void* p = mspace_malloc(m_primary_space.buf.get(), count * sizeof(T));
                if (NULL == p)
                {
                    p = mspace_malloc(m_secondary_space.buf.get(), count * sizeof(T));
                }
                if (p == NULL)
                {
                    throw std::bad_alloc();
                }
                return (T*) p;
            }

            void* realloc(void* oldmem, size_t bytes)
            {
                void* p = NULL;
                if (NULL == oldmem || m_primary_space.InSpace(oldmem))
                {
                    p = mspace_realloc(m_primary_space.buf.get(), oldmem, bytes);
                }
                else
                {
                    p = mspace_realloc(m_secondary_space.buf.get(), oldmem, bytes);
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
                //mp_mngr->deallocate((void*) ipcdetail::to_raw_pointer(ptr));
                //mspace_free(m_first_space.buf, ptr);
                T* p = (T*) ptr;
                if (m_primary_space.InSpace(p))
                {
                    mspace_free(m_primary_space.buf.get(), p);
                }
                else
                {
                    mspace_free(m_secondary_space.buf.get(), p);
                }
            }
            inline void deallocate(const pointer &ptr, size_type n = 1)
            {
                deallocate_ptr(ptr.get(), n);
            }

            template <typename R>
            void destroy(R* p)
            {
                p->~R();
                deallocate_ptr((T*)p);
            }

            //!Returns the number of elements that could be allocated.
            //!Never throws
            size_type max_size() const
            {
                return m_primary_space.size / sizeof(T);
                //return mspace_max_footprint(m_first_space) / sizeof(T);
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
            void construct(const pointer &ptr, const_reference p)
            {
                ::new ((void*) (ptr.get())) value_type(p);
            }

            //!Destroys object. Throws if object's
            //!destructor throws
            void destroy(const pointer &ptr)
            {
                //(*ptr).~value_type();
                T* p = ptr.get();
                (*p).~value_type();
            }

            MemorySpaceOffset get_primary_space() const
            {
                return m_primary_space;
            }
            MemorySpaceOffset get_secondary_space() const
            {
                return m_secondary_space;
            }
    };

    //!Equality test for same type
    //!of allocator
    template<class T> inline
    bool operator==(const Allocator<T> &alloc1, const Allocator<T> &alloc2)
    {
        return alloc1.get_primary_space().buf == alloc2.get_primary_space().buf
                && alloc1.get_secondary_space().buf == alloc2.get_secondary_space().buf;
    }

    //!Inequality test for same type
    //!of allocator
    template<class T> inline
    bool operator!=(const Allocator<T> &alloc1, const Allocator<T> &alloc2)
    {
        return alloc1.get_primary_space().buf != alloc2.get_primary_space().buf
                || alloc1.get_secondary_space().buf != alloc2.get_secondary_space().buf;
    }
    template<class T1, class T2> inline
    bool operator!=(const Allocator<T1> &alloc1, const Allocator<T2> &alloc2)
    {
        return alloc1.get_primary_space().buf != alloc2.get_primary_space().buf
                || alloc1.get_secondary_space().buf != alloc2.get_secondary_space().buf;
    }
}

#endif /* PTMALLOC_ALLOCATOR_HPP_ */
