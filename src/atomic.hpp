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

#ifndef ATOMIC_H_
#define ATOMIC_H_

#include <stdint.h>

#if (__i386 || __amd64) && __GNUC__
#define GNUC_VERSION (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__)
#if GNUC_VERSION >= 40100
#define HAVE_SYNC_OP
#define atomic_cmpxchg_bool(P, O, N) __sync_bool_compare_and_swap((P), (O), (N))
#define atomic_xadd(P, V) __sync_fetch_and_add((P), (V))
#define cmpxchg(P, O, N) __sync_val_compare_and_swap((P), (O), (N))
#define cmpchg(P, O, N) __sync_bool_compare_and_swap((P), (O), (N))
#define atomic_inc(P) __sync_add_and_fetch((P), 1)
#define atomic_dec(P) __sync_add_and_fetch((P), -1)
#define atomic_add(P, V) __sync_add_and_fetch((P), (V))
#define atomic_set_bit(P, V) __sync_or_and_fetch((P), 1<<(V))
#define atomic_clear_bit(P, V) __sync_and_and_fetch((P), ~(1<<(V)))
#elif !defined(__i386__) && !defined(__x86_64__)
#error    "Arch not supprot!"
#endif
#endif

#define barrier() asm volatile("": : :"memory")
#define cpu_relax() asm volatile("pause\n": : :"memory")
#define mfence(v) asm volatile("mfence\n": : :"memory")
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

namespace mmkv
{

#if !defined HAVE_SYNC_OP

    inline uint64_t atomic64_cmpxchg(volatile uint64_t* ptr, uint64_t old_value, uint64_t new_value)
    {
        uint64_t prev;
        asm volatile("lock;"
                "cmpxchgq %1, %2;"
                : "=a"(prev)
                : "q"(new_value), "m"(*ptr), "a"(old_value)
                : "memory");
        return prev;
    }
    inline uint32_t atomic32_cmpxchg(volatile uint32_t* ptr, uint32_t old_value, uint32_t new_value)
    {
        uint32_t prev;
        asm volatile("lock;"
                "cmpxchgl %1, %2;"
                : "=a"(prev)
                : "q"(new_value), "m"(*ptr), "a"(old_value)
                : "memory");
        return prev;
    }

    inline uint8_t atomic8_xadd(uint8_t val, volatile uint8_t* atom)
    {
        asm volatile ("lock xaddb %0, %1"
                : "+r" (val), "+m" (*atom)
                : : "memory", "cc");
        return val;
    }
    inline uint16_t atomic16_xadd(uint16_t val, volatile uint16_t* atom)
    {
        asm volatile ("lock xaddw %0, %1"
                : "+r" (val), "+m" (*atom)
                : : "memory", "cc");
        return val;
    }
    inline uint32_t atomic32_xadd(uint32_t val, volatile uint32_t* atom)
    {
        asm volatile ("lock xaddl %0, %1"
                : "+r" (val), "+m" (*atom)
                : : "memory", "cc");
        return val;
    }
    inline uint64_t atomic64_xadd(uint64_t val, volatile uint64_t* atom)
    {
        asm volatile ("lock xaddq %0, %1"
                : "+r" (val), "+m" (*atom)
                : : "memory", "cc");
        return val;
    }
    inline uint8_t atomic_xadd(volatile uint8_t* atom, uint8_t val)
    {
        return atomic8_xadd(val, atom);
    }
    inline uint16_t atomic_xadd(volatile uint16_t* atom, uint16_t val)
    {
        return atomic16_xadd(val, atom);
    }
    inline uint32_t atomic_xadd(volatile uint32_t* atom, uint32_t val)
    {
        return atomic32_xadd(val, atom);
    }
    inline uint64_t atomic_xadd(volatile uint64_t* atom, uint64_t val)
    {
        return atomic64_xadd(val, atom);
    }
    inline uint8_t atomic_add(volatile uint8_t* atom, uint8_t val)
    {
        return atomic8_xadd(val, atom) + val;
    }
    inline uint16_t atomic_add(volatile uint16_t* atom, uint16_t val)
    {
        return atomic16_xadd(val, atom)+ val;
    }
    inline uint32_t atomic_add(volatile uint32_t* atom, uint32_t val)
    {
        return atomic32_xadd(val, atom) +val;
    }
    inline uint32_t atomic_add(volatile uint32_t* atom, int32_t val)
    {
        return atomic32_xadd((uint32_t)val, atom) + val;
    }
    inline uint64_t atomic_add(volatile uint64_t* atom, uint64_t val)
    {
        return atomic64_xadd(val, atom)+ val;
    }

    inline uint64_t cmpxchg(volatile uint64_t* ptr, uint64_t old_value, uint64_t new_value)
    {
        return atomic64_cmpxchg(ptr, old_value, new_value);
    }
    inline uint32_t cmpxchg(volatile uint32_t* ptr, uint32_t old_value, uint32_t new_value)
    {
        return atomic32_cmpxchg(ptr, old_value, new_value);
    }


    inline uint32_t cmpchg(volatile uint32_t* ptr, uint32_t old_value, uint32_t new_value)
    {
        return cmpxchg(ptr, old_value, new_value) == old_value;
    }
#endif

    static inline unsigned char xchg_8(void *ptr, unsigned char x)
    {
        asm volatile("xchgb %0,%1"
                :"=r" (x)
                :"m" (*(volatile unsigned char *)ptr), "0" (x)
                :"memory");
        return x;
    }

    static inline unsigned short xchg_16(void *ptr, unsigned short x)
    {
        asm volatile("xchgw %0,%1"
                :"=r" ((unsigned short) x)
                :"m" (*(volatile unsigned short *)ptr), "0" (x)
                :"memory");
        return x;
    }

    static inline unsigned xchg_32(void *ptr, unsigned x)
    {
        asm volatile("xchgl %0,%1"
                :"=r" ((unsigned) x)
                :"m" (*(volatile unsigned *)ptr), "0" (x)
                :"memory");
        return x;
    }

#ifdef __x86_64__
    static inline unsigned long long xchg_64(void *ptr, unsigned long long x)
    {
        asm volatile("xchgq %0,%1"
                :"=r" ((unsigned long long) x)
                :"m" (*(volatile unsigned long long *)ptr), "0" (x)
                :"memory");
        return x;
    }

    static inline void *xchg_ptr(void *ptr, void *x)
    {
        __asm__ __volatile__("xchgq %0,%1"
                :"=r" ((uintptr_t) x)
                :"m" (*(volatile uintptr_t *)ptr), "0" ((uintptr_t) x)
                :"memory");
        return x;
    }
#else
static inline void *xchg_ptr(void *ptr, void *x)
{
    __asm__ __volatile__("xchgl %k0,%1"
            :"=r" ((uintptr_t) x)
            :"m" (*(volatile uintptr_t *)ptr), "0" ((uintptr_t) x)
            :"memory");
    return x;
}
#endif
}

#endif /*ATOMIC head define**/

