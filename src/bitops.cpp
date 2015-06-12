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
#include "utils.hpp"
#include "locks.hpp"
#include "lock_guard.hpp"
#include "mmkv_impl.hpp"
#include <limits.h>
namespace mmkv
{
    /* Count number of bits set in the binary array pointed by 's' and long
     * 'count' bytes. The implementation of this function is required to
     * work with a input string length up to 512 MB. */
    size_t redisPopcount(void *s, long count)
    {
        size_t bits = 0;
        unsigned char *p = (unsigned char *)s;
        uint32_t *p4;
        static const unsigned char bitsinbyte[256] =
            { 0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2,
                    3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3,
                    2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3,
                    4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3,
                    3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4,
                    5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5,
                    3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4,
                    5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8 };

        /* Count initial bytes not aligned to 32 bit. */
        while ((unsigned long) p & 3 && count)
        {
            bits += bitsinbyte[*p++];
            count--;
        }

        /* Count bits 28 bytes at a time */
        p4 = (uint32_t*) p;
        while (count >= 28)
        {
            uint32_t aux1, aux2, aux3, aux4, aux5, aux6, aux7;

            aux1 = *p4++;
            aux2 = *p4++;
            aux3 = *p4++;
            aux4 = *p4++;
            aux5 = *p4++;
            aux6 = *p4++;
            aux7 = *p4++;
            count -= 28;

            aux1 = aux1 - ((aux1 >> 1) & 0x55555555);
            aux1 = (aux1 & 0x33333333) + ((aux1 >> 2) & 0x33333333);
            aux2 = aux2 - ((aux2 >> 1) & 0x55555555);
            aux2 = (aux2 & 0x33333333) + ((aux2 >> 2) & 0x33333333);
            aux3 = aux3 - ((aux3 >> 1) & 0x55555555);
            aux3 = (aux3 & 0x33333333) + ((aux3 >> 2) & 0x33333333);
            aux4 = aux4 - ((aux4 >> 1) & 0x55555555);
            aux4 = (aux4 & 0x33333333) + ((aux4 >> 2) & 0x33333333);
            aux5 = aux5 - ((aux5 >> 1) & 0x55555555);
            aux5 = (aux5 & 0x33333333) + ((aux5 >> 2) & 0x33333333);
            aux6 = aux6 - ((aux6 >> 1) & 0x55555555);
            aux6 = (aux6 & 0x33333333) + ((aux6 >> 2) & 0x33333333);
            aux7 = aux7 - ((aux7 >> 1) & 0x55555555);
            aux7 = (aux7 & 0x33333333) + ((aux7 >> 2) & 0x33333333);
            bits += ((((aux1 + (aux1 >> 4)) & 0x0F0F0F0F) + ((aux2 + (aux2 >> 4)) & 0x0F0F0F0F)
                    + ((aux3 + (aux3 >> 4)) & 0x0F0F0F0F) + ((aux4 + (aux4 >> 4)) & 0x0F0F0F0F)
                    + ((aux5 + (aux5 >> 4)) & 0x0F0F0F0F) + ((aux6 + (aux6 >> 4)) & 0x0F0F0F0F)
                    + ((aux7 + (aux7 >> 4)) & 0x0F0F0F0F)) * 0x01010101) >> 24;
        }
        /* Count the remaining bytes. */
        p = (unsigned char*) p4;
        while (count--)
            bits += bitsinbyte[*p++];
        return bits;
    }

    /* Return the position of the first bit set to one (if 'bit' is 1) or
     * zero (if 'bit' is 0) in the bitmap starting at 's' and long 'count' bytes.
     *
     * The function is guaranteed to return a value >= 0 if 'bit' is 0 since if
     * no zero bit is found, it returns count*8 assuming the string is zero
     * padded on the right. However if 'bit' is 1 it is possible that there is
     * not a single set bit in the bitmap. In this special case -1 is returned. */
    long redisBitpos(void *s, unsigned long count, int bit)
    {
        unsigned long *l;
        unsigned char *c;
        unsigned long skipval, word = 0, one;
        long pos = 0; /* Position of bit, to return to the caller. */
        unsigned long j;

        /* Process whole words first, seeking for first word that is not
         * all ones or all zeros respectively if we are lookig for zeros
         * or ones. This is much faster with large strings having contiguous
         * blocks of 1 or 0 bits compared to the vanilla bit per bit processing.
         *
         * Note that if we start from an address that is not aligned
         * to sizeof(unsigned long) we consume it byte by byte until it is
         * aligned. */

        /* Skip initial bits not aligned to sizeof(unsigned long) byte by byte. */
        skipval = bit ? 0 : UCHAR_MAX;
        c = (unsigned char*) s;
        while ((unsigned long) c & (sizeof(*l) - 1) && count)
        {
            if (*c != skipval)
                break;
            c++;
            count--;
            pos += 8;
        }

        /* Skip bits with full word step. */
        skipval = bit ? 0 : ULONG_MAX;
        l = (unsigned long*) c;
        while (count >= sizeof(*l))
        {
            if (*l != skipval)
                break;
            l++;
            count -= sizeof(*l);
            pos += sizeof(*l) * 8;
        }

        /* Load bytes into "word" considering the first byte as the most significant
         * (we basically consider it as written in big endian, since we consider the
         * string as a set of bits from left to right, with the first bit at position
         * zero.
         *
         * Note that the loading is designed to work even when the bytes left
         * (count) are less than a full word. We pad it with zero on the right. */
        c = (unsigned char*) l;
        for (j = 0; j < sizeof(*l); j++)
        {
            word <<= 8;
            if (count)
            {
                word |= *c;
                c++;
                count--;
            }
        }

        /* Special case:
         * If bits in the string are all zero and we are looking for one,
         * return -1 to signal that there is not a single "1" in the whole
         * string. This can't happen when we are looking for "0" as we assume
         * that the right of the string is zero padded. */
        if (bit == 1 && word == 0)
            return -1;

        /* Last word left, scan bit by bit. The first thing we need is to
         * have a single "1" set in the most significant position in an
         * unsigned long. We don't know the size of the long so we use a
         * simple trick. */
        one = ULONG_MAX; /* All bits set to 1.*/
        one >>= 1; /* All bits set to 1 but the MSB. */
        one = ~one; /* All bits set to 0 but the MSB. */

        while (one)
        {
            if (((one & word) != 0) == bit)
                return pos;
            pos++;
            one >>= 1;
        }

        /* If we reached this point, there is a bug in the algorithm, since
         * the case of no match is handled as a special case before. */
        //redisPanic("End of redisBitpos() reached.");
        return 0; /* Just to avoid warnings. */
    }

    int MMKVImpl::BitCount(DBID db, const Data& key, int start, int end)
    {
        long strlen;
        unsigned char *p;
        char llbuf[32];

        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        MMKVTable::iterator found = kv->find(key);
        if (found == kv->end())
        {
            return 0;
        }
        const Object& value_data = found.value();
        if (value_data.type != V_TYPE_STRING)
        {
            return ERR_INVALID_TYPE;
        }

        /* Set the 'p' pointer to the string, that can be just a stack allocated
         * array if our string was integer encoded. */
        if (value_data.IsInteger())
        {
            p = (unsigned char*) llbuf;
            strlen = ll2string(llbuf, sizeof(llbuf), value_data.IntegerValue());
        }
        else
        {
            p = (unsigned char*) value_data.RawValue();
            strlen = value_data.len;
        }

        if (start < 0)
            start = strlen + start;
        if (end < 0)
            end = strlen + end;
        if (start < 0)
            start = 0;
        if (end < 0)
            end = 0;
        if (end >= strlen)
            end = strlen - 1;

        /* Precondition: end >= 0 && end < strlen, so the only condition where
         * zero can be returned is: start > end. */
        if (start > end)
        {
            return 0;
        }
        else
        {
            long bytes = end - start + 1;
            return redisPopcount(p + start, bytes);
        }
    }
    /* -----------------------------------------------------------------------------
     * Bits related string commands: GETBIT, SETBIT, BITCOUNT, BITOP.
     * -------------------------------------------------------------------------- */

#define BITOP_AND   0
#define BITOP_OR    1
#define BITOP_XOR   2
#define BITOP_NOT   3
    int MMKVImpl::BitOP(DBID db, const std::string& opstr, const Data& dest_key, const DataArray& keys)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        const char *opname = opstr.c_str();
        const Object *o;
        unsigned long op, j, numkeys;

        unsigned long maxlen = 0; /* Array of length of src strings,
         and max len. */
        unsigned long minlen = 0; /* Min len among the input keys. */
        Object res; /* Resulting string. */

        /* Parse the operation name. */
        if ((opname[0] == 'a' || opname[0] == 'A') && !strcasecmp(opname, "and"))
            op = BITOP_AND;
        else if ((opname[0] == 'o' || opname[0] == 'O') && !strcasecmp(opname, "or"))
            op = BITOP_OR;
        else if ((opname[0] == 'x' || opname[0] == 'X') && !strcasecmp(opname, "xor"))
            op = BITOP_XOR;
        else if ((opname[0] == 'n' || opname[0] == 'N') && !strcasecmp(opname, "not"))
            op = BITOP_NOT;
        else
        {
            return ERR_SYNTAX_ERROR;
        }

        /* Sanity check: NOT accepts only a single key argument. */
        if (op == BITOP_NOT && keys.size() != 1)
        {
            return ERR_ARGS_EXCEED_LIMIT;
        }
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return ERR_DB_NOT_EXIST;
        }
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        /* Lookup keys, and store pointers to the string objects into an array. */
        numkeys = keys.size();
        unsigned char *src[numkeys]; /* Array of source strings pointers. */
        unsigned long len[numkeys];
        char llbuf[numkeys][32];
        //MMValue *objects[numkeys]; /* Array of source objects. */
        for (j = 0; j < numkeys; j++)
        {
            o = FindMMValue(kv, keys[j]);
            /* Handle non-existing keys as empty strings. */
            if (o == NULL)
            {
                //objects[j] = NULL;
                src[j] = NULL;
                len[j] = 0;
                minlen = 0;
                continue;
            }
            /* Return an error if one of the keys is not a string. */
            if (o->type != V_TYPE_STRING)
            {
                return ERR_INVALID_TYPE;
            }
            if (o->IsInteger())
            {
                ll2string(llbuf[j], 32, o->IntegerValue());
                src[j] = (unsigned char *)llbuf[j];
            }else
            {
                src[j] =(unsigned char *)o->data;
            }
            //objects[j] = o;

            len[j] = o->len;
            if (len[j] > maxlen)
                maxlen = len[j];
            if (j == 0 || len[j] < minlen)
                minlen = len[j];
        }

        /* Compute the bit operation, if at least one string is not empty. */
        if (maxlen)
        {
            //res = AllocateStringValueSpace(maxlen, false);
            m_segment.ObjectMakeRoom(res, maxlen, false);
            unsigned char output, byte;
            unsigned long i;

            /* Fast path: as far as we have data for all the input bitmaps we
             * can take a fast path that performs much better than the
             * vanilla algorithm. */
            j = 0;
            if (minlen >= sizeof(unsigned long) * 4 && numkeys <= 16)
            {
                unsigned long *lp[16];
                unsigned long *lres = (unsigned long*) res.WritableData();

                /* Note: sds pointer is always aligned to 8 byte boundary. */
                memcpy(lp, src, sizeof(unsigned long*) * numkeys);
                memcpy(res.WritableData(), src[0], minlen);

                /* Different branches per different operations for speed (sorry). */
                if (op == BITOP_AND)
                {
                    while (minlen >= sizeof(unsigned long) * 4)
                    {
                        for (i = 1; i < numkeys; i++)
                        {
                            lres[0] &= lp[i][0];
                            lres[1] &= lp[i][1];
                            lres[2] &= lp[i][2];
                            lres[3] &= lp[i][3];
                            lp[i] += 4;
                        }
                        lres += 4;
                        j += sizeof(unsigned long) * 4;
                        minlen -= sizeof(unsigned long) * 4;
                    }
                }
                else if (op == BITOP_OR)
                {
                    while (minlen >= sizeof(unsigned long) * 4)
                    {
                        for (i = 1; i < numkeys; i++)
                        {
                            lres[0] |= lp[i][0];
                            lres[1] |= lp[i][1];
                            lres[2] |= lp[i][2];
                            lres[3] |= lp[i][3];
                            lp[i] += 4;
                        }
                        lres += 4;
                        j += sizeof(unsigned long) * 4;
                        minlen -= sizeof(unsigned long) * 4;
                    }
                }
                else if (op == BITOP_XOR)
                {
                    while (minlen >= sizeof(unsigned long) * 4)
                    {
                        for (i = 1; i < numkeys; i++)
                        {
                            lres[0] ^= lp[i][0];
                            lres[1] ^= lp[i][1];
                            lres[2] ^= lp[i][2];
                            lres[3] ^= lp[i][3];
                            lp[i] += 4;
                        }
                        lres += 4;
                        j += sizeof(unsigned long) * 4;
                        minlen -= sizeof(unsigned long) * 4;
                    }
                }
                else if (op == BITOP_NOT)
                {
                    while (minlen >= sizeof(unsigned long) * 4)
                    {
                        lres[0] = ~lres[0];
                        lres[1] = ~lres[1];
                        lres[2] = ~lres[2];
                        lres[3] = ~lres[3];
                        lres += 4;
                        j += sizeof(unsigned long) * 4;
                        minlen -= sizeof(unsigned long) * 4;
                    }
                }
            }

            /* j is set to the next byte to process by the previous loop. */
            for (; j < maxlen; j++)
            {
                output = (len[0] <= j) ? 0 : src[0][j];
                if (op == BITOP_NOT)
                    output = ~output;
                for (i = 1; i < numkeys; i++)
                {
                    byte = (len[i] <= j) ? 0 : src[i][j];
                    switch (op)
                    {
                        case BITOP_AND:
                            output &= byte;
                            break;
                        case BITOP_OR:
                            output |= byte;
                            break;
                        case BITOP_XOR:
                            output ^= byte;
                            break;
                    }
                }
                res.WritableData()[j] = output;
            }
        }

        /* Store the computed value into the target key */
        if (maxlen)
        {
            GenericInsertValue(kv, dest_key, res, true);
        }
        else
        {
            GenericDel(kv, dest_key);
        }
        return maxlen;
    }
    int MMKVImpl::BitPos(DBID db, const Data& key, uint8_t bit, int start, int end)
    {
        long strlen;
        unsigned char *p;
        char llbuf[32];

        /* Parse the bit argument to understand what we are looking for, set
         * or clear bits. */
        if (bit != 0 && bit != 1)
        {
            return ERR_BIT_OUTRANGE;
        }
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        MMKVTable::iterator found = kv->find(key);
        if (found == kv->end())
        {
            /* If the key does not exist, from our point of view it is an infinite
             * array of 0 bits. If the user is looking for the fist clear bit return 0,
             * If the user is looking for the first set bit, return -1. */
            return bit ? -1 : 0;
        }
        const Object& value_data = found.value();
        if (value_data.type != V_TYPE_STRING)
        {
            return ERR_INVALID_TYPE;
        }
        if (IsExpired(db, key, value_data))
        {
            return 0;
        }

        /* Set the 'p' pointer to the string, that can be just a stack allocated
         * array if our string was integer encoded. */
        if (value_data.IsInteger())
        {
            p = (unsigned char*) llbuf;
            ll2string(llbuf, sizeof(llbuf), value_data.IntegerValue());
        }
        else
        {
            p = (unsigned char*) value_data.RawValue();
        }
        strlen = value_data.len;

        /* Convert negative indexes */
        if (start < 0)
            start = strlen + start;
        if (end < 0)
            end = strlen + end;
        if (start < 0)
            start = 0;
        if (end < 0)
            end = 0;
        if (end >= strlen)
            end = strlen - 1;

        /* For empty ranges (start > end) we return -1 as an empty range does
         * not contain a 0 nor a 1. */
        if (start > end)
        {
            return -1;
        }
        else
        {
            long bytes = end - start + 1;
            long pos = redisBitpos(p + start, bytes, bit);

            /* If we are looking for clear bits, and the user specified an exact
             * range with start-end, we can't consider the right of the range as
             * zero padded (as we do when no explicit end is given).
             *
             * So if redisBitpos() returns the first bit outside the range,
             * we return -1 to the caller, to mean, in the specified range there
             * is not a single "0" bit. */
            if (bit == 0 && pos == bytes * 8)
            {
                return -1;
            }
            if (pos != -1)
                pos += start * 8; /* Adjust for the bytes we skipped. */
            return pos;
        }
    }
    int MMKVImpl::GetBit(DBID db, const Data& key, int offset)
    {
        /* Limit offset to 512MB in bytes */
        if ((offset < 0) || ((unsigned long long) offset >> 3) >= (512 * 1024 * 1024))
        {
            return ERR_OFFSET_OUTRANGE;
        }

        size_t bitoffset = offset;
        size_t byte, bit;
        size_t bitval = 0;
        MMKVTable* kv = GetMMKVTable(db, false);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        MMKVTable::iterator found = kv->find(key);
        if (found == kv->end())
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        const Object& value_data = found.value();
        if (value_data.type != V_TYPE_STRING)
        {
            return ERR_INVALID_TYPE;
        }
        if (IsExpired(db, key,value_data))
        {
            return 0;
        }

        byte = bitoffset >> 3;
        bit = 7 - (bitoffset & 0x7);
        if (!value_data.IsInteger())
        {
            if (byte < value_data.len)
            {
                bitval = ((uint8_t*) value_data.RawValue())[byte] & (1 << bit);
            }
        }
        else
        {
            char llbuf[32];
            if (byte < (size_t) ll2string(llbuf, sizeof(llbuf), value_data.IntegerValue()))
            {
                bitval = llbuf[byte] & (1 << bit);
            }
        }
        return bitval;
    }
    int MMKVImpl::SetBit(DBID db, const Data& key, int offset, uint8_t on)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        /* Limit offset to 512MB in bytes */
        if ((offset < 0) || ((unsigned long long) offset >> 3) >= (512 * 1024 * 1024))
        {
            return ERR_OFFSET_OUTRANGE;
        }
        MMKVTable* kv = GetMMKVTable(db, true);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        if (on & ~1)
        {
            return ERR_BIT_OUTRANGE;
        }
        size_t bitoffset = offset;
        int byte, bit;
        int byteval, bitval;
        byte = bitoffset >> 3;

        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        std::pair<MMKVTable::iterator, bool> ret = kv->insert(MMKVTable::value_type(key, Object()));
        const Object& kk = ret.first.key();
        Object& value_data = const_cast<Object&>(ret.first.value());
        if (!ret.second)
        {
            if (value_data.type != V_TYPE_STRING)
            {
                return ERR_INVALID_TYPE;
            }
            ClearTTL(db, kk, value_data);
            if (value_data.IsInteger() || value_data.len < (size_t)(byte + 1))
            {
                m_segment.ObjectMakeRoom(value_data, byte + 1, false);
            }
        }
        else
        {
            m_segment.AssignObjectValue(const_cast<Object&>(kk), key, true);
            value_data.type = V_TYPE_STRING;
            m_segment.ObjectMakeRoom(value_data, byte + 1, false);
            memset(const_cast<char*>(value_data.RawValue()), 0, value_data.len);
        }
        /* Get current values */
        byteval = ((uint8_t*) value_data.RawValue())[byte];
        bit = 7 - (bitoffset & 0x7);
        bitval = byteval & (1 << bit);

        /* Update byte with new bit value and return original value */
        byteval &= ~(1 << bit);
        byteval |= ((on & 0x1) << bit);
        ((uint8_t*) value_data.RawValue())[byte] = byteval;
        return bitval;
    }
}

