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
#ifndef SPACE_HPP_
#define SPACE_HPP_
#include <boost/interprocess/offset_ptr.hpp>
#include <boost/interprocess/containers/deque.hpp>
#include <boost/interprocess/containers/list.hpp>
#include "cpp-btree/btree_map.h"
#include "cpp-btree/btree_set.h"
#include "khash.hh"
#include "lz4/xxhash.h"
#include "utils.hpp"
#include "mmkv.hpp"
#include <string.h>
#include <new>
#include <string>
#include <vector>
#include <utility>
#include <alloca.h>

#define ABORT(msg) \
    do {                                \
            (void)fprintf(stderr,  \
                "%s:%d: Failed %s ",     \
                __FILE__,__LINE__,msg);      \
            abort();                    \
    } while (0)

#define OBJ_ENCODING_RAW 0
#define OBJ_ENCODING_PTR 1
#define OBJ_ENCODING_OFFSET_PTR 2
#define OBJ_ENCODING_INT 3

namespace mmkv
{
    template<typename T>
    struct ConstructorProxy
    {
            bool invoke_constructor;
            T* ptr;
            ConstructorProxy() :
                    invoke_constructor(false), ptr(NULL)
            {
            }
            T* operator()()
            {
                if (NULL == ptr)
                {
                    return NULL;
                }
                if (invoke_constructor)
                {
                    ABORT("Not support.");
                }
                else
                {
                    return ptr;
                }
            }
            template<typename R1, typename R2, typename R3, typename R4>
            T* operator()(const R1& a1, const R2& a2, const R3& a3, const R4& a4)
            {
                if (NULL == ptr)
                {
                    return NULL;
                }
                if (invoke_constructor)
                {
                    return ::new (ptr) T(a1, a2, a3, a4);
                }
                else
                {
                    return ptr;
                }
            }
            template<typename R1, typename R2, typename R3>
            T* operator()(const R1& a1, const R2& a2, const R3& a3)
            {
                if (NULL == ptr)
                {
                    return NULL;
                }
                if (invoke_constructor)
                {
                    return ::new (ptr) T(a1, a2, a3);
                }
                else
                {
                    return ptr;
                }
            }
            template<typename R1, typename R2>
            T* operator()(const R1& a1, const R2& a2)
            {
                if (NULL == ptr)
                {
                    return NULL;
                }
                if (invoke_constructor)
                {
                    return ::new (ptr) T(a1, a2);
                }
                else
                {
                    return ptr;
                }
            }
            template<typename R1>
            T* operator()(const R1& a1)
            {
                if (NULL == ptr)
                {
                    return NULL;
                }
                if (invoke_constructor)
                {
                    return ::new (ptr) T(a1);
                }
                else
                {
                    return ptr;
                }
            }
    };

    enum ErrCode
    {
        ERR_ENTRY_NOT_EXIST = -1000,
        ERR_DB_NOT_EXIST = -1001,
        ERR_PERMISSION_DENIED = -1002,
        ERR_INVALID_TYPE = -1003,
        ERR_ENTRY_EXISTED = -1004,
        ERR_NOT_INTEGER = -1005,
        ERR_NOT_NUMBER = -1006,
        ERR_INVALID_NUMBER = -1007,
        ERR_OFFSET_OUTRANGE = -1008,
        ERR_BIT_OUTRANGE = -1009,
        ERR_SYNTAX_ERROR = 1010,
        ERR_ARGS_EXCEED_LIMIT = 1011,
        ERR_NOT_HYPERLOGLOG_STR = -1012,
        ERR_CORRUPTED_HLL_VALUE = -1013,
        ERR_NOT_IMPLEMENTED = -1014,
        ERR_INVALID_MIN_MAX = -1015,
        ERR_POD_SIZE_MISMATCH = -1016,
        ERR_SIZE_MISMATCH = -1017,
        ERR_INVALID_ARGS = -1018,
        ERR_NO_DESTRUCTOR = -1019,
        ERR_INVALID_POD_TYPE = -1020,
        ERR_DUPLICATE_POD_TYPE = -1021,
    };

    enum ObjectType
    {
        V_TYPE_STRING = 0, V_TYPE_HASH = 1, V_TYPE_SET = 2, V_TYPE_ZSET = 3, V_TYPE_LIST = 4, V_TYPE_POD = 5
    };

    struct Object
    {
            char data[8];
            unsigned type :4;
            unsigned encoding :3;
            unsigned hasttl :1;
            unsigned len :24;
            Object() :
                    type(V_TYPE_STRING), encoding(OBJ_ENCODING_RAW), hasttl(0), len(0)
            {
            }
            Object(const Object& other) :
                    type(other.type), encoding(other.encoding), hasttl(other.hasttl), len(other.len)
            {
                if (encoding == OBJ_ENCODING_OFFSET_PTR)
                {
                    *((boost::interprocess::offset_ptr<void>*) data) =
                            *((boost::interprocess::offset_ptr<void>*) other.data);
                }
                else
                {
                    memcpy(data, other.data, 8);
                }
            }
            Object(const Data& v) :
                    type(V_TYPE_STRING), encoding(OBJ_ENCODING_PTR), hasttl(0), len(v.len)
            {
                if (v.data == NULL)
                {
                    SetInteger((int64_t) v.len);
                }
                else
                {
                    *(void**) data = (void*) v.data;
                    memcpy(data, &(v.data), sizeof(v.data));
                }
            }
            Object(const void* v, size_t length) :
                    type(V_TYPE_STRING), encoding(OBJ_ENCODING_PTR), hasttl(0), len(length)
            {
                *(void**) data = (void*) v;
            }
            Object(const char* ss) :
                    type(V_TYPE_STRING), encoding(OBJ_ENCODING_PTR), hasttl(0), len(strlen(ss))
            {
                *(void**) data = (void*) ss;
            }
            Object(const std::string& ss) :
                    type(V_TYPE_STRING), encoding(OBJ_ENCODING_PTR), hasttl(0), len(ss.size())
            {
                *(void**) data = (void*) ss.data();
            }
            Object& operator=(const Object& other)
            {
                type = other.type;
                encoding = other.encoding;
                hasttl = other.hasttl;
                len = other.len;
                if (encoding == OBJ_ENCODING_OFFSET_PTR)
                {
                    *((boost::interprocess::offset_ptr<void>*) data) =
                            *((boost::interprocess::offset_ptr<void>*) other.data);
                }
                else
                {
                    memcpy(data, other.data, 8);
                }
                return *this;
            }
            void SetValue(const void* v)
            {
                *(boost::interprocess::offset_ptr<void>*) data = (void*) v;
                encoding = OBJ_ENCODING_OFFSET_PTR;
            }
            const char* RawValue() const
            {
                switch (encoding)
                {
                    case OBJ_ENCODING_INT:
                    case OBJ_ENCODING_RAW:
                    {
                        return data;
                    }
                    case OBJ_ENCODING_PTR:
                    {
                        return *(char**) data;
                    }
                    case OBJ_ENCODING_OFFSET_PTR:
                    {
                        boost::interprocess::offset_ptr<char>* ptr = (boost::interprocess::offset_ptr<char>*) data;
                        return ptr->get();
                    }
                    default:
                    {
                        return NULL;
                    }
                }
            }
            char* WritableData()
            {
                return const_cast<char*>(RawValue());
            }
            size_t StrLen() const
            {
                return len;
            }
            bool IsInteger() const
            {
                return encoding == OBJ_ENCODING_INT;
            }
            bool IsPtr() const
            {
                return encoding == OBJ_ENCODING_PTR || encoding == OBJ_ENCODING_OFFSET_PTR;
            }
            bool IsOffsetPtr() const
            {
                return encoding == OBJ_ENCODING_OFFSET_PTR;
            }
            int64_t IntegerValue() const
            {
                if (IsInteger())
                {
                    return *(long long*) data;
                }
                else
                {
                    return 0;
                }
            }
            bool SetInteger(int64_t v)
            {
                len = digits10(std::abs(v));
                if (v < 0)
                    len++;
                *(long long*) data = v;
                encoding = OBJ_ENCODING_INT;
                return true;
            }

            bool ToString(std::string& str) const
            {
                if (type != V_TYPE_STRING)
                {
                    return false;
                }
                switch (encoding)
                {
                    case OBJ_ENCODING_INT:
                    {
                        str.resize(len);
                        ll2string(&(str[0]), len, *((long long*) data));
                        return true;
                    }
                    case OBJ_ENCODING_RAW:
                    case OBJ_ENCODING_PTR:
                    case OBJ_ENCODING_OFFSET_PTR:
                    {
                        str.assign(RawValue(), len);
                        return true;
                    }
                    default:
                    {
                        return false;
                    }
                }
            }
            int Compare(const Object& right) const
            {
                if (type != V_TYPE_STRING || right.type != V_TYPE_STRING)
                {
                    abort();
                }
                if (IsInteger() && right.IsInteger())
                {
                    return IntegerValue() - right.IntegerValue();
                }
                size_t min_len = len < right.len ? len : right.len;
                const char* other_raw_data = right.RawValue();
                const char* raw_data = RawValue();
                if (encoding == OBJ_ENCODING_INT)
                {
                    char* data_buf = (char*) alloca(right.len);
                    ll2string(data_buf, len, IntegerValue());
                    raw_data = data_buf;
                }
                if (right.encoding == OBJ_ENCODING_INT)
                {
                    char* data_buf = (char*) alloca(right.len);
                    ll2string(data_buf, len, right.IntegerValue());
                    other_raw_data = data_buf;
                }
                int ret = memcmp(raw_data, other_raw_data, min_len);
                if (ret < 0)
                {
                    return -1;
                }
                else if (ret > 0)
                {
                    return 1;
                }

                if (len == min_len && right.len == min_len)
                {
                    return 0;
                }
                else if (right.len > min_len)
                {
                    return -1;
                }
                else
                {
                    return 1;
                }
            }
            bool operator ==(const Object& s1) const
            {
                return Compare(s1) == 0;
            }
            bool operator <(const Object& s1) const
            {
                return Compare(s1) < 0;
            }
    };

    typedef boost::interprocess::offset_ptr<Object> ObjectOffsetPtr;

    struct ObjectHash
    {
            size_t operator()(const Object& t) const
            {
                if (t.encoding == OBJ_ENCODING_INT)
                {
                    return t.IntegerValue();
                }
                return XXH64(t.RawValue(), t.len, 0);
            }
    };
    struct ObjectEqual
    {
            bool operator()(const Object& s1, const Object & s2) const
            {
                return s1.Compare(s2) == 0;
            }
    };

    struct TTLKey
    {
            DBID db;
            Object key;
            TTLKey() :
                    db(0)
            {
            }
    };

    struct TTLValue
    {
            uint64_t expireat;
            TTLKey key;
            TTLValue() :
                    expireat(0)
            {
            }
            bool operator<(const TTLValue& other) const
            {
                if (expireat < other.expireat)
                {
                    return true;
                }
                if (expireat > other.expireat)
                {
                    return false;
                }
                if (key.db > other.key.db)
                {
                    return false;
                }
                if (key.db < other.key.db)
                {
                    return true;
                }
                return key.key < other.key.key;
            }
    };

    struct TTLKeyHash
    {
            size_t operator()(const TTLKey& t) const
            {
                size_t hash = 0;
                if (t.key.encoding == OBJ_ENCODING_INT)
                {
                    hash = t.key.IntegerValue();
                }
                else
                {
                    hash = XXH64(t.key.RawValue(), t.key.len, 0);
                }
                return t.db ^ hash;
            }
    };
    struct TTLKeyEqual
    {
            bool operator()(const TTLKey& s1, const TTLKey & s2) const
            {
                return s1.db == s2.db && s1.key == s2.key;
            }
    };

    struct ScoreValue
    {
            double score;
            Object value;
            ScoreValue() :
                    score(0)
            {
            }
            int Compare(const ScoreValue& v2) const
            {
                if (score < v2.score)
                {
                    return -1;
                }
                else if (score > v2.score)
                {
                    return 1;
                }
                return value.Compare(v2.value);
            }
            bool operator<(const ScoreValue& right) const
            {
                return Compare(right) < 0;
            }
    };

    struct PODHeader
    {
            uint32_t type;
            char reserverd[4];
            PODHeader() :
                    type(0)
            {
            }
    };



}

#define IS_NOT_EXISTS(err)  (err == mmkv::ERR_ENTRY_NOT_EXIST || err == mmkv::ERR_DB_NOT_EXIST)

#endif /* SPACE_HPP_ */
