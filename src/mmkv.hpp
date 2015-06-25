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

#ifndef MMKV_HPP_
#define MMKV_HPP_

#include <stdio.h>
#include <stdint.h>
#include <strings.h>
#include <string.h>
#include <vector>
#include <utility>
#include "options.hpp"
#include "mmkv_logger.hpp"
#include "allocator.hpp"

namespace mmkv
{
    struct Data
    {
            const char* data;
            size_t len;
            Data() :
                    data(""), len(0)
            {

            }
            Data(const void* v, size_t length) :
                    data((const char*) v), len(length)
            {
            }
            Data(const char* ss) :
                    data(ss), len(strlen(ss))
            {
            }
            Data(const std::string& ss) :
                    data(ss.data()), len(ss.size())
            {
            }
            Data(int64_t v) :
                    data(NULL), len((size_t) v)
            {
            }
            size_t Len() const
            {
                return len;
            }
            const char* Value() const
            {
                return data;
            }
            bool operator ==(const Data& other) const
            {
                if (len != other.len)
                {
                    return false;
                }
                if (data == NULL && other.data == NULL)
                {
                    return true;
                }
                if (data != NULL && other.data != NULL)
                {
                    return strncmp(data, other.data, len) == 0;
                }
                return false;
            }
    };

    struct ScoreData
    {
            double score;
            Data value;
            ScoreData() :
                    score(0)
            {
            }
    };
    typedef uint32_t DBID;
    typedef std::vector<Data> DataArray;
    typedef std::pair<Data, Data> DataPair;
    typedef std::vector<DataPair> DataPairArray;
    typedef std::vector<std::string> StringArray;
    typedef std::vector<ScoreData> ScoreDataArray;
    typedef std::vector<uint32_t> WeightArray;

    typedef void PODestructor(void* p);
    template<typename T>
    struct PODDestructorTemplate
    {
            static void Destruct(void* p)
            {
                T* pp = (T*) p;
                pp->~T();
            }
    };

    class MMKV;
    template<typename T>
    class LockedPOD
    {
        private:
            T* value;
            MMKV* kv;
            bool readonly;
            friend class MMKV;
        public:
            LockedPOD() :
                    value(NULL), kv(NULL), readonly(false)
            {
            }
            LockedPOD(const LockedPOD& other) :
                    value(other.value), kv(other.kv), readonly(other.readonly)
            {
                other.kv = NULL;
                other.value = NULL;
            }
            LockedPOD& operator=(const LockedPOD&other)
            {
                value = other.value;
                kv = other.kv;
                readonly = other.readonly;
                other.kv = NULL;
                other.value = NULL;
                return *this;
            }
            T* Get() const
            {
                return value;
            }
            T* operator->() const
            {
                return Get();
            }
            ~LockedPOD();

    };

    template<typename T>
    struct PODProxy
    {
            bool invoke_constructor;
            T* ptr;
            PODProxy() :
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
                    return ::new (ptr) T;
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

    class MMKV
    {
        private:
            virtual void Lock(bool readonly) = 0;

            virtual bool IsLocked(bool readonly) = 0;
            virtual void* Malloc(size_t size) = 0;
            virtual void Free(void* p) = 0;
            virtual int GetPOD(DBID db, const Data& key, bool created_if_notexist, uint32_t expected_type, Data& v) = 0;
            virtual int RegisterPODDestructor(uint32_t expected_type, PODestructor* des) = 0;
            virtual Allocator<char> GetCharAllocator() = 0;
        public:

            static int Open(const OpenOptions& open_options, MMKV*& kv);
            /*
             * keys' operations
             */
            virtual int Del(DBID db, const DataArray& keys) = 0;
            inline int Del(DBID db, const Data& key)
            {
                return Del(db, DataArray(1, key));
            }
            virtual int Exists(DBID db, const Data& key)= 0;
            virtual int Expire(DBID db, const Data& key, uint32_t secs)= 0;
            virtual int Keys(DBID db, const std::string& pattern, StringArray& keys)= 0;
            virtual int Move(DBID db, const Data& key, DBID destdb)= 0;
            virtual int PExpire(DBID db, const Data& key, uint64_t milliseconds)= 0;
            virtual int PExpireat(DBID db, const Data& key, uint64_t milliseconds_timestamp)= 0;
            virtual int64_t PTTL(DBID db, const Data& key)= 0;
            virtual int RandomKey(DBID db, std::string& key)= 0;
            virtual int Rename(DBID db, const Data& key, const Data& new_key)= 0;
            virtual int RenameNX(DBID db, const Data& key, const Data& new_key)= 0;
            virtual int Scan(DBID db, const Data& key, int cursor, const std::string& pattern = "",
                    int32_t limit_count = 10000)= 0;
            virtual int64_t TTL(DBID db, const Data& key)= 0;
            virtual int Type(DBID db, const Data& key)= 0;
            virtual int Sort(DBID db, const Data& key, const std::string& by, int limit_offset, int limit_count,
                    const StringArray& get_patterns, bool desc, bool alpha_sort, const Data& destination_key)= 0;

            /*
             * string's operations
             */
            virtual int Append(DBID db, const Data& key, const Data& value)= 0;
            virtual int BitCount(DBID db, const Data& key, int start = 0, int end = -1)= 0;
            virtual int BitOP(DBID db, const std::string& op, const Data& dest_key, const DataArray& keys)= 0;
            virtual int BitPos(DBID db, const Data& key, uint8_t bit, int start = 0, int end = -1)= 0;
            virtual int Decr(DBID db, const Data& key, int64_t& new_val)= 0;
            virtual int DecrBy(DBID db, const Data& key, int64_t decrement, int64_t& new_val)= 0;
            virtual int Get(DBID db, const Data& key, std::string& value) = 0;
            virtual int GetBit(DBID db, const Data& key, int offset)= 0;
            virtual int GetRange(DBID db, const Data& key, int start, int end, std::string& value)= 0;
            virtual int GetSet(DBID db, const Data& key, const Data& value, std::string& old_value)= 0;
            virtual int Incr(DBID db, const Data& key, int64_t& new_val)= 0;
            virtual int IncrBy(DBID db, const Data& key, int64_t increment, int64_t& new_val)= 0;
            virtual int IncrByFloat(DBID db, const Data& key, double increment, double& new_val)= 0;
            virtual int MGet(DBID db, const DataArray& keys, StringArray& vals)= 0;
            virtual int MSet(DBID db, const DataPairArray& key_vals)= 0;
            virtual int MSetNX(DBID db, const DataPairArray& key_vals)= 0;
            virtual int PSetNX(DBID db, const Data& key, int64_t milliseconds, const Data& value)= 0;
            /*
             * EX seconds -- Set the specified expire time, in seconds.
             * PX milliseconds -- Set the specified expire time, in milliseconds.
             * NX(nx_xx = 0) -- Only set the key if it does not already exist.
             * XX(nx_xx = 1) -- Only set the key if it already exist.
             */
            virtual int Set(DBID db, const Data& key, const Data& value, int32_t ex = -1, int64_t px = -1,
                    int8_t nx_xx = -1)= 0;
            virtual int SetBit(DBID db, const Data& key, int offset, uint8_t value)= 0;
            virtual int SetEX(DBID db, const Data& key, int32_t secs, const Data& value)= 0;
            virtual int SetNX(DBID db, const Data& key, const Data& value)= 0;
            virtual int SetRange(DBID db, const Data& key, int offset, const Data& value)= 0;
            virtual int Strlen(DBID db, const Data& key)= 0;

            /*
             * hash's operations
             */
            virtual int HDel(DBID db, const Data& key, const DataArray& fields)= 0;
            inline int HDel(DBID db, const Data& key, const Data& field)
            {
                return HDel(db, key, DataArray(1, field));
            }
            virtual int HExists(DBID db, const Data& key, const Data& field)= 0;
            virtual int HGet(DBID db, const Data& key, const Data& field, std::string& val)= 0;
            virtual int HGetAll(DBID db, const Data& key, StringArray& vals)= 0;
            virtual int HIncrBy(DBID db, const Data& key, const Data& field, int64_t increment, int64_t& new_val)= 0;
            virtual int HIncrByFloat(DBID db, const Data& key, const Data& field, double increment, double& new_val)= 0;
            virtual int HKeys(DBID db, const Data& key, StringArray& fields)= 0;
            virtual int HLen(DBID db, const Data& key)= 0;
            virtual int HMGet(DBID db, const Data& key, const DataArray& fields, StringArray& vals)= 0;
            virtual int HMSet(DBID db, const Data& key, const DataPairArray& field_vals)= 0;
            virtual int HScan(DBID db, const Data& key, int cursor, const std::string& pattern = "",
                    int32_t limit_count = -1)= 0;
            virtual int HSet(DBID db, const Data& key, const Data& field, const Data& val, bool nx = false)= 0;
            virtual int HStrlen(DBID db, const Data& key, const Data& field)= 0;
            virtual int HVals(DBID db, const Data& key, StringArray& vals)= 0;

            /*
             * hyperloglog's operations
             */
            virtual int PFAdd(DBID db, const Data& key, const DataArray& elements)= 0;
            inline int PFAdd(DBID db, const Data& key, const Data& element)
            {
                return PFAdd(db, key, DataArray(1, element));
            }
            virtual int PFCount(DBID db, const DataArray& keys)= 0;
            inline int PFCount(DBID db, const Data& key)
            {
                return PFCount(db, DataArray(1, key));
            }
            virtual int PFMerge(DBID db, const Data& destkey, const DataArray& sourcekeys)= 0;

            /*
             * list's operations
             */
            virtual int LIndex(DBID db, const Data& key, int index, std::string& val)= 0;
            virtual int LInsert(DBID db, const Data& key, bool before_ot_after, const Data& pivot, const Data& val)= 0;
            virtual int LLen(DBID db, const Data& key)= 0;
            virtual int LPop(DBID db, const Data& key, std::string& val)= 0;
            virtual int LPush(DBID db, const Data& key, const DataArray& vals, bool nx = false)= 0;
            inline int LPush(DBID db, const Data& key, const Data& val, bool nx = false)
            {
                return LPush(db, key, DataArray(1, val), nx);
            }
            virtual int LRange(DBID db, const Data& key, int start, int stop, StringArray& vals)= 0;
            virtual int LRem(DBID db, const Data& key, int count, const Data& val)= 0;
            virtual int LSet(DBID db, const Data& key, int index, const Data& val)= 0;
            virtual int LTrim(DBID db, const Data& key, int start, int stop)= 0;
            virtual int RPop(DBID db, const Data& key, std::string& val)= 0;
            virtual int RPopLPush(DBID db, const Data& source, const Data& destination, std::string& pop_value)= 0;
            virtual int RPush(DBID db, const Data& key, const DataArray& vals, bool nx = false)= 0;
            inline int RPush(DBID db, const Data& key, const Data& val, bool nx = false)
            {
                return RPush(db, key, DataArray(1, val), nx);
            }

            /*
             * set's operations
             */
            virtual int SAdd(DBID db, const Data& key, const DataArray& elements)= 0;
            inline int SAdd(DBID db, const Data& key, const Data& val)
            {
                return SAdd(db, key, DataArray(1, val));
            }
            virtual int SCard(DBID db, const Data& key)= 0;
            virtual int SDiff(DBID db, const DataArray& keys, StringArray& diffs)= 0;
            virtual int SDiffStore(DBID db, const Data& destination, const DataArray& keys)= 0;
            virtual int SInter(DBID db, const DataArray& keys, StringArray& inters)= 0;
            virtual int SInterStore(DBID db, const Data& destination, const DataArray& keys)= 0;
            virtual int SIsMember(DBID db, const Data& key, const Data& member)= 0;
            virtual int SMembers(DBID db, const Data& key, StringArray& members)= 0;
            virtual int SMove(DBID db, const Data& source, const Data& destination, const Data& member)= 0;
            virtual int SPop(DBID db, const Data& key, StringArray& members, int count = 1)= 0;
            virtual int SRandMember(DBID db, const Data& key, StringArray& members, int count = 1)= 0;
            virtual int SRem(DBID db, const Data& key, const DataArray& elements)= 0;
            virtual int SRem(DBID db, const Data& key, const Data& member)
            {
                return SRem(db, key, DataArray(1, member));
            }
            virtual int SScan(DBID db, const Data& key, int cursor, const std::string& pattern = "",
                    int32_t limit_count = -1)= 0;
            virtual int SUnion(DBID db, const DataArray& keys, StringArray& unions)= 0;
            virtual int SUnionStore(DBID db, const Data& destination, const DataArray& keys)= 0;

            /*
             * zset's operations
             */
            virtual int ZAdd(DBID db, const Data& key, const ScoreDataArray& vals, bool nx = false, bool xx = false,
                    bool ch = false, bool incr = false)= 0;
            inline int ZAdd(DBID db, const Data& key, double score, const Data& val, bool nx = false, bool xx = false,
                    bool ch = false, bool incr = false)
            {
                ScoreData sd;
                sd.score = score;
                sd.value = val;
                return ZAdd(db, key, ScoreDataArray(1, sd), nx, xx, ch, incr);
            }
            virtual int ZCard(DBID db, const Data& key)= 0;
            virtual int ZCount(DBID db, const Data& key, const std::string& min, const std::string& max)= 0;
            virtual int ZIncrBy(DBID db, const Data& key, double increment, const Data& member, double& new_score)= 0;
            virtual int ZLexCount(DBID db, const Data& key, const std::string& min, const std::string& max)= 0;
            virtual int ZRange(DBID db, const Data& key, int start, int stop, bool with_scores, StringArray& vals)= 0;
            virtual int ZRangeByLex(DBID db, const Data& key, const std::string& min, const std::string& max,
                    int limit_offset, int limit_count, StringArray& vals)= 0;
            virtual int ZRangeByScore(DBID db, const Data& key, const std::string& min, const std::string& max,
                    bool with_scores, int limit_offset, int limit_count, StringArray& vals)= 0;
            virtual int ZRank(DBID db, const Data& key, const Data& member)= 0;
            virtual int ZRem(DBID db, const Data& key, const DataArray& members)= 0;
            inline int ZRem(DBID db, const Data& key, const Data& val)
            {
                return ZRem(db, key, DataArray(1, val));
            }
            virtual int ZRemRangeByLex(DBID db, const Data& key, const std::string& min, const std::string& max)= 0;
            virtual int ZRemRangeByRank(DBID db, const Data& key, int start, int stop)= 0;
            virtual int ZRemRangeByScore(DBID db, const Data& key, const std::string& min, const std::string& max)= 0;
            virtual int ZRevRange(DBID db, const Data& key, int start, int stop, bool with_scores,
                    StringArray& vals)= 0;
            virtual int ZRevRangeByLex(DBID db, const Data& key, const std::string& max, const std::string& min,
                    int limit_offset, int limit_count, StringArray& vals)= 0;
            virtual int ZRevRangeByScore(DBID db, const Data& key, const std::string& max, const std::string& min,
                    bool with_scores, int limit_offset, int limit_count, StringArray& vals)= 0;
            virtual int ZRevRank(DBID db, const Data& key, const Data& member)= 0;
            virtual int ZScore(DBID db, const Data& key, const Data& member, double& score)= 0;
            virtual int ZScan(DBID db, const Data& key, int cursor, const std::string& pattern = "",
                    int32_t limit_count = -1)= 0;
            virtual int ZInterStore(DBID db, const Data& destination, const DataArray& keys, const WeightArray& weights,
                    const std::string& aggregate)= 0;
            virtual int ZUnionStore(DBID db, const Data& destination, const DataArray& keys, const WeightArray& weights,
                    const std::string& aggregate)= 0;

            /*
             *
             */
            virtual int64_t DBSize(DBID db) = 0;
            virtual int FlushDB(DBID db) = 0;
            virtual int FlushAll() = 0;

            template<typename T>
            PODProxy<T> NewPOD()
            {
                PODProxy<T> proxy;
                if (!IsLocked(false))
                {
                    //forbid malloc without write lock
                    return proxy;
                }
                void* ptr = Malloc(sizeof(T));
                T* p = ::new (ptr) T;
                proxy.ptr = p;
                proxy.invoke_constructor = true;
                return proxy;
            }
            template<typename T>
            void DeletePOD(T* t)
            {
                if (NULL == t)
                {
                    return;
                }
                if (!IsLocked(false))
                {
                    //forbid free without write lock
                    return;
                }
                t->~T();
                Free(t);
            }

            template<typename T>
            int RegisterPODType(uint32_t expected_type)
            {
                return RegisterPODDestructor(expected_type, PODDestructorTemplate<T>::Destruct);
            }

            /*
             * 'T' MUST inherit from BasePOD
             */
            template<typename T>
            PODProxy<T> GetPOD(DBID db, const Data& key, bool readonly, bool created_if_notexist,
                    uint32_t expected_type, LockedPOD<T>& value, int& err)
            {
                err = 0;
                Data v;
                v.len = sizeof(T);
                PODProxy<T> proxy;
                Lock(readonly);
                if (readonly)
                {
                    created_if_notexist = false;
                }else
                {
                    RegisterPODDestructor<T>(expected_type);
                }
                int ret = GetPOD(db, key, created_if_notexist, expected_type, v);
                if (ret < 0)
                {
                    Unlock(readonly);
                    err = ret;
                    return proxy;
                }
                T* p = (T*) v.data;

                value.kv = this;
                value.readonly = readonly;
                value.value = p;

                proxy.ptr = p;
                if (1 == ret)
                {
                    proxy.invoke_constructor = true;
                }
                return proxy;
            }

            virtual size_t KeySpaceUsed() = 0;
            virtual size_t ValueSpaceUsed() = 0;

            template<typename T>
            Allocator<T> GetAllocator()
            {
                return Allocator<T>(GetCharAllocator());
            }

            virtual void Unlock(bool readonly) = 0;

            virtual int RemoveExpiredKeys(uint32_t max_removed = 10000, uint32_t max_time = 100) = 0;

            virtual int SyncData() = 0;

            virtual ~MMKV()
            {
            }
    };
    template<typename T>
    LockedPOD<T>::~LockedPOD()
    {
        if (NULL != kv)
        {
            kv->Unlock(readonly);
        }
    }

}

#endif /* MMKV_HPP_ */
