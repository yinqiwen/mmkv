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

#include "ut.hpp"

TEST(HSetDelExists, Hash)
{
    g_test_kv->Del(0, "myhash");
    CHECK_EQ(int, g_test_kv->HSet(0, "myhash", "field1", "hello"), 1, "");
    CHECK_EQ(int, g_test_kv->HSet(0, "myhash", "field1", "hello"), 0, "");
    CHECK_EQ(int, g_test_kv->HExists(0, "myhash", "field1"), 1, "");
    CHECK_EQ(int, g_test_kv->HDel(0, "myhash", "field1"), 1, "");
    CHECK_EQ(int, g_test_kv->HDel(0, "myhash", "field1"), 0, "");
    CHECK_EQ(int, g_test_kv->HExists(0, "myhash", "field1"), 0, "");

    CHECK_EQ(int, g_test_kv->HSet(0, "myhash", "field100", "hello", true), 1, "");
    CHECK_EQ(int, g_test_kv->HSet(0, "myhash", "field100", "newval", true), mmkv::ERR_ENTRY_EXISTED, "");
    std::string v;
    CHECK_EQ(int, g_test_kv->HGet(0, "myhash", "field100", v), 0, "");
    CHECK_EQ(std::string, v, "hello", "");
    CHECK_EQ(int, g_test_kv->HStrlen(0, "myhash", "field100"), 5, "");
    CHECK_EQ(int, g_test_kv->HLen(0, "myhash"), 1, "");
}

TEST(HGet, Hash)
{
    g_test_kv->Del(0, "myhash");
    CHECK_EQ(int, g_test_kv->HSet(0, "myhash", "field1", "hello"), 1, "");
    std::string v;
    CHECK_EQ(int, g_test_kv->HGet(0, "myhash", "field1", v), 0, "");
    CHECK_EQ(std::string, v, "hello", "");
    CHECK_EQ(int, g_test_kv->HGet(0, "myhash", "field2", v), mmkv::ERR_ENTRY_NOT_EXIST, "");
    CHECK_EQ(std::string, v, "", "");
}

TEST(HGetAll, Hash)
{
    g_test_kv->Del(0, "myhash");
    CHECK_EQ(int, g_test_kv->HSet(0, "myhash", "field1", "hello"), 1, "");
    CHECK_EQ(int, g_test_kv->HSet(0, "myhash", "field2", "world"), 1, "");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->HGetAll(0, "myhash", vs), 0, "");
    CHECK_EQ(int, vs.size(), 4, "");
    CHECK_EQ(std::string, vs[0], "field2", "");
    CHECK_EQ(std::string, vs[1], "world", "");
    CHECK_EQ(std::string, vs[2], "field1", "");
    CHECK_EQ(std::string, vs[3], "hello", "");
    CHECK_EQ(int, g_test_kv->HKeys(0, "myhash", vs), 0, "");
    CHECK_EQ(int, vs.size(), 2, "");
    CHECK_EQ(std::string, vs[0], "field2", "");
    CHECK_EQ(std::string, vs[1], "field1", "");
    CHECK_EQ(int, g_test_kv->HVals(0, "myhash", vs), 0, "");
    CHECK_EQ(int, vs.size(), 2, "");
    CHECK_EQ(std::string, vs[0], "world", "");
    CHECK_EQ(std::string, vs[1], "hello", "");
}

TEST(HIncr, Hash)
{
    g_test_kv->Del(0, "myhash");
    int64_t v;
    CHECK_EQ(int, g_test_kv->HIncrBy(0, "myhash", "field1", 100, v), 0, "");
    CHECK_EQ(int, v, 100, "");
    CHECK_EQ(int, g_test_kv->HIncrBy(0, "myhash", "field1", 100, v), 0, "");
    CHECK_EQ(int, v, 200, "");
    CHECK_EQ(int, g_test_kv->HIncrBy(0, "myhash", "field1", -100, v), 0, "");
    CHECK_EQ(int, v, 100, "");
    long double dv;
    CHECK_EQ(int, g_test_kv->HIncrByFloat(0, "myhash", "field1", 1.2, dv), 0, "");
    CHECK_EQ(double, dv, 101.2, "");
    g_test_kv->Del(0, "myhash");
    CHECK_EQ(int, g_test_kv->HIncrByFloat(0, "myhash", "field1", 1.2, dv), 0, "");
    CHECK_EQ(double, dv, 1.2, "");
    CHECK_EQ(int, g_test_kv->HSet(0, "myhash", "field1", "hello"), 0, "");
    CHECK_EQ(int, g_test_kv->HIncrByFloat(0, "myhash", "field1", 1.2, dv), mmkv::ERR_NOT_NUMBER, "");
}

TEST(HMGetSet, Hash)
{
    g_test_kv->Del(0, "myhash");
    mmkv::DataPairArray fv;
    fv.resize(2);
    fv[0].first = "field1";
    fv[0].second = "hello";
    fv[1].first = "field2";
    fv[1].second = "world";

    CHECK_EQ(int, g_test_kv->HMSet(0, "myhash", fv), 0, "");
    mmkv::DataArray fs;
    fs.push_back("field1");
    fs.push_back("field2");
    fs.push_back("field4");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->HMGet(0, "myhash", fs, vs), 0, "");
    CHECK_EQ(int, vs.size(), 3, "");
    CHECK_EQ(std::string, vs[0], "hello", "");
    CHECK_EQ(std::string, vs[1], "world", "");
    CHECK_EQ(std::string, vs[2], "", "");
}
