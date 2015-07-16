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
#include "utils.hpp"
#include <sys/types.h>
#include <unistd.h>
#include <sys/wait.h>
#include <vector>


TEST(List, Sort)
{
    g_test_kv->Del(0, "mylist");
    g_test_kv->Del(0, "myhash");
    g_test_kv->Del(0, "weight_100");
    g_test_kv->Del(0, "weight_10");
    g_test_kv->Del(0, "weight_9");
    g_test_kv->Del(0, "weight_1000");

    g_test_kv->RPush(0, "mylist", "100");
    g_test_kv->RPush(0, "mylist", "10");
    g_test_kv->RPush(0, "mylist", "9");
    g_test_kv->RPush(0, "mylist", "1000");

    mmkv::StringArray get_patterns;
    mmkv::StringArray results;
    CHECK_EQ(int, g_test_kv->Sort(0, "mylist", "", 0, -1, get_patterns, false, false, "", results), 0, "");
    CHECK_EQ(int, results.size(), 4, "");
    CHECK_EQ(std::string, results[0], "9", "");
    CHECK_EQ(std::string, results[1], "10", "");
    CHECK_EQ(std::string, results[2], "100", "");
    CHECK_EQ(std::string, results[3], "1000", "");

    results.clear();
    CHECK_EQ(int, g_test_kv->Sort(0, "mylist", "", 1, 2, get_patterns, false, false, "", results), 0, "");
    CHECK_EQ(int, results.size(), 2, "");
    CHECK_EQ(std::string, results[0], "10", "");
    CHECK_EQ(std::string, results[1], "100", "");

    g_test_kv->Set(0, "weight_100", "1000");
    g_test_kv->Set(0, "weight_10", "900");
    g_test_kv->Set(0, "weight_9", "800");
    g_test_kv->Set(0, "weight_1000", "700");

    results.clear();
    CHECK_EQ(int, g_test_kv->Sort(0, "mylist", "weight_*", 0, -1, get_patterns, false, false, "", results),
            0, "");
    CHECK_EQ(int, results.size(), 4, "");
    CHECK_EQ(std::string, results[0], "1000", "");
    CHECK_EQ(std::string, results[1], "9", "");
    CHECK_EQ(std::string, results[2], "10", "");
    CHECK_EQ(std::string, results[3], "100", "");

    g_test_kv->HSet(0, "myhash", "field_100", "hash100");
    g_test_kv->HSet(0, "myhash", "field_10", "hash10");
    g_test_kv->HSet(0, "myhash", "field_9", "hash9");
    g_test_kv->HSet(0, "myhash", "field_1000", "hash1000");

    results.clear();
    get_patterns.push_back("#");
    get_patterns.push_back("myhash->field_*");
    CHECK_EQ(int, g_test_kv->Sort(0, "mylist", "weight_*", 0, -1, get_patterns, false, false, "", results),
            0, "");
    CHECK_EQ(int, results.size(), 8, "");
    CHECK_EQ(std::string, results[0], "1000", "");
    CHECK_EQ(std::string, results[1], "hash1000", "");
    CHECK_EQ(std::string, results[2], "9", "");
    CHECK_EQ(std::string, results[3], "hash9", "");
    CHECK_EQ(std::string, results[4], "10", "");
    CHECK_EQ(std::string, results[5], "hash10", "");
    CHECK_EQ(std::string, results[6], "100", "");
    CHECK_EQ(std::string, results[7], "hash100", "");
}

TEST(Set, Sort)
{
    g_test_kv->Del(0, "myset");
    g_test_kv->Del(0, "myhash");
    g_test_kv->Del(0, "weight_100");
    g_test_kv->Del(0, "weight_10");
    g_test_kv->Del(0, "weight_9");
    g_test_kv->Del(0, "weight_1000");

    g_test_kv->SAdd(0, "myset", "100");
    g_test_kv->SAdd(0, "myset", "10");
    g_test_kv->SAdd(0, "myset", "9");
    g_test_kv->SAdd(0, "myset", "1000");

    mmkv::StringArray get_patterns;
    mmkv::StringArray results;
    CHECK_EQ(int, g_test_kv->Sort(0, "myset", "", 0, -1, get_patterns, false, false, "", results), 0, "");
    CHECK_EQ(int, results.size(), 4, "");
    CHECK_EQ(std::string, results[0], "9", "");
    CHECK_EQ(std::string, results[1], "10", "");
    CHECK_EQ(std::string, results[2], "100", "");
    CHECK_EQ(std::string, results[3], "1000", "");

    results.clear();
    CHECK_EQ(int, g_test_kv->Sort(0, "myset", "", 1, 2, get_patterns, false, false, "", results), 0, "");
    CHECK_EQ(int, results.size(), 2, "");
    CHECK_EQ(std::string, results[0], "10", "");
    CHECK_EQ(std::string, results[1], "100", "");

    g_test_kv->Set(0, "weight_100", "1000");
    g_test_kv->Set(0, "weight_10", "900");
    g_test_kv->Set(0, "weight_9", "800");
    g_test_kv->Set(0, "weight_1000", "700");

    results.clear();
    CHECK_EQ(int, g_test_kv->Sort(0, "myset", "weight_*", 0, -1, get_patterns, false, false, "", results),
            0, "");
    CHECK_EQ(int, results.size(), 4, "");
    CHECK_EQ(std::string, results[0], "1000", "");
    CHECK_EQ(std::string, results[1], "9", "");
    CHECK_EQ(std::string, results[2], "10", "");
    CHECK_EQ(std::string, results[3], "100", "");

    g_test_kv->HSet(0, "myhash", "field_100", "hash100");
    g_test_kv->HSet(0, "myhash", "field_10", "hash10");
    g_test_kv->HSet(0, "myhash", "field_9", "hash9");
    g_test_kv->HSet(0, "myhash", "field_1000", "hash1000");

    results.clear();
    get_patterns.push_back("#");
    get_patterns.push_back("myhash->field_*");
    CHECK_EQ(int, g_test_kv->Sort(0, "myset", "weight_*", 0, -1, get_patterns, false, false, "", results),
            0, "");
    CHECK_EQ(int, results.size(), 8, "");
    CHECK_EQ(std::string, results[0], "1000", "");
    CHECK_EQ(std::string, results[1], "hash1000", "");
    CHECK_EQ(std::string, results[2], "9", "");
    CHECK_EQ(std::string, results[3], "hash9", "");
    CHECK_EQ(std::string, results[4], "10", "");
    CHECK_EQ(std::string, results[5], "hash10", "");
    CHECK_EQ(std::string, results[6], "100", "");
    CHECK_EQ(std::string, results[7], "hash100", "");
}

TEST(ZSet, Sort)
{
    g_test_kv->Del(0, "myzset");
    g_test_kv->Del(0, "myhash");
    g_test_kv->Del(0, "weight_v0");
    g_test_kv->Del(0, "weight_v1");
    g_test_kv->Del(0, "weight_v2");
    g_test_kv->Del(0, "weight_v3");

    g_test_kv->ZAdd(0, "myzset", 100, "v0");
    g_test_kv->ZAdd(0, "myzset", 10, "v1");
    g_test_kv->ZAdd(0, "myzset", 9, "v2");
    g_test_kv->ZAdd(0, "myzset", 1000, "v3");

    mmkv::StringArray get_patterns;
    mmkv::StringArray results;
    CHECK_EQ(int, g_test_kv->Sort(0, "myzset", "", 0, -1, get_patterns, false, false, "", results), 0, "");
    CHECK_EQ(int, results.size(), 4, "");
    CHECK_EQ(std::string, results[0], "v2", "");
    CHECK_EQ(std::string, results[1], "v1", "");
    CHECK_EQ(std::string, results[2], "v0", "");
    CHECK_EQ(std::string, results[3], "v3", "");

    results.clear();
    CHECK_EQ(int, g_test_kv->Sort(0, "myzset", "", 1, 2, get_patterns, false, false, "", results), 0, "");
    CHECK_EQ(int, results.size(), 2, "");
    CHECK_EQ(std::string, results[0], "v1", "");
    CHECK_EQ(std::string, results[1], "v0", "");

    g_test_kv->Set(0, "weight_v0", "10");
    g_test_kv->Set(0, "weight_v1", "20");
    g_test_kv->Set(0, "weight_v2", "30");
    g_test_kv->Set(0, "weight_v3", "40");

    results.clear();
    CHECK_EQ(int, g_test_kv->Sort(0, "myzset", "weight_*", 0, -1, get_patterns, true, false, "", results),
            0, "");
    CHECK_EQ(int, results.size(), 4, "");
    CHECK_EQ(std::string, results[0], "v3", "");
    CHECK_EQ(std::string, results[1], "v2", "");
    CHECK_EQ(std::string, results[2], "v1", "");
    CHECK_EQ(std::string, results[3], "v0", "");

    g_test_kv->HSet(0, "myhash", "field_v0", "hashv0");
    g_test_kv->HSet(0, "myhash", "field_v1", "hashv1");
    g_test_kv->HSet(0, "myhash", "field_v2", "hashv2");
    g_test_kv->HSet(0, "myhash", "field_v3", "hashv3");

    results.clear();
    get_patterns.push_back("#");
    get_patterns.push_back("myhash->field_*");
    CHECK_EQ(int, g_test_kv->Sort(0, "myzset", "weight_*", 0, -1, get_patterns, false, false, "", results),
            0, "");
    CHECK_EQ(int, results.size(), 8, "");
    CHECK_EQ(std::string, results[0], "v0", "");
    CHECK_EQ(std::string, results[1], "hashv0", "");
    CHECK_EQ(std::string, results[2], "v1", "");
    CHECK_EQ(std::string, results[3], "hashv1", "");
    CHECK_EQ(std::string, results[4], "v2", "");
    CHECK_EQ(std::string, results[5], "hashv2", "");
    CHECK_EQ(std::string, results[6], "v3", "");
    CHECK_EQ(std::string, results[7], "hashv3", "");
}
