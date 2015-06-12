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

TEST(Append, String)
{
    g_test_kv->Del(0, "testkey");
    std::string test_value = "testvalue";
    CHECK_EQ(int,g_test_kv->Set(0, "testkey", test_value) , 0, "set testkey failed");
    std::string append_value = "append";
    CHECK_EQ(int,g_test_kv->Append(0, "testkey", append_value) , 0, "append testkey failed");
    std::string v;
    CHECK_EQ(int,g_test_kv->Get(0, "testkey", v) , 0, "get testkey failed");
    CHECK_EQ(std::string,v , test_value + append_value, "get testkey failed");
    g_test_kv->Set(0, "testkey", "100");
    CHECK_EQ(int,g_test_kv->Append(0, "testkey", append_value) , 0, "append testkey failed");
    CHECK_EQ(int,g_test_kv->Get(0, "testkey", v) , 0, "get testkey failed");
    CHECK_EQ(std::string,v , "100" + append_value, "get testkey failed");
    CHECK_EQ(int,g_test_kv->GetSet(0, "testkey", "new_value", v) , 0, "getset testkey failed");
    CHECK_EQ(std::string,v , "100" + append_value, "get testkey failed");
    CHECK_EQ(int,g_test_kv->Get(0, "testkey", v) , 0, "get testkey failed");
    CHECK_EQ(std::string,v , "new_value", "get testkey failed");
}

TEST(GetSet, String)
{
    std::string test_value = "testvalue";
    CHECK_EQ(int,g_test_kv->Set(0, "testkey", test_value) , 0, "set testkey failed");
    std::string v;
    CHECK_EQ(int,g_test_kv->Get(0, "testkey", v) , 0, "get testkey failed");
    CHECK_EQ(std::string,v , test_value, "get testkey failed");
}

TEST(Incr, String)
{
    CHECK_EQ(int,g_test_kv->Set(0, "testkey", "100") , 0, "set testkey failed");
    int64_t v;
    CHECK_EQ(int,g_test_kv->IncrBy(0, "testkey", 101, v) , 0, "incr testkey failed");
    CHECK_EQ(int,v , 201, "incr testkey failed");
    CHECK_EQ(int,g_test_kv->DecrBy(0, "testkey", 1, v) , 0, "incr testkey failed");
    CHECK_EQ(int,v , 200, "incr testkey failed");
    double dv;
    CHECK_EQ(int,g_test_kv->IncrByFloat(0, "testkey", 1.1, dv) , 0, "incr testkey failed");
    CHECK_EQ(double,dv , 201.1, "incr testkey failed");
}

TEST(Strlen, String)
{
    CHECK_EQ(int,g_test_kv->Set(0, "testkey", "100") , 0, "set testkey failed");
    CHECK_EQ(int,g_test_kv->Strlen(0, "testkey") , 3, "Strlen testkey failed");
    CHECK_EQ(int,g_test_kv->Set(0, "testkey", "hello,world") , 0, "set testkey failed");
    CHECK_EQ(int,g_test_kv->Strlen(0, "testkey") , 11, "Strlen testkey failed");
}

TEST(MGetSet, String)
{
    mmkv::DataPairArray pairs;
    mmkv::DataArray keys;
    int batch = 10;
    char keybuf[batch][100], valuebuf[batch][100];
    for (int i = 0; i < batch; i++)
    {
        sprintf(keybuf[i], "k%010d", i);
        sprintf(valuebuf[i], "v%010d", i);
        mmkv::DataPair kv;
        kv.first = keybuf[i];
        kv.second = valuebuf[i];
        pairs.push_back(kv);
        keys.push_back(keybuf[i]);
    }
    CHECK_EQ(int,g_test_kv->MSet(0, pairs) , 0, "mset  failed");
    mmkv::StringArray vals;
    CHECK_EQ(int,g_test_kv->MGet(0, keys, vals) , 0, "mget failed");
    CHECK_EQ(int,vals.size() , batch, "mget failed failed");
    for (int i = 0; i < batch; i++)
    {
        CHECK_EQ(std::string,vals[i] , valuebuf[i], "mget failed");
    }
    CHECK_EQ(int,g_test_kv->Del(0, keys) , batch, "mdel failed");
}

TEST(GetSetRange, String)
{
    CHECK_EQ(int,g_test_kv->Set(0, "mykey", "This is a string"), 0, "set testkey failed");
    std::string v;
    CHECK_EQ(int,g_test_kv->GetRange(0, "mykey", 0, 3, v) , 0, "getrange testkey failed");
    CHECK_EQ(std::string,v , "This", "getrange testkey failed");
    CHECK_EQ(int,g_test_kv->GetRange(0, "mykey", -3, -1, v) , 0, "getrange testkey failed");
    CHECK_EQ(std::string,v , "ing", "getrange testkey failed");
    CHECK_EQ(int,g_test_kv->GetRange(0, "mykey", 0, -1, v) , 0, "getrange testkey failed");
    CHECK_EQ(std::string,v , "This is a string", "getrange testkey failed");
    CHECK_EQ(int,g_test_kv->GetRange(0, "mykey", 10, 100, v) , 0, "getrange testkey failed");
    CHECK_EQ(std::string,v , "string", "getrange testkey failed");

    CHECK_EQ(int,g_test_kv->Set(0, "mykey", "Hello World") , 0, "getrange testkey failed");
    CHECK_EQ(int,g_test_kv->SetRange(0, "mykey", 6, "Redis") , 0, "getrange testkey failed");

    CHECK_EQ(int,g_test_kv->Get(0, "mykey", v) , 0, "get testkey failed");
    CHECK_EQ(std::string,v , "Hello Redis", "getrange testkey failed");
    CHECK_EQ(int,g_test_kv->Del(0, "mykey") , 1, "mdel failed");
    CHECK_EQ(int,g_test_kv->SetRange(0, "mykey", 6, "Redis") , 0, "getrange testkey failed");
    CHECK_EQ(int,g_test_kv->Get(0, "mykey", v) , 0, "get testkey failed");
    std::string cmpv;
    cmpv.resize(6);
    cmpv.append("Redis");
    CHECK_EQ(std::string, cmpv , v, "getrange faild");
}

TEST(BitOPs, String)
{
    g_test_kv->Del(0, "key1");
    g_test_kv->Del(0, "key2");
    g_test_kv->Del(0, "dest");
    g_test_kv->Del(0, "mykey");
    CHECK_EQ(int,g_test_kv->Set(0, "key1", "foobar"), 0, "set testkey failed");
    CHECK_EQ(int,g_test_kv->Set(0, "key2", "abcdef"), 0, "set testkey failed");

    mmkv::DataArray keys;
    keys.push_back("key1");
    keys.push_back("key2");
    CHECK_EQ(int,g_test_kv->BitOP(0, "and", "dest", keys) , 6, "");

    CHECK_EQ(int,g_test_kv->Set(0, "mykey", "foobar"), 0, "");
    CHECK_EQ(int,g_test_kv->BitCount(0, "mykey"), 26, "");
    CHECK_EQ(int,g_test_kv->BitCount(0, "mykey", 0, 0), 4, "");
    CHECK_EQ(int,g_test_kv->BitCount(0, "mykey", 1, 1), 6, "");

    CHECK_EQ(int,g_test_kv->Del(0, "mykey") , 1, "");
    CHECK_EQ(int,g_test_kv->SetBit(0, "mykey", 7, 1), 0, "");
    CHECK_EQ(int,g_test_kv->GetBit(0, "mykey", 0), 0, "");
    CHECK_EQ(int,g_test_kv->GetBit(0, "mykey", 7), 1, "");
    CHECK_EQ(int,g_test_kv->GetBit(0, "mykey", 100), 0, "");
}

