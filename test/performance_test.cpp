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

TEST(GetSet, Performance)
{
    int loop = 1000000;
    int64_t start = mmkv::get_current_micros();
    for (int i = 0; i < loop; i++)
    {
        char key[100], value[100];
        sprintf(key, "k%010d", i);
        sprintf(value, "v%010d", i);
        g_test_kv->HSet(0, key, "f0", value);
    }
    int64_t end = mmkv::get_current_micros();
    printf("###Cost %lldus to hset %d times\n", end - start, loop);
    start = mmkv::get_current_micros();
    for (int i = 0; i < loop; i++)
    {
        char key[100], value[100];
        sprintf(key, "kk%010d", i);
        sprintf(value, "v2%010d", i);
        g_test_kv->Set(0, key, value);
    }
    end = mmkv::get_current_micros();
    printf("###Cost %lldus to set %d times\n", end - start, loop);
    start = mmkv::get_current_micros();
    for (int i = 0; i < loop; i++)
    {
        char key[100];
        sprintf(key, "kk%010d", i);
        std::string v;
        g_test_kv->Get(0, key, v);
    }
    end = mmkv::get_current_micros();
    printf("###Cost %lldus to get %d times\n", end - start, loop);
    start = mmkv::get_current_micros();
    for (int i = 0; i < loop; i++)
    {
        char key[100];
        sprintf(key, "k%010d", i);
        g_test_kv->Del(0, key);
    }
    end = mmkv::get_current_micros();
    printf("###Cost %lldus to del %d times\n", end - start, loop);
}

