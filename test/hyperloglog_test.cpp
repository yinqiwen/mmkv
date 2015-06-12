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

TEST(All, Hyperloglog)
{
    g_test_kv->Del(0, "key1");
    g_test_kv->Del(0, "hll1");
    g_test_kv->Del(0, "hll2");
    int add_count = 7;
    char s[add_count][2];
    mmkv::DataArray elements;
    for (int i = 0; i < 7; i++)
    {
        s[i][0] = 'a' + i;
        s[i][1] = 0;
        elements.push_back(s[i]);
    }

    CHECK_EQ(int, g_test_kv->PFAdd(0, "key1", elements), 1, "");
    CHECK_EQ(int, g_test_kv->PFCount(0, "key1"), 7, "");
    elements.clear();
    elements.push_back("foo");
    elements.push_back("bar");
    elements.push_back("zap");
    elements.push_back("a");
    CHECK_EQ(int, g_test_kv->PFAdd(0, "hll1", elements), 1, "");
    elements.clear();
    elements.push_back("a");
    elements.push_back("b");
    elements.push_back("c");
    elements.push_back("foo");
    CHECK_EQ(int, g_test_kv->PFAdd(0, "hll2", elements), 1, "");

    mmkv::DataArray keys;
    keys.push_back("hll1");
    keys.push_back("hll2");
    CHECK_EQ(int, g_test_kv->PFMerge(0, "hll3", keys), 0, "");
    CHECK_EQ(int, g_test_kv->PFCount(0, "hll3"), 6, "");
}

