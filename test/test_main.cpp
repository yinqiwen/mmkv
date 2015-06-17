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
#include <stdio.h>
#include "string_test.cpp"
#include "list_test.cpp"
#include "hash_test.cpp"
#include "set_test.cpp"
#include "hyperloglog_test.cpp"
#include "zset_test.cpp"
#include "pod_test.cpp"
#include "performance_test.cpp"

mmkv::MMKV* g_test_kv = NULL;

using namespace mmkv;
static int init_mmkv()
{
    OpenOptions open_options;
    open_options.create_if_notexist = true;
    open_options.create_options.size = 1024 * 1024 * 1024;
    return MMKV::Open(open_options, g_test_kv);
}

int main()
{
    int err = 0;
    if((err = init_mmkv()) != 0)
    {
        printf("Failed to init mmkv for err code:%d!\n", err);
        return 0;
    }
    mmkv::RunAllTests();
    printf("##KeySpaceUsed:%llu, ValueSpaceUsed:%llu\n", g_test_kv->KeySpaceUsed(), g_test_kv->ValueSpaceUsed());
    g_test_kv->SyncData();
    return 0;
}
