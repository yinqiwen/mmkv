/*
 * test_main.cpp
 *
 *  Created on: 2015Äê5ÔÂ21ÈÕ
 *      Author: wangqiying
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
    return 0;
}
