/*
 * string_test.cpp
 *
 *  Created on: 2015Äê5ÔÂ21ÈÕ
 *      Author: wangqiying
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
    printf("###Cost %lldus to hset %llu times\n", end - start, loop);
    start = mmkv::get_current_micros();
    for (int i = 0; i < loop; i++)
    {
        char key[100], value[100];
        sprintf(key, "kk%010d", i);
        sprintf(value, "v2%010d", i);
        g_test_kv->Set(0, key, value);
    }
    end = mmkv::get_current_micros();
    printf("###Cost %lldus to set %llu times\n", end - start, loop);
    start = mmkv::get_current_micros();
    for (int i = 0; i < loop; i++)
    {
        char key[100];
        sprintf(key, "kk%010d", i);
        std::string v;
        g_test_kv->Get(0, key, v);
    }
    end = mmkv::get_current_micros();
    printf("###Cost %lldus to get %llu times\n", end - start, loop);
    start = mmkv::get_current_micros();
    for (int i = 0; i < loop; i++)
    {
        char key[100];
        sprintf(key, "k%010d", i);
        g_test_kv->Del(0, key);
    }
    end = mmkv::get_current_micros();
    printf("###Cost %lldus to del %llu times\n", end - start, loop);
}

