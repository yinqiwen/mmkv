/*
 * string_test.cpp
 *
 *  Created on: 2015Äê5ÔÂ21ÈÕ
 *      Author: wangqiying
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

