/*
 * string_test.cpp
 *
 *  Created on: 2015Äê5ÔÂ21ÈÕ
 *      Author: wangqiying
 */

#include "ut.hpp"

TEST(PushPop, List)
{
    g_test_kv->Del(0, "mylist");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "hello"), 1, "");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "world"), 2, "");
    CHECK_EQ(int, g_test_kv->LPush(0, "mylist", "mmkv"), 3, "");
    CHECK_EQ(int, g_test_kv->LPush(0, "mylist", "yinqiwen"), 4, "");
    std::string v;
    CHECK_EQ(int, g_test_kv->LPop(0, "mylist", v), 0, "");
    CHECK_EQ(std::string, v, "yinqiwen", "");
    CHECK_EQ(int, g_test_kv->RPop(0, "mylist", v), 0, "");
    CHECK_EQ(std::string, v, "world", "");
}

TEST(LRange, List)
{
    g_test_kv->Del(0, "mylist");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "one"), 1, "");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "two"), 2, "");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "three"), 3, "");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->LRange(0, "mylist", 0, 0, vs), 0, "");
    CHECK_EQ(int, vs.size(), 1, "");
    CHECK_EQ(std::string, vs[0], "one", "");
    CHECK_EQ(int, g_test_kv->LRange(0, "mylist", -3, 3, vs), 0, "");
    CHECK_EQ(int, vs.size(), 3, "");
    CHECK_EQ(std::string, vs[0], "one", "");
    CHECK_EQ(std::string, vs[1], "two", "");
    CHECK_EQ(std::string, vs[2], "three", "");

    CHECK_EQ(int, g_test_kv->LRange(0, "mylist", -100, 100, vs), 0, "");
    CHECK_EQ(int, vs.size(), 3, "");
    CHECK_EQ(std::string, vs[0], "one", "");
    CHECK_EQ(std::string, vs[1], "two", "");
    CHECK_EQ(std::string, vs[2], "three", "");
    CHECK_EQ(int, g_test_kv->LRange(0, "mylist", 5, 10, vs), 0, "");
}

TEST(LIndexLen, List)
{
    g_test_kv->Del(0, "mylist");
    g_test_kv->LPush(0, "mylist", "world");
    g_test_kv->LPush(0, "mylist", "hello");
    std::string v;
    CHECK_EQ(int, g_test_kv->LIndex(0, "mylist", 0, v), 0, "");
    CHECK_EQ(std::string, v, "hello", "");
    CHECK_EQ(int, g_test_kv->LIndex(0, "mylist", -1, v), 0, "");
    CHECK_EQ(std::string, v, "world", "");
    CHECK_EQ(int, g_test_kv->LIndex(0, "mylist", 3, v), mmkv::ERR_OFFSET_OUTRANGE, "");
    CHECK_EQ(int, g_test_kv->LLen(0, "mylist"), 2, "");
}

TEST(LInsert, List)
{
    g_test_kv->Del(0, "mylist");
    g_test_kv->RPush(0, "mylist", "hello");
    g_test_kv->RPush(0, "mylist", "world");
    CHECK_EQ(int, g_test_kv->LInsert(0, "mylist", true, "world", "there"), 3, "");
    CHECK_EQ(int, g_test_kv->LInsert(0, "mylist", true, "sworld", "there"), -1, "");
}

TEST(LRem, List)
{
    g_test_kv->Del(0, "mylist");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "hello"), 1, "");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "hello"), 2, "");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "foo"), 3, "");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "hello"), 4, "");

    CHECK_EQ(int, g_test_kv->LRem(0, "mylist", -2, "hello"), 2, "");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->LRange(0, "mylist", 0, -1, vs), 0, "");
    CHECK_EQ(int, vs.size(), 2, "");
    CHECK_EQ(std::string, vs[0], "hello", "");
    CHECK_EQ(std::string, vs[1], "foo", "");

    g_test_kv->Del(0, "mylist");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "hello"), 1, "");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "hello"), 2, "");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "foo"), 3, "");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "hello"), 4, "");

    CHECK_EQ(int, g_test_kv->LRem(0, "mylist", 2, "hello"), 2, "");
    CHECK_EQ(int, g_test_kv->LRange(0, "mylist", 0, -1, vs), 0, "");
    CHECK_EQ(int, vs.size(), 2, "");
    CHECK_EQ(std::string, vs[0], "foo", "");
    CHECK_EQ(std::string, vs[1], "hello", "");
}

TEST(LSet, List)
{
    g_test_kv->Del(0, "mylist");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "one"), 1, "");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "two"), 2, "");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "three"), 3, "");
    CHECK_EQ(int, g_test_kv->LSet(0, "mylist", 0, "four"), 0, "");
    CHECK_EQ(int, g_test_kv->LSet(0, "mylist", -2, "five"), 0, "");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->LRange(0, "mylist", 0, -1, vs), 0, "");
    CHECK_EQ(int, vs.size(), 3, "");
    CHECK_EQ(std::string, vs[0], "four", "");
    CHECK_EQ(std::string, vs[1], "five", "");
    CHECK_EQ(std::string, vs[2], "three", "");
}

TEST(RPOPLPUSH, List)
{
    g_test_kv->Del(0, "mylist");
    g_test_kv->Del(0, "myotherlist");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "one"), 1, "");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "two"), 2, "");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "three"), 3, "");
    std::string v;
    CHECK_EQ(int, g_test_kv->RPopLPush(0, "mylist", "myotherlist", v), 0, "");
    CHECK_EQ(std::string, v, "three", "");
    CHECK_EQ(int, g_test_kv->LLen(0, "myotherlist"), 1, "");
}

TEST(LTrim, List)
{
    g_test_kv->Del(0, "mylist");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "one"), 1, "");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "two"), 2, "");
    CHECK_EQ(int, g_test_kv->RPush(0, "mylist", "three"), 3, "");
    CHECK_EQ(int, g_test_kv->LTrim(0, "mylist", 1, -1), 0, "");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->LRange(0, "mylist", 0, -1, vs), 0, "");
    CHECK_EQ(int, vs.size(), 2, "");
    CHECK_EQ(std::string, vs[0], "two", "");
    CHECK_EQ(std::string, vs[1], "three", "");
}
