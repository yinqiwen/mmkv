/*
 * string_test.cpp
 *
 *  Created on: 2015Äê5ÔÂ21ÈÕ
 *      Author: wangqiying
 */

#include "ut.hpp"

TEST(SAddCardMembers, Set)
{
    g_test_kv->Del(0, "myset");
    CHECK_EQ(int, g_test_kv->SAdd(0, "myset", "hello"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "myset", "world"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "myset", "hello"), 0, "");
    CHECK_EQ(int, g_test_kv->SCard(0, "myset"), 2, "");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->SMembers(0, "myset", vs), 0, "");
    CHECK_EQ(int, vs.size(), 2, "");
    CHECK_EQ(std::string, vs[0], "hello", "");
    CHECK_EQ(std::string, vs[1], "world", "");
}

TEST(SRem, Set)
{
    g_test_kv->Del(0, "myset");
    CHECK_EQ(int, g_test_kv->SAdd(0, "myset", "hello"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "myset", "world"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "myset", "there"), 1, "");
    CHECK_EQ(int, g_test_kv->SRem(0, "myset", "there"), 1, "");
    CHECK_EQ(int, g_test_kv->SRem(0, "myset", "there"), 0, "");
    CHECK_EQ(int, g_test_kv->SCard(0, "myset"), 2, "");
}

TEST(SMove, Set)
{
    g_test_kv->Del(0, "myset");
    g_test_kv->Del(0, "myotherset");
    CHECK_EQ(int, g_test_kv->SAdd(0, "myset", "hello"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "myset", "world"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "myset", "there"), 1, "");
    CHECK_EQ(int, g_test_kv->SMove(0, "myset", "myotherset", "there"), 1, "");
    CHECK_EQ(int, g_test_kv->SCard(0, "myset"), 2, "");
    CHECK_EQ(int, g_test_kv->SCard(0, "myotherset"), 1, "");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->SMembers(0, "myotherset", vs), 0, "");
    CHECK_EQ(int, vs.size(), 1, "");
    CHECK_EQ(std::string, vs[0], "there", "");
}

TEST(SPop, Set)
{
    g_test_kv->Del(0, "myset");
    CHECK_EQ(int, g_test_kv->SAdd(0, "myset", "hello"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "myset", "world"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "myset", "ttt"), 1, "");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->SPop(0, "myset", vs, 2), 0, "");
    CHECK_EQ(int, vs.size(), 2, "");
    CHECK_EQ(int, g_test_kv->SCard(0, "myset"), 1, "");
}

TEST(SRandMember, Set)
{
    g_test_kv->Del(0, "myset");
    CHECK_EQ(int, g_test_kv->SAdd(0, "myset", "hello"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "myset", "world"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "myset", "ttt"), 1, "");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->SRandMember(0, "myset", vs, 2), 0, "");
    CHECK_EQ(int, vs.size(), 2, "");
    CHECK_EQ(int, g_test_kv->SRandMember(0, "myset", vs, -3), 0, "");
    CHECK_EQ(int, vs.size(), 3, "");
}

TEST(SDiff, Set)
{
    g_test_kv->Del(0, "key1");
    g_test_kv->Del(0, "key2");
    g_test_kv->Del(0, "key3");
    g_test_kv->Del(0, "destset");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key1", "a"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key1", "b"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key1", "c"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key1", "d"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key2", "c"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key3", "a"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key3", "c"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key3", "e"), 1, "");
    mmkv::DataArray keys;
    keys.push_back("key1");
    keys.push_back("key2");
    keys.push_back("key3");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->SDiff(0, keys, vs), 0, "");
    CHECK_EQ(int, vs.size(), 2, "");
    CHECK_EQ(std::string, vs[0], "b", "");
    CHECK_EQ(std::string, vs[1], "d", "");

    CHECK_EQ(int, g_test_kv->SDiffStore(0, "destset", keys), 0, "");
    CHECK_EQ(int, g_test_kv->SCard(0, "destset"), 2, "");
    CHECK_EQ(int, g_test_kv->SMembers(0, "destset", vs), 0, "");
    CHECK_EQ(int, vs.size(), 2, "");
    CHECK_EQ(std::string, vs[0], "b", "");
    CHECK_EQ(std::string, vs[1], "d", "");
}

TEST(SUnion, Set)
{
    g_test_kv->Del(0, "key1");
    g_test_kv->Del(0, "key2");
    g_test_kv->Del(0, "key3");
    g_test_kv->Del(0, "destset");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key1", "a"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key1", "b"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key1", "c"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key1", "d"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key2", "c"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key3", "a"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key3", "c"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key3", "e"), 1, "");
    mmkv::DataArray keys;
    keys.push_back("key1");
    keys.push_back("key2");
    keys.push_back("key3");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->SUnion(0, keys, vs), 0, "");
    CHECK_EQ(int, vs.size(), 5, "");
    CHECK_EQ(std::string, vs[0], "a", "");
    CHECK_EQ(std::string, vs[1], "b", "");
    CHECK_EQ(std::string, vs[2], "c", "");
    CHECK_EQ(std::string, vs[3], "d", "");
    CHECK_EQ(std::string, vs[4], "e", "");

    CHECK_EQ(int, g_test_kv->SUnionStore(0, "destset", keys), 0, "");
    CHECK_EQ(int, g_test_kv->SCard(0, "destset"), 5, "");
    CHECK_EQ(int, g_test_kv->SMembers(0, "destset", vs), 0, "");
    CHECK_EQ(int, vs.size(), 5, "");
    CHECK_EQ(std::string, vs[0], "a", "");
    CHECK_EQ(std::string, vs[1], "b", "");
    CHECK_EQ(std::string, vs[2], "c", "");
    CHECK_EQ(std::string, vs[3], "d", "");
    CHECK_EQ(std::string, vs[4], "e", "");
}

TEST(SInter, Set)
{
    g_test_kv->Del(0, "key1");
    g_test_kv->Del(0, "key2");
    g_test_kv->Del(0, "key3");
    g_test_kv->Del(0, "destset");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key1", "a"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key1", "b"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key1", "c"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key1", "d"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key2", "c"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key3", "a"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key3", "c"), 1, "");
    CHECK_EQ(int, g_test_kv->SAdd(0, "key3", "e"), 1, "");
    mmkv::DataArray keys;
    keys.push_back("key1");
    keys.push_back("key2");
    keys.push_back("key3");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->SInter(0, keys, vs), 0, "");
    CHECK_EQ(int, vs.size(), 1, "");
    CHECK_EQ(std::string, vs[0], "c", "");

    CHECK_EQ(int, g_test_kv->SInterStore(0, "destset", keys), 0, "");
    CHECK_EQ(int, g_test_kv->SCard(0, "destset"), 1, "");
    CHECK_EQ(int, g_test_kv->SMembers(0, "destset", vs), 0, "");
    CHECK_EQ(int, vs.size(), 1, "");
    CHECK_EQ(std::string, vs[0], "c", "");
}
