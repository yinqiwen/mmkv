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

TEST(ZAddCardScoreRange, ZSet)
{
    g_test_kv->Del(0, "myzset");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 1, "uno"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 1, "one"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 3, "two"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 2, "three"), 1, "");

    CHECK_EQ(int, g_test_kv->ZCard(0, "myzset"), 4, "");
    long double score;
    CHECK_EQ(int, g_test_kv->ZScore(0, "myzset", "uno", score), 0, "");
    CHECK_EQ(double, score, 1, "");

    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->ZRange(0, "myzset", 0, -1, false, vs), 0, "");
    CHECK_EQ(int, vs.size(), 4, "");
    CHECK_EQ(std::string, vs[0], "one", "");
    CHECK_EQ(std::string, vs[1], "uno", "");
    CHECK_EQ(std::string, vs[2], "three", "");
    CHECK_EQ(std::string, vs[3], "two", "");

    CHECK_EQ(int, g_test_kv->ZRange(0, "myzset", 3, 4, false, vs), 0, "");
    CHECK_EQ(int, vs.size(), 1, "");
    CHECK_EQ(std::string, vs[0], "two", "");
    CHECK_EQ(int, g_test_kv->ZRange(0, "myzset", -2, -1, false, vs), 0, "");
    CHECK_EQ(int, vs.size(), 2, "");
    CHECK_EQ(std::string, vs[0], "three", "");
    CHECK_EQ(std::string, vs[1], "two", "");
    CHECK_EQ(int, g_test_kv->ZRange(0, "myzset", 0, 1, true, vs), 0, "");
    CHECK_EQ(int, vs.size(), 4, "");
    CHECK_EQ(std::string, vs[0], "one", "");
    CHECK_EQ(std::string, vs[1], "1", "");
    CHECK_EQ(std::string, vs[2], "uno", "");
    CHECK_EQ(std::string, vs[3], "1", "");
}

TEST(Zcount, ZSet)
{
    g_test_kv->Del(0, "myzset");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 1, "one"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 2, "two"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 3, "three"), 1, "");

    CHECK_EQ(int, g_test_kv->ZCount(0, "myzset", "-inf", "+inf"), 3, "");
    CHECK_EQ(int, g_test_kv->ZCount(0, "myzset", "(1", "3"), 2, "");
}

TEST(ZIncrBy, ZSet)
{
    g_test_kv->Del(0, "myzset");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 1, "one"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 2, "two"), 1, "");
    long double dv;
    CHECK_EQ(int, g_test_kv->ZIncrBy(0, "myzset", 2, "two", dv), 0, "");
    CHECK_EQ(double, dv, 4, "");
    long double score;
    CHECK_EQ(int, g_test_kv->ZScore(0, "myzset", "two", score), 0, "");
    CHECK_EQ(double, dv, score, "");
    CHECK_EQ(int, g_test_kv->ZIncrBy(0, "myzset", 3, "three", dv), 0, "");
    CHECK_EQ(double, dv, 3, "");
    CHECK_EQ(int, g_test_kv->ZScore(0, "myzset", "three", score), 0, "");
    CHECK_EQ(double, dv, score, "");
}

TEST(ZLexCount, ZSet)
{
    g_test_kv->Del(0, "myzset");
    int add_count = 7;
    char s[add_count][2];
    mmkv::ScoreDataArray elements;
    for (int i = 0; i < 7; i++)
    {
        s[i][0] = 'a' + i;
        s[i][1] = 0;
        mmkv::ScoreData sv;
        sv.value = s[i];
        sv.score = 0;
        elements.push_back(sv);
    }
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", elements), 7, "");
    CHECK_EQ(int, g_test_kv->ZLexCount(0, "myzset", "-", "+"), 7, "");
    CHECK_EQ(int, g_test_kv->ZLexCount(0, "myzset", "[b", "[f"), 5, "");
}

TEST(ZRangeByLex, ZSet)
{
    g_test_kv->Del(0, "myzset");
    int add_count = 7;
    char s[add_count][2];
    mmkv::ScoreDataArray elements;
    for (int i = 0; i < 7; i++)
    {
        s[i][0] = 'a' + i;
        s[i][1] = 0;
        mmkv::ScoreData sv;
        sv.value = s[i];
        sv.score = 0;
        elements.push_back(sv);
    }
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", elements), 7, "");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->ZRangeByLex(0, "myzset", "-", "[c", 0, -1, vs), 0, "");
    CHECK_EQ(int, vs.size(), 3, "");
    CHECK_EQ(std::string, vs[0], "a", "");
    CHECK_EQ(std::string, vs[1], "b", "");
    CHECK_EQ(std::string, vs[2], "c", "");

    CHECK_EQ(int, g_test_kv->ZRangeByLex(0, "myzset", "[aaa", "(g", 0, -1, vs), 0, "");
    CHECK_EQ(int, vs.size(), 5, "");
    CHECK_EQ(std::string, vs[0], "b", "");
    CHECK_EQ(std::string, vs[1], "c", "");
    CHECK_EQ(std::string, vs[2], "d", "");
    CHECK_EQ(std::string, vs[3], "e", "");
    CHECK_EQ(std::string, vs[4], "f", "");
}

TEST(ZRangeByScore, ZSet)
{
    g_test_kv->Del(0, "myzset");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 1, "one"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 2, "two"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 3, "three"), 1, "");

    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->ZRangeByScore(0, "myzset", "-inf", "+inf", false, 0, -1, vs), 0, "");
    CHECK_EQ(int, vs.size(), 3, "");
    CHECK_EQ(std::string, vs[0], "one", "");
    CHECK_EQ(std::string, vs[1], "two", "");
    CHECK_EQ(std::string, vs[2], "three", "");

    CHECK_EQ(int, g_test_kv->ZRangeByScore(0, "myzset", "1", "2", false, 0, -1, vs), 0, "");
    CHECK_EQ(int, vs.size(), 2, "");
    CHECK_EQ(std::string, vs[0], "one", "");
    CHECK_EQ(std::string, vs[1], "two", "");

    CHECK_EQ(int, g_test_kv->ZRangeByScore(0, "myzset", "(1", "2", false, 0, -1, vs), 0, "");
    CHECK_EQ(int, vs.size(), 1, "");
    CHECK_EQ(std::string, vs[0], "two", "");

    CHECK_EQ(int, g_test_kv->ZRangeByScore(0, "myzset", "(1", "(2", false, 0, -1, vs), 0, "");
    CHECK_EQ(int, vs.size(), 0, "");
}

TEST(ZRankRevRank, ZSet)
{
    g_test_kv->Del(0, "myzset");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 1, "one"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 2, "two"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 3, "three"), 1, "");

    CHECK_EQ(int, g_test_kv->ZRank(0, "myzset", "one"), 0, "");
    CHECK_EQ(int, g_test_kv->ZRank(0, "myzset", "two"), 1, "");
    CHECK_EQ(int, g_test_kv->ZRank(0, "myzset", "three"), 2, "");
    CHECK_EQ(int, g_test_kv->ZRevRank(0, "myzset", "one"), 2, "");
    CHECK_EQ(int, g_test_kv->ZRevRank(0, "myzset", "two"), 1, "");
    CHECK_EQ(int, g_test_kv->ZRevRank(0, "myzset", "three"), 0, "");
}

TEST(ZRem, ZSet)
{
    g_test_kv->Del(0, "myzset");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 1, "one"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 2, "two"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 3, "three"), 1, "");

    CHECK_EQ(int, g_test_kv->ZRem(0, "myzset", "one"), 1, "");
    CHECK_EQ(int, g_test_kv->ZRem(0, "myzset", "one"), 0, "");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->ZRange(0, "myzset", 0, -1, false, vs), 0, "");
    CHECK_EQ(int, vs.size(), 2, "");
    CHECK_EQ(std::string, vs[0], "two", "");
    CHECK_EQ(std::string, vs[1], "three", "");
}

TEST(ZRemRangeByLex, ZSet)
{
    g_test_kv->Del(0, "myzset");
    int add_count = 7;
    char s[add_count][2];
    mmkv::ScoreDataArray elements;
    for (int i = 0; i < 7; i++)
    {
        s[i][0] = 'a' + i;
        s[i][1] = 0;
        mmkv::ScoreData sv;
        sv.value = s[i];
        sv.score = 0;
        elements.push_back(sv);
    }
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", elements), 7, "");
    CHECK_EQ(int, g_test_kv->ZRemRangeByLex(0, "myzset", "[a", "(c"), 2, "");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->ZRange(0, "myzset", 0, -1, false, vs), 0, "");
    CHECK_EQ(int, vs.size(), 5, "");
    CHECK_EQ(std::string, vs[0], "c", "");
    CHECK_EQ(std::string, vs[1], "d", "");
    CHECK_EQ(std::string, vs[2], "e", "");
    CHECK_EQ(std::string, vs[3], "f", "");
    CHECK_EQ(std::string, vs[4], "g", "");
}

TEST(ZRemRangeByRank, ZSet)
{
    g_test_kv->Del(0, "myzset");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 1, "one"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 2, "two"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 3, "three"), 1, "");
    CHECK_EQ(int, g_test_kv->ZRemRangeByRank(0, "myzset", 0, 1), 2, "");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->ZRange(0, "myzset", 0, -1, false, vs), 0, "");
    CHECK_EQ(int, vs.size(), 1, "");
    CHECK_EQ(std::string, vs[0], "three", "");

}

TEST(ZRemRangeByScore, ZSet)
{
    g_test_kv->Del(0, "myzset");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 1, "one"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 2, "two"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 3, "three"), 1, "");
    CHECK_EQ(int, g_test_kv->ZRemRangeByScore(0, "myzset", "-inf", "(2"), 1, "");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->ZRange(0, "myzset", 0, -1, false, vs), 0, "");
    CHECK_EQ(int, vs.size(), 2, "");
    CHECK_EQ(std::string, vs[0], "two", "");
    CHECK_EQ(std::string, vs[1], "three", "");
}

TEST(ZRevRange, ZSet)
{
    g_test_kv->Del(0, "myzset");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 1, "one"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 2, "two"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 3, "three"), 1, "");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->ZRevRange(0, "myzset", 0, -1, false, vs), 0, "");
    CHECK_EQ(int, vs.size(), 3, "");
    CHECK_EQ(std::string, vs[0], "three", "");
    CHECK_EQ(std::string, vs[1], "two", "");
    CHECK_EQ(int, g_test_kv->ZRevRange(0, "myzset", 2, 3, false, vs), 0, "");
    CHECK_EQ(int, vs.size(), 1, "");
    CHECK_EQ(std::string, vs[0], "one", "");
    CHECK_EQ(int, g_test_kv->ZRevRange(0, "myzset", -2, -1, false, vs), 0, "");
    CHECK_EQ(int, vs.size(), 2, "");
    CHECK_EQ(std::string, vs[0], "two", "");
    CHECK_EQ(std::string, vs[1], "one", "");
}

TEST(ZRevRangeByScore, ZSet)
{
    g_test_kv->Del(0, "myzset");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 1, "one"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 2, "two"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 3, "three"), 1, "");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->ZRevRangeByScore(0, "myzset", "+inf", "-inf", false, 0, -1, vs), 0, "");
    CHECK_EQ(int, vs.size(), 3, "");
    CHECK_EQ(std::string, vs[0], "three", "");
    CHECK_EQ(std::string, vs[1], "two", "");
    CHECK_EQ(int, g_test_kv->ZRevRangeByScore(0, "myzset", "2", "1", false, 0, -1, vs), 0, "");
    CHECK_EQ(int, vs.size(), 2, "");
    CHECK_EQ(std::string, vs[0], "two", "");
    CHECK_EQ(std::string, vs[1], "one", "");
    CHECK_EQ(int, g_test_kv->ZRevRangeByScore(0, "myzset", "2", "(1", false, 0, -1, vs), 0, "");
    CHECK_EQ(int, vs.size(), 1, "");
    CHECK_EQ(std::string, vs[0], "two", "");
    CHECK_EQ(int, g_test_kv->ZRevRangeByScore(0, "myzset", "(2", "(1", false, 0, -1, vs), 0, "");
    CHECK_EQ(int, vs.size(), 0, "");
}

TEST(ZRevRangeByLex, ZSet)
{
    g_test_kv->Del(0, "myzset");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 1, "one"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 2, "two"), 1, "");
    CHECK_EQ(int, g_test_kv->ZAdd(0, "myzset", 3, "three"), 1, "");
    mmkv::StringArray vs;
    CHECK_EQ(int, g_test_kv->ZRevRangeByScore(0, "myzset", "+inf", "-inf", false, 0, -1, vs), 0, "");
    CHECK_EQ(int, vs.size(), 3, "");
    CHECK_EQ(std::string, vs[0], "three", "");
    CHECK_EQ(std::string, vs[1], "two", "");
    CHECK_EQ(int, g_test_kv->ZRevRangeByScore(0, "myzset", "2", "1", false, 0, -1, vs), 0, "");
    CHECK_EQ(int, vs.size(), 2, "");
    CHECK_EQ(std::string, vs[0], "two", "");
    CHECK_EQ(std::string, vs[1], "one", "");
    CHECK_EQ(int, g_test_kv->ZRevRangeByScore(0, "myzset", "2", "(1", false, 0, -1, vs), 0, "");
    CHECK_EQ(int, vs.size(), 1, "");
    CHECK_EQ(std::string, vs[0], "two", "");
    CHECK_EQ(int, g_test_kv->ZRevRangeByScore(0, "myzset", "(2", "(1", false, 0, -1, vs), 0, "");
    CHECK_EQ(int, vs.size(), 0, "");
}

TEST(ZInterStore, ZSet)
{
    g_test_kv->Del(0, "myzset");
}

TEST(ZUnionStore, ZSet)
{
    g_test_kv->Del(0, "myzset");
}
