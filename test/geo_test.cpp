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
#include <sys/types.h>
#include <unistd.h>
#include <sys/wait.h>
#include <vector>

TEST(AddSearch, Geo)
{
    g_test_kv->Del(0, "mygeo");
    double x = 300.3;
    double y = 300.3;

    double p_x = 1000.0;
    double p_y = 1000.0;
    uint32_t radius = 1000;
    uint32_t total = 100000;
    mmkv::GeoPointArray cmp;
    for (uint32_t i = 0; i < total; i++)
    {
        char name[100];
        sprintf(name, "p%u", i);
        /*
         * min accuracy is 0.2meters
         */
        double xx = x + i * 0.3;
        double yy = y + i * 0.3;
        if (((xx - p_x) * (xx - p_x) + (yy - p_y) * (yy - p_y)) <= radius * radius)
        {
            mmkv::GeoPoint p;
            p.x = xx;
            p.y = yy;
            cmp.push_back(p);
        }
        CHECK_EQ(int, g_test_kv->GeoAdd(0, "mygeo", "MERCATOR", xx, yy, name), 1, "");
    }
    CHECK_EQ(int, g_test_kv->ZCard(0, "mygeo"), total, "");

    mmkv::GeoSearchOptions options;
    options.coord_type = "MERCATOR";
    options.by_x = p_x;
    options.by_y = p_y;
    options.radius = radius;
    options.get_patterns.push_back("WITHCOORDINATES");
    options.get_patterns.push_back("WITHDISTANCES");
    options.asc = true;
    mmkv::StringArray results;
    CHECK_EQ(int, g_test_kv->GeoSearch(0, "mygeo", options, results), 0, "");
    CHECK_EQ(int, results.size() / 4, cmp.size(), "");
}

