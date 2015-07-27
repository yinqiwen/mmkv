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

TEST(Save, Backup)
{
    mmkv::make_dir("./backup");
    mmkv::make_dir("./restore");
    g_test_kv->Backup("./backup/snapshot");

    mmkv::DBIDArray ids1, ids2;
    g_test_kv->GetAllDBID(ids1);
    std::vector<int> sizes1, sizes2;
    for(size_t i = 0; i<ids1.size(); i++)
    {
        sizes1.push_back(g_test_kv->DBSize(ids1[i]));
    }

    CHECK_EQ(int, g_test_kv->Restore("./backup/snapshot"), 0, "");
    g_test_kv->GetAllDBID(ids2);
    for(size_t i = 0; i<ids2.size(); i++)
    {
        sizes2.push_back(g_test_kv->DBSize(ids2[i]));
    }
    CHECK_EQ(bool, ids1 == ids2, true, "");
    CHECK_EQ(bool, sizes1 == sizes2, true, "");
}


