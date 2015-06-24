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

TEST(MultiWrite, Concurrent)
{
    int proc_num = 10;
    int write_count = 1000000;
    std::vector<pid_t> childs;
    mmkv::DBID testdb = 5;
    g_test_kv->FlushDB(testdb);
    int64_t start = mmkv::get_current_micros();
    for (int i = 0; i < proc_num; i++)
    {
        pid_t id = fork();
        if (id == 0)
        {
            for (int j = 0; j < write_count; j++)
            {
                char key[100], value[100];
                sprintf(key, "key%d_%d", i, j);
                sprintf(value, "value%d_%d", i, j);
                g_test_kv->Set(testdb, key, value);
            }
            exit(0);
        }
        childs.push_back(id);
    }
    for (size_t i = 0; i < childs.size(); i++)
    {
        int status;
        waitpid(childs[i], &status, 0);
    }
    int64_t end = mmkv::get_current_micros();
    printf("###Cost %lldus to concurrent write %u keys\n", end - start, write_count * proc_num);
    CHECK_EQ(int, g_test_kv->DBSize(testdb), write_count * proc_num, "");
}

TEST(MultiRead, Concurrent)
{
    int proc_num = 10;
    int read_count = 1000000;
    std::vector<pid_t> childs;
    mmkv::DBID testdb = 5;
    int64_t start = mmkv::get_current_micros();
    for (int i = 0; i < proc_num; i++)
    {
        pid_t id = fork();
        if (id == 0)
        {
            for (int j = 0; j < read_count; j++)
            {
                char key[100];
                sprintf(key, "key%d_%d", i, j);
                CHECK_EQ(int, g_test_kv->Exists(testdb, key), 1, "");
            }
            exit(0);
        }
        childs.push_back(id);
    }
    for (size_t i = 0; i < childs.size(); i++)
    {
        int status;
        waitpid(childs[i], &status, 0);
    }
    int64_t end = mmkv::get_current_micros();
    printf("###Cost %lldus to concurrent read %u keys\n", end - start, read_count * proc_num);
}

TEST(MultiReadWrite, Concurrent)
{
    int proc_num = 10;
    int count = 1000000;
    std::vector<pid_t> childs;
    mmkv::DBID testdb = 5;
    int64_t start = mmkv::get_current_micros();
    for (int i = 0; i < proc_num; i++)
    {
        pid_t id = fork();
        if (id == 0)
        {
            for (int j = 0; j < count; j++)
            {
                char key[100], value[100];
                sprintf(key, "key%d_%d", i, j);
                sprintf(value, "value%d_%d", i, j);
                g_test_kv->Set(testdb, key, value);
            }
            exit(0);
        }
        childs.push_back(id);
    }
    for (int i = 0; i < proc_num; i++)
    {
        pid_t id = fork();
        if (id == 0)
        {
            for (int j = 0; j < count; j++)
            {
                char key[100];
                sprintf(key, "key%d_%d", i, j);
                CHECK_EQ(int, g_test_kv->Exists(testdb, key), 1, "");
            }
            exit(0);
        }
        childs.push_back(id);
    }
    for (size_t i = 0; i < childs.size(); i++)
    {
        int status;
        waitpid(childs[i], &status, 0);
    }
    int64_t end = mmkv::get_current_micros();
    printf("###Cost %lldus to concurrent read write %u keys\n", end - start, count * proc_num);
}

