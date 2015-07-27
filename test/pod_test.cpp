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
#include "containers.hpp"
#include "khash.hh"
#include <boost/interprocess/offset_ptr.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <functional>
#include <boost/functional/hash.hpp>

struct TestPOD
{
        int a;
        int64_t b;
        double c;
        boost::interprocess::offset_ptr<int> intptr;
};
TEST(GetSet, POD)
{
    g_test_kv->Del(0, "mypod");
    CHECK_EQ(int, g_test_kv->RegisterPODType<TestPOD>(1), 0, "");
    int err;
    {
        mmkv::LockedPOD<TestPOD> pod;
        g_test_kv->GetPOD(0, "mypod", false, true, 1, pod, err)();
        CHECK_EQ(int, err, 0, "");
        pod->a = 1;
        pod->b = 101;
        pod->c = 1.12;
        pod->intptr = g_test_kv->NewPOD<int>()();
        *(pod->intptr) = 3456;
    }
    {
        mmkv::LockedPOD<TestPOD> pod;
        g_test_kv->GetPOD(0, "mypod", true, false, 1, pod, err)();
        CHECK_EQ(int, err, 0, "");
        CHECK_EQ(int, pod->a, 1, "");
        CHECK_EQ(int, pod->b, 101, "");
        CHECK_EQ(double, pod->c, 1.12, "");
        CHECK_EQ(int, *(pod->intptr), 3456, "");
    }
    {
        mmkv::LockedPOD<TestPOD> pod;
        g_test_kv->GetPOD(0, "mypod", false, false, 1, pod, err)();
        CHECK_EQ(int, err, 0, "");
        if (pod.Get() != NULL)
        {
            g_test_kv->DeletePOD<int>(pod->intptr.get());
        }
    }
    g_test_kv->Del(0, "mypod");
    {
        mmkv::LockedPOD<TestPOD> pod;
        g_test_kv->GetPOD(0, "mypod", false, false, 1, pod, err)();
        CHECK_EQ(int, err, mmkv::ERR_ENTRY_NOT_EXIST, "");
    }
}

struct TestPODContainer
{
        int a;
        typedef boost::interprocess::deque<int, mmkv::Allocator<int> > IntList;
        IntList ids;
        TestPODContainer(const mmkv::Allocator<int>& allocator) :
                a(0), ids(allocator)
        {
        }
        ~TestPODContainer()
        {
            ids.clear();
        }
};

TEST(Containers, POD)
{
    const int pod_type = 2;
    g_test_kv->Del(0, "mypod");
    int err;
    {
        mmkv::LockedPOD<TestPODContainer> pod;
        mmkv::Allocator<int> allocator = g_test_kv->GetAllocator<int>();

        g_test_kv->GetPOD(0, "mypod", false, true, pod_type, pod, err)(allocator);
        TestPODContainer::IntList t1(allocator);
        pod->ids = t1;
        CHECK_EQ(int, err, 0, "");
        pod->a = 1;
        pod->ids.push_back(100);
        pod->ids.push_back(200);
        pod->ids.push_back(300);
    }
    {
        mmkv::LockedPOD<TestPODContainer> pod;
        mmkv::Allocator<int> allocator = g_test_kv->GetAllocator<int>();
        g_test_kv->GetPOD(0, "mypod", true, false, pod_type, pod, err)(allocator);
        CHECK_EQ(int, err, 0, "");
        CHECK_EQ(int, pod->a, 1, "");
        CHECK_EQ(int, pod->ids.size(), 3, "");
        CHECK_EQ(int, pod->ids[0], 100, "");
        CHECK_EQ(int, pod->ids[1], 200, "");
        CHECK_EQ(int, pod->ids[2], 300, "");
    }
    CHECK_EQ(int, g_test_kv->Del(0, "mypod"), 1, "");
    {
        mmkv::LockedPOD<TestPODContainer> pod;
        mmkv::Allocator<int> allocator = g_test_kv->GetAllocator<int>();
        g_test_kv->GetPOD(0, "mypod", false, false, pod_type, pod, err)(allocator);
        CHECK_EQ(int, err, mmkv::ERR_ENTRY_NOT_EXIST, "");
    }
}
typedef boost::interprocess::basic_string<char, std::char_traits<char>, mmkv::Allocator<char> > SHMStr;
typedef boost::interprocess::vector<int, mmkv::Allocator<int> > IntArray;
struct ComplexStruct
{
        uint64_t v0;
        double v1;
        SHMStr s1;
        SHMStr s2;
        IntArray a1;
        IntArray a2;
        ComplexStruct(const mmkv::Allocator<int>& alloc) :
                s1(alloc), s2(alloc), a1(alloc), a2(alloc)
        {
        }
};
typedef std::pair<const int, ComplexStruct> ComplexStructPair;
typedef khmap_t<int, ComplexStruct, boost::hash<int>, std::equal_to<int>, mmkv::Allocator<ComplexStructPair> > ComplexStructTable;

TEST(ComplexStruct, POD)
{
    const uint32_t pod_type = 3;
    g_test_kv->RegisterPODType<ComplexStructTable>(pod_type);
    g_test_kv->Del(0, "mypod1");

    int err;
    {
        mmkv::LockedPOD<ComplexStructTable> pod;
        mmkv::Allocator<ComplexStructPair> allocator = g_test_kv->GetAllocator<ComplexStructPair>();
        g_test_kv->GetPOD(0, "mypod1", false, true, pod_type, pod, err)(allocator);
        for(int i = 0; i < 100; i++)
        {
            ComplexStruct ss(allocator);
            ss.a1.push_back(i);
            ss.a2.push_back(i);
            ss.s1.assign("12");
            ss.s2.assign("3456");
            ss.v0 = i;
            ss.v1 = i;
            pod->insert(i, ss);
        }
    }
}
