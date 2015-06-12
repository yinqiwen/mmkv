/*
 * string_test.cpp
 *
 *  Created on: 2015Äê5ÔÂ21ÈÕ
 *      Author: wangqiying
 */

#include "ut.hpp"
#include "containers.hpp"
#include <boost/interprocess/offset_ptr.hpp>

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
        g_test_kv->Del(0, "mypod");
    }
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
};

TEST(Containers, POD)
{
    g_test_kv->Del(0, "mypod");
    int err;
    {
        mmkv::LockedPOD<TestPODContainer> pod;
        mmkv::Allocator<int> allocator = g_test_kv->GetAllocator<int>();
        g_test_kv->GetPOD(0, "mypod", false, true, 1, pod, err)(allocator);
        CHECK_EQ(int, err, 0, "");
        pod->a = 1;
        pod->ids.push_back(100);
        pod->ids.push_back(200);
        pod->ids.push_back(300);
    }
    {
        mmkv::LockedPOD<TestPODContainer> pod;
        mmkv::Allocator<int> allocator = g_test_kv->GetAllocator<int>();
        g_test_kv->GetPOD(0, "mypod", true, false, 1, pod, err)(allocator);
        CHECK_EQ(int, err, 0, "");
        CHECK_EQ(int, pod->a, 1, "");
        CHECK_EQ(int, pod->ids.size(), 3, "");
        CHECK_EQ(int, pod->ids[0], 100, "");
        CHECK_EQ(int, pod->ids[1], 200, "");
        CHECK_EQ(int, pod->ids[2], 300, "");
    }
    CHECK_EQ(int, g_test_kv->DelPOD<TestPODContainer>(0, "mypod", 1), 0, "");
    {
        mmkv::LockedPOD<TestPODContainer> pod;
        mmkv::Allocator<int> allocator = g_test_kv->GetAllocator<int>();
        g_test_kv->GetPOD(0, "mypod", false, false, 1, pod, err)(allocator);
        CHECK_EQ(int, err, mmkv::ERR_ENTRY_NOT_EXIST, "");
    }
}

